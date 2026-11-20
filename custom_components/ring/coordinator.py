"""Data coordinators for the ring integration."""

import asyncio
from asyncio import TaskGroup
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
import logging
from typing import Any, override

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import (
    BaseDataUpdateCoordinatorProtocol,
    DataUpdateCoordinator,
    UpdateFailed,
)
from python_ring import (
    AuthenticationError,
    Ring,
    RingDevices,
    RingError,
    RingEvent,
    RingTimeout,
)
from python_ring.listen import RingEventListener

from .const import DOMAIN, SCAN_INTERVAL

_LOGGER = logging.getLogger(__name__)

# Delay before retrying a failed start of the event listener, doubled per attempt
LISTEN_RETRY_INTERVAL = 30
LISTEN_RETRY_MAX_INTERVAL = 900


@dataclass
class RingData:
    """Class to support type hinting of ring data collection."""

    api: Ring
    devices: RingDevices
    devices_coordinator: RingDataCoordinator
    listen_coordinator: RingListenCoordinator


type RingConfigEntry = ConfigEntry[RingData]


class RingDataCoordinator(DataUpdateCoordinator[RingDevices]):
    """Base class for device coordinators."""

    config_entry: RingConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: RingConfigEntry,
        ring_api: Ring,
    ) -> None:
        """Initialize my coordinator."""
        super().__init__(
            hass,
            name="devices",
            logger=_LOGGER,
            update_interval=SCAN_INTERVAL,
            config_entry=config_entry,
        )
        self.ring_api: Ring = ring_api
        self.first_call: bool = True

    async def _call_api[*Ts, R](
        self,
        target: Callable[[*Ts], Coroutine[Any, Any, R]],
        *args: *Ts,
    ) -> R:
        try:
            return await target(*args)
        except AuthenticationError as err:
            # Raising ConfigEntryAuthFailed will cancel future updates
            # and start a config flow with SOURCE_REAUTH (async_step_reauth)
            raise ConfigEntryAuthFailed(
                translation_domain=DOMAIN,
                translation_key="api_authentication",
            ) from err
        except RingTimeout as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="api_timeout",
            ) from err
        except RingError as err:
            raise UpdateFailed(
                translation_domain=DOMAIN,
                translation_key="api_error",
            ) from err

    @override
    async def _async_update_data(self) -> RingDevices:
        """Fetch data from API endpoint."""
        update_method: str = (
            "async_update_data" if self.first_call else "async_update_devices"
        )
        await self._call_api(getattr(self.ring_api, update_method))
        self.first_call = False
        devices: RingDevices = self.ring_api.devices()
        subscribed_device_ids = set(self.async_contexts())
        for device in devices.all_devices:
            # Don't update all devices in the ring api, only those that set
            # their device id as context when they subscribed.
            if device.id in subscribed_device_ids:
                try:
                    async with TaskGroup() as tg:
                        if device.has_capability("history"):
                            tg.create_task(
                                self._call_api(
                                    lambda device: device.async_history(limit=10),
                                    device,
                                )
                            )
                        tg.create_task(
                            self._call_api(
                                device.async_update_health_data,
                            )
                        )
                except ExceptionGroup as eg:
                    raise eg.exceptions[0]  # noqa: B904

        return devices


class RingListenCoordinator(BaseDataUpdateCoordinatorProtocol):
    """Global notifications coordinator."""

    config_entry: RingConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: RingConfigEntry,
        ring_api: Ring,
        listen_credentials: dict[str, Any] | None,
        listen_credentials_updater: Callable[[dict[str, Any]], None],
    ) -> None:
        """Initialize my coordinator."""
        self.hass = hass
        self.logger = _LOGGER
        self.ring_api: Ring = ring_api
        self.event_listener = RingEventListener(
            ring_api, listen_credentials, listen_credentials_updater
        )
        self._listeners: dict[CALLBACK_TYPE, tuple[CALLBACK_TYPE, object | None]] = {}
        self._listen_callback_id: int | None = None
        self._retry_task: asyncio.Task[None] | None = None

        self.config_entry = config_entry
        self.start_timeout = 10
        self.config_entry.async_on_unload(self.async_shutdown)
        self.index_alerts()

    def index_alerts(self) -> None:
        "Index the active alerts."
        self.alerts = {
            (alert.doorbot_id, alert.kind): alert
            for alert in self.ring_api.active_alerts()
        }

    async def async_shutdown(self) -> None:
        """Cancel any scheduled call, and ignore new runs."""
        if self.event_listener.started or self._retry_task is not None:
            await self._async_stop_listen()

    async def _async_stop_listen(self) -> None:
        self.logger.debug("Stopping ring listener")
        if self._retry_task is not None:
            self._retry_task.cancel()
            self._retry_task = None
        await self.event_listener.stop()
        # Stopping the listener drops all of its notification callbacks
        self._listen_callback_id = None
        self.logger.debug("Stopped ring listener")

    async def _async_try_start_listen(self) -> bool:
        """Try to start the listener, return whether it is started."""
        self.logger.debug("Starting ring listener.")
        try:
            await self.event_listener.start(
                timeout=self.start_timeout,
            )
        except Exception as err:  # noqa: BLE001
            # The library raises on FCM registration, Ring subscription and
            # timeout errors; none of them should end the listener for good.
            self.logger.warning(
                "Ring event listener failed to start: %s",
                repr(err),
            )
            return False
        if self.event_listener.started is not True:
            self.logger.warning(
                "Ring event listener failed to start after %s seconds",
                self.start_timeout,
            )
            return False
        self.logger.debug("Started ring listener")
        if self._listen_callback_id is None:
            self._listen_callback_id = self.event_listener.add_notification_callback(
                self._on_event
            )
        self.index_alerts()
        # Update the listeners so they switch from Unavailable to Unknown
        self._async_update_listeners()
        return True

    async def _async_start_listen(self) -> None:
        """Start listening for realtime events."""
        if await self._async_try_start_listen() or self._retry_task is not None:
            return
        self._retry_task = self.config_entry.async_create_background_task(
            self.hass,
            self._async_retry_start_listen(),
            "Ring event listener retry",
        )

    async def _async_retry_start_listen(self) -> None:
        """Keep trying to start the listener until it is started."""
        delay = LISTEN_RETRY_INTERVAL
        while self._listeners:
            self.logger.debug("Retrying ring listener start in %s seconds", delay)
            await asyncio.sleep(delay)
            if not self._listeners or await self._async_try_start_listen():
                break
            delay = min(delay * 2, LISTEN_RETRY_MAX_INTERVAL)
        self._retry_task = None

    def _on_event(self, event: RingEvent) -> None:
        self.logger.debug("Ring event received: %s", event)
        self.index_alerts()
        self._async_update_listeners(event.doorbot_id)

    @callback
    def _async_update_listeners(self, doorbot_id: int | None = None) -> None:
        """Update all registered listeners."""
        for update_callback, device_api_id in list(self._listeners.values()):
            if not doorbot_id or device_api_id == doorbot_id:
                update_callback()

    @callback
    @override
    def async_add_listener(
        self, update_callback: CALLBACK_TYPE, context: Any = None
    ) -> Callable[[], None]:
        """Listen for data updates."""
        start_listen = not self._listeners

        @callback
        def remove_listener() -> None:
            """Remove update listener."""
            self._listeners.pop(remove_listener)
            if not self._listeners:
                self.config_entry.async_create_task(
                    self.hass,
                    self._async_stop_listen(),
                    "Ring event listener stop",
                    eager_start=True,
                )

        self._listeners[remove_listener] = (update_callback, context)

        # This is the first listener, start the event listener.
        if start_listen:
            self.config_entry.async_create_task(
                self.hass,
                self._async_start_listen(),
                "Ring event listener start",
                eager_start=True,
            )
        return remove_listener
