"""Base class for Ring entity."""

from collections.abc import Awaitable, Callable, Coroutine
import logging
from typing import Any, Concatenate, Generic, TypeVar, cast, override

from homeassistant.core import callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.device_registry import CONNECTION_NETWORK_MAC, DeviceInfo
from homeassistant.helpers.update_coordinator import (
    BaseCoordinatorEntity,
    CoordinatorEntity,
)
from python_ring import (
    AuthenticationError,
    RingDevices,
    RingError,
    RingGeneric,
    RingTimeout,
)

from .const import ATTRIBUTION, DOMAIN
from .coordinator import RingDataCoordinator, RingListenCoordinator

RingDeviceT = TypeVar("RingDeviceT", bound=RingGeneric, default=RingGeneric)

_RingCoordinatorT = TypeVar(
    "_RingCoordinatorT",
    bound=(RingDataCoordinator | RingListenCoordinator),
)

_LOGGER = logging.getLogger(__name__)


def exception_wrap[RingBaseEntityT: RingBaseEntity[Any, Any], **P, R](
    async_func: Callable[Concatenate[RingBaseEntityT, P], Coroutine[Any, Any, R]],
) -> Callable[Concatenate[RingBaseEntityT, P], Coroutine[Any, Any, R]]:
    """Define a wrapper to catch exceptions and raise HomeAssistant errors."""

    async def _wrap(self: RingBaseEntityT, *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await async_func(self, *args, **kwargs)
        except AuthenticationError as err:
            self.coordinator.config_entry.async_start_reauth(self.hass)
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="api_authentication",
            ) from err
        except RingTimeout as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="api_timeout",
            ) from err
        except RingError as err:
            _LOGGER.debug(
                "Error calling %s in platform %s: ", async_func.__name__, self.platform
            )
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="api_error",
            ) from err

    return _wrap


def refresh_after[RingEntityT: RingEntity[Any], **P](
    func: Callable[Concatenate[RingEntityT, P], Awaitable[None]],
) -> Callable[Concatenate[RingEntityT, P], Coroutine[Any, Any, None]]:
    """Define a wrapper to handle api call errors or refresh after success."""

    @exception_wrap
    async def _wrap(self: RingEntityT, *args: P.args, **kwargs: P.kwargs) -> None:
        await func(self, *args, **kwargs)
        await self.coordinator.async_request_refresh()

    return _wrap


class RingBaseEntity(
    BaseCoordinatorEntity[_RingCoordinatorT], Generic[_RingCoordinatorT, RingDeviceT]
):
    """Base implementation for Ring device."""

    _attr_attribution = ATTRIBUTION
    _attr_has_entity_name = True

    def __init__(
        self,
        device: RingDeviceT,
        coordinator: _RingCoordinatorT,
    ) -> None:
        """Initialize a sensor for Ring device."""
        super().__init__(coordinator, context=device.id)
        self._device = device
        self._attr_extra_state_attributes = {}
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, device.device_id)},  # device_id is the mac
            connections={(CONNECTION_NETWORK_MAC, device.device_id)},
            manufacturer="Ring",
            model=device.model,
            name=device.name,
        )


class RingEntity(
    RingBaseEntity[RingDataCoordinator, RingDeviceT],
    CoordinatorEntity[RingDataCoordinator],
):
    """Implementation for Ring devices."""

    def _get_coordinator_data(self) -> RingDevices:
        return self.coordinator.data

    @callback
    @override
    def _handle_coordinator_update(self) -> None:
        self._device = cast(
            RingDeviceT,
            self._get_coordinator_data().get_device(self._device.device_api_id),
        )
        super()._handle_coordinator_update()
