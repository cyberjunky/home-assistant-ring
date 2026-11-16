"""The tests for the Ring number platform."""

from unittest.mock import Mock

from homeassistant.components.number import (
    ATTR_VALUE,
    DOMAIN as NUMBER_DOMAIN,
    SERVICE_SET_VALUE,
)
from homeassistant.const import ATTR_ENTITY_ID, Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
import pytest
from pytest_homeassistant.common import snapshot_platform
from syrupy.assertion import SnapshotAssertion

from .common import MockConfigEntry, async_check_entity_translations, setup_platform
from .device_mocks import DOWNSTAIRS_DEVICE_ID, FRONT_DOOR_DEVICE_ID


@pytest.mark.parametrize(
    ("entity_id", "unique_id"),
    [
        ("number.downstairs_volume", "123456-volume"),
        ("number.front_door_volume", "987654-volume"),
        ("number.ingress_doorbell_volume", "185036587-doorbell_volume"),
        ("number.ingress_mic_volume", "185036587-mic_volume"),
        ("number.ingress_voice_volume", "185036587-voice_volume"),
    ],
)
async def test_entity_registry(
    hass: HomeAssistant,
    entity_registry: er.EntityRegistry,
    mock_ring_client: Mock,
    entity_id: str,
    unique_id: str,
) -> None:
    """Tests that the devices are registered in the entity registry."""
    await setup_platform(hass, Platform.NUMBER)

    entry = entity_registry.async_get(entity_id)
    assert entry is not None and entry.unique_id == unique_id


async def test_states(
    hass: HomeAssistant,
    mock_ring_client: Mock,
    mock_config_entry: MockConfigEntry,
    entity_registry: er.EntityRegistry,
    snapshot: SnapshotAssertion,
) -> None:
    """Test states."""

    mock_config_entry.add_to_hass(hass)
    await setup_platform(hass, Platform.NUMBER)
    await async_check_entity_translations(
        hass, entity_registry, mock_config_entry.entry_id, NUMBER_DOMAIN
    )
    await snapshot_platform(hass, entity_registry, snapshot, mock_config_entry.entry_id)


@pytest.mark.parametrize(
    ("entity_id", "new_value"),
    [
        ("number.downstairs_volume", "4.0"),
        ("number.front_door_volume", "3.0"),
        ("number.ingress_doorbell_volume", "7.0"),
        ("number.ingress_mic_volume", "2.0"),
        ("number.ingress_voice_volume", "5.0"),
    ],
)
async def test_volume_can_be_changed(
    hass: HomeAssistant,
    mock_ring_client: Mock,
    entity_id: str,
    new_value: str,
) -> None:
    """Tests the volume can be changed correctly."""
    await setup_platform(hass, Platform.NUMBER)

    state = hass.states.get(entity_id)
    assert state is not None
    old_value = state.state

    # otherwise this test would be pointless
    assert old_value != new_value

    await hass.services.async_call(
        NUMBER_DOMAIN,
        SERVICE_SET_VALUE,
        {ATTR_ENTITY_ID: entity_id, ATTR_VALUE: new_value},
        blocking=True,
    )

    await hass.async_block_till_done()
    state = hass.states.get(entity_id)
    assert state is not None and state.state == new_value


@pytest.mark.parametrize(
    ("entity_id", "device_id", "set_method", "clear_method"),
    [
        pytest.param(
            "number.front_door_motion_snooze",
            FRONT_DOOR_DEVICE_ID,
            "async_set_motion_snooze",
            "async_clear_motion_snooze",
            id="motion_snooze",
        ),
        pytest.param(
            "number.downstairs_do_not_disturb",
            DOWNSTAIRS_DEVICE_ID,
            "async_set_do_not_disturb",
            "async_clear_do_not_disturb",
            id="do_not_disturb",
        ),
    ],
)
async def test_snooze_can_be_set_and_cleared(
    hass: HomeAssistant,
    mock_ring_client: Mock,
    mock_ring_devices: Mock,
    entity_id: str,
    device_id: int,
    set_method: str,
    clear_method: str,
) -> None:
    """Setting minutes starts a snooze and setting 0 clears it."""
    await setup_platform(hass, Platform.NUMBER)
    device = mock_ring_devices.get_device(device_id)

    state = hass.states.get(entity_id)
    assert state is not None
    assert state.state == "0"

    await hass.services.async_call(
        NUMBER_DOMAIN,
        SERVICE_SET_VALUE,
        {ATTR_ENTITY_ID: entity_id, ATTR_VALUE: 30},
        blocking=True,
    )
    getattr(device, set_method).assert_called_once_with(30)
    state = hass.states.get(entity_id)
    assert state is not None
    assert state.state == "30"

    await hass.services.async_call(
        NUMBER_DOMAIN,
        SERVICE_SET_VALUE,
        {ATTR_ENTITY_ID: entity_id, ATTR_VALUE: 0},
        blocking=True,
    )
    getattr(device, clear_method).assert_called_once_with()
    state = hass.states.get(entity_id)
    assert state is not None
    assert state.state == "0"
