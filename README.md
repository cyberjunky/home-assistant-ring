# Ring for Home Assistant (python-ring)

A custom version of the Home Assistant Ring integration. It uses
[python-ring](https://github.com/cyberjunky/python-ring) instead of
ring-doorbell, so it supports newer Ring devices and has a few extra features.

It replaces the built-in Ring integration (same `ring` domain), so your existing
Ring config entry, devices and entities keep working.

## What's different from the built-in integration

- **Newer devices are recognised**, like the Battery Doorbell Pro, Wired
  Doorbell Plus (2nd Gen), Wired/Battery Doorbell 4K Pro, Outdoor Cam Plus/Pro,
  Indoor Cam Plus, Floodlight Cam 2K and Pro (2nd Gen), Spotlight Cam 4K Pro,
  Elite cameras and Chime (2nd Gen). Before, these showed up as "Unknown" and
  had no ding or motion events.
- **Motion snooze**: a number entity on cameras and doorbells. Set the minutes
  to snooze motion alerts, set 0 to stop.
- **Chime do not disturb**: the same, for chimes.
- **Smart detection on the motion event**: the motion event fires `human`,
  `moving_vehicle`, `package_delivery`, `package_pickup`, `loitering` or
  `other_motion` when Ring sends that, and `motion` otherwise.
- **Battery 1 and Battery 2 sensors** for devices with two batteries.
- **Live view for Ring Intercom Video**.
- Fixes for the event listener stopping, unknown chime types breaking the
  switch platform, and WebRTC errors in the log.
- Discovery of Ring devices with MAC prefix 64:9A:63.

## Install

1. In HACS, add `https://github.com/cyberjunky/home-assistant-ring` as a custom
   repository of type Integration.
2. Install "Ring (python-ring)" and restart Home Assistant.

Requires Home Assistant 2026.11 or newer.

To go back to the built-in integration, remove it from HACS and restart.

## Development

Tests use `pytest-homeassistant` and need Linux or macOS:

```
uv sync --group test
uv run pytest
```

## License

Apache-2.0, see [LICENSE](LICENSE). Based on the Ring integration from
[Home Assistant Core](https://github.com/home-assistant/core) 2026.11.1,
copyright the Home Assistant authors.
