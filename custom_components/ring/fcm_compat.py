"""Work around firebase_messaging push messages that stop the event listener.

firebase_messaging 0.4.5 decodes the ``crypto-key`` and ``encryption`` values of
a push message with strict base64. Google sends these as unpadded base64url and
the ``crypto-key`` value may hold several ``;`` separated parameters. Decoding
then raises, the exception ends the receive loop and the push client shuts
down. The message is never acknowledged, so it comes back on every reconnect.

Decode the values tolerantly, and drop a message that still cannot be decrypted
instead of shutting the push client down. A dropped message is acknowledged.
"""

from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Callable
import logging
from typing import Any

from firebase_messaging.fcmpushclient import FcmPushClient

_LOGGER = logging.getLogger(__name__)


def _b64_param(value: str, name: str) -> bytes:
    """Decode one parameter of a push header as unpadded base64url.

    The library strips a ``name=`` prefix by position, so ``value`` may still
    hold other ``;`` separated parameters.
    """
    parts = [part.strip() for part in value.split(";") if part.strip()]
    data = parts[0] if parts else ""
    for part in parts:
        if part.startswith(f"{name}="):
            data = part[len(name) + 1 :]
            break
    data = data.rstrip("=")
    return urlsafe_b64decode(data + "=" * (-len(data) % 4))


def _patch() -> None:
    """Patch the push client once."""
    if getattr(FcmPushClient, "_ring_patched", False):
        return

    original_decrypt: Callable[..., bytes] = FcmPushClient._decrypt_raw_data  # noqa: SLF001
    original_handle: Callable[..., None] = FcmPushClient._handle_data_message  # noqa: SLF001

    def _decrypt_raw_data(
        credentials: dict[str, dict[str, str]],
        crypto_key_str: str,
        salt_str: str,
        raw_data: bytes,
    ) -> bytes:
        # Pass re-encoded padded values to the original implementation
        crypto_key = _b64_param(crypto_key_str, "dh")
        salt = _b64_param(salt_str, "salt")
        return original_decrypt(
            credentials,
            _b64encode(crypto_key),
            _b64encode(salt),
            raw_data,
        )

    def _handle_data_message(self: FcmPushClient, msg: Any) -> None:
        try:
            original_handle(self, msg)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning(
                "Dropping Ring push message %s that could not be decrypted: %r "
                "(app data keys: %s)",
                getattr(msg, "persistent_id", None),
                err,
                [item.key for item in getattr(msg, "app_data", [])],
            )

    FcmPushClient._decrypt_raw_data = staticmethod(_decrypt_raw_data)  # type: ignore[method-assign]  # noqa: SLF001
    FcmPushClient._handle_data_message = _handle_data_message  # type: ignore[method-assign]  # noqa: SLF001
    FcmPushClient._ring_patched = True  # type: ignore[attr-defined]  # noqa: SLF001


def _b64encode(data: bytes) -> str:
    """Encode bytes as padded base64url."""
    return urlsafe_b64encode(data).decode("ascii")


_patch()
