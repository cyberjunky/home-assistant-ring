"""Tests for the firebase_messaging push message workarounds."""

from base64 import urlsafe_b64encode
import logging
import os
from unittest.mock import MagicMock

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)
from firebase_messaging.fcmpushclient import FcmPushClient
from http_ece import encrypt as http_encrypt
import pytest

from custom_components.ring import fcm_compat  # noqa: F401


def _b64(data: bytes, *, padded: bool) -> str:
    encoded = urlsafe_b64encode(data).decode("ascii")
    return encoded if padded else encoded.rstrip("=")


def _push_message() -> tuple[dict, str, str, bytes, bytes]:
    """Encrypt a payload like a web push server for a fresh client key."""
    receiver_key = ec.generate_private_key(ec.SECP256R1())
    sender_key = ec.generate_private_key(ec.SECP256R1())
    auth_secret = os.urandom(16)
    salt = os.urandom(16)
    payload = b'{"data": "hello"}'
    raw_data = http_encrypt(
        payload,
        salt=salt,
        private_key=sender_key,
        dh=receiver_key.public_key().public_bytes(
            Encoding.X962, PublicFormat.UncompressedPoint
        ),
        version="aesgcm",
        auth_secret=auth_secret,
    )
    credentials = {
        "keys": {
            "private": _b64(
                receiver_key.private_bytes(
                    Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
                ),
                padded=False,
            ),
            "secret": _b64(auth_secret, padded=False),
        }
    }
    sender_public = sender_key.public_key().public_bytes(
        Encoding.X962, PublicFormat.UncompressedPoint
    )
    return credentials, sender_public, salt, raw_data, payload


@pytest.mark.parametrize("padded", [True, False], ids=["padded", "unpadded"])
def test_decrypt_unpadded_values(padded: bool) -> None:
    """Test push messages decrypt with padded and unpadded header values."""
    credentials, sender_public, salt, raw_data, payload = _push_message()

    # The library strips "dh=" and "salt=" before calling the decrypt method
    decrypted = FcmPushClient._decrypt_raw_data(
        credentials,
        _b64(sender_public, padded=padded),
        _b64(salt, padded=padded),
        raw_data,
    )

    assert decrypted == payload


def test_decrypt_crypto_key_with_more_parameters() -> None:
    """Test a crypto-key header holding more parameters than the dh key."""
    credentials, sender_public, salt, raw_data, payload = _push_message()
    header = f"p256ecdsa=abc;dh={_b64(sender_public, padded=False)}"

    decrypted = FcmPushClient._decrypt_raw_data(
        credentials, header[3:], _b64(salt, padded=False), raw_data
    )

    assert decrypted == payload


def test_undecryptable_message_dropped(caplog: pytest.LogCaptureFixture) -> None:
    """Test a message that cannot be decrypted is dropped, not raised."""
    client = MagicMock(spec=FcmPushClient)
    client.callback = MagicMock()
    client.credentials = {
        "gcm": {"app_id": "app"},
        "keys": {"private": "", "secret": ""},
    }
    msg = MagicMock()
    msg.persistent_id = "0:123"
    msg.app_data = [
        MagicMock(key="crypto-key", value="dh=!!!"),
        MagicMock(key="encryption", value="salt=!!!"),
        MagicMock(key="subtype", value="app"),
    ]
    client._app_data_by_key = lambda m, key, do_not_raise=False: next(
        item.value for item in m.app_data if item.key == key
    )

    with caplog.at_level(logging.WARNING):
        FcmPushClient._handle_data_message(client, msg)

    assert "Dropping Ring push message 0:123" in caplog.text
    client.callback.assert_not_called()
