import os

from ovirt_image_proxy import ticket

TEST_DIR = os.path.dirname(__file__)

SIGNING_KEY_FILE = os.path.join(TEST_DIR, "pki/private/private_key.pem")
SIGNING_CERT_FILE = os.path.join(TEST_DIR, "pki/certs/signing_cert.der")
CERT_FILE = os.path.join(TEST_DIR, "pki/certs/public_cert.pem")
with open(CERT_FILE) as f:
    CERT_DATA = f.read()

TEST_TOKEN_FILE = os.path.join(TEST_DIR, "resources/test_ticket.out")
TEST_TOKEN_PAYLOAD = '{"key": "value"}\n'  # Encoded in TEST_TOKEN_FILE


def test_decode():
    decoder = ticket.TicketDecoder(ca=CERT_FILE,
                                   eku=None,
                                   peer=CERT_DATA)

    with open(TEST_TOKEN_FILE, 'r') as f:
        existing_token = f.read()
    payload = decoder.decode(existing_token)

    assert payload == TEST_TOKEN_PAYLOAD


def test_encode():
    # Unfortunately we can't test encoding alone, because it operates based
    # on an offset from the current time rather than a set expiration--so a
    # TEST_TOKEN_FILE in the source tree would never match one from a test run.
    encoder = ticket.TicketEncoder(cert=SIGNING_CERT_FILE,
                                   key=SIGNING_KEY_FILE,
                                   lifetime=300)

    decoder = ticket.TicketDecoder(ca=CERT_FILE,
                                   eku=None,
                                   peer=CERT_DATA)

    encoded = encoder.encode(TEST_TOKEN_PAYLOAD)
    decoded = decoder.decode(encoded)

    assert decoded == TEST_TOKEN_PAYLOAD
