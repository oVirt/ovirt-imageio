import os

from ovirt_image_proxy import ticket

TEST_DIR = os.path.dirname(__file__)

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
