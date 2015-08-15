"""
A small utility to encode/decode oVirt tickets
"""

import sys

from ovirt_image_proxy import ticket

def main():
    if len(sys.argv) == 5 and sys.argv[1] == 'encode':
        encodeTicket(*sys.argv[2:])
    elif len(sys.argv) == 4 and sys.argv[1] == 'decode':
        decodeTicket(*sys.argv[2:])
    else:
        sys.stderr.write('''\
Usage: python {n} <encode | decode>
  encode arguments: <cert> <key> <lifetime> < <data>
  decode arguments: <cert> <key>
  For any options to skip, supply "-"

Examples:
  {n} encode ~/ovirt-engine/etc/pki/ovirt-engine/certs/ca.der \\
      ~/ovirt-engine/etc/pki/ovirt-engine/private/ca.pem 3600
  {n} decode /tmp/engine_cert.pem /tmp/engine_cert.pem

Note that the engine cert can be retrieved with:
  wget http://localhost:8080/ovirt-engine/services/pki-resource?resource=engine-certificate&format=X509-PEM-CA
CA cert:
  wget http://localhost:8080/ovirt-engine/services/pki-resource?resource=ca-certificate&format=X509-PEM-CA
'''.format(n=sys.argv[0]))
        sys.exit(1)


def encodeTicket(cert, key, lifetime):
    pcert = cert if cert != '-' else None
    pkey = key if key != '-' else None
    plifetime = int(lifetime) if lifetime != '-' else None
    ticketEncoder = ticket.TicketEncoder(pcert, pkey, plifetime)
    payload = sys.stdin.read()
    t = ticketEncoder.encode(payload)
    print t


def decodeTicket(cert, key):
    pcert = cert if cert != '-' else None
    if key != '-':
        with open(key, 'r') as f:
            pkey = f.read()
    else:
        pkey = None
    print "pcert is " + str(pcert)
    print "pkey is " + str(pkey)
    ticketDecoder = ticket.TicketDecoder(pcert, None, pkey)
    t = sys.stdin.read()
    payload = ticketDecoder.decode(t)
    print payload


if __name__ == '__main__':
    main()
