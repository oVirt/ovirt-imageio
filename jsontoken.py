"""
A small utility to easily generate JSON tokens to send to the image proxy.
"""

import argparse
import json

mappings = {
    'nbf': (int, 'not-before time (timestamp)'),
    'exp': (int, 'expiration (timestamp)'),
    'iat': (int, 'issued-at time (timestamp)'),
    'imaged-uri': (str, 'uri with port to imaged host'),
    'transfer-ticket': (str, 'uuid transfer ticket'),
}

def main():
    parser = argparse.ArgumentParser(
        description="Generate JSON tokens to send to the image proxy.",
        epilog=(
            "Example:\n"
            "  python %(prog)s \\\n"
            "   --iat 1439510425 --nbf 1439510425 --exp 1639510425 \\\n"
            "   --transfer-ticket 11112222-aaaa-bbbb-cccc-777788889999 \\\n"
            "   --imaged-uri http://192.168.1.30:54322"),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    for k, v in mappings.iteritems():
        parser.add_argument('--' + k, action='store', help=v[1], type=v[0])
    parser.add_argument('-i', '--int', action='append',
                        help="additional int arguments in k=v form")
    parser.add_argument('-s', '--str', action='append',
                        help="additional str arguments in k=v form")
    parser.add_argument('-e', '--encode', action='append',
                        help="encode oVirt ticket")
    args = parser.parse_args()

    d = dict(((k.replace('_', '-'), v)
              for k, v
              in vars(args).iteritems()
              if v is not None and k not in ('int', 'str', 'encode')))
    d.update(dict((k, v)      for k, v in (s.split('=') for s in (args.str if args.str else []))))
    d.update(dict((k, int(v)) for k, v in (s.split('=') for s in (args.int if args.str else []))))

    print json.dumps(d)


if __name__ == '__main__':
    main()
