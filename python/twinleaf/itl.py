#!/usr/bin/env python
from . import Device
def interact(url: str = 'tcp://localhost'):
    import argparse
    parser = argparse.ArgumentParser(prog='itl',
                                   description='Interactive Twinleaf I/O.')

    parser.add_argument("url",
                      nargs='?',
                      default='tcp://localhost',
                      help='URL: tcp://localhost')
    parser.add_argument("-s",
                      default='',
                      help='Routing: /0/1...')
    args = parser.parse_args()

    dev = Device(url=args.url, route=args.s, announce=True)
    del argparse

    dev._interact()

if __name__ == "__main__":
  interact()
