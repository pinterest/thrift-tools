# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import pprint
import sys

try:
    import mmap
    HAS_MMAP = True
except ImportError:
    HAS_MMAP = False

from .thrift_message import ThriftMessage


def get_flags():
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument('file', type=str, default='-',
                   help='File from which to read Thrift messages')
    p.add_argument('--pretty', default=False, action='store_true',
                   help='Pretty print each Thrift message')
    p.add_argument('--finagle-thrift', default=False, action='store_true',
                   help='Detect finagle-thrift traffic (i.e.: with '
                   'request headers)')
    p.add_argument('--max-messages', type=int, default=-1, metavar='<max>',
                   help='Read up to <max> messages and then exit')
    p.add_argument('--skip-values', default=False, action='store_true',
                   help='Skip the values when reading each message (faster)')
    p.add_argument('--show-holes', default=False, action='store_true',
                   help='Show the holes in between messages (if any)')
    p.add_argument('--padding', type=int, default=0,
                   help='Number of bytes in between each message. For example, '
                   'if you using a binary log format that prepends a timestamp '
                   '(long) + the message length (int), the padding would be of '
                   '12 bytes')
    p.add_argument('--debug', default=False, action='store_true',
                   help='Display debugging messages')

    return p.parse_args()


def main():
    flags = get_flags()

    if flags.file == '-':
        fh = sys.stdin
    else:
        try:
            fh = open(flags.file)
        except IOError as ex:
            print('Couldn\'t open %s: %s' % (flags.file, ex))
            sys.exit(1)

    if HAS_MMAP and flags.file != '-':
        data = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        view = None
    else:
        # this might hurt...
        data = fh.read()
        view = memoryview(data)

    pp = pprint.PrettyPrinter(indent=4)

    start = flags.padding
    nread = 0

    # each hole is a tuple of (start_position, nbytes)
    holes = []

    try:
        done = False
        while start < len(data) and not done:
            for idx in range(start, len(data)):
                try:
                    msg, msglen = ThriftMessage.read(
                        view[idx:].tobytes() if view else data[idx:],
                        finagle_thrift=flags.finagle_thrift,
                        read_values=not flags.skip_values)

                    print(pp.pformat(msg.as_dict) if flags.pretty else msg)
                    nread += 1

                    # did we skip any bytes?
                    nbytes = idx - start
                    if nbytes > 0:
                        holes.append((start, nbytes))

                    start = idx + msglen + flags.padding

                    if flags.max_messages > 0 and nread > flags.max_messages:
                        done = True

                    break

                except Exception as ex:
                    if flags.debug:
                        print('Bad message: %s (idx=%d)' % (ex, idx))

    except KeyboardInterrupt:
        pass

    if holes:
        print('Read msgs: %d\nHoles: %d\n' % (nread, len(holes)))
        if flags.show_holes:
            for idx, hole in enumerate(holes, start=1):
                print('#%d: start=%d, size=%d' % (idx, hole[0], hole[1]))
    else:
        print('Read msgs: %d\nNo bytes skipped' % nread)
