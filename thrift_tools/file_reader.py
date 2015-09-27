# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import pprint
import sys

from thrift_tools.thrift_file import ThriftMessageFile, ThriftFile


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
    try :
        thrift_msg_file = ThriftMessageFile(flags.file, flags.finagle_thrift,
                                            not flags.skip_values, flags.padding)
    except ThriftFile.Error as ex:
        print(ex.message)
        sys.exit(1)

    pp = pprint.PrettyPrinter(indent=4)
    holes = []
    total_msg_read = 0
    try:
        for msg, hole in thrift_msg_file:
            print(pp.pformat(msg.as_dict) if flags.pretty else msg)
            if hole:
                holes.append(hole)
            total_msg_read += 1
            if 0 < flags.max_messages <= total_msg_read:
                break
    except KeyboardInterrupt:
        pass

    if holes:
        print('Read msgs: %d\nHoles: %d\n' % (total_msg_read, len(holes)))
        if flags.show_holes:
            for idx, hole in enumerate(holes, start=1):
                print('#%d: start=%d, size=%d' % (idx, hole[0], hole[1]))
    else:
        print('Read msgs: %d\nNo bytes skipped' % total_msg_read)
