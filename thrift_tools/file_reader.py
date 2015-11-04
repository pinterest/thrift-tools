# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import pprint
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.protocol.TJSONProtocol import TJSONProtocol

from .thrift_file import (
    ThriftFile,
    ThriftMessageFile,
    ThriftStructFile
)


VALID_PROTOCOLS = 'binary, compact or json'


def get_flags():
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument('file', type=str, default='-',
                   help='File from which to read Thrift messages')
    p.add_argument('--structs', default=False, action='store_true',
                   help='Read structs instead messages')
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
    p.add_argument('--protocol', type=str, default='binary',
                   help='Use a specific protocol for reading structs. Options: %s' %
                   VALID_PROTOCOLS)
    p.add_argument('--debug', default=False, action='store_true',
                   help='Display debugging messages')

    return p.parse_args()


def main():
    flags = get_flags()
    run(flags)


def run(flags, output=sys.stdout):
    try :
        if flags.structs:
            # which protocol to use
            if flags.protocol == 'binary':
                protocol = TBinaryProtocol
            elif flags.protocol == 'compact':
                protocol = TCompactProtocol
            elif flags.protocol == 'json':
                protocol = TJSONProtocol
            else:
                output.write('Unknown protocol: %s' % flags.protocol)
                output.write('Valid options for --protocol are: %s' % VALID_PROTOCOLS)
                sys.exit(1)

            thrift_file = ThriftStructFile(
                protocol,
                file_name=flags.file,
                read_values=not flags.skip_values,
                padding=flags.padding,
                debug=flags.debug
            )
        else:
            thrift_file = ThriftMessageFile(
                file_name=flags.file,
                finagle_thrift=flags.finagle_thrift,
                read_values=not flags.skip_values,
                padding=flags.padding,
                debug=flags.debug
            )
    except ThriftFile.Error as ex:
        output.write(ex.message)
        sys.exit(1)

    pp = pprint.PrettyPrinter(indent=4)
    holes = []
    total_msg_read = 0
    try:
        for msg, hole in thrift_file:
            output.write(pp.pformat(msg.as_dict) if flags.pretty else msg)
            output.write('\n')
            if hole:
                holes.append(hole)
            total_msg_read += 1
            if 0 < flags.max_messages <= total_msg_read:
                break
    except KeyboardInterrupt:
        pass

    what = 'structs' if flags.structs else 'msgs'
    if holes:
        output.write('Read %s: %d\nHoles: %d\n' % (what, total_msg_read, len(holes)))
        if flags.show_holes:
            for idx, hole in enumerate(holes, start=1):
                output.write('#%d: start=%d, size=%d' % (idx, hole[0], hole[1]))
    else:
        output.write('Read %s: %d\nNo bytes skipped' % (what, total_msg_read))
