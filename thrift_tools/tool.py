# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import signal
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.protocol.TJSONProtocol import TJSONProtocol

from .message_sniffer import MessageSnifferOptions, MessageSniffer
from .printer import FormatOptions, LatencyPrinter, PairedPrinter, Printer


VALID_PROTOCOLS = 'auto, binary, compact or json'


def get_flags():
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # general options
    p.add_argument('--iface', type=str, default='eth0', metavar='<iface>',
                   help='The interface to sniff from')
    p.add_argument('--port', type=int, default=9090, metavar='<port>',
                   help='The port the Thrift service listens too')
    p.add_argument('--max-queued', type=int, default=20*1024,
                   metavar='<maxqueued>',
                   help='Max number of queued messages')
    p.add_argument('--max-message-size', type=int, default=10*1024,
                   help='Max bytes size for a Thrift message')
    p.add_argument('--ip', type=str, nargs='+',
                   help='Only show messages from/to this IP(s)')
    p.add_argument('--finagle-thrift', default=False, action='store_true',
                   help='Detect finagle-thrift traffic (i.e.: with '
                   'request headers)')
    p.add_argument('--pcap-file', type=str, default='',
                   help='Path to pcap file, for offline introspection')
    p.add_argument('--debug', default=False, action='store_true',
                   help='Display debugging messages')
    p.add_argument('--protocol', type=str, default='auto',
                   help='Use a specific protocol. Options: %s' %
                   VALID_PROTOCOLS)

    cmds = p.add_subparsers(dest='cmd')

    # dump
    dump = cmds.add_parser('dump')
    dump.add_argument('--pretty', default=False, action='store_true',
                      help='Pretty print the Thrift structs')
    dump.add_argument('--color', default=False, action='store_true',
                      help='Use a different color for each stream')
    dump.add_argument('--unpaired', default=False, action='store_true',
                      help='Print (requests, replies) as they arrive, '
                      'possibly out of order')
    dump.add_argument('--show-header', default=False, action='store_true',
                      help='Show the header of each message, if any.'
                      'This makes sense with --finagle-thrift, given that '
                      'Finagle uses a RequestHeader')
    dump.add_argument('--show-fields', default=False, action='store_true',
                      help='Show the message\'s field')
    dump.add_argument('--show-values', default=False, action='store_true',
                      help='Show the values of each Thrift data type')
    dump.add_argument('--show-all', default=False, action='store_true',
                      help='Shows header, fields and values')
    dump.add_argument('--json', default=False, action='store_true',
                      help='Outputs messages as JSON')

    # stats
    stats = cmds.add_parser('stats')
    stats.add_argument('--count', type=int, default=0, metavar='<count>',
                       help='Waits for <count> pairs of (request, reply) and '
                       'reports the avg, p95 and p99 latencies')

    return p.parse_args()


def main():
    flags = get_flags()

    # route to the appropriate command
    if flags.cmd == 'stats':
        printer = LatencyPrinter(expected_calls=flags.count)
        read_values = False
    elif flags.cmd == 'dump':
        printer_cls = Printer if flags.unpaired else PairedPrinter
        format_opts = FormatOptions(
            flags.pretty,
            flags.color,
            flags.show_header or flags.show_all,
            flags.show_fields or flags.show_all,
            flags.json,
            )
        printer = printer_cls(format_opts)
        read_values = flags.show_values or flags.show_all
    else:
        print('Unknown command: %s' % flags.cmd)
        sys.exit(1)

    # which protocol to use
    if flags.protocol == 'auto':
        protocol = None
    elif flags.protocol == 'binary':
        protocol = TBinaryProtocol
    elif flags.protocol == 'compact':
        protocol = TCompactProtocol
    elif flags.protocol == 'json':
        protocol = TJSONProtocol
    else:
        print('Unknown protocol: %s' % flags.protocol)
        print('Valid options for --protocol are: %s' % VALID_PROTOCOLS)
        sys.exit(1)

    # launch the thrift message sniffer
    options = MessageSnifferOptions(
        iface=flags.iface,
        port=flags.port,
        ip=flags.ip,
        pcap_file=flags.pcap_file,
        protocol=protocol,
        finagle_thrift=flags.finagle_thrift,
        read_values=read_values,
        max_queued=flags.max_queued,
        max_message_size=flags.max_message_size,
        debug=flags.debug
        )
    message_sniffer = MessageSniffer(options, printer)

    def sigusr_handler(*args):
        print('message sniffer status: %s' % message_sniffer.status(),
              file=sys.stderr)

    # wire up sigusr1 for debugging info
    signal.signal(signal.SIGUSR1, sigusr_handler)

    # loop forever
    try:
        while message_sniffer.isAlive():
            message_sniffer.join(1)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        # force a stop
        message_sniffer.stop(wait_for_stopped=True)


if __name__ == '__main__':
    main()
