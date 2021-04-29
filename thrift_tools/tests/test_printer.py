try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import re
import unittest

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift_tools import idl
from thrift_tools.message_sniffer import MessageSnifferOptions, MessageSniffer
from thrift_tools.printer import (
    FormatOptions, LatencyPrinter, PairedPrinter, Printer)

from .util import get_pcap_path, get_thrift_path


def options():
    return MessageSnifferOptions(
        iface=None,
        port=9090,
        ip=None,
        pcap_file=get_pcap_path('calc-service-binary'),
        protocol=TBinaryProtocol,
        finagle_thrift=False,
        read_values=True,
        max_queued=2000,
        max_message_size=2000,
        debug=False)


def format_options(show_fields=False, idl_file=None):
    return FormatOptions(
        pretty_printer=False,
        is_color=False,
        show_header=False,
        show_fields=show_fields,
        json=False,
        idl_file=idl_file
        )


class PrinterTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_default(self):
        output = StringIO()
        printer = Printer(format_options(), output)

        message_sniffer = MessageSniffer(options(), printer)
        message_sniffer.join()

        self._assertMessages(output)

    def test_paired(self):
        output = StringIO()
        printer = PairedPrinter(format_options(), output)

        message_sniffer = MessageSniffer(options(), printer)
        message_sniffer.join()

        self._assertMessages(output)

    def test_latency_printer(self):
        output = StringIO()
        printer = LatencyPrinter(expected_calls=3, output=output)

        message_sniffer = MessageSniffer(options(), printer)
        message_sniffer.join()

        self.assertIn(
            'method       count       avg       min       max       p90       p95       p99      p999',
            output.getvalue())
        self.assertIn(
            'ping             1  0.000156  0.000156  0.000156  0.000156  0.000156  0.000156  0.000156',
            output.getvalue())
        self.assertIn(
            'calculate        1  0.000144  0.000144  0.000144  0.000144  0.000144  0.000144  0.000144',
            output.getvalue())
        self.assertIn(
            'add              1  0.000104  0.000104  0.000104  0.000104  0.000104  0.000104  0.000104',
            output.getvalue())

    def _assertMessages(self, output):
        """ this is a bit fragile... """
        self.assertIn(
            '127.0.0.1:51112 -> 127.0.0.1:9090: method=ping, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:9090 -> 127.0.0.1:51112: method=ping, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:51112 -> 127.0.0.1:9090: method=add, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:9090 -> 127.0.0.1:51112: method=add, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:51112 -> 127.0.0.1:9090: method=calculate, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:9090 -> 127.0.0.1:51112: method=calculate, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:51112 -> 127.0.0.1:9090: method=calculate, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:9090 -> 127.0.0.1:51112: method=calculate, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:51112 -> 127.0.0.1:9090: method=getStruct, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '127.0.0.1:9090 -> 127.0.0.1:51112: method=getStruct, type=reply, seqid=0\n',
            output.getvalue())

    def test_paired_idl(self):
        output = StringIO()
        format_opts = format_options(
            show_fields=True,
            idl_file=get_thrift_path('tutorial'),
        )
        printer = PairedPrinter(format_opts, output)

        message_sniffer = MessageSniffer(options(), printer)
        message_sniffer.join()

        out = '\n'.join(
            re.sub(r'\[\d+:\d+:\d+:\d+\] ', '', line)
            for line in output.getvalue().splitlines()
        )
        expected = '\n'.join([
            "127.0.0.1:51112 -> 127.0.0.1:9090: method=ping, type=call, seqid=0",
            "fields: []",
            "------>127.0.0.1:9090 -> 127.0.0.1:51112: method=ping, type=reply, seqid=0",
            "        fields: fields=[]",
            "127.0.0.1:51112 -> 127.0.0.1:9090: method=add, type=call, seqid=0",
            "fields: [('num1', 1), ('num2', 1)]",
            "------>127.0.0.1:9090 -> 127.0.0.1:51112: method=add, type=reply, seqid=0",
            "        fields: 2",
            "127.0.0.1:51112 -> 127.0.0.1:9090: method=calculate, type=call, seqid=0",
            "fields: [('logid', 1), ('w', ('Work', [('num1', 1), ('num2', 0), ('op', 'DIVIDE'), ('comment', None)]))]",
            "------>127.0.0.1:9090 -> 127.0.0.1:51112: method=calculate, type=reply, seqid=0",
            "        fields: ('InvalidOperation', [('whatOp', 4), ('why', 'Cannot divide by 0')])",
            "127.0.0.1:51112 -> 127.0.0.1:9090: method=calculate, type=call, seqid=0",
            "fields: [('logid', 1), ('w', ('Work', [('num1', 15), ('num2', 10), ('op', 'SUBTRACT'), ('comment', None)]))]",
            "------>127.0.0.1:9090 -> 127.0.0.1:51112: method=calculate, type=reply, seqid=0",
            "        fields: 5",
            "127.0.0.1:51112 -> 127.0.0.1:9090: method=getStruct, type=call, seqid=0",
            "fields: [('key', 1)]",
            "------>127.0.0.1:9090 -> 127.0.0.1:51112: method=getStruct, type=reply, seqid=0",
            "        fields: ('SharedStruct', [('key', 1), ('value', '5')])",
        ])
        self.assertEqual(out, expected)
