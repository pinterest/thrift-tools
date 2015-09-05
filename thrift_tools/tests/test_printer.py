from StringIO import StringIO

import unittest

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift_tools.message_sniffer import MessageSnifferOptions, MessageSniffer
from thrift_tools.printer import (
    FormatOptions, LatencyPrinter, PairedPrinter, Printer)

from .util import get_pcap_path


def options():
    return MessageSnifferOptions(
        iface='ignore',
        port=9090,
        ip=None,
        pcap_file=get_pcap_path('calc-service-binary'),
        protocol=TBinaryProtocol,
        finagle_thrift=False,
        read_values=True,
        max_queued=2000,
        max_message_size=2000,
        debug=False)


def format_options():
    return FormatOptions(
        pretty_printer=False,
        is_color=False,
        show_header=False,
        show_fields=False,
        json=False,
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
            'method       count          avg          min          max          p90          p95          p99         p999',
            output.getvalue())
        self.assertIn(
            'ping             1  0.000156164  0.000156164  0.000156164  0.000156164  0.000156164  0.000156164  0.000156164',
            output.getvalue())
        self.assertIn(
            'calculate        1  0.000144005  0.000144005  0.000144005  0.000144005  0.000144005  0.000144005  0.000144005',
            output.getvalue())
        self.assertIn(
            'add              1  0.000103951  0.000103951  0.000103951  0.000103951  0.000103951  0.000103951  0.000103951',
            output.getvalue())

    def _assertMessages(self, output):
        """ this is a bit fragile... """
        self.assertIn(
            '[22:55:50:387214] 127.0.0.1:51112 -> 127.0.0.1:9090: method=ping, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:387370] 127.0.0.1:9090 -> 127.0.0.1:51112: method=ping, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:387492] 127.0.0.1:51112 -> 127.0.0.1:9090: method=add, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:387596] 127.0.0.1:9090 -> 127.0.0.1:51112: method=add, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:387696] 127.0.0.1:51112 -> 127.0.0.1:9090: method=calculate, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:387840] 127.0.0.1:9090 -> 127.0.0.1:51112: method=calculate, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:388615] 127.0.0.1:51112 -> 127.0.0.1:9090: method=calculate, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:388725] 127.0.0.1:9090 -> 127.0.0.1:51112: method=calculate, type=reply, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:388811] 127.0.0.1:51112 -> 127.0.0.1:9090: method=getStruct, type=call, seqid=0',
            output.getvalue())
        self.assertIn(
            '[22:55:50:388905] 127.0.0.1:9090 -> 127.0.0.1:51112: method=getStruct, type=reply, seqid=0\n',
            output.getvalue())
