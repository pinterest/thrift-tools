from collections import deque

import unittest

from thrift_tools.sniffer import Sniffer
from thrift_tools.stream_handler import StreamHandler
from thrift_tools.thrift_struct import ThriftField, ThriftStruct

from .util import get_pcap_path


class BasicTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_calculator_service_binary(self):
        self._test_protocol('binary')

    def test_calculator_service_compact(self):
        self._test_protocol('compact')

    def test_calculator_service_json(self):
        self._test_protocol('json')

    def test_finagle(self):
        queue = deque()
        pcap_file = get_pcap_path('finagle-thrift')
        handler = StreamHandler(queue, read_values=True, finagle_thrift=True)

        sniffer = Sniffer('ignore', 9090, handler, offline=pcap_file)
        sniffer.join()

        self.assertEquals(len(queue), 22)

        # is this finagle-thrift indeed?
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, '__can__finagle__trace__v3__')
        self.assertEquals(msg.type, 'call')
        self.assertEquals(len(msg.args), 0)

        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, '__can__finagle__trace__v3__')
        self.assertEquals(msg.type, 'reply')
        self.assertEquals(len(msg.args), 0)

        # the search() call
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'search')
        self.assertEquals(msg.type, 'call')

        # inspect the header & the contexts
        self.assertEquals(len(msg.header), 4)
        self.assertEquals(msg.header[0], ThriftField('i64', 1, -8277104800942727271))
        self.assertEquals(msg.header[1], ThriftField('i64', 2, -8277104800942727271))
        self.assertEquals(msg.header[2], ThriftField('i64', 7, 0))

        contexts = msg.header[3].value
        self.assertEquals(contexts[0][0].value,
                          'com.twitter.finagle.tracing.TraceContext')
        self.assertEquals(contexts[1][0].value,
                          'com.twitter.finagle.Deadline')

        self.assertEquals(msg.args, ThriftStruct([ThriftField('string', 1, 'foo')]))

        # the reply
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'search')
        self.assertEquals(msg.type, 'reply')
        self.assertEquals(msg.args, ThriftStruct([ThriftField('list', 0, ['one', 'two', 'three'])]))

    def _test_protocol(self, protoname):
        queue = deque()
        pcap_file = get_pcap_path('calc-service-%s' % protoname)
        handler = StreamHandler(queue, read_values=True, debug=True)

        sniffer = Sniffer('ignore', 9090, handler, offline=pcap_file)
        sniffer.join()

        self.assertEquals(len(queue), 10)

        # the ping call
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'ping')
        self.assertEquals(msg.type, 'call')
        self.assertEquals(len(msg.args), 0)

        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'ping')
        self.assertEquals(msg.type, 'reply')
        self.assertEquals(len(msg.args), 0)

        # a succesful add
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'add')
        self.assertEquals(msg.type, 'call')
        self.assertEquals(len(msg.args), 2)
        self.assertEquals(msg.args[0], ThriftField('i32', 1, 1))
        self.assertEquals(msg.args[1], ThriftField('i32', 2, 1))

        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'add')
        self.assertEquals(msg.type, 'reply')
        self.assertEquals(len(msg.args), 1)
        self.assertEquals(msg.args[0], ThriftField('i32', 0, 2))

        # a failed calculate call
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'calculate')
        self.assertEquals(msg.type, 'call')
        self.assertEquals(len(msg.args), 2)
        self.assertEquals(msg.args[0], ThriftField('i32', 1, 1))
        self.assertEquals(
            msg.args[1],
            ThriftField('struct', 2,
                        ThriftStruct(
                            [ThriftField('i32', 1, 1),
                             ThriftField('i32', 2, 0),
                             ThriftField('i32', 3, 4)])))
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'calculate')
        self.assertEquals(msg.type, 'reply')
        self.assertEquals(len(msg.args), 1)
        self.assertEquals(
            msg.args[0],
            ThriftField('struct', 1,
                        ThriftStruct([ThriftField('i32', 1, 4),
                                      ThriftField('string', 2, 'Cannot divide by 0')])))

        # a successful calculate call
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'calculate')
        self.assertEquals(msg.type, 'call')
        self.assertEquals(len(msg.args), 2)
        self.assertEquals(
            msg.args[1],
            ThriftField('struct', 2, ThriftStruct([ThriftField('i32', 1, 15),
                                                   ThriftField('i32', 2, 10),
                                                   ThriftField('i32', 3, 2)])))

        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'calculate')
        self.assertEquals(msg.type, 'reply')
        self.assertEquals(len(msg.args), 1)
        self.assertEquals(msg.args[0], ThriftField('i32', 0, 5))

        # getStruct
        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'getStruct')
        self.assertEquals(msg.type, 'call')
        self.assertEquals(len(msg.args), 1)
        self.assertEquals(msg.args[0], ThriftField('i32', 1, 1))

        _, src, dst, msg = queue.popleft()
        self.assertEquals(msg.method, 'getStruct')
        self.assertEquals(msg.type, 'reply')
        self.assertEquals(len(msg.args), 1)
        self.assertEquals(
            msg.args[0],
            ThriftField('struct', 0, ThriftStruct([ThriftField('i32', 1, 1),
                                                   ThriftField('string', 2, '5')])))
