""" helpers for deserializing Thrift messages """

from struct import unpack
from thrift.Thrift import TMessageType

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.protocol.TJSONProtocol import TJSONProtocol
from thrift.transport import TTransport
from thrift_tools.thrift_struct import ThriftStruct


class ThriftMessage(object):
    def __init__(self, method, mtype, seqid, args, header=(), length=-1):
        self._method = method
        self._type = mtype
        self._seqid = seqid
        if not isinstance(args, ThriftStruct):
            raise ValueError('args must be a ThriftStruct instance')
        self._args = args
        self._header = header  # finagle-thrift prepends this to each call
        self._length = length

    def __len__(self):
        return self._length

    @property
    def method(self):
        return self._method

    @property
    def type(self):
        return self._type

    @property
    def seqid(self):
        return self._seqid

    @property
    def args(self):
        return self._args

    @property
    def header(self):
        return self._header

    def __str__(self):
        return 'method=%s, type=%s, seqid=%s, header=%s, fields=%s' % (
            self.method, self.type, self.seqid, self.header, str(self.args))

    @property
    def as_dict(self):
        return {
            'method': self.method,
            'type': self.type,
            'seqid': self.seqid,
            'header': self.header,
            'args': self.args,
            'length': len(self),
        }

    MAX_METHOD_LENGTH = 70

    # For Binary, this is i32 + str + i32
    # For Compact, the empty ping() gets through in 8 bytes
    MIN_MESSAGE_SIZE = 8

    # some sane defaults to keep memory usage tight
    MAX_FIELDS = 1000
    MAX_LIST_SIZE = 10000
    MAX_MAP_SIZE = 10000
    MAX_SET_SIZE = 10000

    @classmethod
    def read(cls, data,
             protocol=None,
             fallback_protocol=TBinaryProtocol,
             finagle_thrift=False,
             max_fields=MAX_FIELDS,
             max_list_size=MAX_LIST_SIZE,
             max_map_size=MAX_MAP_SIZE,
             max_set_size=MAX_SET_SIZE,
             read_values=False):
        """ tries to deserialize a message, might fail if data is missing """

        # do we have enough data?
        if len(data) < cls.MIN_MESSAGE_SIZE:
            raise ValueError('not enough data')

        if protocol is None:
            protocol = cls.detect_protocol(data, fallback_protocol)
        trans = TTransport.TMemoryBuffer(data)
        proto = protocol(trans)

        # finagle-thrift prepends a RequestHeader
        #
        # See: http://git.io/vsziG
        header = None
        if finagle_thrift:
            try:
                header = ThriftStruct.read(
                    proto,
                    max_fields,
                    max_list_size,
                    max_map_size,
                    max_set_size,
                    read_values)
            except:
                # reset stream, maybe it's not finagle-thrift
                trans = TTransport.TMemoryBuffer(data)
                proto = protocol(trans)

        # unpack the message
        method, mtype, seqid = proto.readMessageBegin()
        mtype = cls.message_type_to_str(mtype)

        if len(method) == 0 or method.isspace() or method.startswith(' '):
            raise ValueError('no method name')

        if len(method) > cls.MAX_METHOD_LENGTH:
            raise ValueError('method name too long')

        # we might have made it until this point by mere chance, so filter out
        # suspicious method names
        valid = range(33, 127)
        if any(ord(char) not in valid for char in method):
            raise ValueError('invalid method name' % method)

        args = ThriftStruct.read(
            proto,
            max_fields,
            max_list_size,
            max_map_size,
            max_set_size,
            read_values)

        proto.readMessageEnd()

        # Note: this is a bit fragile, the right thing would be to count bytes
        # as we read them (i.e.: when calling readI32, etc).
        msglen = trans._buffer.tell()

        return cls(method, mtype, seqid, args, header, msglen), msglen

    @classmethod
    def detect_protocol(cls, data, default=None):
        """ TODO: support fbthrift, finagle-thrift, finagle-mux, CORBA """
        if cls.is_compact_protocol(data):
            return TCompactProtocol
        elif cls.is_binary_protocol(data):
            return TBinaryProtocol
        elif cls.is_json_protocol(data):
            return TJSONProtocol

        if default is None:
            raise ValueError('Unknown protocol')

        return default

    COMPACT_PROTOCOL_ID = 0x82

    @classmethod
    def is_compact_protocol(cls, data):
        result, = unpack('!B', data[0])
        return result == cls.COMPACT_PROTOCOL_ID

    BINARY_PROTOCOL_VERSION_MASK = -65536  # 0xffff0000
    BINARY_PROTOCOL_VERSION_1 = -2147418112  # 0x80010000

    @classmethod
    def is_binary_protocol(cls, data):
        val, = unpack('!i', data[0:4])
        if val >= 0:
            return False
        version = val & cls.BINARY_PROTOCOL_VERSION_MASK
        return version == cls.BINARY_PROTOCOL_VERSION_1

    @classmethod
    def is_json_protocol(cls, data):
        # FIXME: more elaborate parsing would make this more robust
        return data.startswith('[1')

    @staticmethod
    def message_type_to_str(mtype):
        if mtype == TMessageType.CALL:
            return 'call'
        elif mtype == TMessageType.REPLY:
            return 'reply'
        elif mtype == TMessageType.EXCEPTION:
            return 'exception'
        elif mtype == TMessageType.ONEWAY:
            return 'oneway'
        else:
            raise ValueError('Unknown message type: %s' % mtype)