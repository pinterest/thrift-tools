from __future__ import print_function

import abc
import sys

try:
    import mmap
    HAS_MMAP = True
except ImportError:
    HAS_MMAP = False

from thrift.transport import TTransport

from .thrift_message import ThriftMessage
from .thrift_struct import ThriftStruct


class ThriftFile(object):
    """
    An abstract base class for implementations that read files contaning
    different Thrift types.
    """

    __metaclass__ = abc.ABCMeta

    class Error(Exception):
        pass

    """
    A base class that represents a file containing thrift objects
    """

    def __init__(self, file_name='-', read_values=False, debug=False):
        self._debug = debug
        self._read_values = read_values
        if file_name == '-':
            fh = sys.stdin
        else:
            try:
                fh = open(file_name)
            except IOError as ex:
                raise ThriftFile.Error('Could not open %s: %s' % (file_name, ex))

        if HAS_MMAP and file_name != '-':
            self._data = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
            self._view = None
        else:
            # this might hurt...
            self._data = fh.read()
            self._view = memoryview(self._data)

    def _data_slice(self, idx):
        return self._view[idx:].tobytes() if self._view else self._data[idx:]

    def __iter__(self):
        """
        An iterator that yields a tuple of (thrift object, hole), where
        hole is a tuple of (start, skipped) bytes.
        """
        idx = self._padding
        while idx < len(self._data):
            tobject, hole = self._read_next(idx, len(self._data))
            if tobject is None:
                return
            if hole:
                idx += hole[1]  # number of bytes skipped
            idx += tobject.bytes_length + self._padding
            yield tobject, hole

    @abc.abstractmethod
    def _read_next(self, start, end):
        pass


class ThriftMessageFile(ThriftFile):
    """
    A file containing thrift messages. Allows iteration via standard iterator
    protocol

    Ex:
    >> thrift_msg_file = ThriftMessageFile(file_name)
    >> for message in thrift_msg_file:
        print message.as_dict()
    """

    def __init__(self, file_name='-', finagle_thrift=False,
                 read_values=False, padding=0, debug=False):
        super(ThriftMessageFile, self).__init__(file_name, read_values, debug)
        self._padding = padding
        self._finagle_thrift = finagle_thrift

    def _read_next(self, start, end):
        for idx in range(start, end):
            try:
                msg, _ = ThriftMessage.read(self._data_slice(idx),
                                            finagle_thrift=self._finagle_thrift,
                                            read_values=self._read_values)
                skipped = idx - start
                return (msg, None) if skipped == 0 else (msg, (start, skipped))
            except Exception as ex:
                if self._debug:
                    print('Bad message: %s (idx=%d)' % (ex, idx))

        # nothing found
        return (None, None)


class ThriftStructFile(ThriftFile):
    """
    A file containing thrift structs. Allows iteration via standard iterator
    protocol

    Ex:
    >> thrift_struct_file = ThriftStructFile(file_name)
    >> for tstruct in thrift_msg_file:
        print tstruct.as_dict()
    """

    MAX_FIELDS = 10000
    MAX_LIST_SIZE = 10000
    MAX_MAP_SIZE = 10000
    MAX_SET_SIZE = 10000

    def __init__(self, protocol, file_name='-', read_values=False, padding=0, debug=False):
        super(ThriftStructFile, self).__init__(file_name, read_values, debug)
        self._padding = padding
        self._protocol = protocol

    def _read_next(self, start, end):
        for idx in range(start, end):
            try:
                trans = TTransport.TMemoryBuffer(self._data_slice(idx))
                proto = self._protocol(trans)
                tstruct = ThriftStruct.read(
                    proto,
                    max_fields=ThriftStructFile.MAX_FIELDS,
                    max_list_size=ThriftStructFile.MAX_LIST_SIZE,
                    max_map_size=ThriftStructFile.MAX_MAP_SIZE,
                    max_set_size=ThriftStructFile.MAX_SET_SIZE,
                    read_values=self._read_values)
                skipped = idx - start
                return (tstruct, None) if skipped == 0 else (tstruct, (start, skipped))
            except Exception as ex:
                if self._debug:
                    print('Bad message: %s (idx=%d)' % (ex, idx))

        # nothing found
        return (None, None)
