from __future__ import print_function

import sys

try:
    import mmap
    HAS_MMAP = True
except ImportError:
    HAS_MMAP = False

from .thrift_message import ThriftMessage


class ThriftFile(object):

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
                 read_values=False, padding=0):
        super(ThriftMessageFile, self).__init__(file_name, read_values, True)
        self._padding = padding
        self._finagle_thrift = finagle_thrift

    def __iter__(self):
        idx = self._padding
        while idx < len(self._data):
            msg, hole = self._read_next(idx, len(self._data))
            if hole:
                idx += hole[1]  # number of bytes skipped
            idx += msg.length + self._padding
            yield msg, hole

    def _data_slice(self, idx):
        return self._view[idx:].tobytes() if self._view else self._data[idx:]

    def _read_next(self, start, end):
        for idx in range(start, end):
            try:
                msg, _ = ThriftMessage.read(self._data_slice(idx),
                                            finagle_thrift=self._finagle_thrift,
                                            read_values=self._read_values)
                skipped = idx - start
                return msg, None if skipped == 0 else (msg, (start, skipped))
            except Exception, ex:
                if self._debug:
                    print('Bad message: %s (idx=%d)' % (ex, idx))
