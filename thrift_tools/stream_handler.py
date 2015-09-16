""" A StreamHandler that extracts messages from streams """

from __future__ import print_function

from collections import defaultdict

import sys
import traceback

from .thrift_message import ThriftMessage


class StreamContext(object):
    def __init__(self):
        self.bytes = ''


class StreamHandler(object):
    def __init__(self,
                 outqueue,
                 protocol=None,
                 finagle_thrift=False,
                 max_message_size=1024*1000,
                 read_values=False,
                 debug=False):
        self._contexts_by_streams = defaultdict(StreamContext)
        self._pop_size = 1024  # TODO: what's a good value here?
        self._outqueue = outqueue
        self._protocol = protocol
        self._finagle_thrift = finagle_thrift
        self._max_message_size = max_message_size
        self._debug = debug
        self._read_values = read_values
        self._seen_messages = 0
        self._recognized_streams = set()  # streams from which msgs have been read

    def __call__(self, *args, **kwargs):
        self.handler(*args, **kwargs)

    @property
    def seen_streams(self):
        return len(self._contexts_by_streams)

    @property
    def recognized_streams(self):
        return len(self._recognized_streams)

    @property
    def unrecognized_streams(self):
        return self.seen_streams - self.recognized_streams

    @property
    def pending_thrift_msgs(self):
        return len(self._outqueue)

    @property
    def seen_thrift_msgs(self):
        return self._seen_messages

    def handler(self, stream):
        context = self._contexts_by_streams[stream]
        bytes, timestamp = stream.pop_data(self._pop_size)
        context.bytes += bytes

        # EMSGSIZE
        if len(context.bytes) >= self._max_message_size:
            if self._debug:
                print('Dropping bytes, dropped size: %d' % len(context.bytes))
            context.bytes = ''
            return

        # FIXME: a bit of brute force to find the start of a message.
        #        Is there a magic byte/string we can look for?

        view = memoryview(context.bytes)
        for idx in range(0, len(context.bytes)):
            try:
                data_slice = view[idx:].tobytes()
                msg, msglen = ThriftMessage.read(
                    data_slice,
                    protocol=self._protocol,
                    finagle_thrift=self._finagle_thrift,
                    read_values=self._read_values)
            except EOFError:
                continue
            except Exception as ex:
                if self._debug:
                    print('Bad message for stream %s: %s: %s\n(idx=%d) '
                          '(context size=%d)' % (
                            stream,
                            ex,
                            traceback.format_exc(),
                            idx,
                            len(context.bytes)),
                          file=sys.stderr
                          )
                continue

            self._recognized_streams.add(stream)
            self._seen_messages += 1
            self._outqueue.append((timestamp, stream.src, stream.dst, msg))
            context.bytes = context.bytes[idx + msglen:]
            break
