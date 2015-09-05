from __future__ import print_function

from collections import deque, namedtuple
from threading import Thread

import time

from .sniffer import Sniffer
from .stream_handler import StreamHandler


MessageSnifferOptions = namedtuple('MessageSnifferOptions', [
    'iface',
    'port',
    'ip',
    'pcap_file',
    'protocol',
    'finagle_thrift',
    'read_values',
    'max_queued',
    'max_message_size',
    'debug',
])


STOP_MESSAGE = object()
DONE_TUPLE = (None, None, None, STOP_MESSAGE)


class MessageSniffer(Thread):
    def __init__(self, options, handler=None):
        self._options = options
        self._handlers = []
        self._queue = deque(maxlen=options.max_queued)

        self._handler = StreamHandler(
            self._queue,
            protocol=options.protocol,
            finagle_thrift=options.finagle_thrift,
            max_message_size=options.max_message_size,
            read_values=options.read_values,
            debug=options.debug)

        self._sniffer = Sniffer(
            options.iface, options.port,
            stream_handler=self._handler,
            ip=options.ip,
            offline=options.pcap_file)

        self.add_handler(handler)

        super(MessageSniffer, self).__init__()
        self.setDaemon(True)
        self.start()

    def status(self):
        return """
alive:                  %s
queue size:             %d
seen streams:           %d
unrecognized streams:   %d
seen thrift msgs:       %d
pending thrift msgs:    %d
sniffer alive:          %s
pending ip packets:     %d
dispatcher alive:       %s
""" % (self.isAlive(),
       len(self._queue),
       self._handler.seen_streams,
       self._handler.unrecognized_streams,
       self._handler.seen_thrift_msgs,
       self._handler.pending_thrift_msgs,
       self._sniffer.isAlive(),
       self._sniffer.pending_ip_packets,
       self._sniffer.dispatcher.isAlive()
       )

    def add_handler(self, handler):
        if handler is None:
            return

        if handler in self._handlers:
            raise ValueError('handler already registered')

        self._handlers.append(handler)

    def run(self):
        opts = self._options
        handler = self._handler
        sniffer = self._sniffer

        # main loop
        running = True
        while running:

            # if the sniffer finished and the queue is empty, we are done
            if not sniffer.isAlive() and len(self._queue) == 0:
                break

            try:
                timestamp, src, dst, msg = self._queue.popleft()
            except IndexError:
                time.sleep(0.001)
                continue

            if msg == STOP_MESSAGE:
                break

            # dispatch thrift messages to handlers
            for handler in self._handlers:
                try:
                    rv = handler(timestamp, src, dst, msg)
                    if not rv:
                        running = False
                        break
                except Exception as ex:
                    print('handler exception: %s' % ex)

        # leaving...
        if len(self._queue):
            print('%d messages left in the queue' % len(self._queue))

    def stop(self, wait_for_stopped=False):
        if not self.isAlive():
            return

        self._queue.append(DONE_TUPLE)

        if wait_for_stopped:
            while self.isAlive():
                time.sleep(0.01)
