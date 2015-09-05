from __future__ import print_function

from collections import deque
from threading import Lock, Thread

import logging
import sys
import time
import traceback

from util import get_ip, get_ip_packet

from scapy.sendrecv import sniff
from scapy.config import conf as scapy_conf


scapy_conf.logLevel = logging.ERROR  # shush scappy


class Stream(object):
    """ A representation of a TCP stream. """
    def __init__(self, src, dst):
        self._packets = []
        self._src = src
        self._dst = dst
        self._length = 0
        self._remaining = 0
        self._next_seq_id = -1
        self._lock_packets = Lock()

    def __str__(self):
        return '%s<->%s (length: %d, remaining: %d, seq_id: %d)' % (
            self.src, self.dst, self.length, self.remaining, self._next_seq_id)

    @property
    def length(self):
        """ how many bytes have been through the stream """
        return self._length

    @property
    def remaining(self):
        return self._remaining

    @property
    def src(self):
        return self._src

    @property
    def dst(self):
        return self._dst

    def pop(self, nbytes):
        """ pops packets with _at least_ nbytes of payload """
        size = 0
        popped = []
        with self._lock_packets:
            while size < nbytes:
                try:
                    packet = self._packets.pop(0)
                    size += len(packet.data.data)
                    self._remaining -= len(packet.data.data)
                    popped.append(packet)
                except IndexError:
                    break
        return popped

    def pop_data(self, nbytes):
        """ similar to pop, but returns payload + last timestamp """
        last_timestamp = 0
        data = []
        for packet in self.pop(nbytes):
            last_timestamp = packet.timestamp
            data.append(packet.data.data)

        return ''.join(data), last_timestamp

    def push(self, ip_packet):
        """ push the packet into the queue """

        data_len = len(ip_packet.data.data)
        seq_id = ip_packet.data.seq

        if data_len == 0:
            self._next_seq_id = seq_id
            return False

        # have we seen this packet?
        if self._next_seq_id != -1 and seq_id != self._next_seq_id:
            return False

        self._next_seq_id = seq_id + data_len

        with self._lock_packets:
            # Note: we only account for payload (i.e.: tcp data)
            self._length += len(ip_packet.data.data)
            self._remaining += len(ip_packet.data.data)

            self._packets.append(ip_packet)

        return True


class Dispatcher(Thread):
    """Dispatches streams to handlers """
    def __init__(self, packet_queue):
        super(Dispatcher, self).__init__()
        self.setDaemon(True)
        self._queue = packet_queue
        self._streams = {}
        self._handlers = []
        self.start()

    @property
    def empty(self):
        return len(self._queue) == 0

    def add_handler(self, stream_handler):
        if stream_handler is None:
            return

        if stream_handler in self._handlers:
            raise ValueError('handler already registered')

        self._handlers.append(stream_handler)

    def run(self, *args, **kwargs):
        """ Deal with the incoming packets """
        while True:
            try:
                timestamp, ip_p = self._queue.popleft()

                src_ip = get_ip(ip_p, ip_p.src)
                dst_ip = get_ip(ip_p, ip_p.dst)

                src = intern('%s:%s' % (src_ip, ip_p.data.sport))
                dst = intern('%s:%s' % (dst_ip, ip_p.data.dport))
                key = intern('%s<->%s' % (src, dst))

                stream = self._streams.get(key)
                if stream is None:
                    stream = Stream(src, dst)
                    self._streams[key] = stream

                # HACK: save the timestamp
                setattr(ip_p, 'timestamp', timestamp)
                pushed = stream.push(ip_p)

                if not pushed:
                    continue

                # let listeners know about the updated stream
                for handler in self._handlers:
                    try:
                        handler(stream)
                    except Exception as ex:
                        print('handler exception: %s' % ex)
            except Exception:
                time.sleep(0.00001)


class Sniffer(Thread):
    """ A generic & simple packet sniffer """

    def __init__(self, iface, port, stream_handler=None, offline=None, ip=None):
        """A Sniffer that merges packets into a stream

        Params:
            ``iface``           The interface in which to listen
            ``port``            The TCP port that we care about
            ``stream_handler``  The callback for each stream
            ``offline``         Path to a pcap file
            ``ip``              A list of IPs that we care about
        """
        super(Sniffer, self).__init__()
        self.setDaemon(True)

        self._iface = iface
        self._port = port
        self._offline = offline
        self._ip = ip if ip else []
        self._queue = deque()  # TODO: maxlen?
        self._dispatcher = Dispatcher(self._queue)

        self._dispatcher.add_handler(stream_handler)

        self._wants_stop = False

        self.start()

    @property
    def dispatcher(self):
        return self._dispatcher

    @property
    def pending_ip_packets(self):
        return len(self._queue)

    def add_handler(self, stream_handler):
        self._dispatcher.add_handler(stream_handler)

    def run(self):
        pfilter = 'port %d' % self._port
        try:
            kwargs = {
                'filter': pfilter,
                'store': 0,
                'prn': self._handle_packet,
                'iface': self._iface,
                'stop_filter': lambda p: self._wants_stop,
                }

            if self._offline:
                kwargs['offline'] = self._offline

            sniff(**kwargs)
        except Exception as ex:
            if 'Not a pcap capture file' in str(ex):
                print('%s is not a valid pcap file' % self._offline)
                return
            print('Error: %s: %s (device: %s)' % (ex, traceback.format_exc(), self._iface))
        finally:
            if self._offline:
                # drain dispatcher
                while not self._dispatcher.empty:
                    time.sleep(0.1)

    def stop(self, wait_for_stopped=False):
        if not self.isAlive():
            return

        self._wants_stop = True

        if wait_for_stopped:
            while self.isAlive():
                time.sleep(0.01)

    def _handle_packet(self, packet):
        try:
            ip_p = get_ip_packet(packet.load, 0, self._port)
        except ValueError:
            return

        ip_data = getattr(ip_p, 'data', None)
        if ip_data is None:
            return

        if ip_data.sport != self._port and ip_data.dport != self._port:
            return

        if self._ip:
            src_ip = get_ip(ip_p, ip_p.src)
            dst_ip = get_ip(ip_p, ip_p.dst)

            if src_ip not in self._ip and dst_ip not in self._ip:
                return

        self._queue.append((packet.time, ip_p))
