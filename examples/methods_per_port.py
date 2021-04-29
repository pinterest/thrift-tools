#!/usr/bin/env python

"""
This script discovers all the listening ports and then enumerates the method
calls it sees on each port.

Make sure you have the library installed:

```
$ pip install thrift-tools
```

And then run it as root:

```
$ sudo examples/methods_per_port.py
On port 3030, method rewrite was called
On port 3031, method search was called
...

```

"""

from __future__ import print_function

import argparse
import os
import sys

from thrift_tools.message_sniffer import MessageSnifferOptions, MessageSniffer


PROC_TCP = '/proc/net/tcp'


def listening_ports():
    """ Reads listening ports from /proc/net/tcp """
    ports = []

    if not os.path.exists(PROC_TCP):
        return ports

    with open(PROC_TCP) as fh:
        for line in fh:
            if '00000000:0000' not in line:
                continue
            parts = line.lstrip(' ').split(' ')
            if parts[2] != '00000000:0000':
                continue

            local_port = parts[1].split(':')[1]
            local_port = int('0x' + local_port, base=16)
            ports.append(local_port)

    return ports


def discover_on_port(port, iface, handler):
    options = MessageSnifferOptions(
        iface=iface,
        port=port,
        ip=None,
        pcap_file=None,
        protocol=None,
        finagle_thrift=False,
        read_values=False,
        max_queued=20000,
        max_message_size=2000,
        debug=False)

    return MessageSniffer(options, handler)


class MsgHandler(object):
    def __init__(self, port):
        self._methods = set()
        self._port = port

    def __call__(self, timestamp, src, dst, msg):
        if msg.method in self._methods:
            return True

        self._methods.add(msg.method)
        print('On port %d, method %s was called' % (self._port, msg.method))

        return True  # must return true, or sniffer will exit


def discover_methods(iface):
    sniffers = []
    for port in listening_ports():
        sniff = discover_on_port(port, iface, MsgHandler(port))
        sniffers.append(sniff)

    # done when all sniffers are done (or the user gets tired)
    try:
        while True:
            if all(not sniff.is_alive() for sniff in sniffers):
                break
    except KeyboardInterrupt:
        pass


def get_flags():
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--iface', type=str, default='eth0', metavar='<iface>',
                   help='The interface to sniff from')

    return p.parse_args()


if __name__ == '__main__':
    if os.getuid() != 0:
        print('Must be root (or have CAP_NET_ADMIN)')
        sys.exit(1)

    flags = get_flags()
    discover_methods(flags.iface)
