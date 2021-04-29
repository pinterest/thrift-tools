#!/usr/bin/env python3

"""
This script calls the ping method for a Thrift service in the given
host and port. By default, it uses the TBinary protocol.

```
$ examples/ping_service.py 127.0.0.1 9090
Received: method=ping, type=reply, seqid=0, header=None, fields=fields=[(string, 0, None)]
...

"""

import argparse
import os
import socket
import sys

if os.getenv('FROM_SOURCE') is not None:
    sys.path.insert(0,  '..')
    sys.path.insert(0, '.')

from thrift_tools.thrift_message import ThriftMessage


def ping(host, port, protocol_class):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))

        # Send request
        data = ThriftMessage.ping()
        s.sendall(data)

        # Read response
        data = s.recv(8*1024)
        # We skip the frame length
        data = data[4:]
        msg, _ = ThriftMessage.read(data, protocol=protocol_class)
        print('Received: %s' % msg)


def get_flags():
    p = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('host', type=str, help='The destination host')
    p.add_argument('port', type=int, help='The destination port')
    p.add_argument('--protocol', type=str, default='binary',
                   help='Protocol to use')

    return p.parse_args()


if __name__ == '__main__':
    flags = get_flags()
    proto = ThriftMessage.protocol_str_to_class(flags.protocol)
    ping(flags.host, flags.port, proto)
