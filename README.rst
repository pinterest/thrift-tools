thrift-tools |Build Status| |Coverage Status| |PyPI version|
============================================================

**Table of Contents**

-  `tl;dr <#tldr>`__
-  `Installing <#installing>`__
-  `Tools <#tools>`__
-  `Library <#library>`__
-  `Tests <#tests>`__

tl;dr
~~~~~

thrift-tools is a library and a set of tools to introspect `Apache
Thrift <https://thrift.apache.org/>`__ traffic.

Installing
~~~~~~~~~~

You can install thrift-tools via pip:

::

    $ pip install thrift-tools

Or run it from source (if you have the dependencies installed, see
below):

::

    $ git clone ...
    $ cd thrift-tools
    $ sudo FROM_SOURCE=1 bin/thrift-tool --iface=eth0 --port 9091 dump --show-all --pretty
    ...
    $ FROM_SOURCE=1 bin/thrift-tool --port 9090 \
        --pcap-file thrift_tools/tests/resources/calc-service-binary.pcap \
        dump --show-all --pretty --color \
        --idl-file=thrift_tools/tests/resources/tutorial.thrift

Tools
~~~~~

thrift-tool can be used in interactive mode to analyze live thrift
messages:

::

    $ sudo thrift-tool --iface eth0 --port 9091 dump --show-all --pretty
    [00:39:42:850848] 10.1.8.7:49858 -> 10.1.2.20:3636: method=dosomething, type=call, seqid=1120
    header: ()
    fields: [   (   'struct',
            1,
            [   ('string', 1, 'something to do'),
                ('i32', 3, 0),
                (   'struct',
                    9,
                    [   ('i32', 3, 2),
                        ('i32', 14, 0),
                        ('i32', 16, 0),
                        ('i32', 18, 25)])])]
    ------>[00:39:42:856204] 10.1.2.20:3636 -> 10.1.8.7:49858: method=dosomething, type=reply, seqid=1120
            header: ()
            fields: [   (   'struct',
            0,
            [   ('string', 1, 'did something'),
                ('string', 2, 'did something else'),
                ('string', 3, 'did some other thing'),
                ('string', 4, 'did the last thing'),
                ('i32', 6, 3),
                ('i32', 7, 11),
                ('i32', 8, 0),
                ('i32', 9, 0),
                ('list', 10, [0]),
    ...

Alternatively, offline pcap files may be introspected:

::

    $ sudo thrift-tool --port 9091 --pcap-file /path/to/myservice.pcap dump
    ...

Note that you still need to set the right port.

If you are using `Finagle <https://twitter.github.io/finagle/>`__, try
something like:

::

    $ sudo thrift-tool --iface eth0 --port 9091 dump --show-all --pretty --finagle-thrift
    ...

JSON output is available for easy filtering & querying via jq. For
instance, you can enumerate all the IPs calling the method 'search' via:

::

    $ sudo thrift-tool --port 3030 dump --unpaired --json | jq 'select(.method == "search" and .type == "call") | .src'
    "10.1.18.5:48534"
    "10.1.60.2:52008"
    "10.1.10.27:49856"
    "10.1.23.24:48116"
    "10.1.26.7:60462"
    "10.1.11.10:41895"
    "10.1.15.13:35285"
    "10.1.7.17:39759"
    "10.1.1.19:35481"
    ...

Gathering per method latency stats is available via the ``stats``
command:

::

    $ sudo thrift-tool --port 6666 stats --count 100
    method      count         avg         min         max         p90         p95         p99        p999
    --------  -------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
    search2        61  0.00860996  0.00636292  0.0188479   0.010778    0.015192    0.0174422   0.0187074
    doc            39  0.00134846  0.00099802  0.00274897  0.00177183  0.00199242  0.00256242  0.00273031
    287 unmatched calls

You can also specify .thrift file for nicer output:

::

    $ sudo thrift-tool --port 9091 dump --show-all --pretty --color --idl-file /path/to/myidl.thrift
    ...

To list all the available options:

::

    $ thrift-tool --help

Note that for servers with high throughput (i.e.: > couple Ks packets
per second), it might be hard for thrift-tools to keep up because start
of message detection is a bit expensive (and you can only go so fast
with Python). For these cases, you are better off saving a pcap file
(i.e.: via tcpdump) and then post-processing it, i.e.:

::

    $ tcpdump -nn -t port 3030 -w dump.pcap
    $ sudo thrift-tool --port 3030 --pcap-file dump.pcap stats --count 40000
    method      count         avg         min         max         p90         p95         p99        p999
    --------  -------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
    resize      40000  0.00850996  0.00336091  0.0101364   0.008071    0.009132    0.009890   0.01005665

Library
~~~~~~~

To use thrift-tools from another (Python) application, you can import it
via:

::

    from thrift_tools.message_sniffer import MessageSnifferOptions, MessageSniffer

    options = MessageSnifferOptions(
        iface='eth0',
        port='3636',
        ip=None,                         # include msgs from all IPs
        pcap_file=None,                  # don't read from a pcap file, live sniff
        protocol=None,                   # auto detect protocol
        finagle_thrift=False,            # apache thrift (not twitter's finagle)
        read_values=True,                # read the values of each msg/struct
        max_queued=20000,                # decent sized queue
        max_message_size=2000,           # 2k messages to keep mem usage frugal
        debug=False                      # don't print parsing errors, etc
        )

    def printer(timestamp, src, dst, msg):
      print '%s %s %s %s' % (timestamp, src, dst, msg)

    message_sniffer = MessageSniffer(options, printer)

    # loop forever
    message_sniffer.join()

Of if you want to use a pcap file:

::

    options = MessageSnifferOptions(
        iface='eth0',
        port='3636',
        ip=None,
        pcap_file="/tmp/myservice.pcap",
        protocol=None,
        finagle_thrift=False,
        read_values=True,
        max_queued=20000,
        max_message_size=2000,
        debug=False
        )

    ...

If you want to filter messages for specific IPs:

::

    options = MessageSnifferOptions(
        iface='eth0',
        port='3636',
        ip=['172.16.24.3', '172.16.24.4'],  # ignores everyone else
        pcap_file="/tmp/myservice.pcap",
        protocol=None,
        finagle_thrift=False,
        read_values=True,
        max_queued=20000,
        max_message_size=2000,
        debug=False
        )

    ...

See examples/ for more ways to use this library!

Tests
~~~~~

To run the tests:

::

    $ python setup.py nosetests

.. |Build Status| image:: https://travis-ci.org/pinterest/thrift-tools.svg?branch=master
   :target: https://travis-ci.org/pinterest/thrift-tools
.. |Coverage Status| image:: https://coveralls.io/repos/pinterest/thrift-tools/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/pinterest/thrift-tools?branch=master
.. |PyPI version| image:: https://badge.fury.io/py/thrift-tools.svg
   :target: http://badge.fury.io/py/thrift-tools
