from collections import defaultdict, deque, namedtuple
from datetime import datetime

import json
import pprint
import sys
import time

from tabulate import tabulate

import colors

from .stats import percentile


fromtimestamp = datetime.fromtimestamp


FormatOptions = namedtuple('FormatOptions', [
    'pretty_printer',
    'is_color',
    'show_header',
    'show_fields',
    'json',
])


def print_color(s, color_id, output=sys.stdout):
    attr = colors.COLORS[color_id % len(colors.COLORS)]
    cfunc = getattr(colors, attr)
    output.write(cfunc(s))
    output.flush()


def print_msg(timestamp, src, dst, msg, format_opts,
              prefix='', indent=0, output=sys.stdout):
    timestr = fromtimestamp(timestamp).strftime('%H:%M:%S:%f')

    def pretty(part):
        if format_opts.pretty_printer:
            pp = pprint.PrettyPrinter(indent=4)
            part = pp.pformat(part)
        return part

    indent = ' ' * indent if indent else ''

    if format_opts.show_header:
        header_line = '%sheader: %s\n' % (indent, pretty(msg.header))
    else:
        header_line = ''

    if format_opts.show_fields:
        fields_line = '%sfields: %s\n' % (indent, pretty(msg.args))
    else:
        fields_line = ''

    if format_opts.json:
        parts = {
            'time': timestr,
            'src': src,
            'dst': dst,
            'method': msg.method,
            'type': msg.type,
            'seqid': msg.seqid,
            }

        if format_opts.show_header:
            parts['header'] = msg.header

        if format_opts.show_fields:
            parts['fields'] = msg.args

        outputstr = json.dumps(parts, indent=4) + '\n'
    else:
        outputstr = '%s[%s] %s -> %s: method=%s, type=%s, seqid=%d\n%s%s' % (
            prefix, timestr, src, dst, msg.method, msg.type, msg.seqid,
            header_line, fields_line)

    if format_opts.is_color:
        print_color(outputstr, src.__hash__())
    else:
        output.write(outputstr)
        output.flush()


class Printer(object):
    """ A simple message printer """

    def __init__(self, format_opts, output=sys.stdout):
        self._format_opts = format_opts
        self._output = output

    def __call__(self, timestamp, src, dst, msg):
        print_msg(timestamp, src, dst, msg, self._format_opts,
                  output=self._output)
        return True  # keep the sniffer running


class PairedPrinter(object):
    """ Pairs each request with its reply """
    def __init__(self, format_opts, output=sys.stdout):
        self._format_opts = format_opts
        self._output = output

        # msgs by [src][dst][method_name]
        self._requests = defaultdict(
            lambda: defaultdict(lambda: defaultdict(deque)))
        # ditto
        self._replies = defaultdict(
            lambda: defaultdict(lambda: defaultdict(deque)))

    def __call__(self, timestamp, src, dst, msg):
        """
        We need to match up each (request, reply) pair. Presumably,
        pcap _shouldn't_ deliver packets out of order, but
        things could get mixed up somewhere withing the
        TCP stream being reassembled and the StreamHandler
        thread. So, we don't assume that a 'reply' implies
        the corresponding 'call' has been seen.

        It could also be that we started sniffing after
        the 'call' message... but there's no easy way to tell
        (given we don't keep the startup time around...)
        """
        if msg.type == 'call':
            replies = self._replies[dst][src][msg.method]
            if len(replies) > 0:
                reply_timestamp, reply = replies.popleft()
                self._print_pair(
                    timestamp, msg, reply_timestamp, reply, src, dst)
            else:
                self._requests[src][dst][msg.method].append(
                    (timestamp, msg))
        elif msg.type == 'reply':
            requests = self._requests[dst][src][msg.method]
            if len(requests) > 0:
                request_timestamp, request = requests.popleft()
                self._print_pair(
                    request_timestamp, request, timestamp, msg,
                    dst, src)
            else:
                self._replies[src][dst][msg.method].append(
                    (timestamp, msg))
        else:
            print_msg(timestamp, src, dst, msg, self._format_opts,
                      output=self._output)

        return True  # keep the sniffer running

    def _print_pair(self, reqtime, request, reptime, reply, src, dst):
        print_msg(reqtime, src, dst, request, self._format_opts,
                  output=self._output)
        print_msg(reptime, dst, src, reply, self._format_opts,
                  prefix='------>', indent=8, output=self._output)


class LatencyPrinter(object):
    """ Reports latencies for the seen (req, rep) pairs """
    def __init__(self, expected_calls, output=sys.stdout):
         # msgs by [src][dst][method_name][seqid]
        self._requests = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict)))

        self._expected = expected_calls
        self._output = output

        self._seen = 0
        self._pairs = []
        self._latencies_by_method = defaultdict(list)

    def __call__(self, timestamp, src, dst, msg):
        """
        Slightly simplified logic wrt what PairedPrinter has:
        we assume 'call' messages will be seen before their
        corresponding 'reply'.
        """
        if msg.type == 'call':
            self._requests[src][dst][msg.method][msg.seqid] = (timestamp, msg)
        elif msg.type == 'reply':
            # have we seen the corresponding call?
            calls = self._requests[dst][src][msg.method]
            if msg.seqid in calls:
                request_timestamp, request = calls.pop(msg.seqid)
                latency = timestamp - request_timestamp
                self._latencies_by_method[msg.method].append(latency)
                self._seen += 1

                # let the user know we are still working
                self._output.write('\rCollecting (%d/%d)' % (
                        self._seen, self._expected))
                self._output.flush()

        if self._seen < self._expected:
            return True  # we still need more messages

        self.report()

        # what about unmatched calls?
        unmatched = 0
        for src, dst in self._requests.items():
            for method, calls in dst.items():
                unmatched += len(calls)

        if unmatched > 0:
            self._output.write('%d unmatched calls\n' % unmatched)

        return False  # we are done

    def report(self):
        """ get stats & show them """
        self._output.write('\r')

        sort_by = 'avg'
        results = {}
        for key, latencies in self._latencies_by_method.items():
            result = {}
            result['count'] = len(latencies)
            result['avg'] = sum(latencies) / len(latencies)
            result['min'] = min(latencies)
            result['max'] = max(latencies)
            latencies = sorted(latencies)
            result['p90'] = percentile(latencies, 0.90)
            result['p95'] = percentile(latencies, 0.95)
            result['p99'] = percentile(latencies, 0.99)
            result['p999'] = percentile(latencies, 0.999)
            results[key] = result

        headers = ['method', 'count', 'avg', 'min', 'max', 'p90', 'p95', 'p99', 'p999']
        data = []
        results = sorted(results.items(), key=lambda it: it[1][sort_by], reverse=True)

        def row(key, res):
            data = [key] + [res[header] for header in headers[1:]]
            return tuple(data)

        data = [row(key, result) for key, result in results]

        self._output.write('%s\n' % tabulate(data, headers=headers))
        self._output.flush()
