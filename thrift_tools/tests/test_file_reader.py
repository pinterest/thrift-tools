from collections import namedtuple
from StringIO import StringIO

import unittest

from thrift_tools.file_reader import run

from .util import get_log_path


PARAMS = [
    'file',
    'padding',
    'protocol',
    'structs',
    'finagle_thrift',
    'skip_values',
    'pretty',
    'max_messages',
    'debug',
    'show_holes'
]

PARAM_NAMES = ' '.join(PARAMS)


class Params(namedtuple('Params', PARAM_NAMES)):
    pass


class FileReaderTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_read_structs(self):
        params = Params(
            file=get_log_path('structs'),
            padding=0,
            protocol='binary',
            structs=True,
            finagle_thrift=False,  # ignored when structs
            skip_values=False,
            pretty=True,
            max_messages=0,
            debug=False,
            show_holes=False
        )

        output = StringIO()
        run(params, output)

        self.assertIn('Cannot divide by 0', output.getvalue())
        self.assertIn("'field_type': 'i32', 'value': 1", output.getvalue())

    def test_read_messages(self):
        params = Params(
            file=get_log_path('messages'),
            padding=0,
            protocol='binary',
            structs=False,
            finagle_thrift=False,
            skip_values=False,
            pretty=True,
            max_messages=0,
            debug=False,
            show_holes=False
        )

        output = StringIO()
        run(params, output)

        self.assertIn('ping', output.getvalue())
        self.assertIn('search', output.getvalue())
        self.assertIn('reply', output.getvalue())
