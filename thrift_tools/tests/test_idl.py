import unittest

import ptsd.ast
from thrift_tools import idl

from .util import get_thrift_path


class IdlTestCase(unittest.TestCase):
    maxDiff = None

    def test_parse_idl_file(self):
        parsed = idl.parse_idl_file(get_thrift_path("tutorial"))

        self.assertEqual(
            parsed.functions,
            [
                idl.Function(
                    name="getStruct",
                    arguments=[
                        idl.Field(
                            is_required=True, name="key", tag=1, type=ptsd.ast.I32
                        )
                    ],
                    type=idl.Struct(
                        fields=[
                            idl.Field(
                                is_required=False, name="key", tag=1, type=ptsd.ast.I32
                            ),
                            idl.Field(
                                is_required=False,
                                name="value",
                                tag=2,
                                type=ptsd.ast.String,
                            ),
                        ],
                        name="SharedStruct",
                    ),
                    throws=[],
                ),
                idl.Function(name="ping", type=idl.Void(), arguments=[], throws=[]),
                idl.Function(
                    name="add",
                    arguments=[
                        idl.Field(
                            is_required=True, name="num1", tag=1, type=ptsd.ast.I32
                        ),
                        idl.Field(
                            is_required=True, name="num2", tag=2, type=ptsd.ast.I32
                        ),
                    ],
                    type=ptsd.ast.I32,
                    throws=[],
                ),
                idl.Function(
                    name="calculate",
                    arguments=[
                        idl.Field(
                            is_required=True, name="logid", tag=1, type=ptsd.ast.I32
                        ),
                        idl.Field(
                            is_required=True,
                            name="w",
                            tag=2,
                            type=idl.Struct(
                                fields=[
                                    idl.Field(
                                        is_required=False,
                                        name="num1",
                                        tag=1,
                                        type=ptsd.ast.I32,
                                    ),
                                    idl.Field(
                                        is_required=False,
                                        name="num2",
                                        tag=2,
                                        type=ptsd.ast.I32,
                                    ),
                                    idl.Field(
                                        is_required=False,
                                        name="op",
                                        tag=3,
                                        type=idl.Enum(
                                            name="Operation",
                                            values=[
                                                ("ADD", 1),
                                                ("SUBTRACT", 2),
                                                ("MULTIPLY", 3),
                                                ("DIVIDE", 4),
                                            ],
                                        ),
                                    ),
                                    idl.Field(
                                        is_required=False,
                                        name="comment",
                                        tag=4,
                                        type=ptsd.ast.String,
                                    ),
                                ],
                                name="Work",
                            ),
                        ),
                    ],
                    type=ptsd.ast.I32,
                    throws=[
                        idl.Field(
                            is_required=False,
                            name="ouch",
                            tag=1,
                            type=idl.Exc(
                                name="InvalidOperation",
                                fields=[
                                    idl.Field(
                                        is_required=False,
                                        name="whatOp",
                                        tag=1,
                                        type=ptsd.ast.I32,
                                    ),
                                    idl.Field(
                                        is_required=False,
                                        name="why",
                                        tag=2,
                                        type=ptsd.ast.String,
                                    ),
                                ],
                            ),
                        )
                    ],
                ),
                idl.Function(name="zip", arguments=[], type=idl.Void(), throws=[]),
            ],
        )
