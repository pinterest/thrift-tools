import unittest

from thrift_tools.thrift_diff import ThriftDiff as Diff
from thrift_tools.thrift_message import ThriftMessage
from thrift_tools.thrift_struct import ThriftStruct, ThriftField


class ThriftDiffTestCase(unittest.TestCase):
    def test_is_diff_compatible(self):
        def _diff_compatibility(msg1, msg2, allowed):
            ok_to_diff, reason = Diff.can_diff(msg1, msg2)
            if allowed:
                self.assertTrue(ok_to_diff, reason)
            else:
                self.assertFalse(ok_to_diff)
                self.assertIsNotNone(reason)

        # Method name doesn't match
        rpc1 = ThriftMessage("ping", None, None, ThriftStruct([]))
        rpc2 = ThriftMessage("pong", None, None, ThriftStruct([]))
        _diff_compatibility(rpc1, rpc2, False)

        # Method names match, so does argument signature
        rpc1 = ThriftMessage("ping", None, None,
                             ThriftStruct([ThriftField("string", 1, "a")]))
        rpc2 = ThriftMessage("ping", None, None,
                             ThriftStruct([ThriftField("string", 1, "b")]))
        _diff_compatibility(rpc1, rpc2, True)

        # Method names match, but argument signature is different
        rpc1 = ThriftMessage("ping", None, None,
                             ThriftStruct([ThriftField("string", 1, "a")]))
        rpc2 = ThriftMessage("ping", None, None,
                             ThriftStruct([ThriftField("i32", 1, "b")]))
        _diff_compatibility(rpc1, rpc2, False)

    def test_diff_of_structs(self):
        f1 = ThriftField("string", 1, "one")
        f2 = ThriftField("i32", 2, 2)
        f3 = ThriftField("bool", 3, True)
        f4 = ThriftField("i32", 2, 4)
        s1 = ThriftStruct([f1, f2, f3])
        s2 = ThriftStruct([f1, f4])

        t_diff = Diff.of_structs(s1, s2)
        self.assertListEqual([(f1, f1), (f2, f4)], t_diff.common_fields)
        self.assertListEqual([f1], t_diff.fields_with_same_value)
        self.assertEqual(1, len(t_diff._fields_with_different_value))
        self.assertListEqual([f3], t_diff.fields_only_in_a)
        self.assertTrue(len(t_diff.fields_only_in_b) == 0)

    def test_diff_of_messages(self):
        f1 = ThriftField("string", 1, "one")
        f2 = ThriftField("i32", 2, 2)
        s1 = ThriftStruct([f1])
        s2 = ThriftStruct([f2])
        m1 = ThriftMessage("ping", "call", 1, s1)
        m2 = ThriftMessage("ping", "call", 2, s2)
        with self.assertRaisesRegexp(ValueError, 'argument signature'):
            Diff.of_messages(m1, m2)

        # Diff messages with all primitive args
        self.assertEqual(0, len(Diff.of_messages(m1, m1)),
                         "Message based diff only consider args of type struct")

        # Diff messages with mixed type of args
        f3 = ThriftField("struct", 1, ThriftStruct([f1, f2]))
        m3 = ThriftMessage("ping", "call", 2, ThriftStruct([f1, f2, f3, f3]))
        diffs = Diff.of_messages(m3, m3)
        self.assertEqual(2, len(diffs))

        # We diffed identical messages, verify that diff reflects that
        self.assertListEqual([f1, f2], diffs[0].fields_with_same_value)
        self.assertEqual(0, len(diffs[0].field_with_different_value))


