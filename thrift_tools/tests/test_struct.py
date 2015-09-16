import unittest
from thrift_tools.thrift_struct import ThriftField, ThriftStruct


class ThriftStructTestCase(unittest.TestCase):
    """ Unit tests for ThriftStruct"""

    def setUp(self):
        self.monday = ThriftField("string", 1, "monday")
        self.tuesday = ThriftField("string", 1, "tuesday")
        self.todo_1 = ThriftField("string", 2, "run")
        self.todo_2 = ThriftField("string", 2, "swim")
        self.weekday = ThriftField("bool", 1, True)
        self.calendar_1 = ThriftField("struct", 1,
                                      ThriftStruct((self.monday, self.todo_1)))
        self.calendar_2 = ThriftField("struct", 1,
                                      ThriftStruct((self.tuesday, self.todo_2)))
        self.calendar_3 = ThriftField("struct", 1,
                                      ThriftStruct((self.monday, self.todo_1)))
        self.calendar_4 = ThriftField("struct", 2,
                                      ThriftStruct((self.monday, self.todo_1)))

    def tearDown(self):
        pass

    def test_is_isomorphic_to(self):
        struct_1 = self.calendar_1.value
        struct_2 = self.calendar_2.value
        struct_3 = ThriftStruct([self.monday])
        self.assertTrue(struct_1.is_isomorphic_to(struct_2))
        self.assertFalse(struct_1.is_isomorphic_to(struct_3))


class ThriftFieldTestCase(ThriftStructTestCase):
    """ Unit tests for ThriftField"""

    def test_is_isomorphic_to(self):
        # Test primitive fields
        self.assertTrue(self.monday.is_isomorphic_to(self.tuesday))
        self.assertFalse(self.monday.is_isomorphic_to(self.todo_1))
        self.assertFalse(self.monday.is_isomorphic_to(self.weekday))
        # Tests struct fields
        self.assertTrue(self.calendar_1.is_isomorphic_to(self.calendar_2))
        self.assertFalse(self.calendar_1.is_isomorphic_to(self.monday))

    def test_eq(self):
        self.assertFalse(self.monday == self.tuesday)
        self.assertFalse(self.monday == self.tuesday)
        self.assertTrue(self.calendar_1 == self.calendar_3)
        self.assertFalse(self.calendar_1 == self.calendar_4)