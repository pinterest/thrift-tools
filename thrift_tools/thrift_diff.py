"""
    Diff thrift structures & messages

    Examples:
        > while not finished:
                msg_a, _ = ThriftMessage.read(data_stream_a, read_values=True)
                msg_b, _ = ThriftMessage.read(data_stream_b, read_values=True)
                args_diff = ThriftDiff.of_messages(msg_a, msg_b)
                for diff in args_diff:
                    print diff.common_fields()
        [(ThriftField(field_type=str, field_id=1, value="hello world"),
            ThriftField(field_type=str, field_id=1, value="bye world"))]

        > t_diff = ThriftDiff.of_structs(struct_a, struct_b)
        > print t_diff.fields_with_same_value()
        ThriftField(field_type=str, field_id=1, value="hello world")
    """


class ThriftDiff:

    def __init__(self, struct_a, struct_b):
        self._a = struct_a
        self._b = struct_b
        self._common_fields = []
        self._fields_only_in_a = []
        self._fields_only_in_b = []
        self._fields_with_different_value = []
        self._fields_with_same_value = []

    @classmethod
    def of_structs(cls, a, b):
        """
        Diff two thrift structs and return the result as a ThriftDiff instance
        """
        t_diff = ThriftDiff(a, b)
        t_diff._do_diff()
        return t_diff

    @classmethod
    def of_messages(cls, msg_a, msg_b):
        """
        Diff two thrift messages by comparing their args, raises exceptions if
        for some reason the messages can't be diffed. Only args of type 'struct'
        are compared.

        Returns a list of ThriftDiff results - one for each struct arg
        """
        ok_to_diff, reason = cls.can_diff(msg_a, msg_b)
        if not ok_to_diff:
            raise ValueError(reason)
        return [cls.of_structs(x.value, y.value)
                for x, y in zip(msg_a.args, msg_b.args)
                if x.field_type == 'struct']

    @staticmethod
    def can_diff(msg_a, msg_b):
        """
        Check if two thrift messages are diff ready.

        Returns a tuple of (boolean, reason_string), i.e. (False, reason_string)
        if the messages can not be diffed along with the reason and
        (True, None) for the opposite case
        """
        if msg_a.method != msg_b.method:
            return False, 'method name of messages do not match'
        if len(msg_a.args) != len(msg_b.args) \
                or not msg_a.args.is_isomorphic_to(msg_b.args):
            return False, 'argument signature of methods do not match'
        return True, None

    def _do_diff(self):
        self._common_fields = [(x, y)
                               for x, y in zip(self._a.fields, self._b.fields)
                               if x.is_isomorphic_to(y)]
        self._fields_only_in_a = self._unique_fields(self._common_fields,
                                                     self._a.fields)
        self._fields_only_in_b = self._unique_fields(self._common_fields,
                                                     self._b.fields)
        # Go over the common fields and filter them into two sets, one for
        # the case when field values match and other for the ones where they
        # don't match
        for field_pair in self._common_fields:
            if field_pair[0] == field_pair[1]:
                # For common fields with same values, just record one of the
                # fields in the pair
                self._fields_with_same_value.append(field_pair[0])
            else:
                self._fields_with_different_value.append(field_pair)

    @staticmethod
    def _unique_fields(common_fields, all_fields):
        unique_fields = []
        for x in all_fields:
            is_unique = True
            for field_pair in common_fields:
                if x.is_isomorphic_to(field_pair[0]):
                    is_unique = False
                    break
            if is_unique:
                unique_fields.append(x)
        return unique_fields

    @property
    def common_fields(self):
        """
        List of isomorphically equivalent field pairs which may or may not have
        same value
        """
        return self._common_fields

    @property
    def fields_only_in_a(self):
        """
        List of fields exclusive to first struct
        """
        return self._fields_only_in_a

    @property
    def fields_only_in_b(self):
        """
        List of fields exclusive to second struct
        """
        return self._fields_only_in_b

    @property
    def fields_with_same_value(self):
        """
        List of isomorphically equivalent fields for which value is also equal
        Note: this doesn't return a list of 'pairs'
        """
        return self._fields_with_same_value

    @property
    def field_with_different_value(self):
        """
        List of isomorphically equivalent field pairs for which value is NOT
        equal
        """
        return self._fields_with_different_value