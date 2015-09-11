
class ThriftStruct(object):
    """A thrift struct"""

    def __init__(self, fields):
        self._fields = fields

    @property
    def fields(self):
        return self._fields

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.__dict__ == other.__dict__)

    def __len__(self):
        return len(self._fields)

    def __getitem__(self, key):
        return self._fields[key]

    def __iter__(self):
        return iter(self._fields)


class ThriftField(object):
    """A thrift field"""

    def __init__(self, field_type, field_id, value):
        self._field_type = field_type
        self._field_id = field_id
        self._value = value

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return 'field_type=%s, field_id=%s, value=%s' % (
            self.field_type, self.field_id, self._value)

    def eq_meta(self, other):
        return (isinstance(other, self.__class__)
                and self.field_type == other.field_type
                and self.field_id == other.field_id)

    @property
    def field_type(self):
        return self._field_type

    @property
    def field_id(self):
        return self._field_id

    @property
    def value(self):
        return self._value