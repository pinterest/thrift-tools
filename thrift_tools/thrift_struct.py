from thrift.Thrift import TType


class ThriftStruct(object):
    """A thrift struct"""

    def __init__(self, fields):
        self._fields = fields

    @property
    def fields(self):
        return self._fields

    def is_isomorphic_to(self, other):
        """
        Returns true if all fields of other struct are isomorphic to this
        struct's fields
        """
        return (isinstance(other, self.__class__)
                and
                len(self.fields) == len(other.fields)
                and
                all(a.is_isomorphic_to(b) for a, b in zip(self.fields,
                                                          other.fields)))

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.__dict__ == other.__dict__)

    def __len__(self):
        return len(self._fields)

    def __getitem__(self, key):
        return self._fields[key]

    def __iter__(self):
        return iter(self._fields)

    def __repr__(self):
        return "fields=%s" % self.fields

    class ObjectTooBig(Exception):
        pass

    @classmethod
    def read(cls,
             proto,
             max_fields,
             max_list_size,
             max_map_size,
             max_set_size,
             read_values=False):
        fields = []
        nfields = 0
        proto.readStructBegin()
        while True:
            nfields += 1
            if nfields >= max_fields:
                raise cls.ObjectTooBig('too many fields: %d' % nfields)

            _, ftype, fid = proto.readFieldBegin()
            if ftype == TType.STOP:
                break

            value = cls.read_field_value(
                proto, ftype,
                max_fields,
                max_list_size,
                max_map_size,
                max_set_size,
                read_values)

            proto.readFieldEnd()

            fields.append(ThriftField(cls.field_type_to_str(ftype), fid, value))
        proto.readStructEnd()
        return cls(fields)

    @classmethod
    def read_field_value(cls, proto, ftype,
                         max_fields,
                         max_list_size,
                         max_map_size,
                         max_set_size,
                         read_values):
        value = None

        def _read(_type):
            return cls.read_field_value(
                proto,
                _type,
                max_fields,
                max_list_size,
                max_map_size,
                max_set_size,
                read_values)

        # ##
        # There's a lot going on here:
        #
        # * for known scalar types, we check if we want the value or we skip
        # * for known collections, ditto but with sane size/limit checks
        #  * for the rest we skip
        #
        # Touching a line here should warrant writing another test case :-)
        #
        # FIXME: the way bytes are skipped is very lame, we should calculate
        #        the total number of bytes that are to be skipped, and have
        #        the transport seek() to that point.

        if ftype == TType.STRUCT:
            value = cls.read(
                proto,
                max_fields,
                max_list_size,
                max_map_size,
                max_set_size,
                read_values
            )
        elif ftype == TType.I32:
            if read_values:
                value = proto.readI32()
            else:
                proto.skip(ftype)
        elif ftype == TType.I64:
            if read_values:
                value = proto.readI64()
            else:
                proto.skip(ftype)
        elif ftype == TType.STRING:
            if read_values:
                value = proto.readString()
            else:
                proto.skip(ftype)
        elif ftype == TType.LIST:
            (etype, size) = proto.readListBegin()
            if size > max_list_size:
                raise cls.ObjectTooBig('list too long: %d' % size)
            value = []
            if read_values:
                value = [_read(etype) for _ in xrange(size)]
            else:
                for i in xrange(size):
                    proto.skip(etype)
            proto.readListEnd()
        elif ftype == TType.MAP:
            (ktype, vtype, size) = proto.readMapBegin()
            if size > max_map_size:
                raise cls.ObjectTooBig('map too big: %d' % size)
            value = {}
            if read_values:
                for i in xrange(size):
                    k = _read(ktype)
                    v = _read(vtype)
                    value[k] = v
            else:
                for i in xrange(size):
                    proto.skip(ktype)
                    proto.skip(vtype)
            proto.readMapEnd()
        elif ftype == TType.SET:
            (etype, size) = proto.readSetBegin()
            if size > max_set_size:
                raise cls.ObjectTooBig('set too big: %d' % size)
            value = set()
            if read_values:
                for i in xrange(size):
                    value.add(_read(etype))
            else:
                for i in xrange(size):
                    proto.skip(etype)
            proto.readSetEnd()
        else:
            # for now, we ignore all other values
            proto.skip(ftype)

        return value

    @staticmethod
    def field_type_to_str(ftype):
        if ftype == TType.STOP:
            return 'stop'
        elif ftype == TType.VOID:
            return 'void'
        elif ftype == TType.BOOL:
            return 'bool'
        elif ftype == TType.BYTE:
            return 'byte'
        elif ftype == TType.I08:
            return 'i08'
        elif ftype == TType.DOUBLE:
            return 'double'
        elif ftype == TType.I16:
            return 'i16'
        elif ftype == TType.I32:
            return 'i32'
        elif ftype == TType.I64:
            return 'i64'
        elif ftype == TType.STRING:
            return 'string'
        elif ftype == TType.UTF7:
            return 'utf7'
        elif ftype == TType.STRUCT:
            return 'struct'
        elif ftype == TType.MAP:
            return 'map'
        elif ftype == TType.SET:
            return 'set'
        elif ftype == TType.LIST:
            return 'list'
        elif ftype == TType.UTF8:
            return 'utf8'
        elif ftype == TType.UTF16:
            return 'utf16'
        else:
            raise ValueError('Unknown type: %s' % ftype)


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

    def __repr__(self):
        return '(%s, %s, %s)' % (
            self.field_type, self.field_id, self._value)

    def is_isomorphic_to(self, other):
        """
        Returns true if other field's meta data (everything except value)
        is same as this one
        """
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