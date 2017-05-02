from thrift_message import ThriftMessage
import thrift_message_read_module

class ThriftMessageFactory(object):
    message_class = ThriftMessage

    @classmethod
    def use_native_implementation(cls):
        """ use C version"""
        cls.message_class = thrift_message_read_module

    @classmethod
    def use_python_implementation(cls):
        cls.message_class = ThriftMessage

    @classmethod
    def reader_class(cls):
        return cls.message_class

# use the C version reader                                                                                                                                                         ThriftMessageFactory.use_native_implementation() reader = ThriftMessageFactory.reader_class reader.read()                                                                                                                                                 