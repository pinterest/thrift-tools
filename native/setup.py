from distutils.core import setup, Extension

module2 = Extension('thrift_struct_read_module', 
                    sources = ['thrift_struct_read_module.c'])

setup (name = 'thrift_struct_read_module',
       version = '1.0',
       description = 'This is the port from the read function in thrift_struct',
       ext_modules = [module2]
       )
module3 = Extension('thrift_message_read_module',
                    sources = ['thrift_message_read_module.c','thrift_utils.c','thrift_struct_read_module.c'])

setup (name = 'thrift_message_read_module',
       version = '1.0',
       description = 'This is the port from the read function in thrift_message',
       ext_modules = [module3]
       )