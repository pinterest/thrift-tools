import os

from ptsd import ast
from ptsd.parser import Parser


class IdlNode(object):
    def __repr__(self):
        return "%s(%s)" % (
            type(self).__name__,
            ", ".join("%s=%s" % item for item in vars(self).items()),
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False


class Type(IdlNode):
    def parse(self, value):
        pass


class Void(Type):
    pass


class TypeDef(Type):
    def __init__(self, name, type):
        self.name = name
        self.type = type


class Enum(Type):
    def __init__(self, name, values):
        self.name = name
        self.values = values
        self.names_by_tags = dict((value, name) for (name, value) in self.values)

    def parse(self, value):
        return self.names_by_tags[value]


class Field(IdlNode):
    def __init__(self, tag, name, is_required, type):
        self.tag = tag
        self.name = name
        self.is_required = is_required
        self.type = type


class Struct(Type):
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.fields_by_tags = dict((x.tag, x) for x in self.fields)

    def parse(self, fields):
        struct_fields = []

        fields_by_tag = dict((x.field_id, x) for x in fields)

        for field_def in self.fields:
            value = None

            field = fields_by_tag.get(field_def.tag)

            if field is not None:
                value = field.value

                if isinstance(field_def.type, Type):
                    value = field_def.type.parse(value)

            struct_fields.append((field_def.name, value))

        return (self.name, struct_fields)


class Exc(Struct):
    pass


class List(Type):
    def __init__(self, type):
        self.type = type

    def parse(self, values):
        list_values = []

        for value in values:
            if isinstance(self.type, Type):
                value = self.type.parse(value)

            list_values.append(value)

        return list_values


class Set(Type):
    def __init__(self, type):
        self.type = type

    def parse(self, values):
        set_values = set()

        for value in values:
            if isinstance(self.type, Type):
                value = self.type.parse(value)

            set_values.add(value)

        return set_values


class Map(Type):
    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type

    def parse(self, map):
        map_values = {}

        for key, value in map.items():
            if isinstance(self.key_type, Type):
                key = self.key_type.parse(key)
            if isinstance(self.value_type, Type):
                value = self.value_type.parse(value)

            map_values[key] = value

        return map_values


class Function(IdlNode):
    def __init__(self, name, arguments, type, throws):
        self.name = name
        self.arguments = arguments
        self.arguments_by_tags = dict((x.tag, x) for x in self.arguments)
        self.type = type
        self.throws = throws
        self.throws_by_tags = dict((x.tag, x) for x in self.throws)

    def get_args(self, msg):
        if msg.type == "call":
            args = []

            args_by_tag = dict((x.field_id, x) for x in msg.args)

            for arg in self.arguments:
                value = args_by_tag.get(arg.tag).value

                if isinstance(arg.type, Type):
                    value = arg.type.parse(value)

                args.append((arg.name, value))

            return args

        if msg.type == "reply":
            if msg.args:
                if msg.args[0].field_id == 0:  # return
                    value = msg.args[0].value

                    if isinstance(self.type, Type):
                        value = self.type.parse(value)

                    return value

                value = msg.args[0].value
                throw_type = self.throws_by_tags[msg.args[0].field_id].type

                if isinstance(throw_type, Type):
                    value = throw_type.parse(value)

                return value

        return msg.args


class Idl(object):
    def __init__(self, functions):
        self.functions = functions
        self.functions_by_name = dict((x.name, x) for x in self.functions)

    def get_function(self, name):
        return self.functions_by_name.get(name, None)


class IdlParser(object):
    def __init__(self):
        self.functions = []
        self.types_by_name = {}

    def parse_file(self, path):
        with open(path, "r") as thrift_file:
            tree = Parser().parse(thrift_file.read())

        for include in tree.includes:
            include_path = os.path.join(os.path.dirname(path), include.path.value)

            self.parse_file(include_path)

        self.parse_body(tree.body)

    def resolve_type(self, node_type):
        if isinstance(node_type, ast.Identifier):
            raw_type = node_type.value.split('.')[-1]
            resolved_type = self.types_by_name[raw_type]

            if isinstance(resolved_type, TypeDef):
                return self.resolve_type(resolved_type.type)

            return resolved_type
        if isinstance(node_type, ast.List):
            return List(type=self.resolve_type(node_type.value_type))
        if isinstance(node_type, ast.Set):
            return Set(type=self.resolve_type(node_type.value_type))
        if isinstance(node_type, ast.Map):
            return Map(
                key_type=self.resolve_type(node_type.key_type),
                value_type=self.resolve_type(node_type.value_type),
            )
        if node_type == "void":
            return Void()

        return type(node_type)

    def parse_typedef(self, node):
        typedef = TypeDef(name=node.name.value, type=self.resolve_type(node.type))

        self.types_by_name[typedef.name] = typedef

    def parse_enum(self, node):
        values = [(x.name.value, x.tag) for x in node.values]

        enum = Enum(name=node.name.value, values=values)

        self.types_by_name[enum.name] = enum

    def parse_struct(self, node):
        fields = []

        for field_ast in node.fields:
            field = Field(
                tag=field_ast.tag,
                name=field_ast.name.value,
                is_required=field_ast.required,
                type=self.resolve_type(field_ast.type),
            )

            fields.append(field)

        struct = Struct(name=node.name.value, fields=fields)

        self.types_by_name[struct.name] = struct

    def parse_exception(self, node):
        fields = []

        for field_ast in node.fields:
            field = Field(
                tag=field_ast.tag,
                name=field_ast.name.value,
                is_required=field_ast.required,
                type=self.resolve_type(field_ast.type),
            )

            fields.append(field)

        exc = Exc(name=node.name.value, fields=fields)

        self.types_by_name[exc.name] = exc

    def parse_function(self, node):
        arguments = []

        for argument_ast in node.arguments:
            argument = Field(
                tag=argument_ast.tag,
                name=argument_ast.name.value,
                is_required=True,
                type=self.resolve_type(argument_ast.type),
            )

            arguments.append(argument)

        throws = []

        for throw_ast in node.throws:
            throw = Field(
                tag=throw_ast.tag,
                name=throw_ast.name.value,
                is_required=False,
                type=self.resolve_type(throw_ast.type),
            )

            throws.append(throw)

        function = Function(
            name=node.name.value,
            arguments=arguments,
            type=self.resolve_type(node.type),
            throws=throws,
        )

        # TODO check for name colisions
        self.functions.append(function)

    def parse_service(self, node):
        for function in node.functions:
            self.parse_function(function)

    def parse_body(self, node):
        for body_part in node:
            if isinstance(body_part, ast.Typedef):
                self.parse_typedef(body_part)
            elif isinstance(body_part, ast.Enum):
                self.parse_enum(body_part)
            elif isinstance(body_part, ast.Struct):
                self.parse_struct(body_part)
            elif isinstance(body_part, ast.Exception_):
                self.parse_exception(body_part)
            elif isinstance(body_part, ast.Service):
                self.parse_service(body_part)


def parse_idl_file(path):
    idl_parser = IdlParser()
    idl_parser.parse_file(path)

    return Idl(functions=idl_parser.functions)
