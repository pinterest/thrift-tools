#ifndef THRIFT_CONSTS_H
    #define THRIFT_CONSTS_H
    #define STOP 0
    #define VOID 1
    #define BOOL 2
    #define BYTE 3
    #define I08 3
    #define DOUBLE 4
    #define I16 6
    #define I32 8
    #define I64 10
    #define STRING 11
    #define UTF7 11
    #define STRUCT 12
    #define MAP 13
    #define SET 14
    #define LIST 15
    #define UTF8 16
    #define UTF16 17

    //JSON constants
    #define COMMA ','
    #define COLON ':'
    #define LBRACE '{'
    #define RBRACE '}'
    #define LBRACKET '['
    #define RBRACKET ']'
    #define QUOTE '"'
    #define BACKSLASH '\\'
    #define ZERO '0'
    #define PAIRCONTEXT 0
    #define LISTCONTEXT 1
    #define BASECONTEXT 2
    //Compact Protocol constants

    #define CLEAR 0
    #define FIELD_WRITE 1
    #define VALUE_WRITE 2
    #define CONTAINER_WRITE 3
    #define BOOL_WRITE 4
    #define FIELD_READ 5
    #define CONTAINER_READ 6
    #define VALUE_READ 7
    #define BOOL_READ 8
    #define COMPACT_PROTOCOL_ID 130
    #define COMPACT_VERSION 1
    #define COMPACT_VERSION_MASK 31
    #define TYPE_MASK 224
    #define TYPE_BITS 7
    #define TYPE_SHIFT_AMOUNT 5 
    #define COMPACT_TYPE_TRUE 1
    #define COMPACT_TYPE_FALSE 2

    #define BINARY_PROTOCOL_VERSION_MASK -65536
    #define BINARY_PROTOCOL_VERSION_1 -2147418112
    #define MIN_MESSAGE_SIZE 8
    #define BINARY_PROTOCOL_TYPE_MASK 255
    #define MAX_FIELDS 1000
    #define MAX_LIST_SIZE 10000
    #define MAX_MAP_SIZE 10000
    #define MAX_SET_SIZE 10000
    #define MAX_METHOD_LENGTH 70
    #define CALL 1
    #define REPLY 2
    #define EXCEPTION 3
    #define ONEWAY 4
    #define JSONVERSION 1
#endif