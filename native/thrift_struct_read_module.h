#ifndef THRIFT_STRUCT_READ_MODULE_H
    #define THRIFT_STRUCT_READ_MODULE_H
    
    PyObject *thrift_struct_read(char*, char*, int, int, int, int, int);

    int readI32(char*, char*);
    char *readString(char*, char*);
    int readByteC(char*);
    char *readBinary(char*, char*);

    void readJSONArrayStart(char*);
    void readJSONArrayEnd(char*);
    int readJsonInteger(char*);
    char *readJSONString(char*, int);

    int readVarint(char*);
#endif