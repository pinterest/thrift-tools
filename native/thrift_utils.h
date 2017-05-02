#ifndef THRIFT_UTILS_H
    #define THRIFT_UTILS_H
    #include <stdlib.h>
    #include <string.h>
    #include <math.h>

    int is_compact_protocol(char*);
    int is_binary_protocol(char*);
    int is_json_protocol(char*);
    char *const detect_protocol(char*,char*);
    int dataLen(char*);
#endif