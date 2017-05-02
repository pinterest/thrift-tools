#include "thrift_utils.h"
#include "thrift_consts.h"

int
dataLen(char *data)
{
    int i;
    int count;

    i = 0;
    count = 0;
    while (data[i] != '\0') {
        if (data[i] == '\\' && data[i+1] == 'x') {
            count++;
            i = i+4;
        }
        else if (data[i] == '\\') {
            count++;
            i = i+2;
        }
        else {
            count++;
            i++;
        }
    }
    return count;
}

char *const
detect_protocol(char *data, char *const fallbackProtocol)
{
    if (is_compact_protocol(data)) {
        return "TCompactProtocol";
    }
    if (is_binary_protocol(data)) {
        return "TBinaryProtocol";
    }
    if (is_json_protocol(data)) {
        return "TJSONProtocol";
    }
    if (fallbackProtocol == NULL) {
        return "unknownProtocol";
    }
    return fallbackProtocol;
}

int
is_compact_protocol(char *data)
{
    char aux[5];
    if (strlen(data) < 1) {
        return 0;
    }
    if (strlen(data) >= 4) {
        aux[0] = '0';
        aux[1] = 'x';
        aux[2] = data[2];
        aux[3] = data[3];
        aux[4] = '\0';
        if (data[0] == '\\' && data[1] == 'x') {
            return !(strcmp(aux, "0x82"));
        }
    }
    return (data[0] == strtol("0x82", NULL, 16))?1:0;
}

int
is_binary_protocol(char *data)
{
    int val;
    int version;

    if (strlen(data) < 1) {
        return 0;
    }
    val = 0;
    if (strlen(data) >= 4) {
        val = data[0]*pow(2,24)+data[1]*pow(2,16)+data[2]*pow(2,8)+data[3];
    }
    if (val >= 0) {
        return 0;
    }
    
    version = val & BINARY_PROTOCOL_VERSION_MASK;
    return (version == BINARY_PROTOCOL_VERSION_1)?1:0;
}

int
is_json_protocol(char *data)
{
    if (strlen(data) < 2) {
        return 0;
    }
    if (data[0] == '[' && data[1] == '1') {
        return 1;
    }
    return 0;
}
