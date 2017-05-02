#include <Python.h>
#include <string.h>
#include <math.h>
#include "thrift_consts.h"


/*some structs to handle stacks needed in TCompactProtocol and TJSONProtocol*/

struct compactStructs
{
    int state;
    int last_fid;
};


/*stack manage*/
void
pushStructs(int newstate, int new_last_fid)
{
    PyObject *structsStack;
    PyObject *py_size;
    Py_ssize_t structsTop;
    PyObject *globalVars;

    globalVars = PyThreadState_GetDict();
    structsStack = PyDict_GetItemString(globalVars, "structsStack");
    py_size = PyDict_GetItemString(globalVars, "structsStackTop");
    if (!PyArg_Parse(py_size, "n", &structsTop)) {
            return ;
    }
    if (structsTop+1 == PyList_Size(structsStack)) {
        PyList_Append(structsStack, Py_BuildValue("(ii)", newstate, new_last_fid));
        structsTop++;            
    }
    else {
        structsTop++;
        PyList_SetItem(structsStack, structsTop, Py_BuildValue("(ii)", newstate, new_last_fid));
    }
    PyDict_SetItemString(globalVars,"structsStack",structsStack);
    PyDict_SetItemString(globalVars,"structsStackTop",Py_BuildValue("n", structsTop));

}

void
pushContainers(int newstate)
{
    PyObject *ContainersStack;
    PyObject *py_size;
    Py_ssize_t ContainersTop;
    PyObject *globalVars;

    globalVars = PyThreadState_GetDict();
    ContainersStack = PyDict_GetItemString(globalVars, "containersStack");
    py_size = PyDict_GetItemString(globalVars, "containersStackTop");
    if (!PyArg_Parse(py_size, "n", &ContainersTop)) {
            return ;
    }
    if (ContainersTop+1 == PyList_Size(ContainersStack)) {
        PyList_Append(ContainersStack, Py_BuildValue("(i)", newstate));
        ContainersTop++;            
    }
    else {
        ContainersTop++;
        PyList_SetItem(ContainersStack, ContainersTop, Py_BuildValue("(i)", newstate));
    }
    PyDict_SetItemString(globalVars, "containersStack", ContainersStack);
    PyDict_SetItemString(globalVars, "containersStackTop", Py_BuildValue("n", ContainersTop));}


struct compactStructs
popStructs(void)
{
    PyObject *structsStack;
    PyObject *py_size;
    Py_ssize_t structsTop;
    PyObject *globalVars;
    struct compactStructs retval;
        
    globalVars = PyThreadState_GetDict();
    
    structsStack = PyDict_GetItemString(globalVars, "structsStack");
    py_size = PyDict_GetItemString(globalVars, "structsStackTop");
    if (!PyArg_Parse(py_size, "n", &structsTop)) {
            retval.state = -1;
            retval.last_fid = -1;
            return retval;
    }

    if (structsTop != -1) {
        PyArg_ParseTuple(PyList_GetItem(structsStack, structsTop), "ii", &(retval.state), &(retval.last_fid));
        structsTop--;
        PyDict_SetItemString(globalVars, "structsStackTop", Py_BuildValue("n", structsTop));
        return retval;
    }
    return retval;
}

int 
popContainers(void)
{
    PyObject *ContainersStack;
    PyObject *py_size;
    Py_ssize_t containersTop;
    PyObject *globalVars;
    int retval;
        
    globalVars = PyThreadState_GetDict();
    
    ContainersStack = PyDict_GetItemString(globalVars, "containersStack");
    py_size = PyDict_GetItemString(globalVars, "containersStackTop");
    if (!PyArg_Parse(py_size, "n", &containersTop)) {
            retval = -1;
            return retval;
    }

    if (containersTop != -1) {
        PyArg_ParseTuple(PyList_GetItem(ContainersStack, containersTop), "i", &retval);
        containersTop--;
        PyDict_SetItemString(globalVars, "containersStackTop", Py_BuildValue("n", containersTop));
        return retval;
    }
    return retval;
}


PyObject *thrift_struct_read(char*, char*, int, int, int, int, int);
PyObject *read_field_value_C(char* ,char*, int, int, int, int, int, int);
static void readStructBeginC(char*, char*);
static void readStructEndC(char*, char*);
static int readFieldBeginC(char*, char*, char*, int*);
static void readFieldEndC(char*, char*);
int readByteC(char*);
static int readBool(char*, char*);
int readI16(char*, char*);
int readI32(char*, char*);
long int readI64(char*, char*);
double readDouble(char*, char*);
char *readString(char*, char*);
char *readBinary(char*, char*);
void readMapBegin(char*, char*, int*, int*, int*);
void readMapEnd(char*, char*);
void readSetBegin(char*, char*, int*, int*);
void readSetEnd(char*,  char*);
void readListBegin(char *, char*, int*, int*);
void readListEnd(char*, char*);
static void skip(char*, char*, int);
static char * const field_type_to_str(int);

static int readJson(char*);
static int peekJson(char*);
static void contextRead(char*);
static int escapeNum(void);
static void readJSONObjectStart(char*);
static void readJSONObjectEnd(char*);
void readJSONArrayStart(char*);
void readJSONArrayEnd(char*);
static void readJSONSyntaxChar(char*, char);
int readJsonInteger(char*);
static void readJsonQuotes(char*);
static int readJsonNumericChars(char*);
static int JTypesToInt(char*);
char *readJSONString(char*, int);
double readJSONDouble(char*);
static void pushContext(int, int, int);
static void popContext(void);

static int readSize(char*);
int readVarint(char*);
static int fromZigZag(int);
static int getTType(int);
static int readerZigZag(char*);

/*export some clases from Thrift Struct to return something python can use*/
PyObject *
get_ThriftField(void)
{
    PyObject *module = PyImport_ImportModule("thrift_tools.thrift_struct");
    if (!module) {
        return NULL;
    }
    return PyObject_GetAttrString(module,"ThriftField");
}

PyObject *
get_ThriftStruct(void)
{
    PyObject *module = PyImport_ImportModule("thrift_tools.thrift_struct");
    if (!module) {
        return NULL;
    }
    return PyObject_GetAttrString(module,"ThriftStruct");
}

/*this function could be called as an extension from python with proper enviroment configuration
right now the enviroment supports only TBinaryProtocol
includes python parameter parsing*/
static PyObject *
thrift_struct_read_module(PyObject *self, PyObject *args)
{
	char *proto, *token, *rawData, *protocol, *s;
	int max_fields, max_list_size, max_map_size;
    int max_set_size, read_values, startByte;
    PyObject* data;
    PyObject* objectsRepresentation;
    int i;

    if (!PyArg_ParseTuple(args, "Osiiiiii", &data,
                                            &proto,
                                            &max_fields,
                                            &max_list_size,
                                            &max_map_size,
                                            &max_set_size,
                                            &read_values,
                                            &startByte))
    {
        return NULL;
    }

    objectsRepresentation = PyObject_Repr(data);
    s = PyString_AsString(objectsRepresentation);
    
    token=strtok(s+1, "'");
    rawData=token;
    token=strtok(proto, ".");
    token=strtok(NULL, ".");
    token=strtok(NULL, ".");
    protocol=token;
    
    for (i = 0; i < startByte; i++) {
        readByteC(rawData);
    }
    if (!strcmp(protocol, "TJSONProtocol")) {
        return thrift_struct_read(rawData,
                                    protocol,
                                    max_fields,
                                    max_list_size,
                                    max_map_size,
                                    max_set_size,
                                    read_values);
    }

	return NULL;
}

/*this is the implementation of read from thrift struct
it uses a python list to store and return the messages, just like the original*/
PyObject *
thrift_struct_read(char* data,
                    char* proto,
                    int max_fields,
                    int max_list_size,
                    int max_map_size,
                    int max_set_size,
                    int read_values)
{
    PyObject *fields, *value;
    PyObject *ThriftField=get_ThriftField();
    PyObject *ThriftStruct=get_ThriftStruct();
    int nfields=0;
    int start;
    int fid;
    int ftype;
    char *name;
    int end, len;
    int byteCounter;
    PyObject *globalVars;
    PyObject *byte;

    globalVars = PyThreadState_GetDict();
    byte = PyDict_GetItemString(globalVars,"byteCounter");
    if (!PyArg_Parse(byte, "i", &byteCounter)) {
        return NULL;
    }

    start = byteCounter;
    fields = PyList_New(0);
    name = NULL;
    readStructBeginC(data, proto);
    while (1) {
        nfields++;
        if (nfields >= max_fields) {
            PyErr_SetString(PyExc_ValueError,"too many fields");
            return NULL;
        }
        ftype = readFieldBeginC(data,proto,name,&fid);
        if (ftype == STOP) {
            break;
        }

        value = read_field_value_C(data,
            proto,
            ftype,
            max_fields,
            max_list_size,
            max_map_size,
            max_set_size,
            read_values);
        readFieldEndC(data, proto);

        PyList_Append(fields, 
                        PyObject_CallFunction(ThriftField,
                                            "siO",
                                            field_type_to_str(ftype),
                                            fid,
                                            value));
    }

    readStructEndC(data, proto);

    byte = PyDict_GetItemString(globalVars,"byteCounter");
    if (!PyArg_Parse(byte, "i", &byteCounter)) {
        return NULL;
    }
    end = byteCounter;
    len = end-start;
    return PyObject_CallFunction(ThriftStruct,"Oi",fields,len);
}

/*read field value from thrift struct*/
PyObject*
read_field_value_C(char* data,
                    char* proto,
                    int ftype,
                    int max_fields,
                    int max_list_size,
                    int max_map_size,
                    int max_set_size,
                    int read_values)
{
    int i, etype, ktype, vtype, size;
    PyObject *value, *k, *v;
    char *s;

    value = NULL;
    if (ftype == STRUCT) {
            value = thrift_struct_read(data,
                                        proto,
                                        max_fields,
                                        max_list_size,
                                        max_map_size,
                                        max_set_size,
                                        read_values);
    }
    else if (ftype == I32) {
            if (read_values) {
                value = Py_BuildValue("i", readI32(data,proto));
            }
            else {
                skip(data, proto, ftype);
            }
    }
    else if (ftype == I64) {
            if (read_values) {
                value = Py_BuildValue("l", readI64(data,proto));
            }
            else {
                skip(data, proto, ftype);
            }
    }
    else if (ftype == STRING) {
            if (read_values) {
                s = readString(data,proto);
                value = Py_BuildValue("s", s);
                free(s);
            }
            else {
                skip(data, proto, ftype);
            }
    }
    else if (ftype == LIST) {
            readListBegin(data, proto, &etype, &size);
            value = PyList_New(0);
            if (read_values) {
                for(i = 0; i < size; i++) {
                    PyList_Append(value, read_field_value_C(data,
                                                            proto,
                                                            etype,
                                                            max_fields,
                                                            max_list_size,
                                                            max_map_size,
                                                            max_set_size,
                                                            read_values));
                }
            }
            else {
                for (i = 0; i < size; i++) {
                    skip(data, proto, etype);
                }
            }
            readListEnd(data, proto);
    }
    else if (ftype == MAP) {
            readMapBegin(data, proto, &ktype, &vtype, &size);
            value = PyDict_New();
            if (read_values) {
                for (i = 0; i < size; i++) {
                    k = read_field_value_C(data,
                                            proto,
                                            ktype,
                                            max_fields,
                                            max_list_size,
                                            max_map_size,
                                            max_set_size,
                                            read_values);
                    v = read_field_value_C(data,
                                            proto,
                                            vtype,
                                            max_fields,
                                            max_list_size,
                                            max_map_size,
                                            max_set_size,
                                            read_values);
                    PyDict_SetItem(value, k, v);
                }
            }
            else {
                for (i = 0; i < size; i++) {
                    skip(data, proto, ktype);
                    skip(data, proto, vtype);
                }
            }
            readMapEnd(data, proto);
    }
    else if (ftype == SET) {
            readSetBegin(data, proto, &etype, &size);
            value = PySet_New(NULL);
            if (read_values) {
                for (i = 0; i < size; i++) {
                    PySet_Add(value,read_field_value_C(data,
                                                        proto,
                                                        etype,
                                                        max_fields,
                                                        max_list_size,
                                                        max_map_size,
                                                        max_set_size,
                                                        read_values));
                }
            }
            else {
                for (i = 0; i < size; i++) {
                    skip(data, proto, etype);
                }
            }
            readSetEnd(data, proto);
        }
        else{
            skip(data, proto, ftype);
        }

        return value;
}

/*from now we have ports from TBinaryProtocol, TJSONProtocol and TCompactPRotocol from Thrift*/
static void
readStructBeginC(char* data,char* protocol)
{
    if (!strcmp(protocol, "TBinaryProtocol")) {
        return;
    }
    else if (!strcmp(protocol, "TJSONProtocol")) {
        readJSONObjectStart(data);
    }
    else if (!strcmp(protocol, "TCompactProtocol")) {
        int last_fid, state;
        PyObject *globalVars;
        PyObject *py_last_fid;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_last_fid = PyDict_GetItemString(globalVars,"last_fid");
        if (!PyArg_Parse(py_last_fid, "i", &last_fid)) {
            return ;
        }
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }

        if (state != CLEAR &&
            state != CONTAINER_READ &&
            state != VALUE_READ)
        {
            PyErr_SetString(PyExc_ValueError, "Wrong State");
            return ;
        }

        pushStructs(state, last_fid);
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",FIELD_READ));    
        PyDict_SetItemString(globalVars,"last_fid",Py_BuildValue("i",0));
    }
}

static void
readStructEndC(char *data, char *proto)
{
    if (!strcmp(proto, "TJSONProtocol")) {
        readJSONObjectEnd(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        struct compactStructs retval;
        int state;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }
        if (state != FIELD_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong State");
            return ;
        }
        retval = popStructs();
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",retval.state));
        PyDict_SetItemString(globalVars,"last_fid",Py_BuildValue("i",retval.last_fid));
    }
}

static int
readFieldBeginC(char* data, char* protocol,char *name, int* fid)
{
    int type;
    char *ctype;

    if (!strcmp(protocol, "TBinaryProtocol")) {
        type = readByteC(data);
        if (type == STOP) { 
            name = NULL;
             *fid = 0;
             return type;
        }
        name = NULL;
        *fid = readI16(data, protocol);
        return type;
    }
    else if (!strcmp(protocol, "TJSONProtocol")) {
        char character = peekJson(data);

        type = 0;
        *fid = 0;
        if (character == RBRACE) {
            type = STOP;
        }
        else {
            name = NULL;
            *fid = readJsonInteger(data);
            readJSONObjectStart(data);
            ctype = readJSONString(data, 0);
            type = JTypesToInt(ctype);
            free(ctype);
        }
        return type;
    }
    else if (!strcmp(protocol, "TCompactProtocol")) {
        int delta;
        int last_fid;
        int state;
        PyObject *globalVars;
        PyObject *py_last_fid;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_last_fid = PyDict_GetItemString(globalVars,"last_fid");
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return -1;
        }
        if (!PyArg_Parse(py_last_fid, "i", &last_fid)) {
            return -1;
        }
        if (state != FIELD_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong State");
            return -1;
        }
        type = readByteC(data);
        if ((type & 15) == STOP) {
            *fid = 0;
            name = NULL;
            return 0;
        }
        delta = type >> 4;
        if (delta == 0) {
            *fid = fromZigZag(readVarint(data));
        }
        else {
            *fid = last_fid+delta;
        }
        PyDict_SetItemString(globalVars,"last_fid",Py_BuildValue("i",*fid));
        type = type & 15;
        if (type == COMPACT_TYPE_TRUE) {
            PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",BOOL_READ));
            PyDict_SetItemString(globalVars,"bool_value",Py_BuildValue("i",1));
        }
        else if (type == COMPACT_TYPE_FALSE) {
            PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",BOOL_READ));
            PyDict_SetItemString(globalVars,"bool_value",Py_BuildValue("i",0));
        }
        else {
            PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",VALUE_READ));
        }
        name = NULL;
        return getTType(type);
    }
    return 0;
}

static void
readFieldEndC(char *data, char *proto)
{
    if (!strcmp(proto, "TJSONProtocol")) {
        readJSONObjectEnd(data);   
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int state;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }
        if (state != BOOL_READ && state != VALUE_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong State");
            return;
        }
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",FIELD_READ));
    }

}

static int
readBool(char* data, char *protocol)
{
    if (!strcmp(protocol, "TBinaryProtocol")) {
        return (readByteC(data) == 0)?0:1;
    }
    else if (!strcmp(protocol, "TJSONProtocol")){
        return (readJsonInteger(data) == 0)?0:1;
    }
    else if (!strcmp(protocol, "TCompactProtocol")){
        int bool_value;
        int state;
        PyObject *globalVars;
        PyObject *py_bool;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_bool = PyDict_GetItemString(globalVars,"byteCounter");
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return 0;
        }if (!PyArg_Parse(py_bool, "i", &bool_value)) {
            return 0;
        }
        if (state == BOOL_READ) {
            return bool_value == COMPACT_TYPE_TRUE;
        }
        else if (state == CONTAINER_READ) {
            return readByteC(data) == COMPACT_TYPE_TRUE;
        }
        else {
            PyErr_SetString(PyExc_ValueError, "wrong state");
        }
    }
    return 0;
}

int
readI16(char* data, char *protocol)
{
    if (!strcmp(protocol, "TBinaryProtocol")) {
        int aux[2];

        aux[0] = readByteC(data);
        aux[1] = readByteC(data);
        return aux[0]*pow(2, 8)+aux[1];
    }
    else if (!strcmp(protocol, "TJSONProtocol")) {
        return readJsonInteger(data);
    }
    else if (!strcmp(protocol, "TCompactProtocol")){
        return readerZigZag(data);
    }
    else {
        return -1;
    }
}

int
readI32(char* data, char *protocol)
{
    if (!strcmp(protocol, "TBinaryProtocol")) {
        int aux[4];

        aux[0] = readByteC(data);
        aux[1] = readByteC(data);
        aux[2] = readByteC(data);
        aux[3] = readByteC(data);
        return aux[0]*(int)pow(2, 24) +
                aux[1]*(int)pow(2, 16) +
                aux[2]*(int)pow(2, 8) +
                aux[3];
    }
    else if (!strcmp(protocol, "TJSONProtocol")) {
        return readJsonInteger(data);
    }
    else if (!strcmp(protocol, "TCompactProtocol")) {
        return readerZigZag(data);
    }
    else {
        return -1;
    }
}

long int
readI64(char* data, char *protocol)
{
    if (!strcmp(protocol, "TBinaryProtocol")) {
        int aux[8];

        aux[0] = readByteC(data);
        aux[1] = readByteC(data);
        aux[2] = readByteC(data);
        aux[3] = readByteC(data);
        aux[4] = readByteC(data);
        aux[5] = readByteC(data);
        aux[6] = readByteC(data);
        aux[7] = readByteC(data);

        return aux[0]*pow(2, 56) +
                aux[1]*pow(2, 48) +
                aux[2]*pow(2, 40) +
                aux[3]*pow(2, 32) +
                aux[4]*pow(2, 24) +
                aux[5]*pow(2, 16) +
                aux[6]*pow(2, 8) +
                aux[7];
    }
    else if (!strcmp(protocol, "TJSONProtocol")) {
        return readJsonInteger(data);
    }
    else if (!strcmp(protocol, "TCompactProtocol")) {
        return readerZigZag(data);
    }
    return -1;
}

double
readDouble(char* data, char *protocol)
{
     if (!strcmp(protocol, "TBinaryProtocol")) {
        char aux[8];
        int sign, exponent;
        double fraction;
        int bit, i, j, retval;

        aux[0] = readByteC(data);
        aux[1] = readByteC(data);
        aux[2] = readByteC(data);
        aux[3] = readByteC(data);
        aux[4] = readByteC(data);
        aux[5] = readByteC(data);
        aux[6] = readByteC(data);
        aux[7] = readByteC(data);
        sign = (aux[0]>>7==0)?1:-1;
        exponent = (aux[0]%(int)pow(2, 7))*pow(2, 4) + (aux[1]>>4) - 1023;
        fraction = 0;
        retval = -1;

        for (j = 3; j >= 0; j--) {
            bit = ((aux[1]>>j)%2);
            fraction = fraction+bit*pow(2, retval);
            retval--;
        }
        for (i = 2; i < 8; i++) {
            for (j = 7; j >= 0; j--) {
                bit=((aux[i]>>j)%2);
                fraction=fraction+bit*pow(2, retval);
                retval--;
            }
        }
        fraction = 1+fraction;
        return sign*fraction*pow(2,exponent);
    }
    else if (!strcmp(protocol, "TJSONProtocol")) {
            readJSONDouble(data);
    }
    else if (!strcmp(protocol, "TCompactProtocol")) {
        char aux[8];
        int sign, exponent;
        double fraction;
        int bit, i, j, retval;

        aux[7] = readByteC(data);
        aux[6] = readByteC(data);
        aux[5] = readByteC(data);
        aux[4] = readByteC(data);
        aux[3] = readByteC(data);
        aux[2] = readByteC(data);
        aux[1] = readByteC(data);
        aux[0] = readByteC(data);
        sign = (aux[0]>>7==0)?1:-1;
        exponent = (aux[0]%(int)pow(2, 7))*pow(2, 4) + (aux[1]>>4) - 1023;
        fraction = 0;
        retval = -1;

        for (j = 3; j >= 0; j--) {
            bit = ((aux[1]>>j)%2);
            fraction = fraction+bit*pow(2, retval);
            retval--;
        }
        for (i = 2; i < 8; i++) {
            for (j = 7; j >= 0; j--) {
                bit = ((aux[i]>>j)%2);
                fraction = fraction+bit*pow(2, retval);
                retval--;
            }
        }
        fraction = 1+fraction;
        return sign*fraction*pow(2, exponent);
    }
    return -1;
}

/*the parsing of arguments from python causes some chars to be stored in hexadecimal or escape values
thats why we need to read bytes in a diferent form
if this parse can be improved then this function must be modified*/
int
readByteC(char* data)
{
    char aux[5];
    char retval;
    int bufferCounter;
    int byteCounter;
    PyObject *globalVars;
    PyObject *buff;
    PyObject *byte;

    globalVars = PyThreadState_GetDict();
    buff = PyDict_GetItemString(globalVars,"bufferCounter");
    if (!PyArg_Parse(buff, "i", &bufferCounter)) {
        return -1;
    }
    byte = PyDict_GetItemString(globalVars,"byteCounter");
    if (!PyArg_Parse(byte, "i", &byteCounter)) {
        return -1;
    }
    byteCounter++;
    PyDict_SetItemString(globalVars,"byteCounter",Py_BuildValue("i",byteCounter));
    
    aux[0] = '0';
    aux[1] = 'x';
    aux[4] = '\0';
    if (data[bufferCounter] == '\\' && data[bufferCounter+1] == 'x') {
        aux[2] = data[bufferCounter+2];
        aux[3] = data[bufferCounter+3];
        bufferCounter = bufferCounter+4;
        PyDict_SetItemString(globalVars,"bufferCounter",Py_BuildValue("i",bufferCounter));
        return strtol(aux, NULL, 16);
    }
    else if (data[bufferCounter] == '\\') {  
        bufferCounter = bufferCounter+2;
        PyDict_SetItemString(globalVars,"bufferCounter",Py_BuildValue("i",bufferCounter));
        if (data[bufferCounter-1] == 't') {
            return '\t';       
        }
        else if (data[bufferCounter-1] == 'n') {
            return '\n';
        }
        else if (data[bufferCounter-1] == 'r') {
            return '\r';
        }
        else if (data[bufferCounter-1] == '\\') {
            return '\\';
        }
    }
    retval = data[bufferCounter];
    bufferCounter++;
    PyDict_SetItemString(globalVars,"bufferCounter",Py_BuildValue("i",bufferCounter));
    return retval;
}

char *
readString(char* data, char *proto)
{
    if (!strcmp(proto, "TBinaryProtocol")) {
        return readBinary(data, proto);
    }
    else if (!strcmp(proto, "TJSONProtocol")) {
        return readJSONString(data, 0);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        return readBinary(data, proto);
    }
    else {
        return NULL;
    }
}

/*there is no need to write the JSON version since we wont need it*/
char *
readBinary(char* data, char *proto)
{
    if (!strcmp(proto, "TBinaryProtocol")) {
        int size, i;
        char *retval;

        size = readI32(data, proto);
        if (size < 0) {
            PyErr_SetString(PyExc_ValueError, "Size lower than 0");
            return NULL;
        }
        retval = (char *)malloc(sizeof(char)*(size+1));
        for (i = 0; i < size; i++) {
            retval[i] = readByteC(data);
        }
        retval[size] = '\0';
        return retval;
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int size, i;
        char *retval;

        size = readSize(data);
        if (size < 0) {
            PyErr_SetString(PyExc_ValueError, "Size lower than 0");
            return NULL;
        }
        retval = (char *)malloc(sizeof(char)*(size+1));
        for (i = 0; i < size; i++) {
            retval[i] = readByteC(data);
        }
        retval[size] = '\0';
        return retval;
    }
    else {
        return NULL;
    }
}

/*complex structures like mappings, sets and list may need further testing*/
void
readMapBegin(char *data, char *proto, int *ktype, int *vtype, int *size)
{
    char *s;
    if (!strcmp(proto, "TBinaryProtocol")) {
        *ktype = readByteC(data);
        *vtype = readByteC(data);
        *size = readI32(data,proto);
    }
    else if (!strcmp(proto, "TJSONProtocol")) {
        readJSONArrayStart(data);
        s = readJSONString(data, 0);
        *ktype = JTypesToInt(s);
        free(s);
        s = readJSONString(data, 0);
        *vtype = JTypesToInt(s);
        free(s);
        *size = readJsonInteger(data);
        readJSONObjectStart(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int type;
        int state;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }

        if (state != VALUE_READ && state != CONTAINER_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong state");
            return ;
        }
        *size = readSize(data);
        type = 0;
        if (size > 0) {
            type = readByteC(data);
        }
        *vtype = getTType(type);
        *ktype = getTType(type>>4);
        pushContainers(state);
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",CONTAINER_READ));
    }
}

void
readMapEnd(char *data, char *proto)
{
    if (!strcmp(proto, "TJSONProtocol")) {
        readJSONObjectEnd(data);
        readJSONArrayEnd(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int state, retval;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }

        if (state != CONTAINER_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong state");
            return ;
        }
        retval = popContainers();
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",retval));
    }
}

void
readSetBegin(char *data, char *proto, int *etype, int *size)
{
    char *s;
    if (!strcmp(proto, "TBinaryProtocol")) {
        *etype = readByteC(data);
        *size = readI32(data, proto);
    }
    else if (!strcmp(proto, "TJSONProtocol")) {
        readJSONArrayStart(data);
        s = readJSONString(data, 0);
        *etype = JTypesToInt(s);
        free(s);
        *size = readJsonInteger(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int size_type;
        int state;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }

        if (state != VALUE_READ && state != CONTAINER_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong state");
            return;
        }
        size_type = readByteC(data);
        *size = size_type>>4;
        *etype = getTType(size_type);
        if(*size == 15) {
            *size = readSize(data);
        }
        pushContainers(state);
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",CONTAINER_READ));
    }
}

void
readSetEnd(char *data, char *proto)
{
    if (!strcmp(proto, "TJSONProtocol")) {
        readJSONArrayEnd(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int state;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }
        if (state != CONTAINER_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong state");
            return;
        }
        popContainers();
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",0));
    }
    return;
}

void
readListBegin(char *data, char *proto, int *etype, int *size)
{
    char *s;

    if (!strcmp(proto, "TBinaryProtocol")) {
        *etype = readByteC(data);
        *size = readI32(data, proto);
    }
    else if (!strcmp(proto, "TJSONProtocol")) {
        readJSONArrayStart(data);
        s = readJSONString(data, 0);
        *etype = JTypesToInt(s);
        free(s);
        *size = readJsonInteger(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int size_type;
        int state;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }

        if (state != VALUE_READ && state != CONTAINER_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong state");
            return;
        }
        size_type = readByteC(data);
        *size = size_type>>4;
        *etype = getTType(size_type);
        if (*size == 15) {
            *size = readSize(data);
        }
        pushContainers(state);
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",CONTAINER_READ));
    }
}

void
readListEnd(char *data, char *proto)
{
    if (!strcmp(proto, "TJSONProtocol")) {
        readJSONArrayEnd(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int retval;
        int state;
        PyObject *globalVars;
        PyObject *py_state;

        globalVars = PyThreadState_GetDict();
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }

        if (state != CONTAINER_READ) {
            PyErr_SetString(PyExc_ValueError, "Wrong state");
            return;
        }
        retval = popContainers();
        PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",retval));
    }
}

/*this is skip from thrift*/
static void
skip(char* data, char* proto, int ttype)
{
    int id, i;
    int ktype, vtype, etype, size;
    char *name;

    name = NULL;
    switch (ttype) {
        case STOP :
        {
            return ;
        }
        case BOOL :
        {
            readBool(data, proto);
            break;
        }
        case BYTE :
        {
            readByteC(data);
            break;
        }
        case I16 :
        {
            readI16(data, proto);
            break;
        }
        case I32 :
        {
            readI32(data, proto);
            break;
        }
        case I64 :
        {
            readI64(data, proto);
            break;
        }
        case DOUBLE:
        {
            readDouble(data, proto);
            break;
        }
        case STRING:
        {
            name = readString(data, proto);
            free(name);
            break;
        }
        case STRUCT:
        {
            readStructBeginC(data, proto);
            while (1) {
                ttype = readFieldBeginC(data, proto, name, &id);
                if (ttype == STOP) {
                    break;
                }
                skip(data, proto, ttype);
                readFieldEndC(data, proto);
            }
            readStructEndC(data, proto);
            break;
        }
        case MAP:
        {
            readMapBegin(data, proto, &ktype, &vtype, &size);
            for (i = 0; i < size ;i++) {
                skip(data, proto, ktype);
                skip(data, proto, vtype);
            }
            readMapEnd(data, proto);
            break;
        }
        case SET:
        {
            readSetBegin(data, proto, &etype, &size);
            for (i = 0; i < size; i++) {
                skip(data, proto, ktype);
            }
            readSetEnd(data, proto);
        }
        case LIST:
        {
            readListBegin(data, proto, &etype, &size);
            for (i = 0; i < size; i++) {
                skip(data, proto, ktype);
            }
            readListEnd(data, proto);
            break;
        }
        default :
            break;
    }
}

static char * const
field_type_to_str(int ftype)
{
    switch (ftype) {
        case STOP:
        {
            return "stop";
        }
        case VOID:
        {
            return "void";
        }
        case BOOL:
        {
            return "bool";
        }
        case BYTE:
        {
            return "byte";
        }
        case DOUBLE:
        {
            return "double";
        }
        case I16:
        {
            return "i16";
        }
        case I32:
        {
            return "i32";
        }
        case I64:
        {
            return "i64";
        }
        case STRING:
        {
            return "string";
        }
        case STRUCT:
        {
            return "struct";
        }
        case MAP:
        {
            return "map";
        }
        case SET:
        {
            return "set";
        }
        case LIST:
        {
            return "list";
        }
        case UTF8:
        {
            return "utf8";
        }
        case UTF16:
        {
            return "utf16";
        }
        default :
        {
            return NULL;
        }
    }
}

/*JSON needs some extra content*/
static int
readJson(char *data)
{
    int hasData,jsonData;
    PyObject *globalVars;
    PyObject *hData;
    PyObject *jData;

    globalVars = PyThreadState_GetDict();
    hData = PyDict_GetItemString(globalVars,"hasData");
    if (!PyArg_Parse(hData, "i", &hasData)) {
        return -1;
    }
    jData = PyDict_GetItemString(globalVars,"jsonData");
    if (!PyArg_Parse(jData, "i", &jsonData)) {
        return -1;
    }

    if (hasData) {
        hasData = 0;
    }
    else {
        jsonData = readByteC(data);
    }

    PyDict_SetItemString(globalVars,"hasData",Py_BuildValue("i",hasData));
    PyDict_SetItemString(globalVars,"jsonData",Py_BuildValue("i",jsonData));

    return jsonData;
}

static void
contextRead(char* data)
{
    int first;
    int colon;
    int contextType;
    PyObject *globalVars;
    PyObject *py_first;
    PyObject *py_context;
    PyObject *py_colon;

    globalVars = PyThreadState_GetDict();
    py_first = PyDict_GetItemString(globalVars,"first");
    if (!PyArg_Parse(py_first, "i", &first)) {
        return ;
    }
    py_context = PyDict_GetItemString(globalVars,"contextType");
    if (!PyArg_Parse(py_context, "i", &contextType)) {
        return ;
    }
    py_colon = PyDict_GetItemString(globalVars,"colon");
    if (!PyArg_Parse(py_colon, "i", &colon)) {
        return ;
    }
    if (contextType == PAIRCONTEXT) {
        if (first) {
            first = 0;
            colon = 1;
        }
        else {
            if (colon) {
                readJSONSyntaxChar(data, COLON);
            }
            else {
                readJSONSyntaxChar(data, COMMA);
            }
            colon = (colon)?0:1;
        }
    }
    else if (contextType == LISTCONTEXT) {
        if (first) {
            first = 0;
        }
        else {
            readJSONSyntaxChar(data, COMMA);
        }
    }
    PyDict_SetItemString(globalVars,"colon",Py_BuildValue("i",colon));    
    PyDict_SetItemString(globalVars,"first",Py_BuildValue("i",first));    
}

static int
peekJson(char *data)
{
    int hasData,jsonData;
    PyObject *globalVars;
    PyObject *hData;
    PyObject *jData;

    globalVars = PyThreadState_GetDict();
    hData = PyDict_GetItemString(globalVars,"hasData");
    if (!PyArg_Parse(hData, "i", &hasData)) {
        return -1;
    }
    jData = PyDict_GetItemString(globalVars,"jsonData");
    if (!PyArg_Parse(jData, "i", &jsonData)) {
        return -1;
    }
    if (!hasData) {
        jsonData = readByteC(data);
    }
    hasData = 1;
    PyDict_SetItemString(globalVars,"hasData",Py_BuildValue("i",hasData));
    PyDict_SetItemString(globalVars,"jsonData",Py_BuildValue("i",jsonData));
    
    return jsonData;
}

static int
escapeNum(void)
{
    int contextType;
    PyObject *globalVars;
    PyObject *py_context;

    globalVars = PyThreadState_GetDict();
    py_context = PyDict_GetItemString(globalVars,"contextType");
    if (!PyArg_Parse(py_context, "i", &contextType)) {
        return -1;
    }
    if (contextType == PAIRCONTEXT) {
        int colon;
        PyObject *py_colon;

        py_colon = PyDict_GetItemString(globalVars,"colon");
        if (!PyArg_Parse(py_colon, "i", &colon)) {
            return -1;
        }
        return colon;
    }
    else {
        return 0;
    }
}

int
readJsonInteger(char *data)
{
    int numeric;

    contextRead(data);
    readJsonQuotes(data);
    numeric = readJsonNumericChars(data);
    readJsonQuotes(data);
    return numeric;
}

static int
readJsonNumericChars(char *data)
{
    int number = 0;
    char character;

    while (1) {
        character = peekJson(data);
        if(character < '0' || character > '9') {
            break;
        }
        number = 10*number+(readJson(data)-'0');
    }
    return number;
}

double
readJSONDouble(char *data)
{
    char *s;
    double r;

    contextRead(data);
    if (peekJson(data) == QUOTE) {
        s = readJSONString(data, 1);
        r = strtod(s,NULL);
        free(s);
        return r;
    }
    else {
        if (escapeNum()) {
            readJSONSyntaxChar(data, QUOTE);
        }
    return readJsonNumericChars(data);
    }
}

static void
readJsonQuotes(char *data)
{
    if (escapeNum()) {
        readJSONSyntaxChar(data, QUOTE);
    }
}

static void
readJSONObjectStart(char *data)
{   
    int first;
    int colon;
    PyObject *globalVars;
    PyObject *py_first;
    PyObject *py_colon;

    globalVars = PyThreadState_GetDict();
    py_first = PyDict_GetItemString(globalVars,"first");
    if (!PyArg_Parse(py_first, "i", &first)) {
        return ;
    }
    py_colon = PyDict_GetItemString(globalVars,"colon");
    if (!PyArg_Parse(py_colon, "i", &colon)) {
        return ;
    }

    contextRead(data);
    readJSONSyntaxChar(data, LBRACE);
    pushContext(PAIRCONTEXT, first, colon);
}

void
readJSONArrayStart(char *data)
{
    int colon;
    PyObject *globalVars;
    PyObject *py_colon;

    globalVars = PyThreadState_GetDict();
    py_colon = PyDict_GetItemString(globalVars,"colon");
    if (!PyArg_Parse(py_colon, "i", &colon)) {
        return ;
    }

    contextRead(data);
    readJSONSyntaxChar(data, LBRACKET);
    pushContext(LISTCONTEXT, 0, colon);
}

void
readJSONArrayEnd(char *data)
{
    readJSONSyntaxChar(data, RBRACKET);
    popContext();
}

static void
readJSONObjectEnd(char *data)
{
    readJSONSyntaxChar(data, RBRACE);
    popContext();
}

static void
readJSONSyntaxChar(char *data, char character)
{
    char current;

    current = readJson(data);
    if (current != character) {
        PyErr_SetString(PyExc_ValueError, "Unexpected character");
        return;
    }
}

static char
JsonToChar(char high, char low)
{
    int codepoint;

    if (low == 0) {
        return high;
    }
    else {
        codepoint = (1<<16)+((high & 1023)<<10);
        codepoint = codepoint+(low & 1023);
        return codepoint;
    }
}

char
*readJSONString(char *data,int skipContext)
{
    int highSurrogate, i;
    char *strReturn;
    char character;
    char aux[5];
    int codeunit;
    int count;

    highSurrogate=0;
    count=1;
    aux[4] = '\0';
    strReturn = malloc(sizeof(char));
    strReturn[0] = '\0';
    if (skipContext == 0) {
        contextRead(data);
    }
    readJSONSyntaxChar(data, QUOTE);
    while (1) {
        character = readJson(data);
        if (character == QUOTE) {
            break;
        }
        if (character == '\\') {
            character = readJson(data);
            if (character == 'u') {
                for (i = 0; i < 4; i++) {
                    aux[i] = readJson(data);
                }
                codeunit = strtol(aux, NULL, 16);
                if (codeunit >= 55296 && codeunit <= 56319) {
                    if (highSurrogate) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected low Surrogate char");
                    }
                    highSurrogate = codeunit;
                    continue;
                }
                else if (codeunit >= 56320 && codeunit <= 57343) {
                    if (highSurrogate == 0) {
                        PyErr_SetString(PyExc_ValueError,
                                        "Expected low Surrogate char");
                    }
                    character = JsonToChar(highSurrogate, codeunit);
                    highSurrogate = 0;
                }
                else {
                    character = JsonToChar(codeunit,0);
                }
            }
            else {
                if (character == '"') {
                    character = '"';
                }
                else if (character == '\\') {
                    character = '\\';
                }
                else if (character == 'b') {
                    character = '\b';
                }
                else if (character == 'f') {
                    character = '\f';
                }
                else if (character == 'n') {
                    character = '\n';
                }
                else if (character == 'r') {
                    character = '\r';
                }
                else if (character == 't') {
                    character = '\t';
                }
                else if (character == '/') {
                    character = '/';
                }
                else {
                    PyErr_SetString(PyExc_ValueError,
                                    "Expected low Surrogate char");
                }
            }
        }
        else if (character == '"' ||
                character == '\\' ||
                character == '\b' ||
                character == '\f' ||
                character == '\n' ||
                character == '\r' ||
                character == '\t') 
        {
            PyErr_SetString(PyExc_ValueError, "Unescaped control char");
        }
        strReturn = (char*)realloc(strReturn, sizeof(char)*(count+1));
        strReturn[count-1] = character;
        strReturn[count] = '\0';
        count++;
    }
    return strReturn;
}

static void
pushContext(int contextType, int first, int newcolon)
{
    PyObject *globalVars;
    PyObject *contextStack;
    PyObject *py_size;
    Py_ssize_t contextTop;
    
    globalVars = PyThreadState_GetDict();
    contextStack = PyDict_GetItemString(globalVars, "contextStack");
    py_size = PyDict_GetItemString(globalVars, "contextStackTop");
    if (!PyArg_Parse(py_size, "n", &contextTop)) {
            return ;
    }
    
   if (contextTop+1 == PyList_Size(contextStack)) {
        PyList_Append(contextStack, Py_BuildValue("(iii)", contextType, first, newcolon));
        contextTop++;            
    }
    else {
        contextTop++;
        PyList_SetItem(contextStack, contextTop, Py_BuildValue("(iii)", contextType, first, newcolon));
    }
    PyDict_SetItemString(globalVars, "contextStack", contextStack);
    PyDict_SetItemString(globalVars, "contextStackTop", Py_BuildValue("n", contextTop));
    
    if (contextType == PAIRCONTEXT) {
        PyDict_SetItemString(globalVars,"contextType",Py_BuildValue("i",PAIRCONTEXT));
        PyDict_SetItemString(globalVars,"first",Py_BuildValue("i",1));
        PyDict_SetItemString(globalVars,"colon",Py_BuildValue("i",1));
    }
    if (contextType == LISTCONTEXT) {
        PyDict_SetItemString(globalVars,"contextType",Py_BuildValue("i",LISTCONTEXT));
        PyDict_SetItemString(globalVars,"first",Py_BuildValue("i",1));
        PyDict_SetItemString(globalVars,"colon",Py_BuildValue("i",0));
    }
}

static void
popContext(void)
{
    int contextType, first, newcolon;
    Py_ssize_t contextTop;
    PyObject *globalVars;
    PyObject *contextStack;
    PyObject *py_size;

    globalVars = PyThreadState_GetDict();
    contextStack = PyDict_GetItemString(globalVars, "contextStack");
    py_size = PyDict_GetItemString(globalVars, "contextStackTop");
    if (!PyArg_Parse(py_size, "n", &contextTop)) {
            return ;
    }
    if (contextTop >= 0) {
        contextTop--;        
    }
    if (contextTop != -1) {
        PyArg_ParseTuple(PyList_GetItem(contextStack, contextTop),"iii", &contextType, &first, &newcolon);
        PyDict_SetItemString(globalVars,"contextType",Py_BuildValue("i",contextType));
        PyDict_SetItemString(globalVars,"first",Py_BuildValue("i",first));
    }
    else {
        PyDict_SetItemString(globalVars,"contextType",Py_BuildValue("i",BASECONTEXT));
        PyDict_SetItemString(globalVars,"first",Py_BuildValue("i",1));
    }
}

static int
JTypesToInt(char *CharTypes)
{
    if (!strcmp(CharTypes, "tf")) {
        return BOOL;
    }
    else if (!strcmp(CharTypes, "i8")) {
        return BYTE;
    }
    else if (!strcmp(CharTypes, "dbl")) {
        return DOUBLE;
    }
    else if (!strcmp(CharTypes, "i16")) {
        return I16;
    }
    else if (!strcmp(CharTypes, "i32")) {
        return I32;
    }
    else if (!strcmp(CharTypes, "i64")) {
        return I64;
    }
    else if (!strcmp(CharTypes, "str")) {
        return STRING;
    }
    else if (!strcmp(CharTypes, "rec")) {
        return STRUCT;
    }
    else if (!strcmp(CharTypes, "map")) {
        return MAP;
    }
    else if (!strcmp(CharTypes, "set")) {
        return SET;
    }
    else if (!strcmp(CharTypes, "lst")) {
        return LIST;
    }
    else {
        return -1;
    }
}

/*TCompact C functions*/
static int
readSize(char *data)
{
    int result;

    result = readVarint(data);
    if (result < 0) {
        PyErr_SetString(PyExc_ValueError, "Length <0");
        return -1;
    }
    return result;
}

int
readVarint(char *data)
{
    int result;
    int shift;
    int x;

    result = 0;
    shift = 0;
    while (1) {
        x = readByteC(data);
        result = result | ((x & 127)<<shift);
        if (x>>7 == 0) {
            return result;
        }
        shift = shift+7;
    }
}

static int
fromZigZag(int n)
{
    return (n>>1)^-(n&1);
}

static int
readerZigZag(char *data)
{
    int state;
    PyObject *globalVars;
    PyObject *py_state;

    globalVars = PyThreadState_GetDict();
    py_state = PyDict_GetItemString(globalVars,"state");
    if (!PyArg_Parse(py_state, "i", &state)) {
        return -1;
    }

    if (state != VALUE_READ && state != CONTAINER_READ) {
        PyErr_SetString(PyExc_ValueError, "wrong estate");
        return -1;
    }
    return fromZigZag(readVarint(data));
}

static int
getTType(int byte)
{
    int type;

    type = byte & 15;
    switch (type) {
        case 0 :
        {
            return STOP;
        }
        case 1 :
        {
            return BOOL;
        }
        case 2 :
        {
            return BOOL;
        }
        case 4 :
        {
            return I16;
        }
        case 5 :
        {
            return I32;
        }
        case 6 :
        {
            return I64;
        }
        case 7 :
        {
            return DOUBLE;
        }
        case 8 :
        {
            return STRING;
        }
        case 9 :
        {
            return LIST;
        }
        case 10 :
        {
            return SET;
        }
        case 11 :
        {
            return MAP;
        }
        case 12 :
        {
            return STRUCT; 
        }
        default :
            return -1;
    }
}

/*just some functions to make C modules callable from python.*/
static PyMethodDef
thrift_struct_read_module_Methods[] = {
  {"thrift_struct_read_module", thrift_struct_read_module, METH_VARARGS},
  {NULL, NULL}
};

PyMODINIT_FUNC
initthrift_struct_read_module(void)
{
    (void) Py_InitModule("thrift_struct_read_module",
                            thrift_struct_read_module_Methods);
}