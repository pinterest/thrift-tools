#include <Python.h>
#include <string.h>
#include "thrift_struct_read_module.h"
#include "thrift_consts.h"
#include "thrift_utils.h"

static char *readMessageBegin(char *,char *,int *,int *);
static void readMessageEnd(char*,char*);
static char *const message_type_to_str(int);

PyObject
*get_ThriftMessage(void)
{
    PyObject *module = PyImport_ImportModule("thrift_tools.thrift_message");
    if (!module) {
        return NULL;
    }
    return PyObject_GetAttrString(module, "ThriftMessage");
}

static PyObject*
thrift_message_read_module(PyObject *self, PyObject *args, PyObject *keywds)
{
    char *token, *data, *proto, *protocol;
    char *method;
    char *smtype;
    int mtype, seqid, msglen;
    int finagle_thrift, read_values;
    int i;
    int byteCounter;
    PyObject* rawData;
    PyObject *protoc;
    PyObject *msg;
    PyObject *result;
    PyObject *header = Py_None;
    PyObject *ThriftMessage = get_ThriftMessage();
    PyObject* objectsRepresentation;
    char* s;
    PyObject* globalVars;
    PyObject* byte;
    static char *kwlist[] = {"data","protocol", "finagle_thrift", "read_values", NULL};

    proto="None";
    finagle_thrift = 0;
    read_values = 0;
    if (!PyArg_ParseTupleAndKeywords(args, keywds,"OOii", kwlist,&rawData, &protoc, &finagle_thrift, &read_values)) {
        return NULL;
    }

    objectsRepresentation = PyObject_Repr(rawData);
    s = PyString_AsString(objectsRepresentation);
    token = strtok(s+1, "'");
    data = malloc(sizeof(char)*(strlen(token)+1));
    strcpy(data, token);
    objectsRepresentation = PyObject_Repr(protoc);
    proto = PyString_AsString(objectsRepresentation);
    
    if (strcmp(proto, "None") == 0) {
        protocol = detect_protocol(data, "TBinaryProtocol");
    }
    else {
        token = strtok(proto, ".");
        token = strtok(NULL, ".");
        token = strtok(NULL, ".");
        protocol=token;
    }

    globalVars=PyThreadState_GetDict();
    PyDict_SetItemString(globalVars,"bufferCounter",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"byteCounter",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"hasData",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"jsonData",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"first",Py_BuildValue("i",1));
    PyDict_SetItemString(globalVars,"colon",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"bool_value",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"last_fid",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"state",Py_BuildValue("i",0));
    PyDict_SetItemString(globalVars,"contextStack",PyList_New(0));
    PyDict_SetItemString(globalVars,"contextStackTop",Py_BuildValue("i",-1));
    PyDict_SetItemString(globalVars,"structsStack",PyList_New(0));
    PyDict_SetItemString(globalVars,"structsStackTop",Py_BuildValue("i",-1));
    PyDict_SetItemString(globalVars,"containersStack",PyList_New(0));
    PyDict_SetItemString(globalVars,"containersStackTop",Py_BuildValue("i",-1));
    
    if (dataLen(data) < MIN_MESSAGE_SIZE) {
        PyErr_SetString(PyExc_ValueError, "not enough data");
        free(data);
        return NULL;
    }
    if (finagle_thrift) {
        header = thrift_struct_read(data,
                                    protocol,
                                    MAX_FIELDS,
                                    MAX_LIST_SIZE,
                                    MAX_MAP_SIZE,
                                    MAX_SET_SIZE,
                                    read_values);
    }

    method = readMessageBegin(data, protocol, &mtype, &seqid);
    if (method == NULL || strlen(method) == 0 || method[0] == ' ') {
        PyErr_SetString(PyExc_ValueError, "no method name");
        free(data);
        return NULL;
    }
    if (strlen(method) > MAX_METHOD_LENGTH) {
        PyErr_SetString(PyExc_ValueError, "method name too long");
        free(method);
        free(data);
        return NULL;
    }
    for (i = 0; i < strlen(method); i++) {
        if (method[i] < 33 || method[i] > 127) {
            PyErr_SetString(PyExc_ValueError, "invalid method name");
            free(method);
            free(data);
            return NULL;
        }
    }

    msg = thrift_struct_read(data,
                            protocol,
                            MAX_FIELDS,
                            MAX_LIST_SIZE,
                            MAX_MAP_SIZE,
                            MAX_SET_SIZE,
                            read_values);
    readMessageEnd(data, protocol);
    byte = PyDict_GetItemString(globalVars,"byteCounter");
    if (!PyArg_Parse(byte, "i", &byteCounter)) {
        free(method);
        free(data);
        return NULL;
    }
    msglen = byteCounter;

    result = PyTuple_New(2);
    smtype = message_type_to_str(mtype);
    PyTuple_SetItem(result, 0, PyObject_CallFunction(ThriftMessage,
                                                   "ssiOOi",
                                                   method,
                                                   smtype,
                                                   seqid,
                                                   msg,
                                                   header,
                                                   msglen));
    PyTuple_SetItem(result, 1, Py_BuildValue("i", msglen));
    
    free(method);
    free(data);

    return result;
}

static char *
readMessageBegin(char *data, char *proto, int *type, int *seqid)
{
    int sz;
    char *name;
    PyObject *globalVars;
    
    globalVars = PyThreadState_GetDict();

    if (!strcmp(proto,"TBinaryProtocol")) {
        sz = readI32(data, proto);
        if (sz < 0) {
            int version = sz & BINARY_PROTOCOL_VERSION_MASK;
            if (version != BINARY_PROTOCOL_VERSION_1) {
                PyErr_SetString(PyExc_ValueError,
                                "Bad version in readMessageBegin");
                return NULL;
            }
            *type = sz & BINARY_PROTOCOL_TYPE_MASK;
            name = readString(data, proto);
            *seqid = readI32(data, proto);
            return name;
        }
        else {
            int i;
            int byteCounter;
            PyObject *byte;

            byte = PyDict_GetItemString(globalVars,"byteCounter");
            if (!PyArg_Parse(byte, "i", &byteCounter)) {
                return NULL;
            }
            if (byteCounter+sz+5 <= dataLen(data)) {
                name = (char *)malloc(sizeof(char)*(sz+1));
                i = 0;
                do {
                    name[i] = readByteC(data);
                    i++;
                } while (i != sz && name[i-1] != '\0');
                name[i] = '\0';
                *type = readByteC(data);
                *seqid = readI32(data, proto);
                return name;
            }
        }
    }
    else if (!strcmp(proto, "TJSONProtocol")) {
        PyObject *contextStack;
        PyObject *py_size;
        Py_ssize_t contextTop;
        
        contextStack = PyDict_GetItemString(globalVars, "contextStack");
        py_size = PyDict_GetItemString(globalVars, "contextStackTop");
        if (!PyArg_Parse(py_size, "n", &contextTop)) {
                return NULL;
        }
        PyDict_SetItemString(globalVars,"first",Py_BuildValue("i",1));
        PyDict_SetItemString(globalVars,"contextType",Py_BuildValue("i",BASECONTEXT));
        PyDict_SetItemString(globalVars,"colon",Py_BuildValue("i",0));
        if (contextTop+1 == PyList_Size(contextStack)) {
            PyList_Append(contextStack, Py_BuildValue("(iii)", BASECONTEXT, 1, 0));
            contextTop++;            
        }
        else {
            contextTop++;
            PyList_SetItem(contextStack, contextTop, Py_BuildValue("(iii)", BASECONTEXT, 1, 0));
        }
        PyDict_SetItemString(globalVars,"contextStack",contextStack);
        PyDict_SetItemString(globalVars,"contextStackTop",Py_BuildValue("n",contextTop));

        readJSONArrayStart(data);
        if (readJsonInteger(data) != JSONVERSION) {
            PyErr_SetString(PyExc_ValueError,
                            "Message contained bad version");
            return NULL;
        }
        name = readJSONString(data, 0);
        *type = readJsonInteger(data);
        *seqid = readJsonInteger(data);
        return name;

    }
    if (!strcmp(proto, "TCompactProtocol")) {
        int ver_type;
        int version;
        int state;
        PyObject *py_state;

        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return NULL;
        }
        if (state != CLEAR) {
            PyErr_SetString(PyExc_ValueError, "Bad state");
            return NULL;
        }
        int proto_id = readByteC(data);
        if (proto_id != COMPACT_PROTOCOL_ID) {
            PyErr_SetString(PyExc_ValueError, "Bad protocol ID");
            return NULL;
        }
        ver_type = readByteC(data);
        *type = (ver_type>>TYPE_SHIFT_AMOUNT) & TYPE_BITS;
        version = ver_type & COMPACT_VERSION_MASK;
        if (version != COMPACT_VERSION) {
             PyErr_SetString(PyExc_ValueError,
                            "Bad compact message version");
             return NULL;
        }
        *seqid = readVarint(data);
        return readBinary(data, proto);
    }
    return NULL;
}

static void
readMessageEnd(char *data, char *proto)
{
    if (!strcmp(proto, "TJSONProtocol")) {
        readJSONArrayEnd(data);
    }
    else if (!strcmp(proto, "TCompactProtocol")) {
        int state;
        PyObject *globalVars;
        PyObject *py_state;
        PyObject *py_size;
        Py_ssize_t containersTop;

        globalVars = PyThreadState_GetDict();
        py_size = PyDict_GetItemString(globalVars, "containersStackTop");
        if (!PyArg_Parse(py_size, "n", &containersTop)) {
                return ;
        }
        py_state = PyDict_GetItemString(globalVars,"state");
        if (!PyArg_Parse(py_state, "i", &state)) {
            return ;
        }
        if (state != CLEAR) {
            PyErr_SetString(PyExc_ValueError, "Wrong State");
            return ;
        }
        if (containersTop != -1) {
            PyErr_SetString(PyExc_ValueError,
                            "structs stack is not empty");
            return ;
        }
    }
}

static char *const
message_type_to_str(int mtype)
{
        switch (mtype) {
            case CALL:
            {
                return "call";
            }
            case REPLY:
            {
                return "reply";
            }
            case EXCEPTION:
            {
                return "exception";
            }
            case ONEWAY:
            {
                return "oneway";
            }
            default :
            {
                return "unknown";
            }
        }
}

//just some functions to make C modules callable from python.
static PyMethodDef 
thrift_message_read_module_Methods[] = {
  {"read", (PyCFunction)thrift_message_read_module, METH_VARARGS | METH_KEYWORDS},
  {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
initthrift_message_read_module(void)
{
    (void) Py_InitModule("thrift_message_read_module", thrift_message_read_module_Methods);
}