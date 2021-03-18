
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include "tis_type.c"
#include "tag_type.c"

const int FILE_READ_BUFFER_BYTE_ITEMS = 31024;
const int FILE_READ_BUFFER_BYTE_SIZE = sizeof(byte);
//const uint INT_MAX = (~(uint) 0 / 2);

struct _Object;
typedef enum _objType {
    type_string,
    type_int,
    type_null,
    type_bool,
    type_byte,
    type_long,
    type_float,
    type_double,
    type_tuple
} ObjType;

typedef struct _ObjectTypeProcess {
    ObjType type;

    void (*process)(struct _Object *);
} ObjectTypeProcess;


typedef struct _Object {
    ObjectTypeProcess *typeProc;
    union {
        int intVal;
        long longVal;
        string strVal;
        bool boolVal;
        byte byteVal;
        float floatVal;
        double doubleVal;
    };
    bool isEndObj;
} Object;

typedef struct _JavaBinTupleStreamParser {
    // 被处理的文件
//    FILE *processFile;

    void *source;

    int (*readFromSource)(struct _JavaBinTupleStreamParser *parser);

    // 记录最后一个tuple开始的位点
    // void (*lastTupleStartPosOffset)(uint offset, struct _JavaBinTupleStreamParser *parser);

    // 文件读写的缓冲区
    byte *buffer;
    // the offset of read buffer block
    uint bufferOffset;
    // 读到文件缓冲区的长度，在要读到文件末尾时候，此值会小于缓冲区的长度
    uint bufferLength;
    byte tagByte;
    // 本次结果命中条数
    uint numfound;

    uint objectSize;
    uint arraySize;
    ushort status;
    string *stringsList;
    ushort stringsListLen;
    Object *nextTuple;
} JavaBinTupleStreamParser;


void stringProcess(Object *val) {
    printf("%s", val->strVal);
}

void intProcess(Object *val) {
    printf("%d", val->intVal);
}

void nullProcess(Object *val) {
    printf("null");
}

void boolProcess(Object *val) {
    printf("%d", val->boolVal);
}

void byteProcess(Object *val) {
    printf("(byte)%d", val->byteVal);
}

void longProcess(Object *val) {
    printf("(long)%ld", val->longVal);
}

void tupleProcess(Object *val) {
    printf("tupleProcess is not support");
    exit(1);
}

void floatProcess(Object *val) {
    printf("(float)%f", val->floatVal);
}

void floatDouble(Object *val) {
    printf("(double)%f", val->doubleVal);
}


const ObjectTypeProcess _objProcess[]
        = {
                {type_string, stringProcess},
                {type_int,    intProcess},
                {type_null,   nullProcess},
                {type_bool,   boolProcess},
                {type_byte,   byteProcess},
                {type_long,   longProcess},
                {type_float,  floatProcess},
                {type_double, floatDouble},
                {type_tuple,  tupleProcess}
        };


static Object readObject(JavaBinTupleStreamParser *parser);

Object readSmallInt(JavaBinTupleStreamParser *pParser);

Object readAsMap(JavaBinTupleStreamParser *pParser);

int readVInt(JavaBinTupleStreamParser *pParser);

//void initRead()  {
//    _, b, err := parser.fis.readByte()
//    if err != nil {
//        return false, err
//    }
//    if b != 0x2 {
//        return false, errors.New(fmt.Sprint("Invalid version (expected 2, but ", int32(b), ") or the data in not in 'javabin' format"))
//    }
//    return true, nil
//}








//int refill(JavaBinTupleStreamParser *parser, uint *length) {
//    int bufferRead = fread(parser->buffer, FILE_READ_BUFFER_BYTE_SIZE, FILE_READ_BUFFER_BYTE_ITEMS,
//                           parser->processFile);
//    parser->bufferOffset = 0;
//    parser->bufferLength = bufferRead;
//    return bufferRead;
//}

/**
 * try specific byte length from buffer
 * @param parser
 */
void tryRead(JavaBinTupleStreamParser *parser, int length) {
    uint offset = parser->bufferOffset;
    if (offset > parser->bufferLength) {
        printf("offset %d > parser->bufferLength(%d) err,try read:%d", offset, parser->bufferLength, length);
        exit(1);
    }
    if ((offset + length - 1) >= parser->bufferLength) {
        int bufferRead;
        if (length > 1) {
            // if the buffer has any unread byte,still shall be retain
            int index = 0;
            byte *origin = parser->buffer;
            uint i = 0;
            for (i = offset; i < parser->bufferLength; i++) {
                parser->buffer[index++] = parser->buffer[i];
            }
            parser->buffer = (parser->buffer + index);

            bufferRead = parser->readFromSource(parser);

//            bufferRead = fread(parser->buffer, FILE_READ_BUFFER_BYTE_SIZE, FILE_READ_BUFFER_BYTE_ITEMS,
//                               parser->processFile);
            bufferRead += index;
            parser->buffer = origin;
        } else {
            bufferRead = parser->readFromSource(parser);
//            bufferRead = fread(parser->buffer, FILE_READ_BUFFER_BYTE_SIZE, FILE_READ_BUFFER_BYTE_ITEMS,
//                               parser->processFile);
        }


        parser->bufferOffset = 0;
        parser->bufferLength = bufferRead;

        if (bufferRead < 1) {
            printf("bufferRead can not small than 1");
            exit(1);
        }
    }
    offset = parser->bufferOffset;
    if ((offset + length - 1) >= parser->bufferLength) {
        tryRead(parser, length);
        printf("can not satifact\n");
        printf("offset %d > parser->bufferLength(%d) err,try read:%d", offset, parser->bufferLength, length);
        //   exit(1);
    }
}

byte readByte(JavaBinTupleStreamParser *parser) {
    tryRead(parser, 1);
    return parser->buffer[parser->bufferOffset++];
}


int readSize(JavaBinTupleStreamParser *parser) {
    int sz = parser->tagByte & 0x1f;
    if (sz == 0x1f) {
        int v = readVInt(parser);
        sz += v;
    }
    return sz;
}

bool isObjectType(JavaBinTupleStreamParser *parser) {
    byte tagByte = readByte(parser);
    parser->tagByte = tagByte;
    if ((parser->tagByte >> 5) == (ORDERED_MAP >> 5)
        || (parser->tagByte >> 5) == (NAMED_LST >> 5)
            ) {
        int sz = readSize(parser);
        parser->objectSize = sz;
        return true;
    }

    if (parser->tagByte == MAP) {
        int val = readVInt(parser);
        parser->objectSize = val;
        return true;
    }

    if (parser->tagByte == MAP_ENTRY_ITER) {
        parser->objectSize = INT_MAX;
        return true;
    }

    return parser->tagByte == SOLRDOCLST;
}

Object readSolrDocumentAsMap(JavaBinTupleStreamParser *parser) {
    printf("readSolrDocumentAsMap is not supported");
    exit(1);
    //return NULL;
}

int readVInt(JavaBinTupleStreamParser *parser) {
    byte b = readByte(parser);
    int i = (b & 0x7F);
    for (ushort shift = 7; (b & 0x80) != 0; shift += 7) {
        b = readByte(parser);
        i = (b & 0x7F) << shift | i;
    }
    return i;
}

Object readSmallInt(JavaBinTupleStreamParser *parser) {

    int v = parser->tagByte & 0x0F;
    if ((parser->tagByte & 0x10) != 0) {
        int vint = readVInt(parser);
        v = (vint << 4) | v;
    }
    Object result = {&_objProcess[type_int]};
    result.intVal = v;
    return result;
}


Object readAsMap(JavaBinTupleStreamParser *parser) {
    printf("readAsMap is not support");
    exit(1);
    //return NULL;
}

Object readVal(JavaBinTupleStreamParser *parser) {
    byte tagByte = readByte(parser);
    parser->tagByte = tagByte;
    return readObject(parser);
}

Object readMapIter(JavaBinTupleStreamParser *parser) {

    Object result = {&_objProcess[type_tuple]};
    // if (result.vals == NULL) {
    // result.vals =
    parser->nextTuple = (Object *) malloc(20 * sizeof(Object));


    Object key;
    Object val;
    int index = 0;
    while (true) {
        key = readVal(parser);
        if (key.isEndObj) {
            break;
        }
        val = readVal(parser);

        if (key.typeProc->type != type_string) {
            printf("key typeProc must be typeProc of type_string,but now is,%d:", key.typeProc->type);
            exit(1);
        }

        //printf("%s\n", key.strVal);

//        if (val.typeProc != type_tuple) {
//            printf("val typeProc must be typeProc of type_tuple");
//            exit(1);
//        }

        parser->nextTuple[index++] = val;
    }

    return result;
}


Object readMapEntry(JavaBinTupleStreamParser *parser) {
    printf("readMapEntry is not support");
    exit(1);
}

Object readEnumFieldValue(JavaBinTupleStreamParser *parser) {
    printf("readEnumFieldValue is not support");
    exit(1);
}

Object readSolrInputDocument(JavaBinTupleStreamParser *parser) {
    printf("readSolrInputDocument is not support");
    exit(1);
}

Object readIterator(JavaBinTupleStreamParser *parser) {
    printf("readIterator is not support");
    exit(1);
}

Object readByteArray(JavaBinTupleStreamParser *parser) {
    printf("readByteArray is not support");
    exit(1);
}

Object readSolrDocumentList(JavaBinTupleStreamParser *parser) {
    printf("readShort is not support");
    exit(1);
}

Object readSolrDocument(JavaBinTupleStreamParser *parser) {
    printf("readSolrDocument is not support");
    exit(1);
}

Object readMap(JavaBinTupleStreamParser *parser) {
    printf("readMap is not support");
    exit(1);
}

Object readShort(JavaBinTupleStreamParser *parser) {
    printf("readShort is not support");
    exit(1);
}

Object readDouble(JavaBinTupleStreamParser *parser) {
    tryRead(parser, 8);
//    printf("readDouble is not support");
//    exit(1);
    Object result = {&_objProcess[type_double]};
    unsigned char *p = (unsigned char *) &result.doubleVal;
    p[0] = parser->buffer[parser->bufferOffset++];
    p[1] = parser->buffer[parser->bufferOffset++];
    p[2] = parser->buffer[parser->bufferOffset++];
    p[3] = parser->buffer[parser->bufferOffset++];
    p[4] = parser->buffer[parser->bufferOffset++];
    p[5] = parser->buffer[parser->bufferOffset++];
    p[6] = parser->buffer[parser->bufferOffset++];
    p[7] = parser->buffer[parser->bufferOffset++];
    return result;
}

Object readFloat(JavaBinTupleStreamParser *parser) {
//    printf("readFloat is not support");
//    exit(1);
    tryRead(parser, 4);
    Object result = {&_objProcess[type_float]};

    unsigned char *p = (unsigned char *) &result.floatVal;
    p[0] = parser->buffer[parser->bufferOffset++];
    p[1] = parser->buffer[parser->bufferOffset++];
    p[2] = parser->buffer[parser->bufferOffset++];
    p[3] = parser->buffer[parser->bufferOffset++];
    return result;
}

Object readInt(JavaBinTupleStreamParser *parser) {
//    printf("readInt is not support");
//    exit(1);
    tryRead(parser, 4);
    int val;
    byte *b = (byte *) &val;
    for (int i = 0; i < 4; i++) {
        b[i] = parser->buffer[parser->bufferOffset++];
    }
//    = parser->buffer[parser->bufferOffset++] << 24 | parser->buffer[parser->bufferOffset++] << 16
//              | parser->buffer[parser->bufferOffset++] << 8 | parser->buffer[parser->bufferOffset++];

    Object result = {&_objProcess[type_int]};
    result.intVal = val;
    return result;
}

Object readLong(JavaBinTupleStreamParser *parser) {
//    printf("readLong is not support");
//    exit(1);
    tryRead(parser, 8);
    long val;
    byte *b = (byte *) &val;
    for (int i = 0; i < 8; i++) {
        b[i] = parser->buffer[parser->bufferOffset++];
    }

//    b[1] = parser->buffer[parser->bufferOffset++];
//    b[0] = parser->buffer[parser->bufferOffset++];
//    b[0] = parser->buffer[parser->bufferOffset++];
//    b[0] = parser->buffer[parser->bufferOffset++];
//    b[0] = parser->buffer[parser->bufferOffset++];
//    b[0] = parser->buffer[parser->bufferOffset++];
//    b[0] = parser->buffer[parser->bufferOffset++];
//
//    long val = ((long) parser->buffer[parser->bufferOffset++] << 56) |
//               ((long) parser->buffer[parser->bufferOffset++] << 48)
//               | ((long) parser->buffer[parser->bufferOffset++] << 40) |
//               ((long) parser->buffer[parser->bufferOffset++] << 32) |
//               (parser->buffer[parser->bufferOffset++] << 24) | (parser->buffer[parser->bufferOffset++] << 16)
//               | (parser->buffer[parser->bufferOffset++] << 8) | (parser->buffer[parser->bufferOffset++]);

    Object result = {&_objProcess[type_long]};
    result.intVal = val;
    return result;
}

Object readDate(JavaBinTupleStreamParser *parser) {
//    printf("readDate is not support");
//    exit(1);
    return readLong(parser);
}

string _readStr(JavaBinTupleStreamParser *parser, ushort sz) {
    tryRead(parser, sz);
    char *b;
    b = (char *) malloc(sz + 1);

    for (int i = 0; i < sz; i++) {
        b[i] = parser->buffer[parser->bufferOffset++];
    }
    b[sz] = '\0';
    //  printf("_readStr:%s", b);
    return b;

//  return "hello";

//if dis.vBytes == nil || len(dis.vBytes) < sz {
//bytes = make([]byte, sz)
//}
//
//dis.readFully(bytes, 0, sz)
//
//// utf8 []byte, offset int, len int, out strings.Builder, outOffset int
//var b strings.Builder
//// b.Grow(sz)
//UTF8toUTF16(bytes, 0, sz, &b, 0)
////utf8 转utf16
////https://www.cnblogs.com/lianggx6/p/12714797.html
////if (stringCache != null) {
////	return stringCache.get(bytesRef.reset(bytes, 0, sz));
////} else {
////	arr.reset()
////	ByteUtils.UTF8toUTF16(bytes, 0, sz, arr)
////	return arr.toString();
////}
//
//return b.String(), nil
//}
//
}

static string readUtf8(JavaBinTupleStreamParser *parser, ushort sz) {

    return _readStr(parser, sz);
}

string readStr(JavaBinTupleStreamParser *parser) {
    ushort sz = readSize(parser);

    return readUtf8(parser, sz);
}

Object readExternString(JavaBinTupleStreamParser *parser) {
    ushort idx = readSize(parser);
    Object result = {&_objProcess[type_string]};
    if (idx != 0) {
        result.strVal = parser->stringsList[idx - 1];
        return result;
    } else {
        byte b = readByte(parser);
        parser->tagByte = b;
        string str = readStr(parser);
        string *newStringList = NULL;
        if (parser->stringsList == NULL) {
            newStringList = (string *) malloc(1 * sizeof(string));
            newStringList[0] = str;
            parser->stringsList = newStringList;
            parser->stringsListLen = 1;
        } else {
            ushort length = parser->stringsListLen; //sizeof(parser->stringsList) / sizeof(string);
            newStringList = (string *) malloc((length + 1) * sizeof(string));
            for (int i = 0; i < length; i++) {
                newStringList[i] = parser->stringsList[i];
            }
            printf("=================length:%d,[0]:%s,newStr:%s\n", length, parser->stringsList[0], str);
            newStringList[length] = str;
            parser->stringsListLen++;
            free(parser->stringsList);
            parser->stringsList = newStringList;
        }
        result.strVal = str;
        return result;
    }
}

Object readNamedList(JavaBinTupleStreamParser *parser) {
    printf("readNamedList is not support");
    exit(1);
}

Object readOrderedMap(JavaBinTupleStreamParser *parser) {
    printf("readOrderedMap is not support");
    exit(1);
}

Object readArray(JavaBinTupleStreamParser *parser) {
    printf("readArray is not support");
    exit(1);
}

//Object readStr(JavaBinTupleStreamParser *parser) {
//    printf("readStr is not support");
//    exit(1);
//}

long readVLong(JavaBinTupleStreamParser *parser) {
    byte b = readByte(parser);
    long i = (b & 0x7F);
    for (ushort shift = 7; (b & 0x80) != 0; shift += 7) {
        b = readByte(parser);

        i = i | (((b) & 0x7F) << shift);
    }
    return i;
}

Object readSmallLong(JavaBinTupleStreamParser *parser) {
    long v = (parser->tagByte & 0x0F);
    if ((parser->tagByte & 0x10) != 0) {
        long val = readVLong(parser);

        v = (val << 4) | v;
    }
    Object result = {&_objProcess[type_long]};
    result.longVal = v;
    return result;
}

Object superReadObject(JavaBinTupleStreamParser *parser) {
    // if ((tagByte & 0xe0) == 0) {
    // if top 3 bits are clear, this is a normal tag
    // OK, try typeProc + size in single byte
    Object *val;
    switch (parser->tagByte >> 5) {
        case STR >> 5:
            // return parser.readStr(dis, stringCache, readStringAsCharSeq)
            val = &((Object) {&_objProcess[type_string]});
            val->strVal = readStr(parser);
            return *val;
        case SINT >> 5:
            return readSmallInt(parser);
        case SLONG >> 5:
            return readSmallLong(parser);
        case ARR >> 5:
            return readArray(parser);
        case ORDERED_MAP >> 5:
            return readOrderedMap(parser);
        case NAMED_LST >> 5:
            return readNamedList(parser);
        case EXTERN_STRING >> 5:
            return readExternString(parser);
    }

    switch (parser->tagByte) {
        case MAP_ENTRY_ITER:
            return readMapIter(parser);
        case Null:
            return (Object) {&_objProcess[type_null]};
        case DATE:
            return readLong(parser); //new Date()
        case INT:
            return readInt(parser);
        case BOOL_TRUE:
            val = &((Object) {&_objProcess[type_bool]});
            val->boolVal = true;
            return *val;
        case BOOL_FALSE:
            val = &((Object) {&_objProcess[type_bool]});
            val->boolVal = false;
            return *val;
        case FLOAT:
            return readFloat(parser);
        case DOUBLE:
            return readDouble(parser);
        case LONG:
            return readLong(parser);
        case BYTE:
            val = &((Object) {&_objProcess[type_byte]});
            val->byteVal = readByte(parser);
            return *val;
        case SHORT:
            return readShort(parser);
        case MAP:
            return readMap(parser);
        case SOLRDOC:
            return readSolrDocument(parser);
        case SOLRDOCLST:
            return readSolrDocumentList(parser);
        case BYTEARR:
            return readByteArray(parser);
        case ITERATOR:
            return readIterator(parser);
        case END:
            val = &((Object) {&_objProcess[type_null]});
            val->isEndObj = true;
            return *val;
        case SOLRINPUTDOC:
            return readSolrInputDocument(parser);
        case ENUM_FIELD_VALUE:
            return readEnumFieldValue(parser);
        case MAP_ENTRY:
            return readMapEntry(parser);
    }

    //throw new RuntimeException("Unknown typeProc " + tagByte)

    //return nil, errors.New("Unknown typeProc " + type_string(parser.tagByte))
    printf("Unknown typeProc %d", parser->tagByte);
    exit(1);
}

static Object readObject(JavaBinTupleStreamParser *parser) {
    if (parser->tagByte == SOLRDOC) {
        return readSolrDocumentAsMap(parser);
    }

    bool onlyJsonTypes = true;

    if (onlyJsonTypes) {
        Object *result;
        switch (parser->tagByte >> 5) {
            case SINT >> 5:
                return readSmallInt(parser);
            case ORDERED_MAP >> 5:
                return readAsMap(parser);
            case NAMED_LST >> 5:
                return readAsMap(parser);
        }

        switch (parser->tagByte) {
            case INT:
                return readInt(parser);
            case FLOAT:
                return readFloat(parser);
            case BYTE:
                result = &((Object) {&_objProcess[type_byte]});
                result->byteVal = readByte(parser);
                return *result;
            case SHORT:
                return readShort(parser);
            case DATE:
                return readDate(parser);
            default:
                return superReadObject(parser);
        }
    } else {
        return superReadObject(parser);
    }

}


static bool readTillDocs(JavaBinTupleStreamParser *parser, char *entryKey) {

    bool isObject = isObjectType(parser);
    if (isObject) {
        if (parser->tagByte == SOLRDOCLST) {
            readVal(parser); // this is the metadata, throw it away
            parser->tagByte = readByte(parser);
//            if _, b, err := parser.fis.readByte(); err == nil {
//                parser.tagByte = b
//            } else {
//                return false, err
//            }

            int sz = readSize(parser);
            parser->arraySize = sz;
            //fmt.Println(parser.arraySize)
            return true;
        }
        Object obj, *k;
        byte b;
        ushort sz;
        for (uint i = parser->objectSize; i > 0; i--) {
            obj = readVal(parser);
            k = &obj;
            if (k->isEndObj) {
                break;
            }
            if (k->typeProc->type == type_string && strcmp(k->strVal, "docs") == 0) {
                b = readByte(parser);
                parser->tagByte = b;
                if (parser->tagByte == ITERATOR) {
                    return true;
                }
                //docs must be an iterator or
                if ((parser->tagByte >> 5) == ARR >> 5) { // an array
                    sz = readSize(parser);
                    parser->arraySize = sz;
                    return true;
                }
                return false;
            } else {

                if (k->typeProc->type != type_string) {
                    printf("k->typeProc must be type_string,but now is %u", k->typeProc->type);
                    exit(1);
                }
                char *eKey = k->strVal;

                if (readTillDocs(parser, eKey)) {
                    return true;
                }
            }
        }
    } else {
        Object v = readObject(parser);
        Object *val = &v;
        if ((val->typeProc->type == type_int) && strcmp(entryKey, "numFound") == 0) {
            parser->numfound = val->intVal;
        }
        if ((val->typeProc->type == type_int) && strcmp(entryKey, "status") == 0) {
            parser->status = val->intVal;
        }
        return false;
    }

    return false;
}

extern bool getTupleStream(JavaBinTupleStreamParser *p) {
//    // 分配一个缓冲区
//    byte *fileBufferPointer = malloc(FILE_READ_BUFFER_BYTE_ITEMS * FILE_READ_BUFFER_BYTE_SIZE);
//    p->buffer = fileBufferPointer;
    int bufferOffset = 0;
    // JavaBinTupleStreamParser parser = {fp, fileBufferPointer, bufferOffset, 0, 0}, *p = &parser;


    p->bufferOffset = bufferOffset;

    p->arraySize = INT_MAX;
    // p->bufferLength = 0;

    //  tryRead(p, 1);
    // InitRead
//    if (bufferRead < 1) {
//        printf("bufferRead < 1");
//        exit(1);
//    }
    // p->bufferLength = bufferRead;
    byte token = readByte(p); //fileBufferPointer[bufferOffset++];
    if (token != 0x2) {
        printf("Invalid version (expected 2, but %d ) or the data in not in 'javabin' format", token);
        //exit(1);
        return false;
    }

    if (!readTillDocs(p, NULL)) {
        printf("readTillDocs faild");
        //exit(1);
        return false;
    }
    return true;
}

extern bool hasNextTuple(JavaBinTupleStreamParser *parser) {
    //  this->hasNext
    if (parser->arraySize == 0) {
        return false;
    }
    // uint offset = parser->bufferOffset;
    Object o = readVal(parser);
    parser->arraySize--;
    if (o.isEndObj) {
        return false;
    }

//    if (parser->lastTupleStartPosOffset != NULL) {
//        parser->lastTupleStartPosOffset(offset, parser);
//    }
    // parser->nextTuple = o.vals;
    //printf("start offset:%d\n", offset);
    return true;
}