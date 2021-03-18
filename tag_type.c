
#include "tis_type.c"

//
// Created by baisui on 2021/3/12.
//
//enum tagType {
#ifndef TIS_TUPLE_VAL_TYPE
#define TIS_TUPLE_VAL_TYPE
const byte Null = 0;
const byte BOOL_TRUE = 1;
const byte BOOL_FALSE = 2;
const byte BYTE = 3;
const byte SHORT = 4;
const byte DOUBLE = 5;
const byte INT = 6;
const byte LONG = 7;
const byte FLOAT = 8;
const byte DATE = 9;
const byte MAP = 10;
const byte SOLRDOC = 11;
const byte SOLRDOCLST = 12;
const byte BYTEARR = 13;
const byte ITERATOR = 14;
//this is a special tag signals an end. No value is associated with it
const byte END = 15;
const byte SOLRINPUTDOC = 16;
const byte MAP_ENTRY_ITER = 17;
const byte ENUM_FIELD_VALUE = 18;
const byte MAP_ENTRY = 19;
const byte UUID = 20; // This is reserved to be used only in LogCodec
// types that combine tag + length (or other info) in a single byte
const byte TAG_AND_LEN = 1 << 5;
const byte STR = 1 << 5;
const byte SINT = 2 << 5;
const byte SLONG = 3 << 5;
const byte ARR = 4 << 5; //
const byte ORDERED_MAP = 5 << 5; // SimpleOrderedMap (a NamedList subclass, and more common)
const byte NAMED_LST = 6 << 5; // NamedList
const byte EXTERN_STRING = 7 << 5;
#endif