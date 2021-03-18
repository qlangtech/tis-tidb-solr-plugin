

#include <stdio.h>

#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <string.h>
#include "tag_type.c"
#include "java_bin_tuple_stream_parser.c"


//hashmap 实现 https://github.com/sheredom/hashmap.h




/**
 * 从文件中读取内容到缓存buffer中
 * @param parser
 * @return
 */
int readBufferFromFile(JavaBinTupleStreamParser *parser) {
    FILE *file = (FILE *) parser->source;
    int bufferRead = fread(parser->buffer, FILE_READ_BUFFER_BYTE_SIZE, FILE_READ_BUFFER_BYTE_ITEMS, file);
    return bufferRead;
}



int main() {

    printf("side of Object:%lu\n", sizeof(Object));
    string str = "我";
// char s[] = "我";
    // printf("%p ,%d \n", str, strlen(s));
    //  printf("ooo:%x,%x,%x\n",s[0],s[1],s[2]);
    char *fileName = "/opt/misc/tidb/plugin/tis/search4employee2_export_all.bin";
   // char *fileName = "/opt/misc/tidb/plugin/tis/search4employee2_export_all_chinese.bin";
//    FILE *fp;
//    //search4employee2_export_all.bin
//    if ((fp = fopen("/Users/mozhenghua/CLionProjects/tis-solr-plugin/search4employee2_export_all.bin", "rb")) == NULL) {
//        exit(1);
//    }

    int start = clock();

    JavaBinTupleStreamParser p, *parser = &p;

    // 分配一个缓冲区
    byte *fileBufferPointer = malloc(FILE_READ_BUFFER_BYTE_ITEMS * FILE_READ_BUFFER_BYTE_SIZE);
    p->buffer = fileBufferPointer;

    FILE *fp = NULL;
    //search4employee2_export_all.bin
    if ((fp = fopen(fileName, "rb")) == NULL) {
        exit(1);
    }
    parser->readFromSource = readBufferFromFile;
    parser->source = fp;

    getTupleStream(parser);

//    printf("========================\n");
//    for (int i = 0; i < 300; i++) {
//        printf("[%d]%x,", i, parser->buffer[i]);
//    }
//    printf("========================\n");
    int assertNumFound = 150014;
    //int assertNumFound = 1;
    if (parser->numfound != assertNumFound) {
        printf("tupleStreamParser.numfound must be %d,but now is:%d", assertNumFound, parser->numfound);
        exit(1);
    }
    Object *nextTuple;
    Object *val;
    // Object *val;
    int i = 0;
    int colCount = 6;
    while (hasNextTuple(parser)) {
        nextTuple = parser->nextTuple;
        if (nextTuple == NULL) {
            printf("nextTuple can not be null");
            exit(1);
        }

        for (int colIndex = 0; colIndex < colCount; colIndex++) {
            val = &nextTuple[colIndex];
            val->typeProc.process(val);
            printf(",");
        }
        printf("\n");

        free(nextTuple);
        // printf("index:%d,offset:%d\n", i++, parser->bufferOffset);
    }

    free(parser->buffer);
//    int tmp = 0;
//    printf("val\n");
//    int i;
//    int items = 30240;
//    int itemSize = sizeof(int);
//    int *p = malloc(items * sizeof(int));
//    do {
//        //printf("%d", *p);
//        i = fread(p, itemSize, items, fp);
//        printf("%d\n", i);
//
//    } while (!(i < items));
//    free(p);
    printf("consume:%fs", (double) (clock() - start) / CLOCKS_PER_SEC);


    // printf("val2:\n");
    // printf("val2:xxxx %s", i);

//
//    fclose(fp);
    return 0;
}


