//
// Created by 莫正华 on 2021/3/16.
//
#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/curlbuild.h>
#include <string.h>
#include "tis_type.c"
#include "java_bin_tuple_stream_parser.c"
#include <pthread.h>
#include <unistd.h>
#include "utils.c"
#include "threadqueue.h"

// C语言实现循环缓冲队列
// https://github.com/charlesdobson/CircularBuffer.git

typedef struct _DataNode {
    byte *content;
    uint length;
    struct _DataNode *next;
} DataNode;

typedef struct _TupleReadContext {
    // 接收curl收到的数据

    JavaBinTupleStreamParser *parser;

    struct threadqueue *queue;
    struct threadmsg msg;
    // 是否已经开始读取tuple流？
    //  bool hasGetTupleStream;
//    DataNode *tmpData;
//    // 写到的节点
//    DataNode *writeCurrent;
//    // 读到的节点
//    DataNode *readCurrent;
//
//    pthread_mutex_t lock;
} TupleReadContext;


static DataNode *createDataNode(const byte *temp, uint bufferSize) {
    DataNode *d = (DataNode *) malloc(sizeof(DataNode));
    byte *newb = (byte *) malloc(sizeof(byte) * bufferSize);
    memcpy(newb, temp, bufferSize);
    d->content = newb;
    d->length = bufferSize;
    d->next = NULL;
    return d;
}

/**
 * 每次从curl 返回的一段内存存放到一个新的block中
 * @param buffer
 * @param size
 * @param nmemb
 * @param userp
 * @return
 */
static size_t write_data(void *buffer, size_t size, size_t nmemb, void *userp) {
    TupleReadContext *readCtx = (TupleReadContext *) userp;
    byte *temp = (byte *) buffer;
    uint bufferSize = size * nmemb;
    //DataNode *lastData;
    DataNode *d = createDataNode(temp, bufferSize);
    thread_queue_add(readCtx->queue, d, 0);
//    if (readCtx->tmpData == NULL) {
//        DataNode *d = createDataNode(temp, bufferSize);
//        readCtx->tmpData = d;
//        readCtx->writeCurrent = d;
//    } else {
//        // find last
//        readCtx->writeCurrent->next = createDataNode(temp, bufferSize);
//        readCtx->writeCurrent = readCtx->writeCurrent->next;
////        lastData = readCtx->tmpData;
////        while (lastData != NULL) {
////            if (lastData->next != NULL) {
////                lastData = lastData->next;
////            } else {
////                lastData->next = createDataNode(temp, bufferSize);
////                break;
////            }
////        }
//    }
//    pthread_mutex_unlock(&readCtx->lock);
//    printf("release pthread_mutex_unlock");
    return bufferSize;
//    pthread_mutex_lock(&readCtx->lock);
//
//    memcpy(readCtx->parser->buffer, temp, size * nmemb);
//
//    pthread_mutex_unlock(&readCtx->lock);
//
//
//    readCtx->parser->buffer = temp;
//    readCtx->parser->bufferLength = size * nmemb;
//
    // if (!readCtx->hasGetTupleStream) {
//        if (!getTupleStream(readCtx->parser)) {
//            printf("getTupleStream faild\n");
//            exit(1);
//        }
//        readCtx->hasGetTupleStream = true;
//    }
//
//
//    // readCtx->buffer
//    //  *memcpy(void *dst, const void *src, size_t n)
//    // memcpy(readCtx->buffer, temp, size * nmemb);
//
//    Object *nextTuple;
//    Object *val;
//    int colCount = 6;
//    while (hasNextTuple(readCtx->parser)) {
//        nextTuple = readCtx->parser->nextTuple;
//        if (nextTuple == NULL) {
//            printf("nextTuple can not be null");
//            exit(1);
//        }
//
//        for (int colIndex = 0; colIndex < colCount; colIndex++) {
//            val = &nextTuple[colIndex];
//            val->typeProc.process(val);
//            printf(",");
//        }
//        printf("\n");
//
//        free(nextTuple);
//    }

    //mem->size++;
    //printf("%s \n=============\n  size:%d", buffer, mem->size++);

}


/**
 * 从文件中读取内容到缓存buffer中
 * @param parser
 * @return
 */
int readBufferFromCurlResponse(JavaBinTupleStreamParser *parser) {
    printf("================offset:%d,bufferLen:%d,allBuffer len:%d\n", parser->bufferOffset, parser->bufferLength,
           FILE_READ_BUFFER_BYTE_ITEMS * FILE_READ_BUFFER_BYTE_SIZE);

    if (parser->bufferOffset > parser->bufferLength) {
        printf("bufferOffset can not big");
        exit(1);
    }
    // MemoryStruct *chunk = (MemoryStruct *) parser->source;
    // parser->buffer
    //  int bufferRead = fread(parser->buffer, FILE_READ_BUFFER_BYTE_SIZE, FILE_READ_BUFFER_BYTE_ITEMS, file);
    TupleReadContext *tupleReadContext = (TupleReadContext *) parser->source;


    if (thread_queue_get(tupleReadContext->queue, NULL, &tupleReadContext->msg) != 0) {
        printf(" can not msg from thread queue\n");
        exit(1);
    }

//    DataNode *curr = tupleReadContext->readCurrent;
//    if (curr == NULL) {
//        curr = tupleReadContext->tmpData;
//    }
//
//    if (curr == NULL) {
//        printf("curr is null\n");
//        exit(1);
//    }
    DataNode *curr = tupleReadContext->msg.data;

    uint len = curr->length;
    memcpy(parser->buffer, curr->content, len);
    // FIXME the content shall be free
    free(curr->content);
    free(curr);
//    aa:
//    if (curr->next == NULL) {
//        printf("curr->next can not be null");
//        sleep_ms(3);
//        //exit(1);
//        goto aa;
//    }
//    tupleReadContext->readCurrent = curr->next;
    //free(curr);

    return len;
}

//static void lastTupleStartPosOffset(uint offset, struct _JavaBinTupleStreamParser *parser) {
//    TupleReadContext *tupleReadContext = (TupleReadContext *) parser->source;
//    tupleReadContext->lastTupleStartPosOffset = offset;
//}

//void callback_thread() {
//    sleep(7);
//    printf("hellworld val\n");
//}

void executeCurl(void *curl) {

    // printf("start executeCurl");
    // 执行HTTP GET操作
    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        fprintf(stderr, "\ncurl_easy_perform() failed: %s\n", curl_easy_strerror(res));
    }
}

void processResult(TupleReadContext *tupleReadContext) {
    JavaBinTupleStreamParser *p = tupleReadContext->parser;
//    if (pthread_mutex_lock(&(tupleReadContext->lock)) != 0) {
//        printf(" can not get the pthread_mutex_lock in child thread\n");
//        exit(1);
//    }
    printf(" get the pthread_mutex_lock\n");
    if (!getTupleStream(p)) {
        printf("getTupleStream faild\n");
        exit(1);
    }

    Object *nextTuple;
    Object *val;
    int colCount = 7;
    uint count = 0;
    while (hasNextTuple(p)) {
        nextTuple = p->nextTuple;
        if (nextTuple == NULL) {
            printf("nextTuple can not be null");
            exit(1);
        }

        for (int colIndex = 0; colIndex < colCount; colIndex++) {
            val = &nextTuple[colIndex];
            if (val->typeProc == NULL) {
                printf("val->typeProc can not be null");
                exit(1);
            }
            val->typeProc->process(val);
            printf(",");
        }
        printf("\n");
        ++count;
        free(nextTuple);
    }
    printf("allcount:%d", count);
    //pthread_mutex_unlock(&(tupleReadContext->lock));
}

/**
 * example:https://curl.se/libcurl/c/getinmemory.html
 * @return
 */
int main() {
    int a = 999;


    // while (i++ < 1000) {
    int start = clock();

    void *curl = curl_easy_init();

    JavaBinTupleStreamParser parser, *p = &parser;

//    tupleReadContext.tmpData = NULL;
//    tupleReadContext.readCurrent = NULL;
//    tupleReadContext.writeCurrent = NULL;

    struct threadqueue queue;
    thread_queue_init(&queue);
    TupleReadContext tupleReadContext = {p, &queue};


    byte *fileBufferPointer = (byte *) malloc(FILE_READ_BUFFER_BYTE_ITEMS * FILE_READ_BUFFER_BYTE_SIZE);
    p->stringsList = NULL;
    p->nextTuple = NULL;
    p->bufferLength = 0;
    p->buffer = fileBufferPointer;
    p->readFromSource = readBufferFromCurlResponse;
    p->source = &tupleReadContext;
    // p->lastTupleStartPosOffset = lastTupleStartPosOffset;
    // getTupleStream(p);
    string applyUrl = "http://192.168.28.200:8080/solr/search4employee4local/export?fl=emp_no%2Cbirth_date%2Cfirst_name%2Clast_name%2Cgender%2Chire_date%2Cnum_rate%2Cnum_double%2Cnum_decimal%2C_version_&q=*:*&sort=emp_no%20asc&wt=javabin";

    // 设置URL
    curl_easy_setopt(curl, CURLOPT_URL, applyUrl);
    // 设置接收数据的处理函数和存放变量
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
    // curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, CURL_MAX_WRITE_SIZE * 2);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &tupleReadContext);
    //  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 6);



    // pthread_mutex_init(&tupleReadContext.lock, NULL);
    pthread_t thing1;
    // pthread_mutex_lock(&tupleReadContext.lock);
    pthread_create(&thing1, NULL, &processResult, (void *) &tupleReadContext);
    executeCurl(curl);



    // 执行HTTP GET操作
//    CURLcode res = curl_easy_perform(curl);
//    if (res != CURLE_OK) {
//        fprintf(stderr, "\ncurl_easy_perform() failed: %s\n", curl_easy_strerror(res));
//    }
//    DataNode *data = tupleReadContext.tmpData;
//    int size = 0;
//    while (data != NULL) {
//        printf("data length:%d\n", data->length);
//        size++;
//        data = data->next;
//    }
    //printf("all buffer size:%d", size);

//    pthread_mutex_lock(&readCtx->lock);
//
//    memcpy(readCtx->parser->buffer, temp, size * nmemb);
//
//    pthread_mutex_unlock(&readCtx->lock);




    printf("i am waitting");
    pthread_join(thing1, NULL);
    // 接受数据存放在out中，输出之
    //cout << out.str() << endl;
    curl_easy_cleanup(curl);
    printf("\nconsume:%fs", (double) (clock() - start) / CLOCKS_PER_SEC);
    //}
}


