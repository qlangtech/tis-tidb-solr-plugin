//
// Created by 莫正华 on 2021/3/17.
//
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include "tis_type.c"

void mainThreadMethod(pthread_mutex_t *lock) {

    sleep(5);
    printf("mainThreadMehod execute\n");
    pthread_mutex_unlock(lock);
}

void processResult(pthread_mutex_t *lock) {
    if (pthread_mutex_lock(lock) != 0) {
        printf("unable to get lock in child thread\n");
        exit(1);
    }
    printf("child execute\n");
    pthread_mutex_unlock(lock);
}

int main() {
    string str[] = {"aa", "bbb", "ccc"};
    string *strp = str;
    int length = sizeof(str) / sizeof(string);
    printf("length:%d,%d", length, sizeof(strp) / sizeof(*strp));
//    uint i = 0;
//    while (i++ < 10000) {
//
//        string *newStringList = (string *) malloc(1 * sizeof(string));
//        newStringList[0] = "hello";
//        printf("1\n");
//       // newStringList[1] = "baisui";
//       // printf("2\n");
//       // newStringList[2] = "xujie";
//       // printf("3\n");
//       // printf("1:%s,2:%s,3:%s\n", newStringList[0], newStringList[1], newStringList[2]);
//        free(newStringList);
//    }
//    pthread_t thing1;
//    pthread_mutex_t lock;
//
//    pthread_mutex_init(&lock, NULL);
//    if (pthread_mutex_lock(&lock) != 0) {
//        printf("unable to get the lock\n");
//        return 1;
//    }
//    pthread_create(&thing1, NULL, &processResult, (void *) &lock);
//    mainThreadMethod(&lock);
//
//    pthread_join(thing1, NULL);
//    pthread_mutex_destroy(&lock);
}