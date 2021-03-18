//
// Created by 莫正华 on 2021/3/18.
//
#include <stdio.h>
#include <sys/select.h>
 void sleep_ms(unsigned int secs) {
    struct timeval tval;
    tval.tv_sec = secs / 1000;
    tval.tv_usec = (secs * 1000) % 1000000;
    select(0, NULL, NULL, NULL, &tval);
}