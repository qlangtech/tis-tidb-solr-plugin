cmake_minimum_required(VERSION 3.17)
project(tis_solr_plugin C)
# https://www.jetbrains.com/help/clion/quick-cmake-tutorial.html#targets-configs
#remote_get.c main.c remote_get.c
add_executable(tis_solr_plugin threadqueue.c  remote_get.c)
# /usr/local/curl/lib/libcurl.a /usr/local/curl/lib/libcurl.la /usr/local/curl/lib/libcurl.dylib
target_link_libraries(tis_solr_plugin  /usr/local/curl/lib/libcurl.dylib  )
# how to install libcurl
# https://stackoverflow.com/questions/41580504/how-to-install-libcurl-under-macos10-12-and-use-for-xcode
include_directories(/usr/local/curl/include/curl/)
#include(/Users/mozhenghua/Downloads/curl-7.75.0/lib/)
#add_library(/usr/local/curl/lib/)
#
#link_directories(demo /Users/mozhenghua/Downloads/curl-7.75.0/lib/)
set(CMAKE_C_STANDARD 99)
#main.c
