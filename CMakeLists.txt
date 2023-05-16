cmake_minimum_required(VERSION 3.21)
project(LSMKV)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(LSMKV
        correctness.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        MurmurHash3.h
        skip.h
        skip.cpp
        sstable.h
        test.h
        utils.h)
