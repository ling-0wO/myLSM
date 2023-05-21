//
// Created by Ling_ on 2023/4/12.
//

#ifndef LSMKV_SSTABLE_H
#define LSMKV_SSTABLE_H
#include <cstdint>
#include <vector>
#include <string>
#include <fstream>
#include <algorithm>
#include "MurmurHash3.h"
#include <iostream>
using namespace std;
//bloom size
const uint64_t bloom_size = 10240;
//file path

struct SSTable{
    //index
    std::vector<std::pair<uint64_t, uint32_t>> index_area;

    //binary search
    int64_t binarySearch(uint64_t key) {

        uint64_t left = 0;
        uint64_t right = index_area.size() - 1;

        while (left <= right) {
            uint64_t mid = left + (right - left) / 2;

            if (index_area[mid].first == key) {
                return mid;
            }

            if (index_area[mid].first < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return -1;
    }
public:
    uint64_t num = 0;
    uint64_t min = 0;
    uint64_t max = 0;
    string file_path;
    uint64_t level = 0;
    uint64_t time = 0;


    //bloom filter
    unsigned char* bloom_filter;

    // Constructor
    SSTable(const std::string& file = ""){

        bloom_filter = new unsigned char[bloom_size];
        for(int i = 0; i < bloom_size; i++){
            bloom_filter[i] = 0;
        }
        file_path = file;
    }



    // Search if a key
    int64_t search(uint64_t key) {
        // 第一步：搜索范围
        if(key > max || key < min){
            return -1;
        }
        // 第二步：布隆过滤器
        uint32_t hash[4];
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);

        for (int i = 0; i < 4; i++) {
            uint32_t pos = hash[i] % bloom_size;
            if (!bloom_filter[pos]) {
                return -1;
            }
        }

        // 第三步：遍历KEY(二分查找）
        return binarySearch(key);
    }

    // 查找Value
    std::string get_value(uint64_t key) {
        // 首先进行查找
        int64_t index = search(key);
        if(index < 0) return "";
        // 根据index 得到相应的值
        string value = "";
        uint32_t offset = index_area[index].second;
        std::ifstream sstable_file(file_path, std::ios::binary);
        if (!sstable_file.is_open()) {
            // Handle file opening error
            std::cerr << "Error: Unable to open file " << file_path << " for get value." << std::endl;
            return "";
        }
        if(index == num){
            sstable_file.seekg(32 + 10240 + (index - 1) * (4 + 8) + 8 + offset);
            sstable_file >> value;
        }
        else{
            uint32_t offset1 = index_area[index].second, offset2 = index_area[index+1].second;;
            sstable_file.seekg(32 + 10240 + (index - 1) * (4 + 8) + 8 + offset1);
            sstable_file.read((char*)&value, offset2 - offset1);
        }
        return value;
    }

};
#endif //LSMKV_SSTABLE_H
