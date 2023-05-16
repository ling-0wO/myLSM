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

class SSTable{
    uint64_t time;
    uint64_t num;
    uint64_t min;
    uint64_t max;
    uint64_t data_size;
    //bloom filter
    std::vector<uint8_t> bloom_filter;

    //index
    std::vector<std::pair<uint64_t, uint32_t>> index_area;

    //data
    std::vector<std::string> data_area;

    //binary search
    int64_t binarySearch(uint64_t key) {
        if(key > max || key < min){
            return -1;
        }
        int64_t left = 0;
        int64_t right = index_area.size() - 1;

        while (left <= right) {
            int64_t mid = left + (right - left) / 2;

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
    // Constructor
    SSTable(const std::string& file = ""){
        file_path = file;
    }
    string file_path;

    // Load the Bloom Filter and index area from the file into memory
    void load(const std::string& file_path) {
        // Open the SSTable file for reading
        std::ifstream file(file_path, std::ios::binary);

        if (!file.is_open()) {
            cout<< "File not open in load" << endl;
            return;
        }

        // Read and ignore the header (32 bytes)
        file.seekg(32);

        // Read the Bloom Filter from the file
        file.read(reinterpret_cast<char*>(bloom_filter.data()), bloom_size);
        // Calculate the data area size
        file.seekg(0, std::ios::end);  // Go to the end of the file
        uint64_t file_size = file.tellg();  // Get the current position (which is the file size)

        data_size = file_size - 32 - bloom_size - (index_area.size() * sizeof(std::pair<uint64_t, uint32_t>));
        file.close();
    }



    // Search if a key
    bool search_bloom(uint64_t key) {
        uint32_t hash[4];
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);

        for (int i = 0; i < 4; i++) {
            uint32_t pos = hash[i] % bloom_size;
            if (!bloom_filter[pos]) {
                return false;
            }
        }
        return true;
    }

    // get value from data area
    std::string get_value(uint64_t key) {
        // Read the index area from the file
        std::ifstream file(file_path, std::ios::binary);

        if (!file.is_open()) {
            cout<< "File not open!" << endl;
            return "";
        }
        uint64_t k;
        uint32_t offset;
        while (file.read(reinterpret_cast<char*>(&k), sizeof(k)) &&
               file.read(reinterpret_cast<char*>(&offset), sizeof(offset))) {
            index_area.emplace_back(key, offset);
        }

        // Close the file
        file.close();
        // First, find the corresponding offset


        int64_t index = binarySearch(key);
        if (index == -1) {
            cout << "Key not found!" << endl;
            return "";
        }

        uint32_t offset1 = index_area[index].second;
        uint32_t offset2 = (index == index_area.size() - 1) ? data_size : index_area[index + 1].second;
        uint32_t value_length = offset2 - offset1;

        // Open the SSTable file for reading
        std::ifstream data(file_path, std::ios::binary);
        if (!data.is_open()) {
            cout << "File not open!"<< endl;
            return "";
        }

        // Seek to the data area using the offset
        data.seekg(32 + bloom_size + index_area.size() * (sizeof(uint64_t) + sizeof(uint32_t)) + offset1);


        // Read the value
        std::vector<char> value(value_length);
        data.read(value.data(), value_length);

        // Close the file
        data.close();

        return std::string(value.begin(), value.end());
    }

};
#endif //LSMKV_SSTABLE_H
