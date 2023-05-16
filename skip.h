//
// Created by Ling_ on 2023/3/8.
//
#pragma once

#include <string>
using std::string;

const int listLen = 1000;//表长
const int maxLevel = 16;//最大高度
const float prob = 0.37  ;//概率
struct node {
    uint64_t key;
    string val;
    node *index[maxLevel];

    node() {};

    node(uint64_t k, const string& v) {
        key = k;
        val = v;
        for (int i = 0; i < maxLevel; i++)
            this->index[i] = NULL;
    };
};

struct skipList{
    int listLevel;
    node *head;
    void insert(uint64_t key, const string& value);
    string search(uint64_t key);
    bool remove(uint64_t key);
    skipList(){
        listLevel = 0;
        head = new node;
        for(int i = 0; i < maxLevel; i++)
            head->index[i] = NULL;
    }
    ~skipList() {
        node *current_node = head;
        while (current_node != nullptr) {
            node *next_node = current_node->index[0];
            delete current_node;
            current_node = next_node;
        }
    }
};


int inline randomLevel(){
    int level = 1;
    while (true){
        int ran = rand() % 10000;//生成一个0-10000的随机数
        if(level < maxLevel && (ran <= 10000 * prob)){
            level++;
        }
        else break;
    }
    return level;
}



//LSMKV_SKIP_H
