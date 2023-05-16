//
// Created by Ling_ on 2023/3/18.
//
#pragma once
#include <string>
#include "skip.h"

using std::string;
void skipList::insert(uint64_t key, const string& value) {
    int level = randomLevel();
    node *update[maxLevel];
    node *p, *q = head;
    bool nodeFound = false;

    for (int l = listLevel; l >= 0; l--) {
        p = q->index[l];
        while (p && p->key < key) {
            q = p;
            p = q->index[l];
        }
        if (p && p->key == key) {
            nodeFound = true;
        }
        update[l] = q;
    }
    while (level > listLevel) {
        listLevel += 1;
        update[listLevel] = head;
    }
    node *tmp = new node(key, value);

    if (nodeFound) {
        for (int i = 0; i < level; i++) {
            p = update[i];
            tmp->index[i] = p->index[i]->index[i];
            p->index[i] = tmp;
        }
    } else {
        for (int i = 0; i < level; i++) {
            p = update[i];
            tmp->index[i] = p->index[i];
            p->index[i] = tmp;
        }
    }
}


string skipList::search(uint64_t key) {
    node *p = NULL;
    node *q = head;

    for (int i = listLevel - 1; i >= 0; i--) {
        p = q->index[i];
        while (p && p->key < key) {
            q = p;
            p = q->index[i];
        }
        if (p && p->key == key)
            return p->val; // Return the string value if the key is found
    }
    return ""; // Return an empty string if the key is not found
}

bool skipList::remove(uint64_t key) {
    node *update[maxLevel];
    node *p, *q = head;
    bool nodeFound = false;

    // Traverse the list and find the node with the matching key
    for (int l = listLevel; l >= 0; l--) {
        p = q->index[l];
        while (p && p->key < key) {
            q = p;
            p = q->index[l];
        }
        if (p && p->key == key) {
            nodeFound = true;
        }
        update[l] = q;
    }

    // If the node is found, update the index pointers and delete the node
    if (nodeFound) {
        for (int i = 0; i < listLevel; i++) {
            if (update[i]->index[i] != p) {
                break;
            }
            update[i]->index[i] = p->index[i];
        }
        delete p;

        // Update the list level if necessary
        while (listLevel > 0 && head->index[listLevel] == NULL) {
            listLevel--;
        }
        return true;
    } else {
        // If the key is not found, print a message
        return false;
    }
}
