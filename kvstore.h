#pragma once

#include "kvstore_api.h"
#include "skip.h"
#include "sstable.h"
#include <vector>
#include <string>
const uint64_t sstable_size = 2 * 1024 * 1024 * 8;  // 2mb
class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    skipList memTable;
    SSTable ssTable;
    std::string sstableDirectory;
    bool bloom[bloom_size];
    void clearSkipList() {
        node *current_node = memTable.head;
        while (current_node != nullptr) {
            node *next_node = current_node->index[0];
            delete current_node;
            current_node = next_node;
        }
        memTable.listLevel = 0;
        memTable.head = new node;
        for(int i = 0; i < maxLevel; i++)
            memTable.head->index[i] = NULL;
    }
    void write_sstable();
    uint64_t count = 0;
    uint64_t str_size = 0;
    uint64_t min, max;
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

    // Insert a key into the Bloom Filter
    void insert_bloom(uint64_t key) {
        uint32_t hash[4];
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);

        for (int i = 0; i < 4; i++) {
            uint32_t pos = hash[i] % bloom_size;
            bloom[pos] = true;
        }
    }
	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
