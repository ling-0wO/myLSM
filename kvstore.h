#pragma once

#include "kvstore_api.h"
#include "skip.h"
#include "sstable.h"
#include <vector>
#include <string>
const uint64_t sstable_size = 2 * 1024 * 1024;  // 2mb
class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    skipList memTable;
    SSTable ssTable;
    std::string sstableDirectory;
    void clearSkipList() {
        unsigned currentKey;
        while (memTable.head->index[0] != nullptr) {
            currentKey = memTable.head->index[0]->key;
            memTable.remove(currentKey);
        }
    }
    void write_sstable();
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
