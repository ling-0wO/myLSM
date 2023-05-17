#include "kvstore.h"
#include <string>
#include "utils.h"
#include <sstream>


KVStore::KVStore(const std::string &dir): KVStoreAPI(dir), sstableDirectory("../data")
{
    if (!utils::dirExists(sstableDirectory)) {
        utils::mkdir(sstableDirectory.c_str());
    }
    ssTable.file_path = sstableDirectory;
}

KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(32 + 10240 + (count + 1) * (sizeof(uint32_t) + sizeof(uint64_t)) + (str_size + sizeof(s)) >= sstable_size){
        write_sstable();
        reset();

    }
    str_size += s.size();
    count++;

    if(count == 1){
        min = max = key;
        for(int i = 0; i < bloom_size; i++){
            bloom[i] = 0;
        }
    }
    else{
        if(key > max) max = key;
        if(key < min) min = key;
    }
    memTable.insert(key, s);
    insert_bloom(key);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	return memTable.search(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	return memTable.remove(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    str_size = 0;
    count = 0;

    clearSkipList();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An em
 * pty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)//用不到
{
    node *p = memTable.head;
    for (int i = memTable.listLevel - 1; i >= 0; i--) {
        while (p->index[i] && p->index[i]->key < key1) {
            p = p->index[i];
        }
    }
    p = p->index[0];

    while (p && p->key <= key2) {
        list.push_back(std::make_pair(p->key, p->val));
        p = p->index[0];
    }
}

//Write memtable to the sstable
void KVStore::write_sstable() {
    // Generate a unique file name for the SSTable
    static uint64_t sstable_id = 0;
    std::string file_path = sstableDirectory + "/level-" + std::to_string(sstable_id) + ".sst";

    // Open the file for writing
    std::ofstream sstable_file(file_path, std::ios::binary);
    if (!sstable_file.is_open()) {
        // Handle file opening error
        std::cerr << "Error: Unable to open file " << file_path << " for writing." << std::endl;
        return;
    }


    // Write the memtable content to the file
    // header
    sstable_file << sstable_id << count << min << max;
    //cout <<"header size:" << sizeof(count) +   sizeof(min) +  sizeof(max)<< endl;
    // bloom
    for(int i = 0; i < bloom_size; i++){
        sstable_file << bloom[i];
    }

    // key and offset
    int size = 0;
    uint64_t current_offset = sizeof(uint32_t) + (count - 1) * (sizeof(uint64_t) + sizeof(uint32_t));
    node *p = memTable.head->index[0];
    while (p) {
        // Write key and offset
        sstable_file << p->key << current_offset;
        // Calculate the next offset
        current_offset += p->val.size() + sizeof(uint64_t) + sizeof(uint32_t);
        // Move to the next node in the memTable
        p = p->index[0];
    }
    // Then write the val to the sstable
    p = memTable.head->index[0];
    while (p) {
        // Write value length and value
        uint32_t value_length = static_cast<uint32_t>(p->val.size());;
        sstable_file.write((const char*)&p->val, value_length);
        size += p->val.size();
        // Move to the next node in the memTable
        p = p->index[0];
    }

    sstable_file.close();

    // Clear the memtable
    reset();
}