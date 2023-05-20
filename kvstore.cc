#include "kvstore.h"
#include <string>
#include "utils.h"
#include <sstream>
#include <cmath>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir), sstableDirectory("../data")
{
    if (!utils::dirExists(sstableDirectory)) {
        utils::mkdir(sstableDirectory.c_str());
    }
    ssTable.file_path = sstableDirectory;
    tableNum.push_back(0);
    for(char & i : bloom){
        i = 0;
    }
    min = 0, max = 0;
}

KVStore::~KVStore()
= default;

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(32 + 10240 + (count + 1) * (sizeof(uint32_t) + sizeof(uint64_t)) + (str_size + s.size()) + 1>= sstable_size){
        write_sstable(0);
        reset();
        if(2 == tableNum[0]){
            compaction(0);
            tableNum[0] = 1;
        }
        tableNum[0]++;
    }
    str_size += s.size();
    count++;

    if(count == 1){
        min = max = key;
        for(char & i : bloom){
            i = 0;
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
        list.emplace_back(p->key, p->val);
        p = p->index[0];
    }
}

//Write memtable to the sstable
void KVStore::write_sstable(int level) {
    // Generate a unique file name for the SSTable

    std::string file_path = sstableDirectory + "/level-" + std::to_string(0) + "/SSTable-" + std::to_string(tableNum[level]) + ".sst";

    // Open the file for writing
    std::ofstream sstable_file(file_path, std::ios::binary);
    if (!sstable_file.is_open()) {
        // Handle file opening error
        std::cerr << "Error: Unable to open file " << file_path << " for writing." << std::endl;
        return;
    }


    // Write the memtable content to the file
    // header
    sstable_file << ++time_stamp << count << min << max;
    //cout <<"header size:" << sizeof(count) +   sizeof(min) +  sizeof(max)<< endl;
    // bloom
    for(char i : bloom){
        sstable_file << i;
    }

    // key and offset
    uint64_t size = 0;
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
        auto value_length = static_cast<uint32_t>(p->val.size());
        sstable_file.write((const char*)&p->val, value_length);
        size += p->val.size();
        // Move to the next node in the memTable
        p = p->index[0];
    }
    sstable_file << '\0';
    sstable_file.close();

    // Clear the memtable
    reset();
}
std::pair<uint64_t, uint64_t > getHead (std::string file)//
{
    //get Timestamp And MinKey From File
    std::ifstream file_stream(file, std::ios::binary);
    if(!file_stream.is_open()){
        std::cerr << "Error: Unable to open file " << file << " for reading." << std::endl;
        return pair<uint64_t, uint64_t>(-1, -1);
    }
    uint64_t  time = 0, _count = 0, _min = 0, _max = 0;
    file_stream >> time >> _count >> _min >> _max;
    return pair<uint64_t, uint64_t>(time, _min);
};
// The compaction operation
void KVStore::compaction(int level) {
    // Read all the files in the folder
    std::vector<std::string> sstable_files, files;
    utils::scanDir(sstableDirectory + "/level-" + std::to_string(level),files);
    std::vector<std::pair<uint64_t, std::string>> merged_entries;

    if(level != 0){
        // TODO:层中优先选择时间戳最小的若干个文件（时间戳相等选择键最小的文件),使得文件数满足层数要求
        // Sort the raw files in order
        std::sort(files.begin(), files.end(),
                  [](const std::string& a, const std::string& b) {
                      auto a_info = getHead(a);
                      auto b_info = getHead(b);
                      if (a_info.first == b_info.first) {  // if timestamps are equal
                          return a_info.second < b_info.second;  // compare keys
                      } else {
                          return a_info.first < b_info.first;  // compare timestamps
                      }
                  });

        // Choose the lowest files into the merged_entries
    }

    // TODO:Levelx+1层所有key范围与“最小 key 到最大 key”有重叠的 SSTable 文件均被选取
    for (const auto& file_name : sstable_files) {
        std::string file_path = sstableDirectory + "/level-" + std::to_string(level) + "/" + file_name;

        // Read the SSTable and store key-value pairs in a vector
        std::ifstream sstable_file(file_path, std::ios::binary);

        // Read the header
        uint64_t  time = 0, _count = 0, _min = 0, _max = 0;
        sstable_file >> time >> _count >> _min >> _max;
        vector<uint64_t> length;
        length.clear();
        // Read the key and offset, store the length of each element too.
        uint64_t key = 0;
        uint32_t offset1 = 0, offset2 = 0;
        sstable_file >> key >> offset1;
        merged_entries.emplace_back(key, "");

        for (int i = 1; i < _count; ++i) {
            sstable_file >> key >> offset2;
            length.push_back(offset2 + sizeof(uint64_t) + sizeof(uint32_t) - offset1);
            offset1 = offset2;
            merged_entries.emplace_back(key, "");
        }

        // Read the value
        char *value;
        for(int i = 0; i < _count - 1; i++){
            sstable_file.read(value, length[i]);
            merged_entries[i].second = value;
        }
        sstable_file >> merged_entries[_count].second;
        sstable_file.close();


        // Sort the merged entries by key
        std::sort(merged_entries.begin(), merged_entries.end(),
                  [](const std::pair<uint64_t, std::string>& a, const std::pair<uint64_t, std::string>& b) {
                      return a.first < b.first;
                  });

        // Write merged entries to the x + 1 level, splitting them into 2MB SSTables
        uint64_t cur_str = 0;
        int cur_count = 0;
        int cur_start = 0;

        for (const auto &entry : merged_entries) {
            cur_str += entry.second.size();
            cur_count++;
            if(32 + 10240 + (cur_count + 1) * (sizeof(uint32_t) + sizeof(uint64_t)) + (cur_str + entry.second.size()) + 1 >= sstable_size)
            {
                std::string next_level_path = sstableDirectory + "/level-" + std::to_string(level + 1);
                if (!utils::dirExists(next_level_path)) {
                    utils::mkdir(next_level_path.c_str());
                    tableNum.push_back(0);
                }

                uint64_t new_sstable_id = tableNum[level + 1]++;
                std::string new_file_path = next_level_path + "/SSTable-" + std::to_string(new_sstable_id) + ".sst";
                std::ofstream new_sstable_file(new_file_path, std::ios::binary);
                if (!new_sstable_file.is_open()) {
                    std::cerr << "Error: Unable to open file " << new_file_path << " for writing." << std::endl;
                    return;
                }
                // Write the new header of the file,
                time_stamp++;
                new_sstable_file << time_stamp << cur_count << merged_entries[cur_start].first << merged_entries[cur_count + cur_start].first;
                // The new bloom
                for(char & i : bloom){
                    i = 0;
                }
                for(int i = cur_start; i < cur_start + cur_count; i++){
                    insert_bloom(merged_entries[i].first);
                }
                for(char i : bloom){
                    new_sstable_file << i;
                }
                // The key and the offset
                uint32_t offset;
                size_t cur_size = 0;
                for(int i = 0; i < cur_count; i++){
                    offset = sizeof(uint32_t) + (cur_count-1) * (sizeof (uint64_t) + sizeof (uint32_t)) + cur_size;
                    new_sstable_file << merged_entries[i + cur_start].first << offset;
                    cur_size += merged_entries[i + cur_start].second.size();
                }
                // The values
                for(int i = cur_start; i < cur_start + cur_count; i++){
                    new_sstable_file << merged_entries[i].second;
                }
                new_sstable_file << '\0';
                cur_start += cur_count;
                cur_count = 0;
                cur_str = 0;
            }
        }

        if(cur_count != 0){

            std::string next_level_path = sstableDirectory + "/level-" + std::to_string(level + 1);
            if (!utils::dirExists(next_level_path)) {
                utils::mkdir(next_level_path.c_str());
                tableNum.push_back(0);
            }

            uint64_t new_sstable_id = tableNum[level + 1]++;
            std::string new_file_path = next_level_path + "/SSTable-" + std::to_string(new_sstable_id) + ".sst";
            std::ofstream new_sstable_file(new_file_path, std::ios::binary);
            if (!new_sstable_file.is_open()) {
                std::cerr << "Error: Unable to open file " << new_file_path << " for writing." << std::endl;
                return;
            }

            // Write the new header of the file,
            new_sstable_file << time_stamp++ << cur_count << merged_entries[cur_start].first << merged_entries[cur_count + cur_start].first;
            // The new bloom
            for(char & i : bloom){
                i = 0;
            }
            for(int i = cur_start; i < cur_start + cur_count; i++){
                insert_bloom(merged_entries[i].first);
            }
            for(char i : bloom){
                new_sstable_file << i;
            }
            // The key and the offset
            uint32_t offset;
            size_t cur_size = 0;
            for(int i = 0; i < cur_count; i++){
                offset = sizeof(uint32_t) + (cur_count-1) * (sizeof (uint64_t) + sizeof (uint32_t)) + cur_size;
                new_sstable_file << merged_entries[i + cur_start].first << offset;
                cur_size += merged_entries[i + cur_start].second.size();
            }
            // The values
            for(int i = cur_start; i < cur_start + cur_count; i++){
                new_sstable_file << merged_entries[i].second;
            }
            new_sstable_file << '\0';
        }
        // Remove the old SSTables
        for (const auto& old_file : sstable_files) {
            std::string old_file_path = sstableDirectory + "/level-" + std::to_string(level) + "/" + old_file;
            if (std::remove(old_file_path.c_str()) != 0) {
                std::cerr << "Error: Unable to delete file " << old_file_path << std::endl;
            }
        }
        if(tableNum[level+1] > pow(2, level + 2))
            compaction(level + 1);
    }
}
