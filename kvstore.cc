#include "kvstore.h"
#include <string>
#include "utils.h"
#include <sstream>
#include <cmath>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir), sstableDirectory("../data"), bloom(NULL)
{
    if (!utils::dirExists(sstableDirectory)) {
        utils::mkdir(sstableDirectory.c_str());
    }
    ssTables.clear();
    tableNum.clear();
    tableNum.push_back(0);
    bloom = new unsigned char[bloom_size];
    for(int i = 0; i < bloom_size; i++){
        bloom[i] = 0;
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
    if(32 + 10240 + (count + 1) * (8 + 4) + (str_size + s.size()) + 1 >= sstable_size){
        write_sstable(0);

        bloom = new unsigned char[bloom_size];
        reset();
        if(2 == tableNum[0]){
            write_sstable(0);
            compaction(0);
            tableNum[0] = -1;
        }
        tableNum[0]++;
    }
    str_size += s.size();
    count++;
    // Malloc a new space for bloom

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
    // 从当前的memtable中搜索
    string value = memTable.search(key);
	if(!value.empty())
        return value;
    if(ssTables.empty())
        return "";
    for (auto s: ssTables) {
        if(s->level == 0){
            value = s->get_value(key);
            if(!value.empty())
                return value;
            continue;
        }
        if(key < s->min)
            continue;
        if(key <= s->max){
            return s->get_value(key);
        }
        return "";
    }
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

// 将Memtable的内容存入disk
void KVStore::write_sstable(int level) {


    // 文件夹
    std::string dir_path = sstableDirectory + "/level-" + std::to_string(0);
    if (!utils::dirExists(dir_path)) {
        utils::mkdir(dir_path.c_str());
    }

    // 文件
    std::string file_path = dir_path + "/SSTable-" + std::to_string(tableNum[level]) + ".sst";

    std::ofstream sstable_file(file_path, std::ios::binary);
    if (!sstable_file.is_open()) {
        std::cerr << "Error: Unable to open file " << file_path << " for writing1." << std::endl;
        return;
    }

    // 写入
    SSTable *newSSTable = new SSTable(file_path);
    // header
    sstable_file << ++time_stamp << count << min << max;
    newSSTable->max = max, newSSTable->min = min, newSSTable->num = count, newSSTable->time = time_stamp;

    // bloom
    for(int i = 0; i < bloom_size; i++){
        sstable_file << bloom[i];
        newSSTable->bloom_filter[i] = bloom[i];
    }

    // key and offset
    uint64_t size = 0;
    uint64_t current_offset = sizeof(uint32_t) + (count - 1) * (sizeof(uint64_t) + sizeof(uint32_t));
    node *p = memTable.head->index[0];
    while (p) {
        // Write key and offset
        sstable_file << p->key << current_offset;
        pair<uint64_t, uint32_t> tmp(p->key, current_offset);
        newSSTable->index_area.emplace_back(tmp);
        // Calculate the next offset
        current_offset += p->val.size() + sizeof(uint64_t) + sizeof(uint32_t);

        p = p->index[0];
    }
    // Value
    p = memTable.head->index[0];
    while (p) {
        auto value_length = static_cast<uint32_t>(p->val.size());
        sstable_file.write((const char*)&p->val, value_length);
        p = p->index[0];
    }
    sstable_file << '\0';
    sstable_file.close();

    // Clear the memtable
    reset();
    ssTables.push_back(newSSTable);
}
pair<uint64_t, uint64_t > KVStore:: getHead (const std::string &file) // 取出时间戳和Key用来比较
{
    for(const auto sst: ssTables){
        if(sst->file_path == file){
            return pair<uint64_t, uint64_t>(sst->time, sst->min);
        }
    }
    exit(1);
};
std::pair<uint64_t, uint64_t> KVStore::getHead2 (const std::string &file)//
{
    for(const auto sst: ssTables){
        if(sst->file_path == file){
            return pair<uint64_t, uint64_t>(sst->min, sst->max);
        }
    }
    exit(1);
};

// The compaction operation
void KVStore::compaction(int level) {
    // Read all the files in the folder
    std::vector<std::string> sstable_files, files;
    utils::scanDir(sstableDirectory + "/level-" + std::to_string(level),files);
    std::vector<std::pair<uint64_t, std::string>> merged_entries;

    if(level != 0){
        // Sort这些初始文件，优先根据time，其次根据key
        std::sort(files.begin(), files.end(),
                  [this](const std::string& a, const std::string& b) {
                      auto a_info = getHead(a);
                      auto b_info = getHead(b);
                      if (a_info.first == b_info.first) {
                          return a_info.second < b_info.second;  // compare keys
                      } else {
                          return a_info.first < b_info.first;  // compare timestamps
                      }
                  });

        // Choose the lowest files into the merged_entries
        int file_amount = tableNum[level] - pow(2, level + 1);
        for(int i = 0; i < file_amount; i++)
        {
            sstable_files.push_back(files[i]);
        }

        std::string next_level_path = sstableDirectory + "/level-" + std::to_string(level + 1);
        if (!utils::dirExists(next_level_path)) {
            goto Travel;
        }
        else{
            // Levelx+1层所有key范围与“最小 key 到最大 key”有重叠的 SSTable 文件均被选取
            uint64_t min_key = getHead2(sstable_files[0]).first;
            uint64_t max_key = getHead2(sstable_files[file_amount - 1]).second;

            vector<string> next_files;
            utils::scanDir(sstableDirectory + "/level-" + std::to_string(level + 1),next_files);
            for(const auto& file : next_files){
                pair<uint64_t, uint64_t> tmp = getHead2(file);
                if(tmp.first <= min_key){
                    if(tmp.second >= min_key)
                        sstable_files.push_back(file);
                }
                else{
                    if(tmp.first <= max_key)
                        sstable_files.push_back(file);
                }
            }
        }


    }
    else{// level 0
        utils::scanDir(sstableDirectory + "/level-" + std::to_string(0),sstable_files);


        std::string next_level_path = sstableDirectory + "/level-" + std::to_string(1);
        vector<string> next_files;
        if (!utils::dirExists(next_level_path)) {
            goto Travel;
        }
        else{
            uint64_t min_key = getHead(sstable_files[0]).first;
            uint64_t max_key = min_key;
            // 得到level0 的最值
            for(const auto sst : sstable_files){
                pair<uint64_t, uint64_t> tmp = getHead2(sst);
                if(tmp.first < min_key)
                    min_key = tmp.first;
                if(tmp.second > max_key)
                    max_key = tmp.second;
            }
            utils::scanDir(sstableDirectory + "/level-" + std::to_string(level + 1),next_files);
            for(const auto& file : next_files){
                pair<uint64_t, uint64_t> tmp = getHead2(file);
                if(tmp.first <= min_key){
                    if(tmp.second >= min_key)
                        sstable_files.push_back(file);
                }
                else{
                    if(tmp.first <= max_key)
                        sstable_files.push_back(file);
                }
            }
        }
    }





Travel:
    // Travel the selected files
    for (const auto& file_name : sstable_files) {
        std::string file_path = sstableDirectory + "/level-" + std::to_string(level) + "/" + file_name;

        // Read the SSTable and store key-value pairs in a vector
        std::ifstream sstable_file(file_path, std::ios::binary);

        // Read the header
        uint64_t  _time = 0, _count = 0, _min = 0, _max = 0;
        sstable_file >> _time >> _count >> _min >> _max;
        vector<uint64_t> length;
        length.clear();

        // 读入key和offset
        uint64_t key = 0;
        uint32_t offset1 = 0, offset2 = 0;
        sstable_file >> key >> offset1;
        merged_entries.emplace_back(key, "");

        for (int i = 1; i < _count; ++i) {
            sstable_file >> key >> offset2;
            length.push_back(offset2 + 12 - offset1);
            offset1 = offset2;
            merged_entries.emplace_back(key, "");
        }

        // 根据记录的长度来读取value
        char *value;
        for(int i = 0; i < (int)_count - 1; i++){
            sstable_file.read(value, length[i]);
            merged_entries[i].second = value;
        }
        sstable_file >> merged_entries[_count - 1].second;
        sstable_file.close();


        // 根据Key排序得到的vector
        std::sort(merged_entries.begin(), merged_entries.end(),
                  [](const std::pair<uint64_t, std::string>& a, const std::pair<uint64_t, std::string>& b) {
                      return a.first < b.first;
                  });

        // 2mb为单位写入新的SSTable内
        uint64_t cur_str = 0;
        int cur_count = 0;
        int cur_start = 0;


        for (const auto &entry : merged_entries) {

            cur_count++;
            if(32 + 10240 + (cur_count - cur_start + 1) * (8 + 4) + (cur_str + entry.second.size()) + 1 >= sstable_size)
            {
                std::string next_level_path = sstableDirectory + "/level-" + std::to_string(level + 1);
                if (!utils::dirExists(next_level_path)) {
                    utils::mkdir(next_level_path.c_str());
                    tableNum.push_back(0);
                }

                uint64_t new_sstable_id = ++tableNum[level + 1];
                std::string new_file_path = next_level_path + "/SSTable-" + std::to_string(new_sstable_id) + ".sst";
                std::ofstream new_sstable_file(new_file_path, std::ios::binary);
                if (!new_sstable_file.is_open()) {
                    std::cerr << "Error: Unable to open file " << new_file_path << " for writing." << std::endl;
                    return;
                }
                // Write the new header of the file,
                time_stamp++;
                SSTable newSStable(new_file_path);
                newSStable.num = cur_count, newSStable.min = merged_entries[cur_start].first, newSStable.max = merged_entries[cur_count + cur_start].first;
                new_sstable_file << time_stamp << cur_count << merged_entries[cur_start].first << merged_entries[cur_count + cur_start].first;
                // The new bloom
                for(int i = 0; i < bloom_size; i++){
                    bloom[i] = 0;
                }
                for(int i = cur_start; i < cur_start + cur_count; i++){
                    insert_bloom(merged_entries[i].first);
                }
                for(int i = 0; i < bloom_size; i++){
                    new_sstable_file << bloom[i];
                    newSStable.bloom_filter[i] = bloom[i];
                }
                // The key and the offset
                uint32_t offset = 0;
                uint64_t cur_size = 0;
                for(int i = 0; i < cur_count; i++){
                    offset = sizeof(uint32_t) + (cur_count-1) * (sizeof (uint64_t) + sizeof (uint32_t)) + cur_size;
                    pair<uint64_t, uint32_t> tmp(merged_entries[i + cur_start].first, offset);
                    new_sstable_file << tmp.first << tmp.second;
                    newSStable.index_area.push_back(tmp);
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
            cur_str += entry.second.size();
        }

        if(cur_count != 0){

            std::string next_level_path = sstableDirectory + "/level-" + std::to_string(level + 1);
            if (!utils::dirExists(next_level_path)) {
                utils::mkdir(next_level_path.c_str());
                tableNum.push_back(0);
            }

            uint64_t new_sstable_id = ++tableNum[level+1];

            std::string new_file_path = next_level_path + "/SSTable-" + std::to_string(new_sstable_id) + ".sst";
            SSTable *newSSTable = new SSTable(new_file_path);
            std::ofstream new_sstable_file(new_file_path, std::ios::binary);
            if (!new_sstable_file.is_open()) {
                std::cerr << "Error: Unable to open file " << new_file_path << " for writing." << std::endl;
                return;
            }

            // Write the new header of the file,
            new_sstable_file << time_stamp++ << cur_count << merged_entries[cur_start].first << merged_entries[cur_count + cur_start].first;
            newSSTable->min = merged_entries[cur_start].first, newSSTable->max = merged_entries[cur_count + cur_start].first, newSSTable->num = cur_count;
            // The new bloom
            for(int i = 0; i < bloom_size; i++){
                bloom[i] = 0;
            }
            for(int i = cur_start; i < cur_start + cur_count; i++){
                insert_bloom(merged_entries[i].first);
            }
            for(int i = 0; i < bloom_size; i++){
                new_sstable_file << bloom[i];
                newSSTable->bloom_filter[i] = bloom[i];
            }
            // The key and the offset
            uint32_t offset = 0;
            uint64_t cur_size = 0;
            for(int i = 0; i < cur_count; i++){
                offset = sizeof(uint32_t) + (cur_count-1) * (sizeof (uint64_t) + sizeof (uint32_t)) + cur_size;
                pair<uint64_t, uint32_t> tmp(merged_entries[i + cur_start].first, offset);
                new_sstable_file << tmp.first << tmp.second;
                newSSTable->index_area.push_back(tmp);
                cur_size += merged_entries[i + cur_start].second.size();
            }
            // The values
            for(int i = cur_start; i < cur_start + cur_count; i++){
                new_sstable_file << merged_entries[i].second;
            }
            new_sstable_file << '\0';
        }

    }

    // Remove the old SSTables
    for (const auto& old_file : sstable_files) {
        std::string old_file_path = sstableDirectory + "/level-" + std::to_string(level) + "/" + old_file;
        if (std::remove(old_file_path.c_str()) != 0) {
            std::cerr << "Error: Unable to delete file " << old_file_path << std::endl;
        }
        // 同时在向量组内删除这些文件
        for(int i = 0; i < ssTables.size(); i++){
            if(ssTables[i]->file_path == old_file_path){
                SSTable *tmp = ssTables[i];
                ssTables.erase(ssTables.begin() + i);
                delete tmp;
            }

        }
    }
    tableNum[level] = 0;
    if(tableNum.size() > level && tableNum[level + 1] > pow(2, level + 2))
        compaction(level + 1);
}
