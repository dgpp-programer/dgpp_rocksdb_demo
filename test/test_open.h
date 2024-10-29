#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/cache.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <iostream>
#include <string>
#include <vector>
#include <time.h>
#include <thread>
#include <cstdint>
#include <chrono>
#include <stdlib.h>
#include <random>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

using namespace std;
using namespace rocksdb;

std::string now_time() {
    auto now = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

void TestOpen() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    Status status = DB::Open(options, "../data/", &db);
    if (!status.ok()) {
        cout << "Open Error : " << status.ToString() << endl;
        return ;
    }
    cout << "Open Successful" << endl;
    string key("");
    string value("");
    //写
    WriteOptions writeOptions;
    key = "key1";
    value = "value1";
    status = db->Put(writeOptions, "key1", "value1");
    //读
    ReadOptions readOptions;
    value = "";
    status = db->Get(readOptions, key, &value);
    if (!status.ok()) {
        cout << "read " << status.ToString() << ", key = " << key << " value = " << value << endl;
    }
    cout << "read " << status.ToString() << ", key = " << key << " value = " << value << endl;
    key = "key2";
    value = "";
    status = db->Get(readOptions, key, &value);
    if (!status.ok()) {
        cout << "read " << status.ToString() << ", key = " << key << " value = " << value << endl;
    }
    delete db;
    return ;
/*
输出结果
Open Successful
read OK, key = key1 value = value1
read NotFound: , key = key2 value = 
*/
}

void TestOpenForReadOnly() {
    DB* db;
    DB* read_db;
    Options options;
    options.create_if_missing = true;

    Status s = DB::OpenForReadOnly(options, "../data/", &db);
    if (!s.ok()) {
        cout << "Open Error : " << s.ToString() << endl;
        return ;
    }
    WriteOptions writeOptions;
    s = db->Put(writeOptions, "keyxx", "valuexx");
    cout << "Put keyxx result : "<< s.ToString() << endl;
    ReadOptions read_options;
    string value("");
    s = db->Get(read_options, "keyxx", &value);
    cout << "Get keyxx result : " << s.ToString() << endl;
    delete db;
    return ;
/*
输出结果
Put keyxx result : Not implemented: Not supported operation in read only mode.
Get keyxx result : NotFound: 
*/
}

//主实例
void TestSecondary() {
    DB* db;
    DB* read_db;
    Options options;
    options.create_if_missing = true;

    Status s = DB::Open(options, "../data/", &db);
    if (!s.ok()) {
        cout << "Open Error : " << s.ToString() << endl;
        return ;
    }
    WriteOptions writeOptions;
    cout << now_time() << " put key1-5" << endl;
    int i = 1;
    for(; i <= 5; ++i) {
        string key = "key" + to_string(i);
        string value = "value" + to_string(i);
        s = db->Put(writeOptions, key, value);
    }
    db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    cout << now_time() << " compact finish" << endl;
    this_thread::sleep_for(chrono::seconds(5));

    cout << now_time() << " put key6-10" << endl;
    for(; i <= 10; ++i) {
        string key = "key" + to_string(i);
        string value = "value" + to_string(i);
        s = db->Put(writeOptions, key, value);
    }
    db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    cout << now_time() << " compact finish" << endl;
    this_thread::sleep_for(chrono::seconds(5));

    cout << now_time() << " put key10-15" << endl;
    for(; i <= 15; ++i) {
        string key = "key" + to_string(i);
        string value = "value" + to_string(i);
        s = db->Put(writeOptions, key, value);
    }
    this_thread::sleep_for(chrono::seconds(120));
    delete db;
    return ;
}
