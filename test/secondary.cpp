// secondary_reader.cpp
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <iostream>
#include <thread>
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

//从实例
int main() {
    DB* db;
    Options options;
    options.create_if_missing = false;

    // 从实例以Secondary模式打开数据库
    // 这里的../secondary_data用于存储从实例的一些元数据
    Status status = DB::OpenAsSecondary(
        options, "../data/", "../secondary_data", &db);
    if (!status.ok()) {
        cerr << "OpenAsSecondary error: " << status.ToString() << endl;
        return 0;
    }
    for(int i = 0; i < 10; ++i) {
        Iterator* it = db->NewIterator(ReadOptions());
        cout << now_time() << " count " << i << ": ";
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            cout <<it->key().ToString() << " ";
        }
        cout << endl;
        delete it;
        this_thread::sleep_for(chrono::seconds(2));
    }
    cout << now_time() << " use TryCatchUpWithPrimary "<< endl;
    // 定期尝试读取最新数据
    // 手动触发同步（从主DB拉取最新更新）
    status = db->TryCatchUpWithPrimary();
    if (!status.ok()) {
        cerr << "TryCatchUpWithPrimary failed: " << status.ToString() << endl;
    }
    Iterator* it = db->NewIterator(ReadOptions());
    cout << now_time() << " scan: ";
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout <<it->key().ToString() << " ";
    }
    cout << endl;
    delete it;
    delete db;
    return 0;
}