#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <iostream>
#include <string>
#include <vector>
#include <time.h>
#include <thread>
#include <cstdint>
#include <chrono>
#include <stdlib.h>
#include <random>

using namespace std;

void test01() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.max_bytes_for_level_base = 32 * 1024 *1024;
    options.max_bytes_for_level_multiplier = 10;
    options.target_file_size_base = 16 * 1024 * 1024;
    options.max_write_buffer_number = 4;
    
    rocksdb::Status status = rocksdb::DB::Open(options, "../data/", &db);
    if (!status.ok()) {
        cout << "Open Error : " << status.ToString() << endl;
        return ;
    }
    this_thread::sleep_for(std::chrono::seconds(1));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, 100000000);

    std::thread write_thread1([&](){
        rocksdb::Status s;
        rocksdb::WriteOptions writeOptions;
        cout << "write_thread1 start"  << endl;
        int i = 0;
        while (true) {
            int random_number = dis(gen);
            string key = "key" + to_string(random_number);
            string value = "value" + to_string(random_number);
            s = db->Put(writeOptions, key, value);
            ++i;
            if (!s.ok()) {
                cout << "[write_thread1] Error : "<<  i <<" "<< s.ToString() << key << " " << value << endl;
                return ;
            }
        }
    });

    std::thread write_thread2([&](){
        rocksdb::Status s;
        rocksdb::WriteOptions writeOptions;
        cout << "write_thread2 start"  << endl;
        while (true) {
            int random_number = dis(gen);
            string key = "key" + to_string(random_number);
            string value = "value" + to_string(random_number);
            s = db->Put(writeOptions, key, value);
            if (!s.ok()) {
                cout << "[write_thread2] Error : " << s.ToString() << key << " " << value << endl;
                return ;
            }
        }
    });

    std::thread read_thread1([&](){
        rocksdb::Status s;
        rocksdb::ReadOptions read_options;
        cout << "read_thread1 start"  << endl;
        while (true) {
            int random_number = dis(gen);
            string key = "key" + to_string(random_number);
            string value;
            s = db->Get(read_options, key, &value);
            if (!s.ok()) {
                //cout << "[read_thread1] Error : " << s.ToString() << key << " " << value << endl;
                //return ;
            }
        }
    });

    std::thread delete_thread1([&](){
        rocksdb::Status s;
        rocksdb::WriteOptions writeOptions;
        cout << "delete_thread1 start"  << endl;
        while (true) {
            int random_number = dis(gen);
            string key = "key" + to_string(random_number);
            s = db->Delete(writeOptions, key);
            if (!s.ok()) {
                cout << "[delete_thread1] Error : " << s.ToString() << key << endl;
                return ;
            }
        }
    });

    write_thread1.join();
    write_thread2.join();
    read_thread1.join();
    delete_thread1.join();
    delete db;
    return ;
}


int main() {
    test01();

    return 0;
}