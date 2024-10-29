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
#include "test/test_open.h"

using namespace std;

int main() {
    //TestOpen();
    //TestOpenForReadOnly();
    TestSecondary();
    return 0;
}