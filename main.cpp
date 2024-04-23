#include "src/DataFrame.h"
#include "src/ConsumerProducerQueue.h"
#include "src/Handlers.h"
#include "src/Reader.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <mutex>
#include <thread>
#include <atomic>
#include <iostream>
#include "src/triggers.h"
#include "src/mock.h"

void mock_files(){
    for (int i = 0; i<1000; i+=10){
        mockCSV();
        mockLogFiles(10, 100,i);
        mockSqliteTable(80);
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}



int main(){
    std::cout<<"Starting the program"<<std::endl;
}