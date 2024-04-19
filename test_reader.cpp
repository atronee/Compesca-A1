#include <iostream>
#include <vector>
#include <any>
#include <string>
#include "src/Reader.h"
#include "src/DataFrame.h"
#include "src/ConsumerProducerQueue.h"
#include <thread>
#include <mutex>
#include <atomic>
std::atomic<bool> isReadingComplete(false);
std::mutex print_mtx;

void print_test_csv( ConsumerProducerQueue<DataFrame*>* queue) {
    while (queue->size() || !isReadingComplete){
        //print size of the queue
        std::cout << "Queue size: " << queue->size() << std::endl;

        DataFrame *df = queue->pop();
        print_mtx.lock();
        df->print();
        print_mtx.unlock();

    }
    std::unique_lock<std::mutex> lck(print_mtx);
    std::cout << "End of test" << std::endl;

}

void test_read_csv(ConsumerProducerQueue<DataFrame*>* queue, std::vector<const std::type_info*> types2){
    FileReader csvReader;
    int end = 0;
    csvReader.read("../testData/data2.csv", types2, ',',0,end, *queue, true, 10);
    isReadingComplete = true;
}

int main(){ 

    std::vector<const std::type_info*> types2 = {&typeid(int), &typeid(std::string), &typeid(std::string)};
    ConsumerProducerQueue<DataFrame*> queue(15);

    std::vector<std::thread> threads;

    threads.emplace_back(test_read_csv, &queue, types2);
    for(int i = 0; i < 1; i++){
        threads.emplace_back(print_test_csv, &queue);
    }
    for(auto& t : threads){
        t.join();
    }

    // Print elements in the queue

//
//    std::cout << "Test without blocks" << std::endl;
//
//    ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>> queue1(15);
//    csvReader.read("../testData/data2.csv", types2, ',', queue1, false);
//    std::cout << "Queue size: " << queue1.size() << std::endl;

//    std::cout << "Test with TXTReader" << std::endl;
//
//    FileReader txtReader;
//    std::vector<const std::type_info*> types3 = {&typeid(int), &typeid(int), &typeid(std::string)};
//    // std::vector<const std::type_info*> types3 = {&typeid(std::string), &typeid(int), &typeid(std::string), &typeid(std::string)};
//    ConsumerProducerQueue<DataFrame> queue2(15);
//    txtReader.read("../testData/data2.txt", types3, '	', queue2, true);
//    std::cout << "Queue size: " << queue2.size() << std::endl;
//
//    while (!queue2.is_empty()) {
//        //print size of the queue
//        std::cout << "Queue size: " << queue2.size() << std::endl;
//
//        DataFrame df = queue2.pop();
//
//        // convert it to a DataFrame
//        for (const auto& row : block_data) {
//            df.add_row(row);
//        }
//
//        auto v1 = df.get_column<int>("id");
//        auto v2 = df.get_column<int>("dv");
//        auto v3 = df.get_column<std::string>("iv");
//
//        for (size_t i = 0; i < v1.size(); i++) {
//            std::cout << v1[i] << " - " << v2[i]  << " - " << v3[i] << std::endl;
//        }
//
//    }
//
////    std::cout << "Test without blocks" << std::endl;
////
////    ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>> queue3(15);
////    txtReader.read("../testData/data2.txt", types3, ' ', queue3, false);
////    std::cout << "Queue size: " << queue3.size() << std::endl;
////
////
////    std::cout << "End of test" << std::endl;

    return 0;
}