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
#include <ctime> 
#include <iomanip> 
#include <chrono>  
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

    std::vector<const std::type_info*> types2 = {&typeid(int), &typeid(std::string), &typeid(std::tm)};
    ConsumerProducerQueue<DataFrame*> queue(20);

    std::vector<std::thread> threads;

    threads.emplace_back(test_read_csv, &queue, types2);
    for(int i = 0; i < 1; i++){
        threads.emplace_back(print_test_csv, &queue);
    }
    for(auto& t : threads){
        t.join();
    }


    return 0;
}