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

int main() {
    // answer the questions save the results of a "log"/ dashboard file
    // 1º question: Número de produtos visualizados por minuto

    // let's use the logs named userBehavior_1..10.csv

    std::vector<const std::type_info *> typesUserBehavior = {&typeid(std::string), &typeid(int), &typeid(std::string),
                                                             &typeid(int), &typeid(std::string), &typeid(std::string),
                                                             &typeid(std::string), &typeid(std::string)};

    // create a reader object
    FileReader csvReader;
    // creates the queues
    ConsumerProducerQueue<std::string> queue_files(15);
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);
    ConsumerProducerQueue<DataFrame *> queue_filter(15);

    EventBasedTrigger eventTrigger;
    std::thread eventTriggerThread([&eventTrigger, &queue_files] {
        eventTrigger.triggerOnApperanceOfNewLogFile("./logs", queue_files);
    });

    //void FileReader::read(const std::string& filename, std::vector<const std::type_info*>& types, char delimiter,
    //                      int start, int & end, ConsumerProducerQueue<DataFrame*>& queue,
    //                      bool read_in_blocks, int block_size)

    auto selector = SelectHandler(&queue_reader, &queue_select);
    auto filter = FilterHandler(&queue_select, &queue_filter);
    auto printer = printHandler(&queue_filter);

    std::vector<std::thread> threads;

    int end = 0;
    threads.emplace_back([&csvReader, &typesUserBehavior, &end, &queue_reader, &queue_files] {
        csvReader.read(typesUserBehavior, ',', 0, end, std::ref(queue_reader), std::ref(queue_files), true, 10);
    });

    std::cout<<"Selecting\n"<<std::endl;

    for (int i = 0; i < 2; i++) {
        threads.emplace_back([&selector] {
            selector.select({"User Author Id","Button Product Id", "Date"});
        });
    }
    std::cout<<"Filtering\n"<<std::endl;

    for (int i = 0; i < 2; i++) {
        threads.emplace_back([&filter] {
            filter.filter("Button Product Id", "!=", "0");
        });
    }

    std::cout<<"Printing\n"<<std::endl;

    threads.emplace_back([&printer] {
        printer.print();
    });

    for (auto &t: threads) {
        t.join();
    }

    return 0;
}



