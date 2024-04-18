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

int main() {

    std::vector<const std::type_info *> types2 = {&typeid(int), &typeid(std::string), &typeid(std::string)};
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);
    ConsumerProducerQueue<DataFrame *> queue_filter(15);

    FileReader csvReader;
    auto selector = SelectHandler(&queue_reader, &queue_select);
    auto filter = FilterHandler(&queue_select, &queue_filter);
    auto printer = printHandler(&queue_filter);


    std::vector<std::thread> threads;

    int end = 0;
    threads.emplace_back([&csvReader, &types2, &end, &queue_reader] {
        csvReader.read("../testData/data2.csv", types2, ',', 0, end, std::ref(queue_reader), true, 10);
    });

    for (int i = 0; i < 2; i++) {
        threads.emplace_back([&selector] {
            selector.select({"id", "semana"});
        });
    }

    for (int i = 0; i < 2; i++) {
        threads.emplace_back([&filter] {
            filter.filter("id", "==", "1");
        });
    }

    threads.emplace_back([&printer] {
        printer.print();
    });

    for (auto &t: threads) {
        t.join();
    }
}