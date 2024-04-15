#include <iostream>
#include <vector>
#include <any>
#include <string>
#include "src/Reader.h"
#include "src/DataFrame.h"
#include "src/ConsumerProducerQueue.h"


int main(){ 
    // CSVReader csvReader;
    // std::vector<const std::type_info*> types2 = {&typeid(int), &typeid(std::string), &typeid(std::string)};
    
    // ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>> queue(15);
    // csvReader.read("../testData/data2.csv", types2, ',', queue, true);
    // std::cout << "Queue size: " << queue.size() << std::endl;
    // // Print elements in the queue
    // while (!queue.is_empty()) {
    //     //print size of the queue
    //     std::cout << "Queue size: " << queue.size() << std::endl;

    //     auto [column_order, block_data] = queue.pop();

    //     // convert it to a DataFrame
    //     DataFrame df(column_order, types2);
    //     for (const auto& row : block_data) {
    //         df.add_row(row);
    //     }

    //     auto v1 = df.get_column<int>("id");
    //     auto v2 = df.get_column<std::string>("semana");
    //     auto v3 = df.get_column<std::string>("data");

    //     for (size_t i = 0; i < v1.size(); i++) {
    //         std::cout << v1[i] << " - " << v2[i] << " - " << v3[i] << std::endl;
    //     }

    // }

    // std::cout << "Test without blocks" << std::endl;

    // ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>> queue1(15);
    // csvReader.read("../testData/data2.csv", types2, ',', queue1, false);
    // std::cout << "Queue size: " << queue1.size() << std::endl;

    // std::cout << "Test with TXTReader" << std::endl;

    TXTReader txtReader;
    std::vector<const std::type_info*> types3 = {&typeid(int), &typeid(int), &typeid(std::string)};
    // std::vector<const std::type_info*> types3 = {&typeid(std::string), &typeid(int), &typeid(std::string), &typeid(std::string)};
    ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>> queue2(15);
    txtReader.read("../testData/data2.txt", types3, '	', queue2, true);
    std::cout << "Queue size: " << queue2.size() << std::endl;

    while (!queue2.is_empty()) {
        //print size of the queue
        std::cout << "Queue size: " << queue2.size() << std::endl;

        auto [column_order, block_data] = queue2.pop();

        // convert it to a DataFrame
        DataFrame df(column_order, types3);
        for (const auto& row : block_data) {
            df.add_row(row);
        }

        auto v1 = df.get_column<int>("id");
        auto v2 = df.get_column<int>("dv");
        auto v3 = df.get_column<std::string>("iv");

        for (size_t i = 0; i < v1.size(); i++) {
            std::cout << v1[i] << " - " << v2[i]  << " - " << v3[i] << std::endl;
        }

    }

    std::cout << "Test without blocks" << std::endl;

    ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>> queue3(15);
    txtReader.read("../testData/data2.txt", types3, ' ', queue3, false);
    std::cout << "Queue size: " << queue3.size() << std::endl;


    std::cout << "End of test" << std::endl;

    return 0;
}