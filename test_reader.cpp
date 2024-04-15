#include <iostream>
#include <vector>
#include <any>
#include <string>
#include "src/Reader.h"
#include "src/DataFrame.h"
#include "src/ConsumerProducerQueue.h"


int main(){ 
    
    // Test the CSVReader class
    // std:: cout << "Test the CSVReader class" << std::endl;
    // CSVReader csvReader;

    // std::vector<const std::type_info*> types = {&typeid(std::string), &typeid(int), &typeid(std::string), &typeid(std::string)};

    // auto [column_order, data] = csvReader.read("../testData/data.csv", types);
    
    // DataFrame df(column_order, types);

    // for (const auto& row : data) {
    //     df.add_row(row);
    // }

    // auto v1 = df.get_column<std::string>("Nome");
    // for (const auto& i : v1) {
    //     std::cout << i << "<--";
    //     std::cout << typeid(i).name() << std::endl;
    // }

    // auto v2 = df.get_column<int>("Idade");
    // for (const auto& i : v2) {
    //     std::cout << i << "<--";
    //     std::cout << typeid(i).name() << std::endl;
    // }

    // auto v3 = df.get_column<std::string>("Cidade");
    // for (const auto& i : v3) {
    //     std::cout << i << "<--";
    //     std::cout << typeid(i).name() << std::endl;
    // }
    
    
    // // Test the TXTReader class    
    // std:: cout << "Test the TXTReader class" << std::endl;
    // TXTReader txtReader;

    // auto [column_order_txt, data_txt] = txtReader.read("../testData/data.txt", types);

    // DataFrame df2(column_order_txt, types);

    // for (const auto& row : data_txt) {
    //     df2.add_row(row);
    // }

    // auto v4 = df2.get_column<std::string>("Nome");
    // for (const auto& i : v4) {
    //     std::cout << i << "<--";
    //     std::cout << typeid(i).name() << std::endl;
    // }

    // auto v5 = df2.get_column<int>("Idade");

    // for (const auto& i : v5) {
    //     std::cout << i << "<--";
    //     std::cout << typeid(i).name() << std::endl;
    // }

    // auto v6 = df2.get_column<std::string>("Cidade");

    // for (const auto& i : v6) {
    //     std::cout << i << "<--";
    //     std::cout << typeid(i).name() << std::endl;
    // }

    CSVReader csvReader;
    std::vector<const std::type_info*> types2 = {&typeid(int), &typeid(std::string), &typeid(std::string)};
    
    ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>> queue(15);
    // std::vector<const std::type_info*> types2 = {&typeid(std::string), &typeid(int), &typeid(std::string), &typeid(std::string)};
    
    csvReader.read("../testData/data2.csv", types2, ',', queue);
    std::cout << "Queue size: " << queue.size() << std::endl;
    // Print elements in the queue
    while (!queue.is_empty()) {
        //print size of the queue
        std::cout << "Queue size: " << queue.size() << std::endl;

        auto [column_order, block_data] = queue.pop();
        
        // print column names
        for (const auto& col : column_order) {
            std::cout << "column: " << col << std::endl;
        }

        // convert it to a DataFrame
        DataFrame df(column_order, types2);
        for (const auto& row : block_data) {
            df.add_row(row);
        }

        auto v1 = df.get_column<int>("id");
        auto v2 = df.get_column<std::string>("data");

        for (size_t i = 0; i < v1.size(); i++) {
            std::cout << v1[i] << " - " << v2[i] << std::endl;
        }

        // TODO: This is giving an error for some reason...

        // auto v3 = df.get_column<std::string>("diasemana");
        // for (const auto& i : v3) {
        //     std::cout << i << "<--";
        //     std::cout << typeid(i).name() << std::endl;
        // }

    }

    std::cout << "End of test" << std::endl;

    return 0;
}