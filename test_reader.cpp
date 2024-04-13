#include <iostream>
#include <vector>
#include <any>
#include <string>
#include "src/Reader.h"
#include "src/DataFrame.h"

int main(){ 
    CSVReader csvReader;

    std::vector<const std::type_info*> types = {&typeid(std::string), &typeid(int), &typeid(std::string), &typeid(std::string)};

    auto [column_order, data] = csvReader.read("../src/testDataFrame/data.csv", types);


    DataFrame df(column_order, types);

    for (const auto& row : data) {
        df.add_row(row);
    }

    auto v1 = df.get_column<std::string>("Nome");
    for (const auto& i : v1) {
        std::cout << i << std::endl;
    }

    auto v2 = df.get_column<int>("Idade");
    for (const auto& i : v2) {
        std::cout << i << std::endl;
    }

    auto v3 = df.get_column<std::string>("Cidade");
    for (const auto& i : v3) {
        std::cout << i << std::endl;
    }
    


    return 0;
}