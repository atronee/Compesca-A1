#include <iostream>
#include <vector>
#include <any>
#include <string>
#include "src/Reader.h"
#include "src/DataFrame.h"

int main(){ 
    CSVReader csvReader;

    std::vector<const std::type_info*> types = {&typeid(std::string), &typeid(int), &typeid(std::string), &typeid(std::string)};

    auto [column_order, data] = csvReader.read("src/testDataFrame/data.csv", types);


    DataFrame df(column_order, types);

    for (const auto& row : data) {
        df.add_row(row);
    }

    auto v4 = df.get_column<std::string>("Nome");
    for (const std::string& s : v4) {
        std::cout << s << std::endl;
    }

    return 0;
}