//
// Created by vinicius on 3/30/24.
//

#include "DataFrame.h"
#include <vector>
#include <unordered_map>
#include <string>


void DataFrame::add_column(const std::string& column_name, const std::vector<T>& column_data) {
    data[column_name] = column_data;
}

std::vector<T> DataFrame::get_column(const std::string& column_name) {
    return data[column_name];
}
}
