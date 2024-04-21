
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <typeindex>
#include "Handlers.h"

using std::vector;
using std::string;

void SelectHandler::select(vector<string> columns) {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        // select the columns
        for (const string& column : df->get_column_order()) {
            if (std::find(columns.begin(), columns.end(), column) == columns.end()) {
                df->remove_column(column);
            }
        }
        queue_out->push(df);
    }
}

void FilterHandler::filter(string column, string operation, string value) {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (df->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column_data = df->get_column<int>(column);
            vector<int> new_column_data;
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] == std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] != std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] < std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] > std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] <= std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] >= std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
            vector<float> column_data = df->get_column<float>(column);
            vector<float> new_column_data;
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] == std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] != std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] < std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] > std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] <= std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] >= std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(string))]) {
            vector<string> column_data = df->get_column<string>(column);
            vector<string> new_column_data;
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] == value) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] != value) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        }
            // Now for time columns
        else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(std::tm))]) {
            vector<std::tm> column_data = df->get_column<std::tm>(column);
            vector<std::tm> new_column_data;
            std::tm tm_value;
            //strptime(value.c_str(), "%Y-%m-%d %H:%M:%S", &tm_value);
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) == std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) != std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) < std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) > std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) <= std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) >= std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }// filter the dataframe
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        queue_out->push(df);
    }
}

void GroupByHandler::group_by(string column, string operation) {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (df->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column_data = df->get_column<int>(column);
            std::sort(column_data.begin(), column_data.end());
            vector<int> new_column_data;
            if (operation == "sum") {
                int sum = 0;
                for (size_t i = 0; i < column_data.size(); ++i) {
                    sum += column_data[i];
                }
                new_column_data.push_back(sum);
            } else if (operation == "mean") {
                int sum = 0;
                for (size_t i = 0; i < column_data.size(); ++i) {
                    sum += column_data[i];
                }
                new_column_data.push_back(sum / column_data.size());
            } else if (operation == "max") {
                new_column_data.push_back(column_data.back());
            } else if (operation == "min") {
                new_column_data.push_back(column_data.front());
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
            vector<float> column_data = df->get_column<float>(column);
            std::sort(column_data.begin(), column_data.end());
            vector<float> new_column_data;
            if (operation == "sum") {
                float sum = 0;
                for (size_t i = 0; i < column_data.size(); ++i) {
                    sum += column_data[i];
                }
                new_column_data.push_back(sum);
            } else if (operation == "mean") {
                float sum = 0;
                for (size_t i = 0; i < column_data.size(); ++i) {
                    sum += column_data[i];
                }
                new_column_data.push_back(sum / column_data.size());
            } else if (operation == "max") {
                new_column_data.push_back(column_data.back());
            } else if (operation == "min") {
                new_column_data.push_back(column_data.front());
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        queue_out->push(df);
    }
}

void SortHandler::sort(string column, string order) {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (df->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column_data = df->get_column<int>(column);
            if (order == "asc") {
                std::sort(column_data.begin(), column_data.end());
            } else if (order == "desc") {
                std::sort(column_data.begin(), column_data.end(), std::greater<int>());
            } else {
                throw std::invalid_argument("Invalid order");
            }
            df->update_column(column, column_data);
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
            vector<float> column_data = df->get_column<float>(column);
            if (order == "asc") {
                std::sort(column_data.begin(), column_data.end());
            } else if (order == "desc") {
                std::sort(column_data.begin(), column_data.end(), std::greater<float>());
            } else {
                throw std::invalid_argument("Invalid order");
            }
            df->update_column(column, column_data);
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        queue_out->push(df);
    }
}


void printHandler::print() {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }

        df->print();
    }
}