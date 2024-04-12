
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include "Handlers.h"

using std::vector;
using std::string;

class Handler {
public:
    Handler() {}
    ~Handler() {}
    ConsumerProducerQueue<DataFrame*> *queue_in;
    ConsumerProducerQueue<DataFrame*> *queue_out;
    DataFrame* df = queue_in->pop();
};

class SelectHandler : public Handler {
public:
    SelectHandler() {}
    ~SelectHandler() {}
    DataFrame* select(vector<string> columns) {
        // select the dataframe
        for (const string& column : df->get_column_order()) {
            if (std::find(columns.begin(), columns.end(), column) == columns.end()) {
                df->remove_column(column);
            }
        }
        queue_out->push(df);
    }
};

class FilterHandler : public Handler {
public:
    FilterHandler() {}
    ~FilterHandler() {}
    DataFrame* filter(string column, string operation, string value) {
        // filter the dataframe
        if (df->get_column_type(column) == &typeid(int)) {
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
            df->add_column(column, new_column_data);
        } else if (df->get_column_type(column) == &typeid(double)) {
            vector<double> column_data = df->get_column<double>(column);
            vector<double> new_column_data;
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
            df->add_column(column, new_column_data);
        } else if (df->get_column_type(column) == &typeid(string)) {
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
            df->add_column(column, new_column_data);
        }
        // Now for time columns
        else if (df->get_column_type(column) == &typeid(std::tm)) {
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
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->add_column(column, new_column_data);
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        queue_out->push(df);
    }
};