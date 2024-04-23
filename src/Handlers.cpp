
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <typeindex>
#include <numeric>
#include "Handlers.h"
#include <any>
#include <iostream>
#include "../libs/sqlite3.h"
#include <fstream>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include "writer.h"
#include <ctime>

using std::vector;
using std::string;

void SelectHandler::select(vector<string> columns) {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (!df->get_number_of_rows()){
            delete df;
            continue;
        }
        // select the columns
        auto df_cols = df->get_column_order();
        for (auto column : df_cols) {
            if (std::find(columns.begin(), columns.end(), column) == columns.end()) {
                df->remove_column(column);
            }
        }
    queue_out->push(df);
}
};

void FilterHandler::filter(string column, string operation, string value) {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (!df->get_number_of_rows()){
            delete df;
            continue;
        }
        if (df->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column_data = df->get_column<int>(column);
            if (operation == "==") {
                for (size_t i = 0; i < df->get_number_of_rows(); ++i) {
                    if (!(column_data[i] == std::stoi(value))&&i < df->get_number_of_rows()) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < df->get_number_of_rows(); ++i) {
                    if (!(column_data[i] != std::stoi(value))&&i < df->get_number_of_rows()) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] < std::stoi(value))&&i < df->get_number_of_rows()) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] > std::stoi(value))&&i < df->get_number_of_rows()) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] <= std::stoi(value))&&i < df->get_number_of_rows()) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] >= std::stoi(value))&&i < df->get_number_of_rows()) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
            vector<float> column_data = df->get_column<float>(column);
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] == std::stod(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] != std::stod(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] < std::stod(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] > std::stod(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] <= std::stod(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] >= std::stod(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(std::string))]) {
            vector<string> column_data = df->get_column<string>(column);
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] == value)) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] != value)) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
        }
            // Now for time columns
        else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(std::tm))]) {
            vector<std::tm> column_data = df->get_column<std::tm>(column);
            std::tm tm_value;
            //strptime(value.c_str(), "%Y-%m-%d %H:%M:%S", &tm_value);
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(std::mktime(&column_data[i]) == std::mktime(&tm_value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(std::mktime(&column_data[i]) != std::mktime(&tm_value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(std::mktime(&column_data[i]) < std::mktime(&tm_value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(std::mktime(&column_data[i]) > std::mktime(&tm_value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(std::mktime(&column_data[i]) <= std::mktime(&tm_value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(std::mktime(&column_data[i]) >= std::mktime(&tm_value))) {
                        df->remove_row(i);
                        i --;
                    }// filter the dataframe
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        queue_out->push(df);
    }
};

bool compareTm(const std::tm& lhs, const std::tm& rhs) {
    return lhs.tm_year == rhs.tm_year &&
           lhs.tm_mon == rhs.tm_mon &&
           lhs.tm_mday == rhs.tm_mday &&
           lhs.tm_hour == rhs.tm_hour &&
           lhs.tm_min == rhs.tm_min &&
           lhs.tm_sec == rhs.tm_sec;
}

DataFrame* groupBy(DataFrame* DF, const string& column , const string& operation) {
    DataFrame* new_df;
    if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
        vector<int> column_data = DF->get_column<int>(column);
        std::unordered_map<int, vector<int>> groups;
        for (size_t i = 0; i < DF->get_number_of_rows(); ++i) {
            groups[column_data[i]].push_back(i);
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<std::size_t> new_column_types;
        new_column_types.reserve(new_column_order.size());
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back((DF->get_column_type(some_column)));
        }
        if (operation == "count") {
            new_column_order.push_back("count");
            new_column_types.push_back(type_to_index[std::type_index(typeid(int))]);
        }
        new_df = new DataFrame(new_column_order, new_column_types);
        for (const auto& group : groups) {
            vector<DataVariant> row_data;
            for (size_t i = 0; i < DF->get_column_order().size(); ++i) {
                string this_column = DF->get_column_order()[i];
                if (this_column == column) {
                    row_data.push_back(group.first);
                } else {
                    if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(int))]) {
                        vector<int> this_column_data = DF->get_column<int>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else if (operation == "sum") {
                            int sum = 0;
                            for (size_t i : group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.push_back(sum);
                        } else if (operation == "mean") {
                            float sum = 0;
                            for (size_t i : group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.push_back(sum / group.second.size());
                        } else if (operation == "min") {
                            int min = this_column_data[group.second[0]];
                            for (size_t i : group.second) {
                                if (this_column_data[i] < min) {
                                    min = this_column_data[i];
                                }
                            }
                            row_data.push_back(min);
                        } else if (operation == "max") {
                            int max = this_column_data[group.second[0]];
                            for (size_t i : group.second) {
                                if (this_column_data[i] > max) {
                                    max = this_column_data[i];
                                }
                            }
                            row_data.push_back(max);
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(float))]) {
                        vector<float> this_column_data = DF->get_column<float>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else if (operation == "sum") {
                            float sum = 0;
                            for (size_t i : group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.push_back(sum);
                        } else if (operation == "mean") {
                            float sum = 0;
                            for (size_t i : group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.push_back(sum / group.second.size());
                        } else if (operation == "min") {
                            float min = this_column_data[group.second[0]];
                            for (size_t i : group.second) {
                                if (this_column_data[i] < min) {
                                    min = this_column_data[i];
                                }
                            }
                            row_data.push_back(min);
                        } else if (operation == "max") {
                            float max = this_column_data[group.second[0]];
                            for (size_t i : group.second) {
                                if (this_column_data[i] > max) {
                                    max = this_column_data[i];
                                }
                            }
                            row_data.push_back(max);
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::string))] || DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::tm))]) {
                        vector<string> this_column_data = DF->get_column<string>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else {
                        throw std::invalid_argument("Invalid column type");
                    }
                }
            }
            if (operation == "count") {
                row_data.push_back((int) group.second.size());
            }

            new_df->add_row(row_data);
        }
    } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
        vector<float> column_data = DF->get_column<float>(column);
        std::unordered_map<float, vector<int>> groups;
        for (size_t i = 0; i < DF->get_number_of_rows(); ++i) {
            groups[column_data[i]].push_back(i);
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<size_t> new_column_types;
        for (const auto &some_column: new_column_order) {
            new_column_types.push_back(DF->get_column_type(some_column));
        }
        if (operation == "count") {
            new_column_order.push_back("count");
            new_column_types.push_back(type_to_index[std::type_index(typeid(int))]);
        }
        new_df = new DataFrame(new_column_order, new_column_types);
        for (const auto &group: groups) {
            vector<DataVariant> row_data;
            for (size_t i = 0; i < DF->get_column_order().size(); ++i) {
                string this_column = DF->get_column_order()[i];
                if (this_column == column) {
                    row_data.emplace_back(group.first);
                } else {
                    if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(int))]) {
                        vector<int> this_column_data = DF->get_column<int>(this_column);
                        if (operation == "count") {
                            row_data.emplace_back((int) group.second.size());
                        } else if (operation == "sum") {
                            int sum = 0;
                            for (size_t i: group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.emplace_back(sum);
                        } else if (operation == "mean") {
                            float sum = 0;
                            for (size_t i: group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.push_back(sum / group.second.size());
                        } else if (operation == "min") {
                            int min = this_column_data[group.second[0]];
                            for (size_t i: group.second) {
                                if (this_column_data[i] < min) {
                                    min = this_column_data[i];
                                }
                            }
                            row_data.push_back(min);
                        } else if (operation == "max") {
                            int max = this_column_data[group.second[0]];
                            for (size_t i: group.second) {
                                if (this_column_data[i] > max) {
                                    max = this_column_data[i];
                                }
                            }
                            row_data.push_back(max);
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(float))]) {
                        vector<float> this_column_data = DF->get_column<float>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else if (operation == "sum") {
                            float sum = 0;
                            for (size_t i: group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.push_back(sum);
                        } else if (operation == "mean") {
                            float sum = 0;
                            for (size_t i: group.second) {
                                sum += this_column_data[i];
                            }
                            row_data.push_back(sum / group.second.size());
                        } else if (operation == "min") {
                            float min = this_column_data[group.second[0]];
                            for (size_t i: group.second) {
                                if (this_column_data[i] < min) {
                                    min = this_column_data[i];
                                }
                            }
                            row_data.push_back(min);
                        } else if (operation == "max") {
                            float max = this_column_data[group.second[0]];
                            for (size_t i: group.second) {
                                if (this_column_data[i] > max) {
                                    max = this_column_data[i];
                                }
                            }
                            row_data.push_back(max);
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) ==
                               type_to_index[std::type_index(typeid(std::string))]) {
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else {
                        throw std::invalid_argument("Invalid column type");
                    }
                }
            }
            if (operation == "count") {
                row_data.push_back((int) group.second.size());
            }
            new_df->add_row(row_data);
        }
    }
    else {
        throw std::invalid_argument("Invalid column type");
    }
    return new_df;
}


void GroupByHandler::group_by(string column, string operation) {

    while(true) {
        DataFrame* DF = queue_in->pop();
        if (DF == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (!DF->get_number_of_rows()){
            delete DF;
            continue;
        }

        DataFrame *new_df = groupBy(DF, column, operation);

        new_df->set_creation_time(DF->get_creation_time());
        delete DF;
        queue_out->push(new_df);
    }
}

//make a function to compare two strings that represent dates in the format YYYY/MM/DD HH:MM

bool compareDates(const std::string& date1, const std::string& date2) {
    // Extract year, month, day, hour, and minute from date1
    int year1 = std::stoi(date1.substr(0, 4));
    int month1 = std::stoi(date1.substr(5, 2));
    int day1 = std::stoi(date1.substr(8, 2));
    int hour1 = std::stoi(date1.substr(11, 2));
    int minute1 = std::stoi(date1.substr(14, 2));

    // Extract year, month, day, hour, and minute from date2
    int year2 = std::stoi(date2.substr(0, 4));
    int month2 = std::stoi(date2.substr(5, 2));
    int day2 = std::stoi(date2.substr(8, 2));
    int hour2 = std::stoi(date2.substr(11, 2));
    int minute2 = std::stoi(date2.substr(14, 2));

    // Compare each component starting from the year down to the minute
    if (year1 != year2) return year1 < year2;
    if (month1 != month2) return month1 < month2;
    if (day1 != day2) return day1 < day2;
    if (hour1 != hour2) return hour1 < hour2;
    return minute1 < minute2;
}

void SortHandler::sort(string& column, string& order) {
    while (true) {
        DataFrame *new_df;
        DataFrame *DF = queue_in->pop();
        if (DF == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (!DF->get_number_of_rows()){
            delete DF;
            continue;
        }
        // sort all rows by column
        if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column_data = DF->get_column<int>(column);
            std::vector<size_t> indices(column_data.size());
            std::iota(indices.begin(), indices.end(), 0);
            if (order == "asc") {
                std::sort(indices.begin(), indices.end(),
                          [&column_data](size_t i1, size_t i2) { return column_data[i1] < column_data[i2]; });
            } else if (order == "desc") {
                std::sort(indices.begin(), indices.end(),
                          [&column_data](size_t i1, size_t i2) { return column_data[i1] > column_data[i2]; });
            } else {
                throw std::invalid_argument("Invalid order");
            }
            vector<string> new_column_order = DF->get_column_order();
            vector<const std::type_info *> new_column_types;
            for (const auto &some_column: new_column_order) {
                new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
            }
            new_df = new DataFrame(new_column_order, new_column_types);
            for (size_t i: indices) {
                new_df->add_row(DF->get_row(i));
            }
        } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
            vector<float> column_data = DF->get_column<float>(column);
            std::vector<size_t> indices(column_data.size());
            std::iota(indices.begin(), indices.end(), 0);
            if (order == "asc") {
                std::sort(indices.begin(), indices.end(),
                          [&column_data](size_t i1, size_t i2) { return column_data[i1] < column_data[i2]; });
            } else if (order == "desc") {
                std::sort(indices.begin(), indices.end(),
                          [&column_data](size_t i1, size_t i2) { return column_data[i1] > column_data[i2]; });
            } else {
                throw std::invalid_argument("Invalid order");
            }
            vector<string> new_column_order = DF->get_column_order();
            vector<const std::type_info *> new_column_types;
            for (const auto &some_column: new_column_order) {
                new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
            }
            new_df = new DataFrame(new_column_order, new_column_types);
            for (size_t i: indices) {
                new_df->add_row(DF->get_row(i));
            }
        } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(std::string))]) {
            vector<string> column_data = DF->get_column<string>(column);
            std::vector<size_t> indices(column_data.size());
            std::iota(indices.begin(), indices.end(), 0);
            if (order == "asc") {
                std::sort(indices.begin(), indices.end(),
                          [&column_data](size_t i1, size_t i2) { return column_data[i1] < column_data[i2]; });
            } else if (order == "desc") {
                std::sort(indices.begin(), indices.end(),
                          [&column_data](size_t i1, size_t i2) { return column_data[i1] > column_data[i2]; });
            } else {
                throw std::invalid_argument("Invalid order");
            }
            vector<string> new_column_order = DF->get_column_order();
            vector<const std::type_info *> new_column_types;
            for (const auto &some_column: new_column_order) {
                new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
            }
            new_df = new DataFrame(new_column_order, new_column_types);
            for (size_t i: indices) {
                new_df->add_row(DF->get_row(i));
            }
        } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(std::tm))]) {
            vector<std::tm> column_data = DF->get_column<std::tm>(column);
            std::vector<size_t> indices(column_data.size());
            std::iota(indices.begin(), indices.end(), 0);
            if (order == "asc") {
                std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {
                    return std::mktime(&column_data[i1]) < std::mktime(&column_data[i2]);
                });
            } else if (order == "desc") {
                std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {
                    return std::mktime(&column_data[i1]) > std::mktime(&column_data[i2]);
                });
            } else {
                throw std::invalid_argument("Invalid order");
            }
            vector<string> new_column_order = DF->get_column_order();
            vector<const std::type_info *> new_column_types;
            for (const auto &some_column: new_column_order) {
                new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
            }
            new_df = new DataFrame(new_column_order, new_column_types);
            for (size_t i: indices) {
                new_df->add_row(DF->get_row(i));
            }
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        new_df->set_creation_time(DF->get_creation_time());
        delete DF;
        queue_out->push(new_df);

    }
}

void printHandler::print() {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            break;
        }

        df->print();
    }
}


    void JoinHandler::join_int(DataFrame* df1, string main_column_name, string join_column_name){
    vector<int> main_column = df1->get_column<int>(main_column_name);
    while (true) {
        DataFrame* incoming_df = queue_in->pop(); // Get data from the input queue
        if (incoming_df == nullptr) {
            queue_out->push(nullptr);
            break; // Stop if no more data
        }

        vector<int> incoming_column = incoming_df->get_column<int>(join_column_name);

        auto column_types_m = df1->get_column_types();
        vector<string> column_order_m = df1->get_column_order();
        auto column_types = df1->get_column_types();
        vector<string> column_order = incoming_df->get_column_order();

        // join the contents of column_order and column_order_m
        int join_column_index = std::find(column_order.begin(), column_order.end(), join_column_name) - column_order.begin();
        column_order.erase(column_order.begin() + join_column_index);
        column_order_m.insert(column_order_m.end(), column_order.begin(), column_order.end());


        // std::cout<<column_order_m.size()<<std::endl;
        // for(int i=0; i<column_order_m.size(); i++){
        //     std::cout<<column_order_m[i]<<std::endl;
        // }

        //types for the result df
        std::vector<const std::type_info *> types;
        size_t type;
        for (const auto& col_name : column_order_m) {
            try
            {
                type = df1->get_column_type(col_name);
            }
            catch(const std::exception& e)
            {
                try
                {
                    type = incoming_df->get_column_type(col_name);
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
            if (type == 0) {
                types.push_back(&typeid(int));
            } else if (type == 1) {
                types.push_back(&typeid(float));
            } else if (type == 2) {
                types.push_back(&typeid(std::string));
            } else if (type == 3) {
                types.push_back(&typeid(std::tm));
            }
        }
        // Create a new DataFrame to hold the result
        DataFrame* result_df = new DataFrame(column_order_m, types);

        // Get the column to join on from the main DataFrame

        for (size_t i = 0; i < main_column.size(); ++i) {
            for (size_t j = 0; j < incoming_column.size(); ++j) {
                if (main_column[i] == incoming_column[j]) {
                    vector<DataVariant> row_data = df1->get_row(i); // Main DataFrame row
                    vector<DataVariant> incoming_row_data = incoming_df->get_row(j); // Incoming DataFrame row

                    incoming_row_data.erase(incoming_row_data.begin() + join_column_index);
                    row_data.insert(row_data.end(), incoming_row_data.begin(), incoming_row_data.end()); 

                    result_df->add_row(row_data);
                }
            }
        } 
        queue_out->push(result_df);
        // Push result to output queue
        // free the result df
    }
};

DataFrame* FinalHandler::aggregate_sort(DataFrame* df, DataFrame* fileDF, string& column, string& order){
    std::unordered_map<std::string, size_t> column_types = df->get_column_types();
    DataFrame* result_df = new DataFrame(df->get_column_order(), column_types);

    int i = 0, j = 0;
    while (i < df->get_number_of_rows() && j < fileDF->get_number_of_rows()) {
        bool bigger = false;
        if (auto intPtr = std::get_if<int>(&df->get_row(i)[df->get_column_index(column)])) {
            if (order == "asc") {
                bigger = *intPtr < std::get<int>(fileDF->get_row(j)[fileDF->get_column_index(column)]);
            } else if (order == "desc") {
                bigger = *intPtr > std::get<int>(fileDF->get_row(j)[fileDF->get_column_index(column)]);
            }
        }
        else if (auto floatPtr = std::get_if<float>(&df->get_row(i)[df->get_column_index(column)])) {
            if (order == "asc") {
                bigger = *floatPtr < std::get<float>(fileDF->get_row(j)[fileDF->get_column_index(column)]);
            } else if (order == "desc") {
                bigger = *floatPtr > std::get<float>(fileDF->get_row(j)[fileDF->get_column_index(column)]);
            }
        }
        else if (auto strPtr = std::get_if<string>(&df->get_row(i)[df->get_column_index(column)])) {
            if (order == "asc") {
                bigger = *strPtr < std::get<string>(fileDF->get_row(j)[fileDF->get_column_index(column)]);
            } else if (order == "desc") {
                bigger = *strPtr > std::get<string>(fileDF->get_row(j)[fileDF->get_column_index(column)]);
            }
        }
        else if (auto tmPtr = std::get_if<std::tm>(&df->get_row(i)[df->get_column_index(column)])) {
            if (order == "asc") {
                bigger = compareTm(*tmPtr, std::get<std::tm>(fileDF->get_row(j)[fileDF->get_column_index(column)]));
            } else if (order == "desc") {
                bigger = !compareTm(*tmPtr, std::get<std::tm>(fileDF->get_row(j)[fileDF->get_column_index(column)]));
            }
        }
        if (bigger){
            result_df->add_row(df->get_row(i++));
        }
        else
            result_df->add_row(fileDF->get_row(j++));
    }

    while (i < df->get_number_of_rows()) {
        result_df->add_row(df->get_row(i++));
    }

    while (j < fileDF->get_number_of_rows()) {
        result_df->add_row(fileDF->get_row(j++));
    }

    return result_df;
}

void FinalHandler::aggregate(string& filePath, string& table, bool sortFlag, bool groupFlag,
                             string columnSort, string columnGroup, string groupOperation,
                             string sortOrder) {
    /*
     * filepath: path to the file where the final DataFrame will be saved
     * table: name of the table
     * sortFlag: true if the DataFrame should be sorted
     * groupFlag: true if the DataFrame should be grouped
     * columnSort: column to sort by
     * columnGroup: column to group by
     * groupOperation: operation to perform on the grouped DataFrame
     * sortOrder: order to sort the DataFrame
     */
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            break;
        }
        if (!df->get_number_of_rows()){
            delete df;
            continue;
        }

        if(!sortFlag && !groupFlag){
            write_to_sqlite(df, filePath, table, false);
        }
        else {
            std::unordered_map<string, std::size_t> column_types = df->get_column_types();
            vector<string> column_order = df->get_column_order();
            auto fileDF = new DataFrame();
            for (const auto & i : column_order) {
                if (column_types[i] == type_to_index[std::type_index(typeid(int))]) {
                    fileDF->add_column(i, vector<int>{});
                } else if (column_types[i] == type_to_index[std::type_index(typeid(float))]) {
                    fileDF->add_column(i, vector<float>{});
                } else if (column_types[i] == type_to_index[std::type_index(typeid(std::string))]) {
                    fileDF->add_column(i, vector<string>{});
                } else if (column_types[i] == type_to_index[std::type_index(typeid(std::tm))]) {
                    fileDF->add_column(i, vector<std::tm>{});
                }
            }
            std::ifstream file(filePath);

            if (file.good()) {
                // Read data from SQLite file
                sqlite3 *db;
                sqlite3_stmt *stmt;
                int fd = open(filePath.c_str(), O_RDWR);
                if (fd == -1) {
                    return ;
                }
                if (flock(fd, LOCK_SH) != 0) {
                    close(fd);
                    return;
                }
                if (sqlite3_open(filePath.c_str(), &db) == SQLITE_OK) {
                    // Get the file descriptor and apply a shared lock

                    std::string sql = "SELECT * FROM " + table;
                    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {

                        while (sqlite3_step(stmt) == SQLITE_ROW) {
                            vector<DataVariant> row_data;
                            for(int i = 0; i < column_order.size(); ++i) {
                                if (column_types[column_order[i]] == type_to_index[std::type_index(typeid(int))]) {
                                    row_data.emplace_back(sqlite3_column_int(stmt, i));
                                } else if (column_types[column_order[i]] == type_to_index[std::type_index(typeid(float))]) {
                                    row_data.emplace_back(static_cast<float>(sqlite3_column_double(stmt, i)));
                                } else if (column_types[column_order[i]] == type_to_index[std::type_index(typeid(std::string))]) {
                                    row_data.emplace_back(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, i))));
                                } else if (column_types[column_order[i]] == type_to_index[std::type_index(typeid(std::tm))]) {
                                    std::tm tm{};
                                    strptime(reinterpret_cast<const char*>(sqlite3_column_text(stmt, i)), "%Y-%m-%d %H:%M:%S", &tm);
                                    row_data.emplace_back(tm);
                                }
                            }
                            fileDF->add_row(row_data);
                        }
                    }
                    sqlite3_finalize(stmt);

                }
                flock(fd, LOCK_UN);
                close(fd);
                sqlite3_close(db);
            }
            if(sortFlag && groupFlag)
            {
                DataFrame *new_df = groupBy(fileDF, columnGroup, groupOperation);
                delete fileDF;
                fileDF = new_df;
                ConsumerProducerQueue<DataFrame *> q_in(2);
                ConsumerProducerQueue<DataFrame *> q_out(2);
                SortHandler sh(&q_in, &q_out);
                q_in.push(fileDF);
                q_in.push(nullptr);
                sh.sort(columnGroup, sortOrder);
                delete fileDF;
                fileDF = q_out.pop();
            }

            else if (groupFlag) {
                DataFrame *new_df;
                fileDF->concatenate(*df);
                if(groupOperation == "count") {
                    new_df = groupBy(fileDF, columnGroup, "sum");
                }
                else
                    new_df = groupBy(fileDF, columnGroup, groupOperation);
                delete fileDF;
                fileDF = new_df;
            } else {
                DataFrame *new_df = aggregate_sort(df, fileDF, columnSort, sortOrder);
                delete fileDF;
                fileDF = new_df;
            }
            write_to_sqlite(fileDF, filePath, table, true);
            delete fileDF;
        }
        delete df;
    }


}

// void JoinHandler::join_float(DataFrame* df1, string main_column_name, string join_column_name){
//     vector<float> main_column = df1->get_column<float>(main_column_name);
//     while (true) {
//         DataFrame* incoming_df = queue_in->pop(); // Get data from the input queue
//         if (incoming_df == nullptr) {
//             queue_out->push(nullptr);
//             break; // Stop if no more data
//         }

//         vector<float> incoming_column = incoming_df->get_column<float>(join_column_name);

//         auto column_types_m = df1->get_column_types();
//         vector<string> column_order_m = df1->get_column_order();
//         auto column_types = df1->get_column_types();
//         vector<string> column_order = incoming_df->get_column_order();

//         // join the contents of column_order and column_order_m
//         int join_column_index = std::find(column_order.begin(), column_order.end(), join_column_name) - column_order.begin();
//         column_order.erase(column_order.begin() + join_column_index);
//         column_order_m.insert(column_order_m.end(), column_order.begin(), column_order.end());


//         // std::cout<<column_order_m.size()<<std::endl;
//         // for(int i=0; i<column_order_m.size(); i++){
//         //     std::cout<<column_order_m[i]<<std::endl;
//         // }

//         //types for the result df
//         std::vector<const std::type_info *> types;
//         size_t type;
//         for (const auto& col_name : column_order_m) {
//             try
//             {
//                 type = df1->get_column_type(col_name);
//             }
//             catch(const std::exception& e)
//             {
//                 try
//                 {
//                     type = incoming_df->get_column_type(col_name);
//                 }
//                 catch(const std::exception& e)
//                 {
//                     std::cerr << e.what() << '\n';
//                 }
//             }
//             if (type == 0) {
//                 types.push_back(&typeid(int));
//             } else if (type == 1) {
//                 types.push_back(&typeid(float));
//             } else if (type == 2) {
//                 types.push_back(&typeid(std::string));
//             } else if (type == 3) {
//                 types.push_back(&typeid(std::tm));
//             }
//         }
//         // Create a new DataFrame to hold the result
//         DataFrame* result_df = new DataFrame(column_order_m, types);

//         // Get the column to join on from the main DataFrame

//         for (size_t i = 0; i < main_column.size(); ++i) {
//             for (size_t j = 0; j < incoming_column.size(); ++j) {
//                 if (main_column[i] == incoming_column[j]) {
//                     vector<DataVariant> row_data = df1->get_row(i); // Main DataFrame row
//                     vector<DataVariant> incoming_row_data = incoming_df->get_row(j); // Incoming DataFrame row

//                     incoming_row_data.erase(incoming_row_data.begin() + join_column_index);
//                     row_data.insert(row_data.end(), incoming_row_data.begin(), incoming_row_data.end()); 

//                     result_df->add_row(row_data);
//                 }
//             }
//         } 
//         queue_out->push(result_df); // Push result to output queue
//         // free the result df
//     }
// };


// void JoinHandler::join_string(DataFrame* df1, string main_column_name, string join_column_name){
//     vector<string> main_column = df1->get_column<string>(main_column_name);
//     while (true) {
//         DataFrame* incoming_df = queue_in->pop(); // Get data from the input queue
//         if (incoming_df == nullptr) {
//             queue_out->push(nullptr);
//             break; // Stop if no more data
//         }

//         vector<string> incoming_column = incoming_df->get_column<string>(join_column_name);

//         auto column_types_m = df1->get_column_types();
//         vector<string> column_order_m = df1->get_column_order();
//         auto column_types = df1->get_column_types();
//         vector<string> column_order = incoming_df->get_column_order();

//         // join the contents of column_order and column_order_m
//         int join_column_index = std::find(column_order.begin(), column_order.end(), join_column_name) - column_order.begin();
//         column_order.erase(column_order.begin() + join_column_index);
//         column_order_m.insert(column_order_m.end(), column_order.begin(), column_order.end());


//         // std::cout<<column_order_m.size()<<std::endl;
//         // for(int i=0; i<column_order_m.size(); i++){
//         //     std::cout<<column_order_m[i]<<std::endl;
//         // }

//         //types for the result df
//         std::vector<const std::type_info *> types;
//         size_t type;
//         for (const auto& col_name : column_order_m) {
//             try
//             {
//                 type = df1->get_column_type(col_name);
//             }
//             catch(const std::exception& e)
//             {
//                 try
//                 {
//                     type = incoming_df->get_column_type(col_name);
//                 }
//                 catch(const std::exception& e)
//                 {
//                     std::cerr << e.what() << '\n';
//                 }
//             }
//             if (type == 0) {
//                 types.push_back(&typeid(int));
//             } else if (type == 1) {
//                 types.push_back(&typeid(float));
//             } else if (type == 2) {
//                 types.push_back(&typeid(std::string));
//             } else if (type == 3) {
//                 types.push_back(&typeid(std::tm));
//             }
//         }
//         // Create a new DataFrame to hold the result
//         DataFrame* result_df = new DataFrame(column_order_m, types);

//         // Get the column to join on from the main DataFrame

//         for (size_t i = 0; i < main_column.size(); ++i) {
//             for (size_t j = 0; j < incoming_column.size(); ++j) {
//                 if (main_column[i] == incoming_column[j]) {
//                     vector<DataVariant> row_data = df1->get_row(i); // Main DataFrame row
//                     vector<DataVariant> incoming_row_data = incoming_df->get_row(j); // Incoming DataFrame row

//                     incoming_row_data.erase(incoming_row_data.begin() + join_column_index);
//                     row_data.insert(row_data.end(), incoming_row_data.begin(), incoming_row_data.end()); 

//                     result_df->add_row(row_data);
//                 }
//             }
//         } 
//         queue_out->push(result_df); // Push result to output queue
//         // free the result df
//     }
// };


// void JoinHandler::join_time(DataFrame* df1, string main_column_name, string join_column_name){
//     vector<std::tm> main_column = df1->get_column<std::tm>(main_column_name);
//     while (true) {
//         DataFrame* incoming_df = queue_in->pop(); // Get data from the input queue
//         if (incoming_df == nullptr) {
//             queue_out->push(nullptr);
//             break; // Stop if no more data
//         }

//         vector<std::tm> incoming_column = incoming_df->get_column<std::tm>(join_column_name);

//         auto column_types_m = df1->get_column_types();
//         vector<string> column_order_m = df1->get_column_order();
//         auto column_types = df1->get_column_types();
//         vector<string> column_order = incoming_df->get_column_order();

//         // join the contents of column_order and column_order_m
//         int join_column_index = std::find(column_order.begin(), column_order.end(), join_column_name) - column_order.begin();
//         column_order.erase(column_order.begin() + join_column_index);
//         column_order_m.insert(column_order_m.end(), column_order.begin(), column_order.end());


//         // std::cout<<column_order_m.size()<<std::endl;
//         // for(int i=0; i<column_order_m.size(); i++){
//         //     std::cout<<column_order_m[i]<<std::endl;
//         // }

//         //types for the result df
//         std::vector<const std::type_info *> types;
//         size_t type;
//         for (const auto& col_name : column_order_m) {
//             try
//             {
//                 type = df1->get_column_type(col_name);
//             }
//             catch(const std::exception& e)
//             {
//                 try
//                 {
//                     type = incoming_df->get_column_type(col_name);
//                 }
//                 catch(const std::exception& e)
//                 {
//                     std::cerr << e.what() << '\n';
//                 }
//             }
//             if (type == 0) {
//                 types.push_back(&typeid(int));
//             } else if (type == 1) {
//                 types.push_back(&typeid(float));
//             } else if (type == 2) {
//                 types.push_back(&typeid(std::string));
//             } else if (type == 3) {
//                 types.push_back(&typeid(std::tm));
//             }
//         }
//         // Create a new DataFrame to hold the result
//         DataFrame* result_df = new DataFrame(column_order_m, types);

//         // Get the column to join on from the main DataFrame

//         for (size_t i = 0; i < main_column.size(); ++i) {
//             for (size_t j = 0; j < incoming_column.size(); ++j) {
//                 bool is_equal = compareTm(main_column[i], incoming_column[j]);
//                 if (is_equal) {
//                     vector<DataVariant> row_data = df1->get_row(i); // Main DataFrame row
//                     vector<DataVariant> incoming_row_data = incoming_df->get_row(j); // Incoming DataFrame row

//                     incoming_row_data.erase(incoming_row_data.begin() + join_column_index);
//                     row_data.insert(row_data.end(), incoming_row_data.begin(), incoming_row_data.end()); 

//                     result_df->add_row(row_data);
//                 }
//             }
//         } 
//         queue_out->push(result_df); // Push result to output queue
//         // free the result df
//     }
// };
    

void JoinHandler::join(DataFrame* df1, string main_column_name, string join_column_name){
    size_t type;
    type = df1->get_column_type(main_column_name);

    if (type == 0) {
        join_int(df1, main_column_name, join_column_name);
    }
    else{
        throw std::invalid_argument("The join only supports int columns");
    }

    // if (column_type == 0) {
    //     join_int(df1, main_column_name, join_column_name);
    // } else if (column_type == 1) {
    //     join_float(df1, main_column_name, join_column_name);
    // } else if (column_type == 2) {
    //     join_string(df1, main_column_name, join_column_name);
    // } else if (column_type == 3) {
    //     join_time(df1, main_column_name, join_column_name);
    // }
}

void JoinHandler::join(DataFrameVersionManager* dfvm, std::string main_column_name, std::string join_column_name){

    while (true) {
        DataFrame* incoming_df = queue_in->pop(); // Get data from the input queue
        if (incoming_df == nullptr) {
            queue_out->push(nullptr);
            break; // Stop if no more data
        }
        DataFrame* df1 = dfvm->getVersionBefore(incoming_df->get_creation_time());
        vector<int> main_column = df1->get_column<int>(main_column_name);
        vector<int> incoming_column = incoming_df->get_column<int>(join_column_name);

        auto column_types_m = df1->get_column_types();
        vector<string> column_order_m = df1->get_column_order();
        auto column_types = df1->get_column_types();
        vector<string> column_order = incoming_df->get_column_order();

        // join the contents of column_order and column_order_m
        int join_column_index = std::find(column_order.begin(), column_order.end(), join_column_name) - column_order.begin();
        column_order.erase(column_order.begin() + join_column_index);
        column_order_m.insert(column_order_m.end(), column_order.begin(), column_order.end());


        // std::cout<<column_order_m.size()<<std::endl;
        // for(int i=0; i<column_order_m.size(); i++){
        //     std::cout<<column_order_m[i]<<std::endl;
        // }

        //types for the result df
        std::vector<const std::type_info *> types;
        size_t type;
        for (const auto& col_name : column_order_m) {
            try
            {
                type = df1->get_column_type(col_name);
            }
            catch(const std::exception& e)
            {
                try
                {
                    type = incoming_df->get_column_type(col_name);
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
            if (type == 0) {
                types.push_back(&typeid(int));
            } else if (type == 1) {
                types.push_back(&typeid(float));
            } else if (type == 2) {
                types.push_back(&typeid(std::string));
            } else if (type == 3) {
                types.push_back(&typeid(std::tm));
            }
        }
        // Create a new DataFrame to hold the result
        DataFrame* result_df = new DataFrame(column_order_m, types);

        // Get the column to join on from the main DataFrame

        for (size_t i = 0; i < main_column.size(); ++i) {
            for (size_t j = 0; j < incoming_column.size(); ++j) {
                if (main_column[i] == incoming_column[j]) {
                    vector<DataVariant> row_data = df1->get_row(i); // Main DataFrame row
                    vector<DataVariant> incoming_row_data = incoming_df->get_row(j); // Incoming DataFrame row

                    incoming_row_data.erase(incoming_row_data.begin() + join_column_index);
                    row_data.insert(row_data.end(), incoming_row_data.begin(), incoming_row_data.end());

                    result_df->add_row(row_data);
                }
            }
        }
        result_df->set_creation_time(incoming_df->get_creation_time());
        delete incoming_df;
        queue_out->push(result_df);
        // Push result to output queue
        // free the result df
    }
}

void DiffHandler::diff(string column1, string column2, string new_column) {
    while (true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }

        if (df->get_column_type(column1) == type_to_index[std::type_index(typeid(int))] && df->get_column_type(column2) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column1_data = df->get_column<int>(column1);
            vector<int> column2_data = df->get_column<int>(column2);
            vector<int> new_column_data;
            for (size_t i = 0; i < df->get_number_of_rows(); ++i) {
                new_column_data.push_back(column1_data[i] - column2_data[i]);
            }
            df->add_column(new_column, new_column_data);
        } else if (df->get_column_type(column1) == type_to_index[std::type_index(typeid(float))] && df->get_column_type(column2) == type_to_index[std::type_index(typeid(float))]) {
            vector<float> column1_data = df->get_column<float>(column1);
            vector<float> column2_data = df->get_column<float>(column2);
            vector<float> new_column_data;
            for (size_t i = 0; i < df->get_number_of_rows(); ++i) {
                new_column_data.push_back(column1_data[i] - column2_data[i]);
            }
            df->add_column(new_column, new_column_data);
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        queue_out->push(df);
    }
}