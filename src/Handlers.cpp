
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <typeindex>
#include <numeric>
#include "Handlers.h"
#include <any>
#include <iostream>

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
        if (df->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column_data = df->get_column<int>(column);
            if (operation == "==") {
                for (size_t i = 0; i < df->get_number_of_rows(); ++i) {
                    if (!(column_data[i] == std::stoi(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < df->get_number_of_rows(); ++i) {
                    if (!(column_data[i] != std::stoi(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] < std::stoi(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] > std::stoi(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] <= std::stoi(value))) {
                        df->remove_row(i);
                        i --;
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (!(column_data[i] >= std::stoi(value))) {
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

void GroupByHandler::group_by(string column, string operation) {
    DataFrame* DF = nullptr;
    //concatenate everything in queue_in
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            break;
        }
        // concatenate df into DF
        if (DF == nullptr) {
            DF = df;
        } else {
            DF->concatenate(*df);
            free(df);
        }
    }

    DataFrame* new_df;

    // group all rows by column
    if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
        vector<int> column_data = DF->get_column<int>(column);
        std::unordered_map<int, vector<int>> groups;
        for (size_t i = 0; i < DF->get_number_of_rows(); ++i) {
            groups[column_data[i]].push_back(i);
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
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
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::string))]) {
                        vector<string> this_column_data = DF->get_column<string>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::tm))]) {
                        vector<std::tm> this_column_data = DF->get_column<std::tm>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else if (operation == "min") {
                            std::tm min = this_column_data[group.second[0]];
                            for (size_t i : group.second) {
                                if (std::mktime(&this_column_data[i]) < std::mktime(&min)) {
                                    min = this_column_data[i];
                                }
                            }
                            row_data.push_back(min);
                        } else if (operation == "max") {
                            std::tm max = this_column_data[group.second[0]];
                            for (size_t i : group.second) {
                                if (std::mktime(&this_column_data[i]) > std::mktime(&max)) {
                                    max = this_column_data[i];
                                }
                            }
                            row_data.push_back(max);
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else {
                        throw std::invalid_argument("Invalid column type");
                    }
                }
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
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
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
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::string))]) {
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
            new_df->add_row(row_data);
        }
    } else if(DF->get_column_type(column) == type_to_index[std::type_index(typeid(std::string))]) {
        vector<string> column_data = DF->get_column<string>(column);
        std::unordered_map<string, vector<int>> groups;
        for (size_t i = 0; i < DF->get_number_of_rows(); ++i) {
            groups[column_data[i]].push_back(i);
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
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
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(float))]) {
                        vector<float> this_column_data = DF->get_column<float>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::string))]) {
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
            new_df->add_row(row_data);
        }
    } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(std::tm))]) {
        vector<std::tm> column_data = DF->get_column<std::tm>(column);
        std::unordered_map<std::tm, vector<int>, decltype(compareTm)*> groups(0, compareTm);
        for (size_t i = 0; i < DF->get_number_of_rows(); ++i) {
            groups[column_data[i]].push_back(i);
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
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
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(float))]) {
                        vector<float> this_column_data = DF->get_column<float>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::string))]) {
                        vector<string> this_column_data = DF->get_column<string>(this_column);
                        if (operation == "count") {
                            row_data.push_back((int) group.second.size());
                        } else {
                            throw std::invalid_argument("Invalid operation");
                        }
                    } else if (DF->get_column_type(this_column) == type_to_index[std::type_index(typeid(std::tm))]) {
                        vector<std::tm> this_column_data = DF->get_column<std::tm>(this_column);
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
            new_df->add_row(row_data);
        }
    } else {
        throw std::invalid_argument("Invalid column type");
    }

    free(DF);
    queue_out->push(new_df);
    queue_out->push(nullptr);
}

void SortHandler::sort(string column, string order) {
    DataFrame* DF = nullptr;
    //concatenate everything in queue_in
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            break;
        }
        // concatenate df into DF
        if (DF == nullptr) {
            DF = df;
        } else {
            DF->concatenate(*df);
            free(df);
        }
    }

    DataFrame* new_df;

    // sort all rows by column
    if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
        vector<int> column_data = DF->get_column<int>(column);
        std::vector<size_t> indices(column_data.size());
        std::iota(indices.begin(), indices.end(), 0);
        if (order == "asc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return column_data[i1] < column_data[i2];});
        } else if (order == "desc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return column_data[i1] > column_data[i2];});
        } else {
            throw std::invalid_argument("Invalid order");
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
        }
        new_df = new DataFrame(new_column_order, new_column_types);
        for (size_t i : indices) {
            new_df->add_row(DF->get_row(i));
        }
    } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
        vector<float> column_data = DF->get_column<float>(column);
        std::vector<size_t> indices(column_data.size());
        std::iota(indices.begin(), indices.end(), 0);
        if (order == "asc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return column_data[i1] < column_data[i2];});
        } else if (order == "desc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return column_data[i1] > column_data[i2];});
        } else {
            throw std::invalid_argument("Invalid order");
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
        }
        new_df = new DataFrame(new_column_order, new_column_types);
        for (size_t i : indices) {
            new_df->add_row(DF->get_row(i));
        }
    } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(std::string))]) {
        vector<string> column_data = DF->get_column<string>(column);
        std::vector<size_t> indices(column_data.size());
        std::iota(indices.begin(), indices.end(), 0);
        if (order == "asc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return column_data[i1] < column_data[i2];});
        } else if (order == "desc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return column_data[i1] > column_data[i2];});
        } else {
            throw std::invalid_argument("Invalid order");
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
        }
        new_df = new DataFrame(new_column_order, new_column_types);
        for (size_t i : indices) {
            new_df->add_row(DF->get_row(i));
        }
    } else if (DF->get_column_type(column) == type_to_index[std::type_index(typeid(std::tm))]) {
        vector<std::tm> column_data = DF->get_column<std::tm>(column);
        std::vector<size_t> indices(column_data.size());
        std::iota(indices.begin(), indices.end(), 0);
        if (order == "asc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return std::mktime(&column_data[i1]) < std::mktime(&column_data[i2]);});
        } else if (order == "desc") {
            std::sort(indices.begin(), indices.end(), [&column_data](size_t i1, size_t i2) {return std::mktime(&column_data[i1]) > std::mktime(&column_data[i2]);});
        } else {
            throw std::invalid_argument("Invalid order");
        }
        vector<string> new_column_order = DF->get_column_order();
        vector<const std::type_info *> new_column_types;
        for (const auto& some_column : new_column_order) {
            new_column_types.push_back(&typeid(DF->get_column_type(some_column)));
        }
        new_df = new DataFrame(new_column_order, new_column_types);
        for (size_t i : indices) {
            new_df->add_row(DF->get_row(i));
        }
    } else {
        throw std::invalid_argument("Invalid column type");
    }

    free(DF);
    queue_out->push(new_df);
    queue_out->push(nullptr);
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

bool compareTm(const std::tm& lhs, const std::tm& rhs) {
    return lhs.tm_year == rhs.tm_year &&
           lhs.tm_mon == rhs.tm_mon &&
           lhs.tm_mday == rhs.tm_mday &&
           lhs.tm_hour == rhs.tm_hour &&
           lhs.tm_min == rhs.tm_min &&
           lhs.tm_sec == rhs.tm_sec;
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

void FinalHandler::aggregate() {
    DataFrame* DF = nullptr;
    //concatenate everything in queue_in
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            break;
        }
        // concatenate df into DF
        if (DF == nullptr) {
            DF = df;
        } else {
            DF->concatenate(*df);
            free(df);
        }
    }

    queue_out->push(DF);
    queue_out->push(nullptr);
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
