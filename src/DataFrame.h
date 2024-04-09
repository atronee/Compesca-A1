//
// Created by vinicius on 3/30/24.
//

#ifndef COMPESCA_A1_DATAFRAME_H
#define COMPESCA_A1_DATAFRAME_H
#include <vector>
#include <unordered_map>
#include <string>

template <typename T>
class DataFrame {
private:
    std::unordered_map<std::string, std::vector<T>> data;
    std::vector<std::string> column_order;
    int n_rows;
public:
    void add_column(const std::string& column_name, const std::vector<T>& column_data) {
        if (data.find(column_name) != data.end()) {
            throw std::invalid_argument("Column name already exists");
        }
        if(!data.empty() && column_data.size() != data[column_order[0]].size()) {
            throw std::invalid_argument("Size of column data does not match number of rows");
        }

        data[column_name] = column_data;
        column_order.push_back(column_name);
        n_rows = column_data.size();
    }

    std::vector<T> get_column(const std::string& column_name) {
        return data[column_name];
    }

    void add_row(const std::vector<T>& row_data) {
        if (row_data.size() != column_order.size()) {
            throw std::invalid_argument("Size of row data does not match number of columns");
        }
        for (size_t i = 0; i < row_data.size(); ++i) {
            data[column_order[i]].push_back(row_data[i]);
        }
        n_rows++;
    }
};
#endif //COMPESCA_A1_DATAFRAME_H
