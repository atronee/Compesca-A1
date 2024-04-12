#include <any>
#include <vector>
#include <unordered_map>
#include <string>
#include <typeinfo>
#include <stdexcept>
#include "DataFrame.h"



const std::type_info* DataFrame::get_column_type(const std::string& column_name) {
    /*
     * returns the type of the data in the specified column. The column name should exist.
     */
    if (column_types.find(column_name) == column_types.end())
        throw std::invalid_argument("Column name does not exist");

    return column_types[column_name];
}

void DataFrame::add_row(const std::initializer_list<std::any>& row_data) {
    /*
     * Adds a new row to the DataFrame. The number of elements in the row_data should match the number of columns.
     * The type of the data in each element should match the type of the corresponding column.
     */
    if (row_data.size() != column_order.size())
        throw std::invalid_argument("Size of row data does not match number of columns");

    auto it = row_data.begin();
    for (size_t i = 0; i < row_data.size(); ++i, ++it) {
        // Check if the type of the value matches the type of the column
        if (it->type() != *column_types[column_order[i]])
            throw std::invalid_argument("Type of value does not match type of column");

        data[column_order[i]].push_back(*it);
    }

    n_rows++;
}

void DataFrame::remove_column(const std::string& column_name) {
    /*
     * Removes the specified column from the DataFrame. The column name should exist.
     */
    if (data.find(column_name) == data.end())
        throw std::invalid_argument("Column name does not exist");

    data.erase(column_name);
    column_order.erase(std::remove(column_order.begin(), column_order.end(), column_name), column_order.end());
    column_types.erase(column_name);
}

void DataFrame::remove_row(int index) {
    /*
     * Removes the row at the specified index. The index should be within the range [0, n_rows).
     */
    if (index < 0 || index >= n_rows)
        throw std::invalid_argument("Index out of bounds");

    for (const auto& column : column_order) {
        data[column].erase(data[column].begin() + index);
    }

    n_rows--;
}