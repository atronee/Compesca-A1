#include <variant>
#include <vector>
#include <unordered_map>
#include <string>
#include <typeinfo>
#include <stdexcept>
#include <iostream>
#include <ctime>
#include <typeindex>
#include <algorithm>
#include <iomanip>
#include "DataFrame.h"
#include <algorithm>



DataFrame::DataFrame(const std::vector<std::string>& column_names, std::vector<const std::type_info*>& column_types) : n_rows(0) {
    /*
     * Constructor to create a DataFrame with the specified column names and types.
     */
    if (column_names.size() != column_types.size())
        throw std::invalid_argument("Number of column names does not match number of column types");

    for (size_t i = 0; i < column_names.size(); ++i) {
        if (this->column_types.find(column_names[i]) != this->column_types.end())
            throw std::invalid_argument("Column name already exists");

        this->column_types[column_names[i]] = type_to_index[std::type_index(*column_types[i])];
        this->column_order.push_back(column_names[i]);
        this->data[column_names[i]] = std::vector<DataVariant>();
    }
}



size_t DataFrame::get_column_type(const std::string& column_name) {
    /*
     * returns the type of the data in the specified column. The column name should exist.
     */
    if (column_types.find(column_name) == column_types.end())
        throw std::invalid_argument("get_column_type: Column name does not exist");


    return column_types[column_name];
}

void DataFrame::add_row(const std::vector<DataVariant>& row_data) {
    /*
     * Adds a new row to the DataFrame. The number of elements in the row_data should match the number of columns.
     * The type of the data in each element should match the type of the corresponding column.
     */
    if (row_data.size() != column_order.size())
        throw std::invalid_argument("Size of row data does not match number of columns");

    auto it = row_data.begin();
    for (size_t i = 0; i < row_data.size(); ++i, ++it) {
        // Check if the type of the value matches the type of the column
        if (it->index() != column_types[column_order[i]])
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
        return;
    //    throw std::invalid_argument("remove_column: Column name does not exist");

    data.erase(column_name);
    column_order.erase(std::remove(column_order.begin(), column_order.end(), column_name), column_order.end());
    column_types.erase(column_name);
}

void DataFrame::remove_row(int index) {
    /*
     * Removes the row at the specified index. The index should be within the range [0, n_rows).
     */
    if (index < 0 || index >= n_rows) {
        if (index < 0)
            throw std::invalid_argument("remove_row <0: Index out of bounds");
        else
            throw std::invalid_argument("remove_row >= n_rows: Index out of bounds");
    }

    for (const auto& column : column_order) {
        data[column].erase(data[column].begin() + index);
    }

    n_rows--;
}

void DataFrame::print() {
    /*
     * Prints the DataFrame to the console.
     */
    for (const auto& column : column_order) {
        std::cout << column << " ";
    }
    std::cout << std::endl;

    for (size_t i = 0; i < n_rows; ++i) {
        for (const auto& column : column_order) {
            if(data[column][i].index() == 0) {
                std::cout<< std::get<int>(data[column][i]) << " ";
            }
            else if(data[column][i].index() == 1) {
                std::cout<<std::get<float>(data[column][i]) << " ";
            }
            else if(data[column][i].index() == 2) {

                std::cout<< std::get<std::string>(data[column][i]) << " ";
            }

            else
                std::cout << "Unsupported type" << " ";
        }
        std::cout << std::endl;
    }
}

std::vector<DataVariant> DataFrame::get_row(int index) const {
    /*
     * Returns the data in the row at the specified index. The index should be within the range [0, n_rows).
     */
    if (index < 0 || index >= n_rows)
        throw std::invalid_argument("Index out of bounds");

    std::vector<DataVariant> row_data;
    for (const auto& column : column_order) {
        row_data.push_back(data.at(column)[index]);
    }

    return row_data;
}

void DataFrame::add_row_at_index(int index, const std::vector<DataVariant>& row_data) {
    /*
     * Adds a new row to the DataFrame at the specified index. The number of elements in the row_data should match the number of columns.
     * The type of the data in each element should match the type of the corresponding column.
     * The index should be within the range [0, n_rows].
     */
    if (row_data.size() != column_order.size())
        throw std::invalid_argument("Size of row data does not match number of columns");

    if (index < 0 || index > n_rows)
        throw std::invalid_argument("Index out of bounds");

    auto it = row_data.begin();
    for (size_t i = 0; i < row_data.size(); ++i, ++it) {
        // Check if the type of the value matches the type of the column
        if (it->index() != column_types[column_order[i]])
            throw std::invalid_argument("Type of value does not match type of column");

        // Insert the value at the specified index
        data[column_order[i]].insert(data[column_order[i]].begin() + index, *it);
    }

    n_rows++;
}
