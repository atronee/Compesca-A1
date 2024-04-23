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



DataFrame::DataFrame(const std::vector<std::string>& column_names, std::vector<const std::type_info*>& column_types) :
    n_rows(0), creation_time(std::time(nullptr)) {
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

DataFrame::DataFrame(const std::vector<std::string>& column_names, std::unordered_map<std::string, std::size_t>& column_types)
    : n_rows(0), creation_time(std::time(nullptr)), column_types(column_types){
    /*
     * Constructor to create a DataFrame with the specified column names and types.
     */
    if (column_names.size() != column_types.size())
        throw std::invalid_argument("Number of column names does not match number of column types");

    for (const auto & column_name : column_names) {
        if (this->column_types.find(column_name) != this->column_types.end())
            throw std::invalid_argument("Column name already exists");

        this->column_order.push_back(column_name);
        this->data[column_name] = std::vector<DataVariant>();
    }
}

DataFrame::DataFrame(const std::vector<std::string> &column_names, std::vector<size_t> &column_types) :
        n_rows(0), creation_time(std::time(nullptr)) {
    /*
     * Constructor to create a DataFrame with the specified column names and types.
     */
    if (column_names.size() != column_types.size())
        throw std::invalid_argument("Number of column names does not match number of column types");

    for (size_t i = 0; i < column_names.size(); ++i) {
        if (this->column_types.find(column_names[i]) != this->column_types.end())
            throw std::invalid_argument("Column name already exists");

        this->column_types[column_names[i]] =  column_types[i];
        this->column_order.push_back(column_names[i]);
        this->data[column_names[i]] = std::vector<DataVariant>();
    }

}



int DataFrame::get_column_index(const std::string& column_name) {
    /*
     * returns the index of the specified column. The column name should exist.
     */
    for (int i = 0; i < column_order.size(); i++) {
        if (column_order[i] == column_name) {
            return i;
        }
    }
    throw std::invalid_argument("get_column_index: Column name does not exist");

}

size_t DataFrame::get_column_type(const std::string& column_name) {
    /*
     * returns the type of the data in the specified column. The column name should exist.
     */
    if (column_types.find(column_name) == column_types.end())
        throw std::invalid_argument("get_column_type:"+ column_name+" Column name does not exist");


    return column_types[column_name];
}

void DataFrame::add_row(const std::vector<DataVariant>& row_data) {
    /*
     * Adds a new row to the DataFrame. The number of elements in the row_data should match the number of columns.
     * The type of the data in each element should match the type of the corresponding column.
     */
    if (row_data.size() != column_order.size())
        throw std::invalid_argument("Size of row data ("+ std::to_string(row_data.size())+") does not match number of columns "
        +std::to_string(column_order.size()));

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
    bool printed = false;
    for (const auto& column : column_order) {
        // Check if the column is not empty
        if (!data[column].empty()) {
            std::cout << column << " ";
            printed = true;
        }
    }
    if (printed) std::cout << std::endl;

    for (size_t i = 0; i < n_rows; ++i) {
        printed = false;
        for (const auto& column : column_order) {
            // Check if the column is not empty
            if (!data[column].empty()) {
                if(data[column][i].index() == 0) {
                    std::cout<< std::get<int>(data[column][i]) << " ";
                }
                else if(data[column][i].index() == 1) {
                    std::cout<<std::get<float>(data[column][i]) << " ";
                }
                else if(data[column][i].index() == 2) {
                    std::cout<< std::get<std::string>(data[column][i]) << " ";
                }
                else if(data[column][i].index() == 3) {
                    std::tm tm_date = std::get<std::tm>(data[column][i]);
                    std::cout << std::put_time(&tm_date, "%d/%m/%Y") << " ";
                }
                else {
                    std::cout << "Unsupported type" << " ";
                }
                printed = true;
            }
        }
        if (printed) std::cout << std::endl;
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

void DataFrame::diff_columns(const std::string& column_name, const std::string& other_column_name) {
    /*
     * Adds a new column to the DataFrame that contains the difference between the values in the specified columns.
     * The column names should exist and the type of the columns should be int or float.
     */
    if (data.find(column_name) == data.end() || data.find(other_column_name) == data.end())
        throw std::invalid_argument("Column name does not exist");

    if (column_types[column_name] != 0 && column_types[column_name] != 1)
        throw std::invalid_argument("Type of column should be int or float");

    if (column_types[other_column_name] != 0 && column_types[other_column_name] != 1)
        throw std::invalid_argument("Type of column should be int or float");

    std::vector<DataVariant> diff_data;
    for (size_t i = 0; i < n_rows; ++i) {
        if (column_types[column_name] == 0) {
            diff_data.push_back(std::get<int>(data[column_name][i]) - std::get<int>(data[other_column_name][i]));
        } else {
            diff_data.push_back(std::get<float>(data[column_name][i]) - std::get<float>(data[other_column_name][i]));
        }
    }

    data["diff"] = diff_data;
    column_order.push_back("diff");
    column_types["diff"] = column_types[column_name];
}


int DataFrame::sum_column(const std::string& column_name) {
    /*
    * Returns the sum of the values in the specified column. The column name should exist and the type of the column should be int or float.
     */
    if (data.find(column_name) == data.end())
        throw std::invalid_argument("Column name does not exist");
    
    if (column_types[column_name] != 0 && column_types[column_name] != 1)
        throw std::invalid_argument("Type of column should be int or float");
    
    int sum = 0;
    for (size_t i = 0; i < n_rows; ++i) {
        if (column_types[column_name] == 0) {
            sum += std::get<int>(data[column_name][i]);
        } else {
            sum += std::get<float>(data[column_name][i]);
        }
    }

    return sum;
}