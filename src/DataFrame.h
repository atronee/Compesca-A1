#ifndef DATAFRAME_H
#define DATAFRAME_H
#include <variant>
#include <vector>
#include <unordered_map>
#include <string>
#include <typeinfo>
#include <stdexcept>
#include <typeindex>
#include <ctime>
#include <iostream>

using DataVariant = std::variant<int, float, std::string, std::tm>;

// Define the mapping from type to index
static std::unordered_map<std::type_index, size_t> type_to_index = {
        {typeid(int), 0},
        {typeid(float), 1},
        {typeid(std::string), 2},
        {typeid(std::tm), 3}

};

static std::unordered_map<size_t, std::string> index_to_type = {
        {0, "int"},
        {1, "float"},
        {2, "string"},
        {3, "time"}
};


class DataFrame {
private:
    std::unordered_map<std::string, std::vector<DataVariant>> data;
    std::unordered_map<std::string, size_t> column_types;
    std::vector<std::string> column_order;
    int n_rows;
public:
    DataFrame() : n_rows(0) {};

    DataFrame(const std::vector<std::string>& column_names, std::vector< const std::type_info*>& column_types);

    size_t get_column_type(const std::string& column_name);

    const std::unordered_map<std::string, size_t>& get_column_types() const {
        return column_types;
    }

    std::vector<DataVariant> get_row(int index) const;

    void add_row(const std::vector<DataVariant>& row_data);

    void remove_column(const std::string& column_name);

    void remove_row(int index);

    void print();

    void add_row_at_index(int index, const std::vector<DataVariant>& row_data);

    template <typename T>
    void update_column(const std::string &column_name, const std::vector<T> &new_column_data) {
        /*
         * Updates the data in the specified column. The column name should exist.
         */
        if (data.find(column_name) == data.end())
            throw std::invalid_argument("update_column: Column name does not exist");

        if (new_column_data.size() != data[column_name].size())
            throw std::invalid_argument("Size of new column data does not match number of rows");
        std::vector<DataVariant> any_column_data(new_column_data.begin(), new_column_data.end());
        data[column_name] = any_column_data;

    }

    template <typename T>
    void add_column(const std::string& column_name, std::vector<T> column_data){
        /*
         * The function adds a new column to the DataFrame. The column name should be unique.
         * T is the type of the data in the column.
         */
        if (data.find(column_name) != data.end())
            throw std::invalid_argument("Column name already exists");

        if(!data.empty() && column_data.size() != data[column_order[0]].size()) {
            throw std::invalid_argument("Size of column data does not match number of rows");
        }

        std::vector<DataVariant> any_column_data(column_data.begin(), column_data.end());
        data[column_name] = any_column_data;
        column_order.push_back(column_name);
        n_rows = column_data.size();

        column_types[column_name] = type_to_index[typeid(T)];
    }

    template <typename T>
       std::vector<T> get_column(const std::string& column_name){
        /*
         * returns the data in the specified column. The column name should exist.
         * T is the type of the data in the column. If the type of the data in the column does not match T,
         * the function throws an exception.
         */
        if (data.find(column_name) == data.end()) {
            std::cout << "Column name: " << column_name << std::endl;
            std::cout << "data: " << std::endl;
            for (const auto& col : data) {
                std::cout << col.first << std::endl;
            }
            throw std::invalid_argument("get_column: Column name does not exist");
        }
        if (type_to_index[typeid(T)] != column_types[column_name])
            throw std::invalid_argument("Type of value does not match type of column");

        std::vector<T> column_data;
        for (const auto& value : data[column_name]) {
            column_data.push_back(std::get<T>(value));
        }

        return column_data;
    }

    std::vector<std::string> get_column_order() const {
        /*
         * returns the order of the columns in the DataFrame.
         */
        return column_order;
    }

    void clear_data(){
        /*
         * Clears only the data in the DataFrame.
         */
        data.clear();
        n_rows = 0;
    }
    int get_number_of_rows() const{
        /*
         * Returns the number of rows in the DataFrame.
         */
        return n_rows;
    }

    void reorder_rows(const std::vector<size_t>& new_order){
        /*
         * Reorders the rows in the DataFrame according to the specified order.
         */
        if (new_order.size() != n_rows)
            throw std::invalid_argument("Size of new order does not match number of rows");

        std::unordered_map<std::string, std::vector<DataVariant>> new_data;
        for (const auto& column : column_order) {
            std::vector<DataVariant> new_column_data;
            for (int i : new_order) {
                new_column_data.push_back(data[column][i]);
            }
            new_data[column] = new_column_data;
        }

        data = new_data;
    }

    void concatenate(const DataFrame& df){
        /*
         * Concatenates the specified DataFrame to the current DataFrame.
         */
        if (column_order != df.column_order)
            throw std::invalid_argument("Column order does not match");

        for (const auto& column : column_order) {
            data[column].insert(data[column].end(), df.data.at(column).begin(), df.data.at(column).end());
        }

        n_rows += df.n_rows;
    }

};

#endif // DATAFRAME_H