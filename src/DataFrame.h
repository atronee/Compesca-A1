#include <any>
#include <vector>
#include <unordered_map>
#include <string>
#include <typeinfo>
class DataFrame {
private:
    std::unordered_map<std::string, std::vector<std::any>> data;
    std::unordered_map<std::string, const std::type_info*> column_types;
    std::vector<std::string> column_order;
    int n_rows;
public:
    template <typename T>
    void add_column(const std::string& column_name, const std::vector<T>& column_data) {
        if (data.find(column_name) != data.end()) {
            throw std::invalid_argument("Column name already exists");
        }
        if(!data.empty() && column_data.size() != data[column_order[0]].size()) {
            throw std::invalid_argument("Size of column data does not match number of rows");
        }

        std::vector<std::any> any_column_data(column_data.begin(), column_data.end());
        data[column_name] = any_column_data;
        column_order.push_back(column_name);
        n_rows = column_data.size();

        column_types[column_name] = &typeid(T);
    }
    template <typename T>
    std::vector<T> get_column(const std::string& column_name) {
        /*
         * The function should return a vector of std::any. To get the data from std::any, you can use std::any_cast.
         */
        if (data.find(column_name) == data.end())
            throw std::invalid_argument("Column name does not exist");
        if (typeid(T) != *column_types[column_name])
            throw std::invalid_argument("Type of value does not match type of column");

        std::vector<T> column_data;
        for (const auto& value : data[column_name]) {
            column_data.push_back(std::any_cast<T>(value));
        }

        return column_data;
    }

    const std::type_info* get_column_type(const std::string& column_name) {
        if (column_types.find(column_name) == column_types.end())
            throw std::invalid_argument("Column name does not exist");

        return column_types[column_name];
    }

    void add_row(const std::initializer_list<std::any>& row_data) {
        if (row_data.size() != column_order.size())
            throw std::invalid_argument("Size of row data does not match number of columns");

        auto it = row_data.begin();
        for (size_t i = 0; i < row_data.size(); ++i, ++it) {
            // Check if the type of the value matches the type of the column
            if (typeid(*it) != *column_types[column_order[i]])
                throw std::invalid_argument("Type of value does not match type of column");

            data[column_order[i]].push_back(*it);
        }

        n_rows++;
    }

};