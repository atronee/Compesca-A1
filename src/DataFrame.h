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
        /*
         * The function adds a new column to the DataFrame. The column name should be unique.
         * T is the type of the data in the column.
         */
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
         * returns the data in the specified column. The column name should exist.
         * T is the type of the data in the column. If the type of the data in the column does not match T,
         * the function throws an exception.
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
        /*
         * returns the type of the data in the specified column. The column name should exist.
         */
        if (column_types.find(column_name) == column_types.end())
            throw std::invalid_argument("Column name does not exist");

        return column_types[column_name];
    }

    void add_row(const std::initializer_list<std::any>& row_data) {
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

    void remove_column(const std::string& column_name) {
        /*
         * Removes the specified column from the DataFrame. The column name should exist.
         */
        if (data.find(column_name) == data.end())
            throw std::invalid_argument("Column name does not exist");

        data.erase(column_name);
        column_order.erase(std::remove(column_order.begin(), column_order.end(), column_name), column_order.end());
        column_types.erase(column_name);
    }

    void remove_row(int index) {
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
};