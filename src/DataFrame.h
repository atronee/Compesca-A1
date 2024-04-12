#include <any>
#include <vector>
#include <unordered_map>
#include <string>
#include <typeinfo>
#include <stdexcept>

class DataFrame {
private:
    std::unordered_map<std::string, std::vector<std::any>> data;
    std::unordered_map<std::string, const std::type_info*> column_types;
    std::vector<std::string> column_order;
    int n_rows;
public:
    const std::type_info* get_column_type(const std::string& column_name);

    void add_row(const std::vector<std::any>& row_data);

    void remove_column(const std::string& column_name);

    void remove_row(int index);

    template <typename T>
    void add_column(const std::string& column_name, const std::vector<T>& column_data){
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
    std::vector<T> get_column(const std::string& column_name){
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


};