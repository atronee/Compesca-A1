//
// Created by vinicius on 3/30/24.
//

#ifndef COMPESCA_A1_DATAFRAME_H
#define COMPESCA_A1_DATAFRAME_H


class DataFrame {
private:
    std::unordered_map<std::string, std::vector<T>> data;
public:

    void add_column(const std::string& column_name, const std::vector<T>& column_data);
    std::vector<T> get_column(const std::string& column_name);
};
#endif //COMPESCA_A1_DATAFRAME_H
