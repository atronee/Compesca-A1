
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <typeindex>
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
        for (const string& column : df->get_column_order()) {
            if (std::find(columns.begin(), columns.end(), column) == columns.end()) {
                df->remove_column(column);
            }
        }
        queue_out->push(df);
    }
}

void FilterHandler::filter(string column, string operation, string value) {
    while(true) {
        DataFrame* df = queue_in->pop();
        if (df == nullptr) {
            queue_out->push(nullptr);
            break;
        }
        if (df->get_column_type(column) == type_to_index[std::type_index(typeid(int))]) {
            vector<int> column_data = df->get_column<int>(column);
            vector<int> new_column_data;
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] == std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] != std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] < std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] > std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] <= std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] >= std::stoi(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(float))]) {
            vector<float> column_data = df->get_column<float>(column);
            vector<float> new_column_data;
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] == std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] != std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] < std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] > std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] <= std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] >= std::stod(value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(string))]) {
            vector<string> column_data = df->get_column<string>(column);
            vector<string> new_column_data;
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] == value) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (column_data[i] != value) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        }
            // Now for time columns
        else if (df->get_column_type(column) == type_to_index[std::type_index(typeid(std::tm))]) {
            vector<std::tm> column_data = df->get_column<std::tm>(column);
            vector<std::tm> new_column_data;
            std::tm tm_value;
            //strptime(value.c_str(), "%Y-%m-%d %H:%M:%S", &tm_value);
            if (operation == "==") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) == std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "!=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) != std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) < std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) > std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == "<=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) <= std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }
                }
            } else if (operation == ">=") {
                for (size_t i = 0; i < column_data.size(); ++i) {
                    if (std::mktime(&column_data[i]) >= std::mktime(&tm_value)) {
                        new_column_data.push_back(column_data[i]);
                    }// filter the dataframe
                }
            } else {
                throw std::invalid_argument("Invalid operation");
            }
            df->update_column(column, new_column_data);
        } else {
            throw std::invalid_argument("Invalid column type");
        }
        queue_out->push(df);
    }
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

void JoinHandler::join(DataFrame* df1, string main_column_name, string join_column_name){
    size_t type = df1->get_column_type(main_column_name);

    vector<int> main_column = df1->get_column<int>(main_column_name);
    vector<string> column_order_m = df1->get_column_order();


    
    vector<string> empty_string = {};
    vector<int> empty_int = {};
    vector<float> empty_float = {};
    vector<std::tm> empty_tm = {};
    while (true) {
        DataFrame* incoming_df = queue_in->pop(); // Get data from the input queue
        if (incoming_df == nullptr) {
            queue_out->push(nullptr);
            break; // Stop if no more data
        }
        DataFrame* result_df = new DataFrame(); // Placeholder for the join result
        // Get the column to join on from the main DataFrame
        vector<int> incoming_column = incoming_df->get_column<int>(join_column_name); // Get the column to join on from the incoming DataFrame
        vector<string> column_order = incoming_df->get_column_order();
        for(int i = 0; i<column_order.size(); i++){
            std::cout<<column_order[i]<<std::endl;
        }
        //Add the columns from the main DataFrame to the result DataFrame
        int p =0;
        for (const auto& col_name : column_order_m) {
                size_t type = df1->get_column_type(col_name);
                if (type == 0) {
                    result_df->add_column(col_name, empty_int);
                } else if (type == 1) {
                    result_df->add_column(col_name, empty_string);
                }
            p++;
        }

        // Add all columns from the incoming DataFrame, excluding the join column
        for (const auto& col_name : column_order) {
            if (col_name != join_column_name) {
                size_t type = incoming_df->get_column_type(col_name);
                if (type == 0) {
                    result_df->add_column(col_name, empty_int);
                } else if (type == 1) {
                    result_df->add_column(col_name, empty_string);
                }
            }
            p++;
        }
        std::cout<<p<<std::endl;

        //find index of join column
        int join_column_index = std::find(column_order.begin(), column_order.end(), join_column_name) - column_order.begin();
        for (int i = 0; i < main_column.size(); i++) {
            for(int j = 0; j<incoming_column.size(); j++){
                int main_value= std::any_cast<int>(main_column[i]);
                int incoming_value= std::any_cast<int>(incoming_column[j]);
                    if (main_value == incoming_value) {
                        // Retrieve the full rows as vectors of DataVariant
                        vector<DataVariant> row_data = df1->get_row(i); // Main DataFrame row
                        vector<DataVariant> incoming_row_data = incoming_df->get_row(j); // Incoming DataFrame row

                        std::cout<<row_data.size()<<std::endl;
                        std::cout<<incoming_row_data.size()<<std::endl;

                        // Create a new vector to hold the combined row data
                        vector<DataVariant> combined_row_data = row_data;

                        // Insert elements from the incoming_row_data, excluding the join column index
                        // Assuming 'join_column_index' is the index of the join column in incoming_row_data
                        incoming_row_data.erase(incoming_row_data.begin() + join_column_index); // Remove the join column from incoming row
                        combined_row_data.insert(combined_row_data.end(), incoming_row_data.begin(), incoming_row_data.end()); 
        
                        std::cout<<combined_row_data.size()<<std::endl;
                        // Add the combined row to the result DataFrame
                        result_df->add_row(combined_row_data);
                        } // Add the row data from the main DataFrame to the result DataFrame
        }
        }
        queue_out->push(result_df); // Push result to output queue
        // free the result df
        delete result_df;
    }
}
