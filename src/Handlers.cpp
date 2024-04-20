
#include "DataFrame.h"
#include "ConsumerProducerQueue.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <typeindex>
#include "Handlers.h"
#include <any>

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

void JoinHandler::join(DataFrame* df1, string main_column, string join_column){
    vector<string> main_column = df1->get_column<string>(main_column); 
    while (true) {
        DataFrame* incoming_df = queue_in->pop(); // Get data from the input queue
        if (incoming_df == nullptr) break; // Stop if no more data

        DataFrame* result_df = new DataFrame(); // Placeholder for the join result
        // Get the column to join on from the main DataFrame
        vector<string> incoming_column = incoming_df->get_column<string>(join_column); // Get the column to join on from the incoming DataFrame
        
        for (int i = 0; i < main_column.size(); i++) {
            for(int j = 0; j<incoming_column.size(); j++){
                string main_value= std::any_cast<string>(main_column[i]);
                string incoming_value= std::any_cast<string>(incoming_column[i]);
                if(main_value == incoming_value){
                    if (main_value == incoming_value) {
                        // Retrieve the full rows as vectors of DataVariant
                        vector<DataVariant> row_data = df1->get_row(i); // Main DataFrame row
                        vector<DataVariant> incoming_row_data = incoming_df->get_row(j); // Incoming DataFrame row

                        // Create a new vector to hold the combined row data
                        vector<DataVariant> combined_row_data = row_data;

                        // Insert elements from the incoming_row_data, excluding the join column index
                        // Assuming 'join_column_index' is the index of the join column in incoming_row_data
                        incoming_row_data.erase(incoming_row_data.begin() + 0); // Remove the join column from incoming row
                        combined_row_data.insert(combined_row_data.end(), incoming_row_data.begin(), incoming_row_data.end());

                        // Add the combined row to the result DataFrame
                        result_df->add_row(combined_row_data);
                        } // Add the row data from the main DataFrame to the result DataFrame
                }

        }
        }
        queue_out->push(result_df); // Push result to output queue
    }
}
