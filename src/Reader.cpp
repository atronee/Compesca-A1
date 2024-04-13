#include "Reader.h"               
#include <fstream>                 
#include <sstream>                 
#include <iostream>               
#include <typeinfo>                
#include <vector>                  

std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>> CSVReader::read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter) {
    /*
        Function to read CSV file and return data as a pair of vectors
    */
    std::ifstream file(filename);  // Open the CSV file for reading
    if (!file.is_open()) {          // Check if file opening failed
        std::cerr << "Error opening CSV file: " << filename << std::endl;  
        return {};                  
    }

    std::vector<std::string> column_order;        // Vector to store column names
    std::vector<std::vector<std::any>> data;      // Vector of vectors to store data
    
    std::string line;               // String to store each line of the CSV file
    bool header = true;             
    while (std::getline(file, line)) {  // Read each line of the file
        std::vector<std::any> row_data;  
        std::vector<std::string> row;    
        std::stringstream ss(line);      
        std::string cell;                 
        while (std::getline(ss, cell, delimiter)) {  // Parse each cell using delimiter
            row.push_back(cell);                    
        }
        if (header) {                             // If it's the first row (header)
            for (const auto& col : row) {         
                column_order.push_back(col);      // Add column name to column_order vector
            }
            header = false;                        
        } else {                                  // For subsequent rows (data)
            for (size_t i = 0; i < row.size(); ++i) { 
                if (i < types.size() && types[i] != nullptr) {  
                    // Check the type of the cell and convert it accordingly
                    if (*types[i] == typeid(int)) {
                        row_data.push_back(std::stoi(row[i]));   
                    } else if (*types[i] == typeid(float)) {
                        row_data.push_back(std::stof(row[i]));   
                    } else if (*types[i] == typeid(std::string)) {
                        row_data.push_back(row[i]);               
                    } else {
                        //TODO: Add more types as needed (data)
                        row_data.push_back(row[i]);               
                    }
                } else {
                    row_data.push_back(row[i]);                   // Default to string
                }
            }
            data.push_back(row_data);             // Add the row data to the main data vector
        }
    }

    file.close();                        
    return {column_order, data};         
}
