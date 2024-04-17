#include "Reader.h"
#include "DataFrame.h"
#include <fstream>                 
#include <sstream>                 
#include <iostream>               
#include <typeinfo>                
#include <vector>                  

void FileReader::read(const std::string& filename, std::vector<const std::type_info*>& types, char delimiter, ConsumerProducerQueue<DataFrame>& queue, bool read_in_blocks, int block_size) {
    std::ifstream file(filename);  // Open the CSV file for reading
    if (!file.is_open()) {          // Check if file opening failed
        std::cerr << "Error opening CSV file: " << filename << std::endl;  
        return;
    }

    std::vector<std::string> column_order;        // Vector to store column names
    std::vector<std::vector<std::any>> block_data; // Vector of vectors to store data for the current block
    std::string line;               // String to store each line of the CSV file
    DataFrame* df = nullptr;        // Pointer to the DataFrame object
    bool header = true;

    size_t line_count = 0;  // Counter for lines read
    while (std::getline(file, line)) {  // Read each line of the file
        std::vector<std::any> row_data;  // Vector to store data for the current row
        std::vector<std::string> row;    // Vector to store individual elements of the current row
        std::stringstream ss(line);      // String stream to parse the current line
        std::string cell;                // String to store each cell of the current row

        while (std::getline(ss, cell, delimiter)) {  // Parse each cell using delimiter
            row.push_back(cell);                   
        }

        if (header) {                             // If it's the first row (header)
            column_order = row;// Store column names
            df = new DataFrame(column_order, types);  // Create a DataFrame with column names and types
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

            df->add_row(row_data);  // Add the row data to the block data vector
            line_count++;

            if (line_count == block_size && read_in_blocks) {  // If block size reached or end of file
                // Add block data to the queue
                queue.push(*df);

                line_count = 0;  // Reset line counter
                df->clear_data();
            }
        }
    }

    if (df != nullptr && !df->get_number_of_rows()) {  // If there is remaining data
        queue.push(*df);  // Add the remaining data to the queue
    }

    file.close();                        
}