#include "Reader.h"
#include "DataFrame.h"
#include <fstream>                 
#include <sstream>                 
#include <iostream>               
#include <typeinfo>                
#include <vector>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <ext/stdio_filebuf.h>
#include "ConsumerProducerQueue.h"
#include <sqlite3.h>
#include <ctime> 
#include <iomanip> 
#include <chrono>          

void FileReader::read(const std::string& filename, std::vector<const std::type_info*>& types, char delimiter,
                      int start, int & end, ConsumerProducerQueue<DataFrame*>& queue,
                      bool read_in_blocks, int block_size) {
    /*
     * Function to read data from a CSV or TXT file and store it in a queue of DataFrames
     * Parameters:
     * - filename: path of the file to read from
     * - types: vector of type_info pointers specifying the types of data to read
     * - delimiter: character used as a delimiter in the file
     * - start: line to start reading
     * - queue: reference to the queue to store the data
     * - read_in_blocks: flag to indicate whether to read data in blocks or not
     * - block_size: number of rows to read in each block
     */

    std::ifstream file(filename);  // Open the CSV file for reading
    if (!file.is_open()) {          // Check if file opening failed
        std::cerr << "Error opening CSV file: " << filename << std::endl;  
        return;
    }
    file.seekg(start); // Move to the start line

    std::vector<std::string> column_order;        // Vector to store column names
    std::string line;               // String to store each line of the CSV file
    DataFrame* df = nullptr;        // Pointer to the DataFrame object
    bool header = true;

    size_t line_count = 0;  // Counter for lines read
    while (std::getline(file, line)) {  // Read each line of the file
        std::vector<DataVariant> row_data;  // Vector to store data for the current row
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
                    } else if (*types[i] == typeid(std::tm)) {
                        // Converter a string de data para um struct std::tm
                        
                        std::istringstream ss(row[i]);
                        std::tm tm_date = {};
                        ss >> std::get_time(&tm_date, "%d/%m/%Y");  

                        // Verificar se a conversão foi bem-sucedida
                        if (ss.fail()) {
                            std::cerr << "Error converting date string to std::tm" << std::endl;
    
                        } else {
                            row_data.push_back(tm_date);
                        }         
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
                queue.push(df);

                line_count = 0;  // Reset line counter
                df = new DataFrame(column_order, types);  // Create a new DataFrame object
            }
        }
    }

    if (df != nullptr && !df->get_number_of_rows()) {  // If there is remaining data
        queue.push(df);  // Add the remaining data to the queue
    }

    end = file.tellg();  // Get the end position of the file

    file.close();                        
}

void SQLiteReader::read(std::vector<const std::type_info*>& types, char delimiter, int start, int & end,
                        ConsumerProducerQueue<DataFrame*>& queue_out, ConsumerProducerQueue<std::string>& queue_in,
                        bool read_in_blocks, int block_size, std::string filenameFormat) {
    /*
     * Function to read data from a SQLite database file and store it in a queue of DataFrames
     * Parameters:
     * - filename: path of the SQLite file to read from
     * - types: vector of type_info pointers specifying the types of data to read
     * - delimiter: character used as a delimiter in the SQLite file
     * - start: line to start reading
     * - queue: reference to the queue to store the data
     * - read_in_blocks: flag to indicate whether to read data in blocks or not
     * - block_size: number of rows to read in each block
     */
    while (true) {
        std::string filename = queue_in.pop();  // Get the filename from the queue
        if (filename == "STOP") {  // Check if the filename is "STOP"
            queue_out.push(nullptr);  // Add a nullptr to the queue
            break;
        }
        if (filename.find(filenameFormat) != 0) {
            continue;
        }

        sqlite3 *db;  // SQLite database object
        int rc = sqlite3_open(filename.c_str(), &db);  // Open the SQLite database
        if (rc) {  // Check if database opening failed
            std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
            sqlite3_close(db);
            return;
        }

        sqlite3_stmt *stmt;  // SQLite statement object
        std::string query = "SELECT * FROM data";  // Query to select all data from the table
        rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);  // Prepare the query
        if (rc != SQLITE_OK) {  // Check if query preparation failed
            std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
            sqlite3_close(db);
            return;
        }

        // Apply a shared lock to the database
        if (sqlite3_db_mutex(db) != SQLITE_OK) {
            std::cerr << "Error locking database: " << filename << std::endl;
            sqlite3_close(db);
            return;
        }

        sqlite3_step(stmt);  // Execute the query

        std::vector<std::string> column_order;  // Vector to store column names
        DataFrame *df = nullptr;  // Pointer to the DataFrame object
        bool header = true;
        
        size_t line_count = 0;  // Counter for lines read

        while (sqlite3_step(stmt) == SQLITE_ROW) {  // Read each row of the result
            std::vector<DataVariant> row_data;  // Vector to store data for the current row

            if (header) {  // If it's the first row (header)
                int column_count = sqlite3_column_count(stmt);  // Get the number of columns
                for (int i = 0; i < column_count; ++i) {
                    column_order.push_back(sqlite3_column_name(stmt, i));  // Store column names
                }
                df = new DataFrame(column_order, types);  // Create a DataFrame with column names and types
                header = false;
            }

            for (int i = 0; i < sqlite3_column_count(stmt); ++i) {
                if (sqlite3_column_type(stmt, i) == SQLITE_INTEGER) {
                    row_data.push_back(sqlite3_column_int(stmt, i));  // Convert integer data
                } else if (sqlite3_column_type(stmt, i) == SQLITE_FLOAT) {
                    row_data.push_back(sqlite3_column_double(stmt, i));  // Convert float data
                } else if (sqlite3_column_type(stmt, i) == SQLITE_TEXT) {
                    row_data.push_back(std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, i))));  // Convert text data
                } else {

                    row_data.push_back(std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, i)));  // Convert text data
                }
            }

            df->add_row(row_data);  // Add the row data to the block data vector
            line_count++;

            if (line_count == block_size && read_in_blocks) {  // If block size reached or end of file
                // Add block data to the queue
                // if the beginning of the filename is in the format filenameFormat then add the block to the queue
                if (filename.find(filenameFormat) == 0) {
                    queue_out.push(df);
                }
                dataframes[std::filesystem::path(filename)].emplace_back(df);  // Store the DataFrame in the map

                line_count = 0;  // Reset line counter
                df = new DataFrame(column_order, types);  // Create a new DataFrame object
            }

        }

        if (df != nullptr && !df->get_number_of_rows()) {  // If there is remaining data
            if (filename.find(filenameFormat) == 0) {
                queue_out.push(df);
            }
            dataframes[std::filesystem::path(filename)].emplace_back(df);  // Store the DataFrame in the map
        }

        end = sqlite3_column_count(stmt);  // Get the end position of the file

        // Release the lock and close the database
        sqlite3_finalize(stmt);
        sqlite3_close(db);
    }
}
