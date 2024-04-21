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
#include <../sqlite3.h>
#include "ConsumerProducerQueue.h"

void FileReader::read(std::vector<const std::type_info*>& types, char delimiter,int start, int & end,
                      ConsumerProducerQueue<DataFrame*>& queue_out, ConsumerProducerQueue<std::string>& queue_in,
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
    while (true) {
        std::string filename = queue_in.pop();  // Get the filename from the queue
        int fd = open(filename.c_str(), O_RDONLY);  // Open the file with POSIX open
        if (fd == -1) {          // Check if file opening failed
            std::cerr << "Error opening CSV file: " << filename << std::endl;
            return;
        }

        __gnu_cxx::stdio_filebuf<char> filebuf(fd, std::ios::in);  // Create a filebuf with the file descriptor
        std::istream file(&filebuf);  // Create an istream with the filebuf

        // Apply a shared lock to the file descriptor
        if (flock(fd, LOCK_SH) != 0) {
            std::cerr << "Error locking file: " << filename << std::endl;
            close(fd);
            return;
        }

        file.seekg(start); // Move to the start line

        std::vector<std::string> column_order;        // Vector to store column names
        std::string line;               // String to store each line of the CSV file
        DataFrame *df = nullptr;        // Pointer to the DataFrame object
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
                    queue_out.push(df);

                    line_count = 0;  // Reset line counter
                    df = new DataFrame(column_order, types);  // Create a new DataFrame object
                }
            }
        }

        if (df != nullptr && !df->get_number_of_rows()) {  // If there is remaining data
            queue_out.push(df);  // Add the remaining data to the queue
        }

        end = file.tellg();  // Get the end position of the file

        // Release the lock and close the file
        flock(fd, LOCK_UN);
        close(fd);
    }
}
/*
void DbReader::readDb(const std::string& filename, ConsumerProducerQueue<DataFrame*>& queue, bool read_in_blocks,
                    int block_size, std::vector<const std::type_info*>& types) {
    /*
     * Function to read data from an SQLite database and store it in a queue of DataFrames
     * Parameters:
     * - dbPath: path to the SQLite database file
     * - query: SQL query to fetch data from the database
     * - types: vector of type_info pointers specifying the types of data to read
     * - queue: reference to the queue to store the data
     */
/*
    sqlite3* db;
    int rc = sqlite3_open(filename.c_str(), &db);  // Open the SQLite database
    if (rc != SQLITE_OK) {
        std::cerr << "Error opening SQLite database: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        return;
    }

    std::string query = "SELECT * FROM data";  // SQL query to fetch all data from the table

    sqlite3_stmt* stmt;
    rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);  // Prepare the SQL query
    if (rc != SQLITE_OK) {
        std::cerr << "Error preparing SQL statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        return;
    }

    // Bind any parameters if needed
    // Example: sqlite3_bind_int(stmt, 1, parameterValue);

    std::vector<std::string> column_order;  // Vector to store column names
    bool header = true;
    int cols = sqlite3_column_count(stmt);  // Get the number of columns in the result set

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {  // Execute the query and fetch rows
        std::vector<DataVariant> row_data;  // Vector to store data for the current row

        if (header) {  // If it's the first row (header)
            for (int i = 0; i < cols; ++i) {
                const char* columnName = sqlite3_column_name(stmt, i);
                column_order.push_back(columnName);
            }
            header = false;
        }

        for (int i = 0; i < cols; ++i) {  // Iterate over columns
            switch (sqlite3_column_type(stmt, i)) {  // Determine column type
                case SQLITE_INTEGER:
                    row_data.push_back(sqlite3_column_int(stmt, i));
                    break;
                case SQLITE_FLOAT:
                    row_data.push_back(sqlite3_column_double(stmt, i));
                    break;
                case SQLITE_TEXT:
                    row_data.push_back(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, i))));
                    break;
                default:
                    row_data.push_back(DataVariant());  // Add null for unsupported types
                    break;
            }
        }

        if (!header) {
            DataFrame* df = new DataFrame(column_order, types);
            df->add_row(row_data);
            queue.push(df);
        }
    }

    sqlite3_finalize(stmt);  // Finalize the prepared statement
    sqlite3_close(db);  // Close the SQLite database
}

void DbReader::read(const std::string& filename, std::vector<const std::type_info*>& types, char delimiter,
                    int start, int & end, ConsumerProducerQueue<DataFrame*>& queue,
                    bool read_in_blocks, int block_size) {
    // not implemented
}

int main()
{
    // code to test the db reader:
    //reads file and prints the data
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    DbReader a;

    int end = 0;
    std::vector<const std::type_info *> types = {&typeid(int), &typeid(std::string), &typeid(std::string), &typeid(std::string), &typeid(std::string), &typeid(std::string)};
    a.readDb("../mock.db", queue_reader, true, 10, types);

    DataFrame *df;

    while (queue_reader.pop() != nullptr) {
        df = queue_reader.pop();
        df->print();
        delete df;
    }
    return 0;
}*/