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

void FileReader::read(char delimiter,int start, int & end,
                      ConsumerProducerQueue<DataFrame*>& queue_out, ConsumerProducerQueue<std::string>& queue_in,
                      bool read_in_blocks, int block_size, std::string filenameFormat) {
    while (true) {
        std::string filename = queue_in.pop();
        if (filename == "STOP") {

            queue_out.push(nullptr);
            break;
        }

        if (filename.find(filenameFormat) == std::string::npos) {
            continue;
        }

        int fd = open(filename.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Error opening CSV file: " << filename << std::endl;
            return;
        }

        __gnu_cxx::stdio_filebuf<char> filebuf(fd, std::ios::in);
        std::istream file(&filebuf);

        if (flock(fd, LOCK_SH) != 0) {
            std::cerr << "Error locking file: " << filename << std::endl;
            close(fd);
            return;
        }

        file.seekg(start);

        std::vector<std::string> column_order;
        std::string line;
        DataFrame *df = nullptr;
        bool header = true;
        std::vector<const std::type_info*> types;

        size_t line_count = 0;
        while (std::getline(file, line)) {
            std::vector<DataVariant> row_data;
            std::vector<std::string> row;
            std::stringstream ss(line);
            std::string cell;

            while (std::getline(ss, cell, delimiter)) {
                row.push_back(cell);
            }

            if (header) {
                column_order = row;
                std::string second_line;
                std::vector<DataVariant> second_row_data;
                if (std::getline(file, second_line)) {
                    std::stringstream ss(second_line);
                    std::string cell;
                    while (std::getline(ss, cell, delimiter)) {
                        if (cell.find_first_not_of("0123456789") == std::string::npos) {
                            int value = std::stoi(cell);

                            types.push_back(&typeid(int));
                            second_row_data.push_back(value);
                        } else if (cell.find_first_not_of("0123456789.") == std::string::npos) {
                            float value = std::stof(cell);

                            types.push_back(&typeid(float));
                            second_row_data.push_back(value);
                        } else {
                                types.push_back(&typeid(std::string));
                                second_row_data.push_back(cell);
                        }
                    }
                }
                df = new DataFrame(column_order, types);
                df->add_row(second_row_data);  // Add the second row data to the DataFrame
                header = false;
            }
            else {

                for (size_t i = 0; i < row.size(); ++i) {
                    if (i < types.size() && types[i] != nullptr) {
                        if (*types[i] == typeid(int)) {
                            row_data.push_back(std::stoi(row[i]));
                        } else if (*types[i] == typeid(float)) {
                            row_data.push_back(std::stof(row[i]));
                        } else if (*types[i] == typeid(std::string)) {
                            row_data.push_back(row[i]);
                        }
                        else if (*types[i] == typeid(std::tm)) {
                            std::tm value = std::tm();
                            std::istringstream ss(row[i]);
                            ss >> std::get_time(&value, "%Y-%m-%d %H:%M:%S");
                            row_data.push_back(value);
                        }
                        else{
                            //Throw exception
                            std::cerr << "Reader CSV: Error getting type of column"<< std::endl;
                        }
                    }

                }

                df->add_row(row_data);
                line_count++;

                if (line_count == block_size && read_in_blocks) {
                    if (filename.find(filenameFormat) != std::string::npos) {
                        queue_out.push(df);
                    }
                    line_count = 0;
                    df = new DataFrame(column_order, types);
                }
            }
        }

        if (df != nullptr && df->get_number_of_rows()) {
            if (filename.find(filenameFormat) != std::string::npos) {

                queue_out.push(df);
            }

        }

        end = file.tellg();

        flock(fd, LOCK_UN);
        close(fd);
    }
}

