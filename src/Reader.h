#ifndef READER_H                   
#define READER_H

#include "DataFrame.h"
#include <vector>                 
#include <string>
#include <map>
#include <unordered_map>
#include <any>
#include <typeinfo> 
#include "ConsumerProducerQueue.h"
#include <iostream>
#include <filesystem>
#include <mutex>

class Reader { // Declaration of the base class Reader
public:
    std::mutex dataframes_mtx;
    std::unordered_map<std::filesystem::path, std::vector<DataFrame *>> dataframes;

    virtual void read(char delimiter, int start, int &end,
                      ConsumerProducerQueue<DataFrame *> &queue_out, ConsumerProducerQueue<std::string> &queue_in,
                      bool read_in_blocks, int block_size, std::string filenameFormat) = 0;
    // Pure virtual function for reading data
    // Parameters: filename - name of the file to read from
};

class FileReader : public Reader {  // Declaration of the derived class CSVReader, inheriting from Reader
public:
    void read(char delimiter,int start, int & end,
              ConsumerProducerQueue<DataFrame*>& queue_out, ConsumerProducerQueue<std::string>& queue_in,
              bool read_in_blocks, int block_size, std::string filenameFormat) override;
    // Implementation of the read function for reading CSV files
    // Parameters: filename - name of the CSV file to read from
    //             types - vector of type_info pointers specifying the types of data to read
    //             delimiter - character used as a delimiter in the CSV file (default: ',')
    //             queue - reference to the queue to store the data
    //             read_in_blocks - flag to indicate whether to read data in blocks or not (default: true)
};

#endif // READER_H               // End of header guard
