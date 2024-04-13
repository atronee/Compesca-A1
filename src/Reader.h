#ifndef READER_H                   
#define READER_H

#include <vector>                 
#include <string>
#include <map>
#include <unordered_map>
#include <any>
#include <typeinfo> 

class Reader {                    // Declaration of the base class Reader
public:
    virtual std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>> read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter = ',') = 0;
};

class CSVReader : public Reader {  // Declaration of the derived class CSVReader, inheriting from Reader
public:
    std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>> read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter = ',') override;
    // Implementation of the read function for reading CSV files
    // Parameters: filename - name of the CSV file to read from
    //             types - vector of type_info pointers specifying the types of data to read
    //             delimiter - character used as a delimiter in the CSV file (default: ',')
};

class TXTReader : public Reader {  // Declaration of the derived class TXTReader, inheriting from Reader
public:
    std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>> read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter = ',') override;
    
};

#endif // READER_H               // End of header guard
