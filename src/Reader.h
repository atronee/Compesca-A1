#ifndef READER_H
#define READER_H

#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <any>
#include <typeinfo> // Para std::type_info

class Reader {
public:
    virtual std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>> read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter = ',') = 0;
};

class CSVReader : public Reader {
public:
    std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>> read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter = ',') override;
};

#endif // READER_H
