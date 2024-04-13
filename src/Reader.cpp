#include "Reader.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <typeinfo> // Para std::type_info
#include <vector>   // Para std::vector

std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>> CSVReader::read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error opening CSV file: " << filename << std::endl;
        return {};
    }

    std::vector<std::string> column_order;
    std::vector<std::vector<std::any>> data;
    
    std::string line;
    bool header = true;
    while (std::getline(file, line)) {
        std::vector<std::any> row_data;
        std::vector<std::string> row;
        std::stringstream ss(line);
        std::string cell;
        while (std::getline(ss, cell, delimiter)) {
            row.push_back(cell);
        }
        if (header) {
            for (const auto& col : row) {
                column_order.push_back(col);
            }
            header = false;
        } else {
            for (size_t i = 0; i < row.size(); ++i) {
                if (i < types.size() && types[i] != nullptr) {
                    // Verifica se o tipo é válido
                    if (*types[i] == typeid(int)) {
                        row_data.push_back(std::stoi(row[i]));
                    } else if (*types[i] == typeid(float)) {
                        row_data.push_back(std::stof(row[i]));
                    } else if (*types[i] == typeid(std::string)) {
                        row_data.push_back(row[i]);
                    } else {
                        row_data.push_back(row[i]); // Tipo padrão: string
                    }
                } else {
                    row_data.push_back(row[i]); // Tipo padrão: string
                }
            }
            // adiciona a linha ao dataframe  
            data.push_back(row_data);
        }
    }

    file.close();
    return {column_order, data};
}
