#include "Reader.h"               
#include <fstream>                 
#include <sstream>                 
#include <iostream>               
#include <typeinfo>                
#include <vector>                  

void CSVReader::read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter, ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>>& queue, bool read_in_blocks) {
    std::ifstream file(filename);  // Open the CSV file for reading
    if (!file.is_open()) {          // Check if file opening failed
        std::cerr << "Error opening CSV file: " << filename << std::endl;  
        return;                  
    }

    std::vector<std::string> column_order;        // Vector to store column names
    std::vector<std::vector<std::any>> block_data; // Vector of vectors to store data for the current block
    
    std::string line;               // String to store each line of the CSV file
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
            column_order = row;                   // Store column names
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

            block_data.push_back(row_data);  // Add the row data to the block data vector
            line_count++;

            if (line_count == 50 && read_in_blocks) {  // If block size reached or end of file
                // Add block data to the queue
                queue.push({column_order, block_data});

                line_count = 0;  // Reset line counter
                block_data.clear();  // Clear block data
            }
        }
    }

    if (!block_data.empty()) {  // If there is remaining data
        queue.push({column_order, block_data});  // Add the remaining data to the queue
    }

    file.close();                        
}
void TXTReader::read(const std::string& filename, const std::vector<const std::type_info*>& types, char delimiter, ConsumerProducerQueue<std::pair<std::vector<std::string>, std::vector<std::vector<std::any>>>>& queue, bool read_in_blocks) {
    std::ifstream file(filename);  // Abre o arquivo TXT para leitura
    if (!file.is_open()) {          // Verifica se a abertura do arquivo falhou
        std::cerr << "Erro ao abrir o arquivo TXT: " << filename << std::endl;  
        return;                  
    }

    std::vector<std::string> column_order;        // Vetor para armazenar os nomes das colunas
    std::vector<std::vector<std::any>> block_data; // Vetor de vetores para armazenar dados para o bloco atual
    
    std::string line;               // String para armazenar cada linha do arquivo TXT
    bool header = true;             
    
    size_t line_count = 0;  // Contador de linhas lidas
    while (std::getline(file, line)) {  // Lê cada linha do arquivo
        std::vector<std::any> row_data;  // Vetor para armazenar dados para a linha atual
        std::vector<std::string> row;    // Vetor para armazenar elementos individuais da linha atual
        std::stringstream ss(line);      // String stream para analisar a linha atual
        std::string cell;                // String para armazenar cada célula da linha atual

        while (std::getline(ss, cell, delimiter)) {  // Analisa cada célula usando o delimitador
            row.push_back(cell);                    
        }

        if (header) {                             // Se for a primeira linha (cabeçalho)
            for (const auto& col : row) {         
                column_order.push_back(col);      // Adiciona o nome da coluna ao vetor column_order
            }
            header = false;                        
        } else {                                  // Para linhas subsequentes (dados)
            for (size_t i = 0; i < row.size(); ++i) { 
                if (i < types.size() && types[i] != nullptr) {  
                    // Verifica o tipo da célula e converte conforme necessário
                    if (*types[i] == typeid(int)) {
                        row_data.push_back(std::stoi(row[i]));   
                    } else if (*types[i] == typeid(float)) {
                        row_data.push_back(std::stof(row[i]));   
                    } else if (*types[i] == typeid(std::string)) {
                        row_data.push_back(row[i]);               
                    } else {
                        //TODO: Adicione mais tipos conforme necessário (dados)
                        row_data.push_back(row[i]);               
                    }
                } else {
                    row_data.push_back(row[i]);                   // Padrão é string
                }
            }

            block_data.push_back(row_data);  // Adiciona os dados da linha ao vetor de dados do bloco
            line_count++;

            if (line_count == 50 && read_in_blocks) {  // Se o tamanho do bloco for atingido ou final do arquivo
                // Adiciona os dados do bloco à fila
                queue.push({column_order, block_data});

                line_count = 0;  // Reinicia o contador de linha
                block_data.clear();  // Limpa os dados do bloco
            }
        }
    }

    if (!block_data.empty()) {  // Se houver dados restantes
        queue.push({column_order, block_data});  // Adiciona os dados restantes à fila
    }

    file.close();                        
}