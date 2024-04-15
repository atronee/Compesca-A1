#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdlib> // For rand() and srand()
#include <ctime>   // For time()
#include <filesystem>
#include "mock.h"

// Function to generate a random integer within a range
int getRandomInt(int min, int max) {
    return min + rand() % (max - min + 1);
}

std::string randomString(int length) {
    const std::string characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string result;
    for (int i = 0; i < length; ++i) {
        result += characters[getRandomInt(0, characters.size() - 1)];
    }
    return result;
}
//ContaVerde mock data

// USUARIO
// ID, NOME, SOBRENOME, ENDEREÇO, DATA DE NASCIMENTO, DATA DE CADASTRO
std::string consumerData() {
    const std::vector<std::string> names = {"Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace"};
    const std::vector<std::string> surNames = {"Smith", "Johnson", "Brown", "Taylor", "Lee", "Martinez", "White"};
    const std::vector<std::string> cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
                                             "San Antonio", "San Diego"};

    std::string name = names[getRandomInt(0, names.size() - 1)];
    std::string surName = surNames[getRandomInt(0, surNames.size() - 1)];
    std::string city = cities[getRandomInt(0, cities.size() - 1)];

    int userId = getRandomInt(1, 1000);

    int bornDay = getRandomInt(1, 30);
    int bornMonth = getRandomInt(1, 12);
    int bornYear = getRandomInt(1950, 2000);

    int registerDay = getRandomInt(1, 30);
    int registerMonth = getRandomInt(1, 12);
    int registerYear = getRandomInt(2019, 2024);

    return std::to_string(userId)+","+ name + "," + surName + "," + city + "," + std::to_string(bornDay) + "/" +
    std::to_string(bornMonth) +"/" + std::to_string(bornYear) + "," + std::to_string(registerDay) + "/" +
    std::to_string(registerMonth) + "/" + std::to_string(registerYear);
}

// PRODUTO
// ID, NOME, IMAGEM, PREÇO, DESCRIÇÃO

std::string productData()
{
    const std::vector<std::string> names = {"Bike", "Car", "Motorcycle", "Scooter", "Skateboard", "Surfboard", "Snowboard"};
    const std::vector<std::string> images = {"bike.jpg", "car.jpg", "motorcycle.jpg", "scooter.jpg", "skateboard.jpg",
                                             "surfboard.jpg", "snowboard.jpg"};
    const std::vector<std::string> descriptions = {"A bike is a vehicle with two wheels that you ride by sitting on its seat and pushing two pedals with your feet.",
                                                   "A car is a road vehicle with an engine, four wheels, and seats for a small number of people.",
                                                   "A motorcycle is a two-wheeled vehicle with an engine that you sit on while driving.",
                                                   "A scooter is a small motorbike with a low seat and small wheels.",
                                                   "A skateboard is a short narrow board with two small wheels fixed to the bottom of either end.",
                                                   "A surfboard is a long narrow board that you stand on in the sea and ride on the waves.",
                                                   "A snowboard is a board that you attach to your feet and use to slide down mountains covered with snow."};
    int productId = getRandomInt(1, 7);

    int product = getRandomInt(0, names.size() - 1);
    std::string name = names[product];
    std::string image = images[product];
    std::string description = descriptions[product];
    int price = getRandomInt(100, 1000);

    return std::to_string(productId) + "," +name + "," + image + "," + std::to_string(price) + "," + description;
}

//Estoque
//ID, Quantidade
std::string stockData()
{
    int productId = getRandomInt(1, 7);
    int quantity = getRandomInt(1, 1000);

    return std::to_string(productId) + "," + std::to_string(quantity);
}

// Ordens de compra
// ID USUARIO, ID PRODUTO, QUANTIDADE, DATA DE COMPRA, DATA DE PAGAMENTO, DATA DE ENVIO, DATA DE ENTREGA

std::string orderData()
{
    int userId = getRandomInt(1, 1000);
    int productId = getRandomInt(1, 7);
    int quantity = getRandomInt(1, 10);

    int purchaseDay = getRandomInt(1, 30);
    int purchaseMonth = getRandomInt(1, 12);
    int purchaseYear = getRandomInt(2019, 2024);

    int paymentDay = getRandomInt(purchaseDay, 30);
    int paymentMonth = getRandomInt(purchaseMonth, 12);
    int paymentYear = getRandomInt(purchaseYear, 2024);

    int shippingDay = getRandomInt(paymentDay, 30);
    int shippingMonth = getRandomInt(paymentMonth, 12);
    int shippingYear = getRandomInt(paymentYear, 2024);

    int deliveryDay = getRandomInt(shippingDay, 30);
    int deliveryMonth = getRandomInt(shippingMonth, 12);
    int deliveryYear = getRandomInt(shippingYear, 2024);

    return std::to_string(userId) + "," + std::to_string(productId) + "," + std::to_string(quantity) + "," +
    std::to_string(purchaseDay) + "/" + std::to_string(purchaseMonth) + "/" + std::to_string(purchaseYear) + "," +
    std::to_string(paymentDay) + "/" + std::to_string(paymentMonth) + "/" + std::to_string(paymentYear) + "," +
    std::to_string(shippingDay) + "/" + std::to_string(shippingMonth) + "/" + std::to_string(shippingYear) + "," +
    std::to_string(deliveryDay) + "/" + std::to_string(deliveryMonth) + "/" + std::to_string(deliveryYear);
}


void mockCSV()
{
    // Gera dados da loja e cria um csv
    // recebe uma função de geração de dados
    // cria 4 arquivos csv com dados de usuario, produto, estoque e ordens de compra


    // Seed the random number generator
    srand(static_cast<unsigned int>(time(nullptr)));

    // Number of mock data records to generate
    const int numRecords = 1000;

    // Output directory path
    const std::string outputDir = "data/";

    // Create the output directory if it doesn't exist
    std::filesystem::create_directory(outputDir);

    // Generate mock data for the consumer
    std::string consumerFilename = outputDir + "consumer.csv";

    // Open the output file
    std::ofstream consumerFile(consumerFilename);
    if (!consumerFile.is_open()) {
        std::cerr << "Error opening output file: " << consumerFilename << "\n";
        return;
    }

    consumerFile << "ID, NOME, SOBRENOME, ENDEREÇO, DATA DE NASCIMENTO, DATA DE CADASTRO\n";

    for (int record = 1; record <= numRecords; ++record) {
        std::string consumer = consumerData();
        consumerFile << consumer << "\n";
    }

    // Close the output file
    consumerFile.close();

    std::cout << "Consumer data generated: " << consumerFilename << "\n";

    // Generate mock data for the product
    std::string productFilename = outputDir + "product.csv";

    // Open the output file
    std::ofstream productFile(productFilename);
    if (!productFile.is_open()) {
        std::cerr << "Error opening output file: " << productFilename << "\n";
        return;
    }

    productFile << "ID, NOME, IMAGEM, PREÇO, DESCRIÇÃO\n";

    for (int record = 1; record <= numRecords; ++record) {
        std::string product = productData();
        productFile << product << "\n";
    }

    // Close the output file
    productFile.close();

    std::cout << "Product data generated: " << productFilename << "\n";

    // Generate mock data for the stock
    std::string stockFilename = outputDir + "stock.csv";

    // Open the output file
    std::ofstream stockFile(stockFilename);
    if (!stockFile.is_open()) {
        std::cerr << "Error opening output file: " << stockFilename << "\n";
        return;
    }

    stockFile << "ID PRODUTO, QUANTIDADE\n";

    for (int record = 1; record <= numRecords; ++record) {
        std::string stock = stockData();
        stockFile << stock << "\n";
    }

    // Close the output file
    stockFile.close();

    std::cout << "Stock data generated: " << stockFilename << "\n";

    // Generate mock data for the order
    std::string orderFilename = outputDir + "order.csv";

    // Open the output file
    std::ofstream orderFile(orderFilename);
    if (!orderFile.is_open()) {
        std::cerr << "Error opening output file: " << orderFilename << "\n";
        return;
    }
    //column names
    orderFile << "ID USUARIO, ID PRODUTO, QUANTIDADE, DATA DE COMPRA, DATA DE PAGAMENTO, DATA DE ENVIO, DATA DE ENTREGA\n";

    for (int record = 1; record <= numRecords; ++record) {
        std::string order = orderData();
        orderFile << order << "\n";
    }

    // Close the output file
    orderFile.close();

    std::cout << "Order data generated: " << orderFilename << "\n";

    std::cout << "Mock data generation complete.\n";
}

//DataCat

/*
O serviço DataCat fornece arquivos de log em texto (formato TXT) disponibilizados
em um diretório da máquina executando o ETL
Os eventos de log podem ser classificados em auditoria, comportamento do
usuário, notificação de falhas e depuração.
Todo evento de log é composto pela data de notificação, pelo seu tipo, e por uma
mensagem com o conteúdo textual do evento.
Um log de auditoria representa adicionalmente o ID do usuário autor e a ação
realizada sobre o sistema.
Um log de comportamento do usuário representa adicionalmente o ID do usuário
autor, o estímulo realizado sobre a interface, e o componente-alvo.
Um log de notificação de falhas representa adicionalmente o componente-alvo, o
arquivo/linha de código, e a gravidade.
 */

std::string generateLogAudit()
{
const std::vector<std::string> actions = {"create", "read", "update", "delete"};
    const std::vector<std::string> actionOnSystem = {"User created a new account", "User read a document",
                                                     "User updated a document", "User deleted a document"};
    std::string textContent = randomString(50);

    std::string action = actions[getRandomInt(0, actions.size() - 1)];
    int userAuthorId = getRandomInt(0, 1000);
    std::string actionDescription = actionOnSystem[getRandomInt(0, actionOnSystem.size() - 1)];

    int day = getRandomInt(1, 30);
    int month = getRandomInt(1, 12);
    int year = getRandomInt(2019, 2024);

    return "audit," + std::to_string(userAuthorId) + "," + action + "," + actionDescription + "," + textContent + "," +
    std::to_string(day) + "/" + std::to_string(month) + "/" + std::to_string(year);
}

std::string generateLogUserBehavior()
{
    const std::vector<std::string> actions = {"click", "hover", "scroll", "drag"};
    const std::vector<std::string> components = {"button", "input", "table", "form"};
    const std::vector<std::string> stimuli = {"User clicked on a button", "User hovered over an input field",
                                              "User scrolled through a table", "User dragged a form element"};
    std::string textContent = randomString(50);

    std::string action = actions[getRandomInt(0, actions.size() - 1)];
    int userAuthorId = getRandomInt(0, 1000);
    std::string stimulus = stimuli[getRandomInt(0, stimuli.size() - 1)];
    std::string component = components[getRandomInt(0, components.size() - 1)];

    int day = getRandomInt(1, 30);
    int month = getRandomInt(1, 12);
    int year = getRandomInt(2019, 2024);

    return "user_behavior," + std::to_string(userAuthorId) + "," + action + "," + stimulus + "," + component + "," +
    textContent + "," + std::to_string(day) + "/" + std::to_string(month) + "/" + std::to_string(year);
}

std::string generateLogFailureNotification()
{
    const std::vector<std::string> components = {"database", "server", "client", "network"};
    const std::vector<std::string> severities = {"low", "medium", "high", "critical"};
    const std::vector<std::string> messages = {"Database connection failed", "Server timeout", "Client error",
                                               "Network failure"};
    std::string textContent = randomString(50);

    int comp = getRandomInt(0, components.size() - 1);
    std::string component = components[comp];
    std::string severity = severities[getRandomInt(0, severities.size() - 1)];
    std::string message = messages[comp];

    int day = getRandomInt(1, 30);
    int month = getRandomInt(1, 12);
    int year = getRandomInt(2019, 2024);

    return "failure," + component + "," + message + "," + severity + "," + textContent + "," + std::to_string(day) + "/" +
    std::to_string(month) + "/" + std::to_string(year);
}

std::string generateLogDebug()
{
    const std::vector<std::string> messages = {"Debug message 1", "Debug message 2", "Debug message 3", "Debug message 4"};
    std::string textContent = randomString(50);

    std::string message = messages[getRandomInt(0, messages.size() - 1)];

    int day = getRandomInt(1, 30);
    int month = getRandomInt(1, 12);
    int year = getRandomInt(2019, 2024);

    return "debug," + message + "," + textContent + "," + std::to_string(day) + "/" + std::to_string(month) + "/" +
    std::to_string(year);
}

// Mocks the behavior of a continuous status machine that constantly creates log files
void mockLogFiles() {
    srand(static_cast<unsigned int>(time(nullptr)));

    // Output directory path
    const std::string outputDir = "logs/";

    // Create the output directory if it doesn't exist
    system(("mkdir -p " + outputDir).c_str());

    // Number of log lines per file
    const int linesPerFile = 100;

    // Number of log files to generate
    const int numFiles = 5;

    for (int fileIndex = 1; fileIndex <= numFiles; ++fileIndex) {
        std::string filename = outputDir + "log_" + std::to_string(fileIndex) + ".txt";

        // Open the output file
        std::ofstream outputFile(filename);
        if (!outputFile.is_open()) {
            std::cerr << "Error opening output file: " << filename << "\n";
            return;
        }
        int randLog = 0;
        for (int line = 1; line <= linesPerFile; ++line) {
            randLog = getRandomInt(1, 4);
            switch (randLog) {
                case 1: {
                    std::string logMessage = generateLogAudit();
                    outputFile << logMessage << "\n";
                    break;
                }
                case 2: {
                    std::string logMessage = generateLogUserBehavior();
                    outputFile << logMessage << "\n";
                    break;
                }
                case 3: {
                    std::string logMessage = generateLogFailureNotification();
                    outputFile << logMessage << "\n";
                    break;
                }
                case 4: {
                    std::string logMessage = generateLogDebug();
                    outputFile << logMessage << "\n";
                    break;
                }
                default:
                    break;
            }
        }
        // Close the output file
        outputFile.close();
        std::cout << "Log file generated: " << filename << "\n";
    }
    std::cout << "Mock log file generation complete.\n";
}
int main()
{
    mockCSV();
    mockLogFiles();

    return 0;
}
