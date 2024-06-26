#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdlib> // For rand() and srand()
#include <random>
#include <ctime>   // For time()
#include <filesystem>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include "mock.h"
#include "../libs/sqlite3.h"
#include <chrono>
#include <math.h>
#include <thread>
#include "ConsumerProducerQueue.h"

// Function to generate a random integer within a range
int getRandomNormalizedInt(int min, int max) {

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min, max);

    double beta = 200.0; // beta distribution parameter
    double alpha = 1.0;
    double u = dis(gen);
    double v = dis(gen);
    double x = std::pow(u, 1.0/alpha)+0.001;
    double y = std::pow(v, 1.0/beta)+0.001;
    return round(min + (max - min) * x / (x + y));
}

int getRandomInt(int min, int max) {
    return min + (rand() % (max - min + 1));
}

std::string getRandomNowBasedDate(int minYear, int minMonth, int minDay, int minHour, int minMinute, int minSecond)
{
    auto now = std::chrono::system_clock::now();

    // Convert the current time to a time_t object (seconds since epoch)
    std::time_t currentTime = std::chrono::system_clock::to_time_t(now);

    // Convert the current time to a local time struct tm
    std::tm* localTime = std::localtime(&currentTime);

    // Extract individual components of the current time

    int year = getRandomNormalizedInt(minYear, localTime->tm_year + 1900);
    int month = getRandomNormalizedInt(minMonth, localTime->tm_mon + 1);
    int day = getRandomNormalizedInt(minDay, localTime->tm_mday+1);
    int hour = getRandomNormalizedInt(minHour, localTime->tm_hour);
    int minute = getRandomNormalizedInt(minMinute, localTime->tm_min);
    int second = getRandomNormalizedInt(minSecond, localTime->tm_sec);

    std::string dayStr = std::to_string(day);
    std::string monthStr = std::to_string(month);
    std::string yearStr = std::to_string(year);
    std::string hourStr = std::to_string(hour);
    std::string minuteStr = std::to_string(minute);
    std::string secondStr = std::to_string(second);

    if(day<10)
    {
        dayStr = "0" + std::to_string(day);
    }
    if(month<10)
    {
        monthStr = "0" + std::to_string(month);
    }
    if(hour<10)
    {
        hourStr = "0" + std::to_string(hour);
    }
    if(minute<10)
    {
        minuteStr = "0" + std::to_string(minute);
    }
    if(second<10)
    {
        secondStr = "0" + std::to_string(second);
    }
    return yearStr + "-" + monthStr + "-" + dayStr + " " + hourStr + ":" + minuteStr + ":" + secondStr;

}

std::string getRandomDate(int minYear, int minMonth, int minDay, int minHour, int minMinute, int minSecond,
                          int maxYear, int maxMonth, int maxDay, int maxHour, int maxMinute, int maxSecond)
{

    // Extract individual components of the current time

    int year = getRandomNormalizedInt(minYear, maxYear);
    int month = getRandomNormalizedInt(minMonth, maxMonth);
    int day = getRandomNormalizedInt(minDay, maxDay);
    int hour = getRandomNormalizedInt(minHour, maxHour);
    int minute = getRandomNormalizedInt(minMinute, maxMinute);
    int second = getRandomNormalizedInt(minSecond, maxSecond);

    std::string dayStr = std::to_string(day);
    std::string monthStr = std::to_string(month);
    std::string yearStr = std::to_string(year);
    std::string hourStr = std::to_string(hour);
    std::string minuteStr = std::to_string(minute);
    std::string secondStr = std::to_string(second);

    if(day<10)
    {
        dayStr = "0" + std::to_string(day);
    }
    if(month<10)
    {
        monthStr = "0" + std::to_string(month);
    }
    if(hour<10)
    {
        hourStr = "0" + std::to_string(hour);
    }
    if(minute<10)
    {
        minuteStr = "0" + std::to_string(minute);
    }
    if(second<10)
    {
        secondStr = "0" + std::to_string(second);
    }
    return yearStr + "-" + monthStr + "-" + dayStr + " " + hourStr + ":" + minuteStr + ":" + secondStr;

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
std::string consumerData(bool isSqlFlag = false) {
    const std::vector<std::string> names = {"Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace"};
    const std::vector<std::string> surNames = {"Smith", "Johnson", "Brown", "Taylor", "Lee", "Martinez", "White"};
    const std::vector<std::string> cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
                                             "San Antonio", "San Diego"};

    std::string name = names[getRandomInt(0, names.size() - 1)];
    std::string surName = surNames[getRandomInt(0, surNames.size() - 1)];
    std::string city = cities[getRandomInt(0, cities.size() - 1)];

    int userId = getRandomInt(1, 1000);

    std::string bornDate = getRandomDate(1950, 1, 1, 0, 0, 0, 2000, 12, 31, 0, 0, 0);
    std::string registerDate = getRandomDate(2019, 1, 1, 0, 0, 0, 2021, 12, 31, 0, 0, 0);

    if (isSqlFlag)
    {
        return "INSERT INTO Consumer (ID, NOME, SOBRENOME, ENDEREÇO, DATA_DE_NASCIMENTO, DATA_DE_CADASTRO) VALUES (" +
        std::to_string(userId) + ", '" + name + "', '" + surName + "', '" + city + "', '" +
        bornDate + "', '" + registerDate + "');";
    }
    return std::to_string(userId)+","+ name + "," + surName + "," + city + "," + bornDate + "," + registerDate;
}

// PRODUTO
// ID, NOME, IMAGEM, PREÇO, DESCRIÇÃO

std::string productData()
{
    const std::vector<std::string> names = {"Bike", "Car", "Motorcycle", "Scooter", "Skateboard", "Surfboard", "Snowboard"};
    const std::vector<std::string> images = {"bike.jpg", "car.jpg", "motorcycle.jpg", "scooter.jpg", "skateboard.jpg",
                                             "surfboard.jpg", "snowboard.jpg"};
    const std::vector<std::string> descriptions = {"A bike is a vehicle with two wheels that you ride by sitting on its seat and pushing two pedals with your feet.",
                                                   "A car is a road vehicle with an engine four wheels and seats for a small number of people.",
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
    int price = getRandomNormalizedInt(100, 1000);

    return std::to_string(productId) + "," +name + "," + image + "," + std::to_string(price) + "," + description;
}

//Estoque
//ID, Quantidade
std::string stockData(int productId)
{
    int quantity = getRandomNormalizedInt(1, 1000);
    return std::to_string(productId) + "," + std::to_string(quantity);
}

// Ordens de compra
// ID USUARIO, ID PRODUTO, QUANTIDADE, DATA DE COMPRA, DATA DE PAGAMENTO, DATA DE ENVIO, DATA DE ENTREGA

std::string orderData()
{
    int userId = getRandomInt(1, 1000);
    int productId = getRandomInt(1, 7);
    int quantity = getRandomInt(1, 10);

    std::string purchaseDate = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);

    std::string paymentDate = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);
    
    std::string shippingDate = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);

    std::string deliveryDate = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);

    return std::to_string(userId) + "," + std::to_string(productId) + "," + std::to_string(quantity) + "," +
    purchaseDate + "," + paymentDate + "," + shippingDate + "," + deliveryDate;
}


void mockCSV(int index, const int numRecords)
{
    // Seed the random number generator
    srand(static_cast<unsigned int>(time(nullptr)));

    // Output directory path
    const std::string outputDir = "data/";

    // Deletes any existing files in the output directory if it exists
    if (std::filesystem::exists(outputDir)) {
        std::filesystem::remove_all(outputDir);
    }

    // Create the output directory if it doesn't exist
    std::filesystem::create_directory(outputDir);

    // Generate mock data for the consumer
    std::string consumerFilename = outputDir + "consumer" + std::to_string(index) + ".csv";

    // Open the output file with POSIX open
    int fd_consumer = open(consumerFilename.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd_consumer == -1) {
        std::cerr << "Error opening output file: " << consumerFilename << "\n";
        return;
    }

    // Apply a lock to the file descriptor
    flock(fd_consumer, LOCK_EX);

    // Use ofstream to write to the file
    std::ofstream consumerFile(consumerFilename);
    if (!consumerFile.is_open()) {
        std::cerr << "Error opening output file: " << consumerFilename << "\n";
        close(fd_consumer);
        return;
    }

    consumerFile << "ID,NOME,SOBRENOME,ENDEREÇO,DATA_DE_NASCIMENTO,DATA_DE_CADASTRO\n";

    for (int record = 1; record <= numRecords; ++record) {
        std::string consumer = consumerData();
        consumerFile << consumer << "\n";
    }

    // Close the output file
    consumerFile.close();
    flock(fd_consumer, LOCK_UN);
    close(fd_consumer);


    std::cout << "Consumer data generated: " << consumerFilename << "\n";

    // Generate mock data for the product
    std::string productFilename = outputDir + "product" + std::to_string(index) + ".csv";

    // Open the output file with POSIX open
    int fd_product = open(productFilename.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd_product == -1) {
        std::cerr << "Error opening output file: " << productFilename << "\n";
        return;
    }

    // Apply a lock to the file descriptor
    flock(fd_product, LOCK_EX);

    // Open the output file
    std::ofstream productFile(productFilename);
    if (!productFile.is_open()) {
        std::cerr << "Error opening output file: " << productFilename << "\n";
        return;
    }

    productFile << "ID,NOME,IMAGEM,PREÇO,DESCRIÇÃO\n";

    for (int record = 1; record <= numRecords; ++record) {
        std::string product = productData();
        productFile << product << "\n";
    }

    // Close the output file
    productFile.close();
    flock(fd_product, LOCK_UN);
    close(fd_product);


    // Generate mock data for the stock
    std::string stockFilename = outputDir + "stock" + std::to_string(index) + ".csv";

    // Open the output file with POSIX open
    int fd_stock = open(stockFilename.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd_stock == -1) {
        std::cerr << "Error opening output file: " << productFilename << "\n";
        return;
    }

    // Apply a lock to the file descriptor
    flock(fd_stock, LOCK_EX);

    // Open the output file
    std::ofstream stockFile(stockFilename);
    if (!stockFile.is_open()) {
        std::cerr << "Error opening output file: " << stockFilename << "\n";
        return;
    }

    stockFile << "ID_PRODUTO,QUANTIDADE_STOCK\n";

    for (int record = 1; record <= 7; ++record) {
        std::string stock = stockData(record);
        stockFile << stock << "\n";
    }

    // Close the output file
    stockFile.close();
    flock(fd_stock, LOCK_UN);
    close(fd_stock);

    // Generate mock data for the order
    std::string orderFilename = outputDir + "order" + std::to_string(index) + ".csv";

    // Open the output file with POSIX open
    int fd_order = open(orderFilename.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd_order == -1) {
        std::cerr << "Error opening output file: " << productFilename << "\n";
        return;
    }

    // Apply a lock to the file descriptor
    flock(fd_order, LOCK_EX);

    // Open the output file
    std::ofstream orderFile(orderFilename);
    if (!orderFile.is_open()) {
        std::cerr << "Error opening output file: " << orderFilename << "\n";
        return;
    }

    //column names
    orderFile << "ID_USUARIO,ID_PRODUTO,QUANTIDADE,DATA_DE_COMPRA,DATA_DE_PAGAMENTO,DATA_DE_ENVIO,DATA_DE_ENTREGA\n";

    for (int record = 1; record <= numRecords; ++record) {
        std::string order = orderData();
        orderFile << order << "\n";
    }

    // Close the output file
    orderFile.close();
    flock(fd_order, LOCK_UN);
    close(fd_order);

}

//DataCat mock data

std::string generateLogAudit()
{
const std::vector<std::string> actions = {"create", "read", "update", "delete"};
    const std::vector<std::string> actionOnSystem = {"User created a new account", "User read a document",
                                                     "User updated a document", "User deleted a document"};
    std::string textContent = randomString(50);

    std::string action = actions[getRandomInt(0, actions.size() - 1)];
    int userAuthorId = getRandomInt(0, 1000);
    std::string actionDescription = actionOnSystem[getRandomInt(0, actionOnSystem.size() - 1)];

    std::string date = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);

    return "audit," + std::to_string(userAuthorId) + "," + action + "," + actionDescription + "," + textContent +
    "," + date;
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
    // if the user clicked on a button, then the column buttonProductId will have the product id else it will be id 0
    int buttonProductId = action == "click" ? getRandomInt(1, 7) : 0;

    std::string date = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);

    return "user_behavior," + std::to_string(userAuthorId) + "," + action + ","+
    std::to_string(buttonProductId)+  "," + stimulus + "," + component + "," + textContent + "," +
    date;
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

    std::string date = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);

    return "failure," + component + "," + message + "," + severity + "," + textContent + "," + date;
}

std::string generateLogDebug()
{
    const std::vector<std::string> messages = {"Debug message 1", "Debug message 2", "Debug message 3", "Debug message 4"};
    std::string textContent = randomString(50);

    std::string message = messages[getRandomInt(0, messages.size() - 1)];

    std::string date = getRandomNowBasedDate(2019, 1, 1, 0, 0, 0);

    return "debug," + message + "," + textContent + "," + date;
}

// Mocks the behavior of a continuous status machine that constantly creates log files

void writeColumnNames(std::ofstream& outputFile, LogType fileType) {
    switch (fileType) {
        case LOG_AUDIT:
            outputFile << "Type,User_Author_Id,Action,Action_Description,Text_Content,Date\n";
            break;
        case LOG_USER_BEHAVIOR:
            outputFile << "Type,User_Author_Id,Action,Button_Product_Id,Stimulus,Component,Text_Content,Date\n";
            break;
        case LOG_FAILURE_NOTIFICATION:
            outputFile << "Type,Component,Message,Severity,Text_Content,Date\n";
            break;
        case LOG_DEBUG:
            outputFile << "Type,Message,Text_Content,Date\n";
            break;
        default:
            break;
    }
}

void mockLogFiles(int filesPerType, int linesPerFile, int startFileIndex) {
    srand(static_cast<unsigned int>(time(nullptr)));

    // Output directory path
    const std::string outputDir = "logs/";

    // Create the output directory if it doesn't exist
    system(("mkdir -p " + outputDir).c_str());

    // Number of log lines per file

    // Number of log files per type

    for (int fileType = LOG_AUDIT; fileType <= LOG_DEBUG; ++fileType) {
        std::string typeLabel;
        switch (fileType) {
            case LOG_AUDIT:
                typeLabel = "audit";
                break;
            case LOG_USER_BEHAVIOR:
                typeLabel = "user_behavior";
                break;
            case LOG_FAILURE_NOTIFICATION:
                typeLabel = "failure";
                break;
            case LOG_DEBUG:
                typeLabel = "debug";
                break;
            default:
                std::cerr << "Invalid file type.\n";
                return;
        }

        for (int fileIndex = startFileIndex; fileIndex <= filesPerType; ++fileIndex) {
            std::string filename = outputDir + typeLabel + "_logs_" + std::to_string(fileIndex) + ".txt";

            // Open the output file with POSIX open
            int fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0644);
            if (fd == -1) {
                std::cerr << "Error opening output file: " << filename << "\n";
                return;
            }

            // Apply a lock to the file descriptor
            flock(fd, LOCK_EX);

            // Open the output file
            std::ofstream outputFile(filename);
            if (!outputFile.is_open()) {
                std::cerr << "Error opening output file: " << filename << "\n";
                return;
            }

            // Write column names
            writeColumnNames(outputFile, static_cast<LogType>(fileType));

            // Write log messages
            for (int line = 1; line <= linesPerFile; ++line) {
                std::string logMessage;

                switch (fileType) {
                    case LOG_AUDIT:
                        logMessage = generateLogAudit();
                        break;
                    case LOG_USER_BEHAVIOR:
                        logMessage = generateLogUserBehavior();
                        break;
                    case LOG_FAILURE_NOTIFICATION:
                        logMessage = generateLogFailureNotification();
                        break;
                    case LOG_DEBUG:
                        logMessage = generateLogDebug();
                        break;
                    default:
                        break;
                }

                outputFile << logMessage << "\n";
            }

            // Close the output file
            outputFile.close();
            flock(fd, LOCK_UN);
            close(fd);
        }
    }
}

// Sqlite Mock data
// creates a sql command that creates the same data as the consumerData function

std::string createConsumerTable()
{
    return "CREATE TABLE Consumer (ID INTEGER PRIMARY KEY, NOME TEXT, SOBRENOME TEXT, ENDEREÇO TEXT, DATA_DE_NASCIMENTO TEXT, DATA_DE_CADASTRO TEXT);";
}

void mockSqliteTable(const int lines)
{
    // creates a sqlite db
    sqlite3 *db;
    int rc = sqlite3_open("../mock.db", &db);
    if(rc)
    {
        std::cerr << "Error opening SQLite3 database: " << sqlite3_errmsg(db) << std::endl << std::endl;
        sqlite3_close(db);
        return;
    }
    else
    {
        std::cout << "Opened mock.db." << std::endl << std::endl;
    }

    // Get the file descriptor and apply a lock
    int fd = -1;
    sqlite3_file_control(db, nullptr, SQLITE_FCNTL_PERSIST_WAL, &fd);
    if (fd == -1) {
        std::cerr << "Error getting file descriptor: " << sqlite3_errmsg(db) << "\n";
        sqlite3_close(db);
        return;
    }
    if (flock(fd, LOCK_EX) != 0) {
        std::cerr << "Error locking file: " << sqlite3_errmsg(db) << "\n";
        close(fd);
        sqlite3_close(db);
        return;
    }

    // creates a table
    char *err_msg = nullptr;
    std::string sql = createConsumerTable();
    rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err_msg);
    if(rc != SQLITE_OK)
    {
        std::cerr << "SQL error: " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }
    else
    {
        std::cout << "Table created successfully." << std::endl;
    }

    // inserts data into the table
    for(int i = 0; i <lines; i++)
    {
        sql = consumerData(true);
        rc = sqlite3_exec(db, sql.c_str(), 0, 0, &err_msg);
        if(rc != SQLITE_OK)
        {
            std::cerr << "SQL error: " << err_msg << std::endl;
            sqlite3_free(err_msg);
        }
    }

    // Release the lock and close the file
    flock(fd, LOCK_UN);
    close(fd);
    sqlite3_close(db);
}

void mockRandomRequest(ConsumerProducerQueue<std::string> &queue, int interval)
{
    // Output directory path
    const std::string outputDir = "request/";
    std::filesystem::create_directory(outputDir);
    srand(static_cast<unsigned int>(time(nullptr)));
    int linesPerFile = getRandomInt(100, 1000);
    int fileIndex = 1;
    while (true) {
        std::string filename = outputDir + "user_behavior" + "_rqt_" + std::to_string(fileIndex) + ".txt";

        // Open the output file with POSIX open
        int fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd == -1) {
            std::cerr << "Error opening output file: " << filename << "\n";
            return;
        }

        flock(fd, LOCK_EX);

        // Open the output file
        std::ofstream outputFile(filename);
        if (!outputFile.is_open()) {
            std::cerr << "Error opening output file: " << filename << "\n";
            return;
        }

        // Write column names
        outputFile << "Type,User_Author_Id,Action,Button_Product_Id,Stimulus,Component,Text_Content,Date\n";

        // Write log messages
        for (int line = 1; line <= linesPerFile; ++line) {
            std::string logMessage;
            logMessage = generateLogUserBehavior();
            outputFile << logMessage << "\n";
        }

        // Close the output file
        outputFile.close();
        flock(fd, LOCK_UN);
        close(fd);
        fileIndex++;
        queue.push(filename);

        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
}

//int main()
//{
//    mockCSV();
//    mockLogFiles(10, 100);
//    mockSqliteTable(80);
//
//    return 0;
//}
