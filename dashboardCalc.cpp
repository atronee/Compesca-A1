#include "src/DataFrame.h"
#include "src/ConsumerProducerQueue.h"
#include "src/Handlers.h"
#include "src/Reader.h"
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <mutex>
#include <thread>
#include <atomic>
#include <iostream>
#include "src/DataFrameVersionManager.h"
#include "src/triggers.h"
#include "src/mock.h"
#include "libs/sqlite3.h"
#include <chrono>


std::tm getCurrentTimeMinusDeltaHours(int Delta) {
    // Get the current time as a time_point
    auto now = std::chrono::system_clock::now();

    // Subtract one hour
    auto DeltaHoursAgo = now - std::chrono::hours(Delta);

    // Convert to time_t
    std::time_t DeltaHoursAgo_time_t = std::chrono::system_clock::to_time_t(DeltaHoursAgo);

    // Convert to std::tm
    std::tm DeltaHoursAgo_tm = *std::localtime(&DeltaHoursAgo_time_t);

    return DeltaHoursAgo_tm;
}

string convert_tm_to_string(std::tm tm){
    return std::to_string(tm.tm_year + 1900) + "/" + std::to_string(tm.tm_mon + 1) + "/" + std::to_string(tm.tm_mday) + " " + std::to_string(tm.tm_hour) + ":" + std::to_string(tm.tm_min) + ":" + std::to_string(tm.tm_sec);
}

void mock_files(){
    for (int i = 0; i<1; i+=10){
        mockCSV();
        mockLogFiles(10, 100,i);
        mockSqliteTable(80);
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}

static int callback(void *data, int argc, char **argv, char **azColName) {
    int i;
    for (i = 0; i < argc; i++) {
        std::cout << azColName[i] << " = " << (argv[i] ? argv[i] : "NULL") << std::endl;
    }
    std::cout << std::endl;
    return 0;
}

void queryDatabase(const std::string& dbPath, const std::string& sqlQuery) {

    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;

    rc = sqlite3_open(dbPath.c_str(), &db);
    if (rc) {
        std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
        return;
    } else {
        std::cout << "Opened database successfully\n";
    }

    rc = sqlite3_exec(db, sqlQuery.c_str(), callback, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << zErrMsg << std::endl;
        sqlite3_free(zErrMsg);
    } else {
        std::cout << "Operation done successfully\n";
    }
    sqlite3_close(db);
}

// void initializeDatabase(const std::string& dbPath) {
//     sqlite3* db;
//     char* zErrMsg = 0;
//     int rc;

//     rc = sqlite3_open(dbPath.c_str(), &db);
//     if (rc) {
//         std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
//         return;
//     }

//     // SQL statement to create a table
//     const char* sqlCreateTable =
//         "CREATE TABLE IF NOT EXISTS MyTable ("
//         "ID INTEGER PRIMARY KEY AUTOINCREMENT, "
//         "Value TEXT);";

//     // Execute SQL statement
//     rc = sqlite3_exec(db, sqlCreateTable, 0, 0, &zErrMsg);
//     if (rc != SQLITE_OK) {
//         std::cerr << "SQL error: " << zErrMsg << std::endl;
//         sqlite3_free(zErrMsg);
//     } else {
//         std::cout << "Table created successfully\n";
//     }
//     sqlite3_close(db);
// }

// Function to insert random data into the database
// void insertRandomData(const std::string& dbPath, int numRecords) {
//     sqlite3* db;
//     char* zErrMsg = 0;
//     int rc;

//     rc = sqlite3_open(dbPath.c_str(), &db);
//     if (rc) {
//         std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
//         return;
//     }

//     srand(time(NULL)); // Seed the random number generator

//     for (int i = 0; i < numRecords; ++i) {
//         // Generate random data
//         int randomValue = rand() % 100;  // Random integer between 0 and 99
//         std::string randomText = "Text" + std::to_string(rand() % 100);  // Random text like "Text42"

//         std::string sqlInsert = "INSERT INTO MyTable (Value) VALUES ('" + randomText + "');";

//         // Execute SQL statement
//         rc = sqlite3_exec(db, sqlInsert.c_str(), 0, 0, &zErrMsg);
//         if (rc != SQLITE_OK) {
//             std::cerr << "SQL error: " << zErrMsg << std::endl;
//             sqlite3_free(zErrMsg);
//         } else {
//             std::cout << "Inserted data successfully\n";
//         }
//     }

//     sqlite3_close(db);
// }

// void pipeline(vector<ConsumerProducerQueue<std::string>*> queues_de_entrada, vector<ConsumerProducerQueue<DataFrame*>*> queues_de_saida, vector<Handler*> handlers){
//     std::vector<std::thread> threads;
//     for (int i = 0; i < queues_de_entrada.size(); i++){
//         threads.emplace_back([queues_de_entrada, queues_de_saida, handlers, i] {
            
            
//             ;
//         });
//     }
//     for (auto &t: threads) {
//         t.join();
//     }
// }

sqlite3* openDatabase(const string& dbPath) {
    sqlite3* db = nullptr;
    int rc = sqlite3_open(dbPath.c_str(), &db);
    if (rc != SQLITE_OK) {
        std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        return nullptr;
    }
    else
        std::cout << "Opened database successfully" << std::endl;
    return db;
}

#include <functional>

void executeQuery(sqlite3* db, const string& sql, std::function<void(int, double)> callback) {
    char* errorMessage = nullptr;

    int rc = sqlite3_exec(db, sql.c_str(), [](void* cb, int argc, char** argv, char** azColName) -> int {
        if (argc == 2) {
            int count = argv[0] ? atoi(argv[0]) : 0;
            double seconds = argv[1] ? atof(argv[1]) : 0.0;
            double minutes = seconds / 60.0;
            auto func = *static_cast<std::function<void(int, double)>*>(cb);
            func(count, minutes);
        }
        return 0;
    }, &callback, &errorMessage);

    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
    }
}



// sqlite3* db;
//     int rc = sqlite3_open(filename.c_str(), &db);
//     if (rc != SQLITE_OK) {
//         std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
//         sqlite3_close(db);
//         return;
//     }

//     std::string sql = "SELECT * FROM Consumer";
//     sqlite3_stmt* stmt;
//     rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
//     if (rc != SQLITE_OK) {
//         std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(db) << std::endl;
//         sqlite3_close(db);
//         return;
//     }



int main() {
    
    mock_files();

    std::cout<<"Mock files created"<<std::endl;
    //Question 1 - Número de produtos visualizados por minuto
    ConsumerProducerQueue<std::string> queue_files1(150);
    ConsumerProducerQueue<DataFrame *> queue_reader1(150);
    ConsumerProducerQueue<DataFrame *> queue_select1(150);
    ConsumerProducerQueue<DataFrame *> queue_filter1(150);
    ConsumerProducerQueue<DataFrame *> queue_print1(150);

    //vector of threads
    std::vector<std::thread> threads;

    std::cout<<"Queues created"<<std::endl;



    EventBasedTrigger eventTrigger;
    eventTrigger.triggerOnApperanceOfNewLogFile("./logs", queue_files1);
    std::cout<<"Event Trigger created"<<std::endl;

    FileReader csvReader1;
    int end1 = 0;
    csvReader1.read(',', 0, end1, queue_reader1, queue_files1, false, true, "user_behavior_logs");

    std::cout<<"Reader created"<<std::endl;

    SelectHandler selectHandler1(&queue_reader1, &queue_select1);
    selectHandler1.select({"Button Product Id","Date"});

    std::cout<<"Select Handler created"<<std::endl;

    FilterHandler filterHandler1(&queue_select1, &queue_filter1);
    filterHandler1.filter("Button Product Id", "!=", "0");

    std::cout<<"Filter Handler created"<<std::endl;

    string dbPath = "./mydatabase.db";
    string tableName = "Table1";
    FinalHandler finalHandler1(&queue_filter1, &queue_print1);
    finalHandler1.aggregate(dbPath, tableName, false,false,"", "", "", "");

    std::cout<<"Final Handler created"<<std::endl;

    //ler a table com sqlite
    //encontra o número de linhas
    //encontrar o maior e o menor valor na coluna date
    //calcular a razão entre o número de linhas e a diferença entre o maior e o menor valor na coluna date em minutos

    auto handleResults = [](int rowCount, double dateDiffMinutes) {
        double ratio = rowCount / dateDiffMinutes;
        std::cout << "Total number of records: " << rowCount << std::endl;
        std::cout << "Difference in dates (minutes): " << dateDiffMinutes << std::endl;
        std::cout << "Ratio of records to time difference: " << ratio << std::endl;
    };

    
    for(auto& t : threads){
        t.join();
        std::cout<<"Thread joined"<<std::endl;
    }
    std::cout<<"Threads joined"<<std::endl;

    // Open the database
    sqlite3* db = openDatabase(dbPath);
    if (db != nullptr) {
        string query = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
        executeQuery(db, query, handleResults);

        // Close the database
        sqlite3_close(db);
    }




    return 0;


}





