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

    //Question 1 - Número de produtos visualizados por minuto
    ConsumerProducerQueue<std::string> queue_files1(15);
    ConsumerProducerQueue<DataFrame *> queue_reader1(15);
    ConsumerProducerQueue<DataFrame *> queue_select1(15);
    ConsumerProducerQueue<DataFrame *> queue_filter1(15);


    EventBasedTrigger eventTrigger;
    std::thread eventTriggerThread([&eventTrigger, &queue_files1] {
        eventTrigger.triggerOnApperanceOfNewLogFile("./log", queue_files1);
    });

    FileReader csvReader1;

    int end = 0;
    std::thread readerThread1([&csvReader1, &queue_files1, &queue_reader1, &end] {
        csvReader1.read(',', 0, end, queue_reader1, queue_files1, false, true, "user_behavior_logs");
    });


    SelectHandler selectHandler1(&queue_reader1, &queue_select1);
    std::thread selectThread1([&selectHandler1] {
        selectHandler1.select({"Button Product Id","Date"});
    });


    FilterHandler filterHandler1(&queue_select1, &queue_filter1);
    std::thread filterThread1([&filterHandler1]{
        filterHandler1.filter("Button Product Id", "!=", "0");
    });

    string dbPath = "mydatabase.db";
    string tableName = "Table1";
    FinalHandler finalHandler1(&queue_filter1, nullptr);
    std::thread finalThread1([&finalHandler1, &dbPath, &tableName] {
        finalHandler1.aggregate(dbPath, tableName, false,false,"", "","", "");
    });


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

    eventTriggerThread.join();
    readerThread1.join();
    selectThread1.join();
    filterThread1.join();
    finalThread1.join();
    // Open the database
    sqlite3* db = openDatabase(dbPath);
    if (db != nullptr) {
        string query = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
        executeQuery(db, query, handleResults);

        // Close the database
        sqlite3_close(db);
    }

    // //Question 2 - Número de produtos comprados por minuto
    // ConsumerProducerQueue<std::string> queue_files2(15);
    // ConsumerProducerQueue<DataFrame *> queue_reader2(15);
    // ConsumerProducerQueue<DataFrame *> queue_select2(15);
    
    // EventBasedTrigger eventTrigger2;
    // std::thread eventTriggerThread2([&eventTrigger2, &queue_files2] {
    //     eventTrigger2.triggerOnApperanceOfNewLogFile("./log", queue_files2);
    // });

    // FileReader csvReader2;

    // int end2 = 0;
    // std::thread readerThread2([&csvReader2, &queue_files2, &queue_reader2, &end2] {
    //     csvReader2.read(',', 0, end2, queue_reader2, queue_files2, false, true, "user_behavior_logs");
    // });

    // SelectHandler selectHandler2(&queue_reader2, &queue_select2);
    // std::thread selectThread2([&selectHandler2] {
    //     selectHandler2.select({"QUANTIDADE","DATA DE COMPRA"});
    // });

    // string tableName2 = "Table2";

    // FinalHandler finalHandler2(&queue_select2, nullptr);
    // std::thread finalThread2([&finalHandler2, &dbPath, &tableName2] {
    //     finalHandler2.aggregate(dbPath, tableName2, false,false,"", "", "", "");
    // });

    // //Question 3 - Número de usuários únicos visulizando cada produto por minuto.

    // //Question 4 - Ranking dos produtos mais comprados na ultima hora.

    // ConsumerProducerQueue<std::string> queue_files4(15);
    // ConsumerProducerQueue<DataFrame *> queue_reader4(15);
    // ConsumerProducerQueue<DataFrame *> queue_select4(15);
    // ConsumerProducerQueue<DataFrame *> queue_filter4(15);
    // ConsumerProducerQueue<DataFrame *> queue_groupby4(15);
    // ConsumerProducerQueue<DataFrame *> queue_aggregate4(15);

    // EventBasedTrigger eventTrigger4;
    // std::thread eventTriggerThread4([&eventTrigger4, &queue_files4] {
    //     eventTrigger4.triggerOnApperanceOfNewLogFile("./data", queue_files4);
    // });

    // FileReader csvReader4;

    // int end4 = 0;
    // std::thread readerThread4([&csvReader4, &queue_files4, &queue_reader4, &end4] {
    //     csvReader4.read(',', 0, end4, queue_reader4, queue_files4, false, true, "order");
    // });

    // auto currentTime = getCurrentTimeMinusDeltaHours(1);
    
    // //convert tm to string
    // string currentTimeString = convert_tm_to_string(currentTime);

    // FilterHandler filterHandler4(&queue_reader4, &queue_filter4);
    // std::thread filterThread4([&filterHandler4, &currentTimeString]{
    //     filterHandler4.filter("DATA DE COMPRA", ">", currentTimeString);
    // });

    // SelectHandler selectHandler4(&queue_filter4, &queue_select4);
    // std::thread selectThread4([&selectHandler4] {
    //     selectHandler4.select({"ID PRODUTO","QUANTIDADE"});
    // });

    // GroupByHandler groupByHandler4(&queue_select4, &queue_groupby4);
    // std::thread groupByThread4([&groupByHandler4] {
    //     groupByHandler4.group_by("ID PRODUTO", "sum");
    // });

    // string tableName4 = "Table4";

    // FinalHandler finalHandler4(&queue_groupby4, nullptr);
    // std::thread finalThread4([&finalHandler4, &dbPath, &tableName4] {
    //     finalHandler4.aggregate(dbPath, tableName4, true,false,"QUANTIDADE", "", "", "DESC");
    // });

    // //query sql para o ranking 


    // //Question 5 - Ranking dos produtos mais visualizados na ultima hora.

    // ConsumerProducerQueue<std::string> queue_files5(15);
    // ConsumerProducerQueue<DataFrame *> queue_reader5(15);
    // ConsumerProducerQueue<DataFrame *> queue_select5(15);
    // ConsumerProducerQueue<DataFrame *> queue_filter5(15);
    // ConsumerProducerQueue<DataFrame *> queue_filter5_2(15);
    // ConsumerProducerQueue<DataFrame *> queue_groupby5(15);
    // ConsumerProducerQueue<DataFrame *> queue_aggregate5(15);

    // EventBasedTrigger eventTrigger5;
    // std::thread eventTriggerThread5([&eventTrigger5, &queue_files5] {
    //     eventTrigger5.triggerOnApperanceOfNewLogFile("./log", queue_files5);
    // });

    // FileReader csvReader5;

    // int end5 = 0;
    // std::thread readerThread5([&csvReader5, &queue_files5, &queue_reader5, &end5] {
    //     csvReader5.read(',', 0, end5, queue_reader5, queue_files5, false, true, "user_behavior_logs");
    // });

    // auto currentTime5 = getCurrentTimeMinusDeltaHours(1);

    // //convert tm to string
    // string currentTimeString5 = convert_tm_to_string(currentTime5);

    // FilterHandler filterHandler5(&queue_reader5, &queue_filter5);
    // std::thread filterThread5([&filterHandler5, &currentTimeString5]{
    //     filterHandler5.filter("Date", ">", currentTimeString5);
    // });

    // SelectHandler selectHandler5(&queue_filter5, &queue_select5);
    // std::thread selectThread5([&selectHandler5] {
    //     selectHandler5.select({"Button Product Id"});
    // });

    // FilterHandler filterHandler5_2(&queue_select5, &queue_filter5_2);
    // std::thread filterThread5_2([&filterHandler5_2]{
    //     filterHandler5_2.filter("Button Product Id", "!=", "0");
    // });

    // GroupByHandler groupByHandler5(&queue_filter5_2, &queue_groupby5);
    // std::thread groupByThread5([&groupByHandler5] {
    //     groupByHandler5.group_by("Button Product Id", "count");
    // });

    // string tableName5 = "Table5";

    // FinalHandler finalHandler5(&queue_groupby5, nullptr);
    // std::thread finalThread5([&finalHandler5, &dbPath, &tableName5] {
    //     finalHandler5.aggregate(dbPath, tableName5, true,false,"count", "", "", "DESC");
    // });

    // //query sql para o ranking

    // //Question 6 - Quantidade média de visualizações de um produto por minuto.

    // ConsumerProducerQueue<std::string> queue_files6(15);
    // ConsumerProducerQueue<DataFrame *> queue_reader6(15);
    // ConsumerProducerQueue<DataFrame *> queue_select6(15);
    // ConsumerProducerQueue<DataFrame *> queue_filter6(15);
    // ConsumerProducerQueue<DataFrame *> queue_groupby6(15);

    // EventBasedTrigger eventTrigger6;
    // std::thread eventTriggerThread6([&eventTrigger6, &queue_files6] {
    //     eventTrigger6.triggerOnApperanceOfNewLogFile("./log", queue_files6);
    // });

    // //standby

    // //Question 7 - Número de produtos vendidos por minuto.

    // ConsumerProducerQueue<std::string> queue_files7(15);
    // ConsumerProducerQueue<DataFrame *> queue_reader7(15);
    // ConsumerProducerQueue<DataFrame *> queue_select7(15);
    // ConsumerProducerQueue<DataFrame *> queue_filter7(15);
    // ConsumerProducerQueue<DataFrame *> queue_groupby7(15);
    // ConsumerProducerQueue<DataFrame *> queue_join7(15);
    // ConsumerProducerQueue<DataFrame *> queue_diff7(15);

    // EventBasedTrigger eventTrigger7;
    // std::thread eventTriggerThread7([&eventTrigger7, &queue_files7] {
    //     eventTrigger7.triggerOnApperanceOfNewLogFile("./data", queue_files7);
    // });

    // FileReader csvReader7;

    // int end7 = 0;
    // std::thread readerThread7([&csvReader7, &queue_files7, &queue_reader7, &end7] {
    //     csvReader7.read(',', 0, end7, queue_reader7, queue_files7, false, true, "order");
    // });

    // SelectHandler selectHandler7(&queue_reader7, &queue_select7);
    // std::thread selectThread7([&selectHandler7] {
    //     selectHandler7.select({"ID PRODUTO","QUANTIDADE"});
    // });

    // GroupByHandler groupByHandler7(&queue_select7, &queue_groupby7);
    // std::thread groupByThread7([&groupByHandler7] {
    //     groupByHandler7.group_by("ID PRODUTO", "sum");
    // });

    // //substituir o dataframe pelo que vai estar no repositório
    // DataFrame* df1 = new DataFrame();

    // JoinHandler joinHandler7(&queue_groupby7, &queue_join7);
    // std::thread joinThread7([&joinHandler7, &df1] {
    //     joinHandler7.join(df1, "ID PRODUTO", "ID PRODUTO");
    // });

    // //diff handler
    // DiffHandler diffHandler7(&queue_join7, &queue_diff7);
    // std::thread diffThread7([&diffHandler7] {
    //     diffHandler7.diff("QUANTIDADE", "QUANTIDADE_STOCK", "FALTA");
    // });

    // FilterHandler filterHandler7(&queue_diff7, &queue_filter7);
    // std::thread filterThread7([&filterHandler7]{
    //     filterHandler7.filter("FALTA", "<", "0");
    // });

    // string tableName7 = "Table7";

    // FinalHandler finalHandler7(&queue_filter7, nullptr);
    // std::thread finalThread7([&finalHandler7, &dbPath, &tableName7] {
    //     finalHandler7.aggregate(dbPath, tableName7, false,false,"", "", "", "");
    // });


    // //retrieve table from sqlite
    // //sum FALTA Column and multiply the result by -1



    return 0;


}





