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

std::tm getCurrentTimeMinusDeltaHours(int Delta)
{
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

string convert_tm_to_string(std::tm tm)
{
    return std::to_string(tm.tm_year + 1900) + "/" + std::to_string(tm.tm_mon + 1) + "/" + std::to_string(tm.tm_mday) + " " + std::to_string(tm.tm_hour) + ":" + std::to_string(tm.tm_min) + ":" + std::to_string(tm.tm_sec);
}

void mock_files()
{
    for (int i = 0; i < 1; i += 10)
    {
        mockCSV(i/10, 1000);
        mockLogFiles(10, 100, i);
    }
}

sqlite3 *openDatabase(const string &dbPath)
{
    sqlite3 *db = nullptr;
    int rc = sqlite3_open(dbPath.c_str(), &db);
    if (rc != SQLITE_OK)
    {
        std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        return nullptr;
    }
    else
        std::cout << "Opened database successfully" << std::endl;
    return db;
}

#include <functional>

// ainda precisamos fazer com que o execute query funcione de forma generica ou encapsular um execute query diferente dentro de cada void pipeline_i()
static int callback1(void *data, int argc, char **argv, char **azColName)
{
    int *count = static_cast<int *>(data);
    double *minutes = static_cast<double *>(data);

    if (argc == 2)
    {
        *count = argv[0] ? atoi(argv[0]) : 0;
        *minutes = argv[1] ? atof(argv[1]) : 0.0;
    }
    return 0;
}

// Function to execute a SQL query and store results
bool executeQuery1(const std::string &sql, int &count, double &minutes, std::string *dbPath)
{
    sqlite3 *db = openDatabase(*dbPath);
    char *errorMessage = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), callback1, &count, &errorMessage);
    if (rc != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
        sqlite3_close(db);
        return false;
    }
    sqlite3_close(db);
    return true;
}

void worker1(string *data, string dbPath, int &count, double &minutes, string sql)
{
    std::this_thread::sleep_for(std::chrono::seconds(5));
    while (true)
    {
        executeQuery1(sql, count, minutes, &dbPath);
        data[0] = std::to_string(count);
        data[1] = std::to_string(minutes);
        data[2] = std::to_string(count / minutes);
        // closedb
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}

int callback2(void *data, int argc, char **argv, char **azColName)
{
    int *count = static_cast<int *>(data);
    double *minutes = static_cast<double *>(data);

    if (argc == 2)
    {
        *count = argv[0] ? atoi(argv[0]) : 0;
        *minutes = argv[1] ? atof(argv[1]) : 0.0;
    }
    return 0;
}

bool executeQuery2(const std::string &sql, std::string *dbPath)
{
    sqlite3 *db = openDatabase(*dbPath);
    char *errorMessage = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), callback2, nullptr, &errorMessage);
    if (rc != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
        sqlite3_close(db);
        return false;
    }
    sqlite3_close(db);
    return true;
}

void worker2(string *data, string dbPath, int &count, double &minutes, string sql)
{
    std::this_thread::sleep_for(std::chrono::seconds(5));
    while (true)
    {
        executeQuery2(sql, &dbPath);
        data[0] = std::to_string(count);
        data[1] = std::to_string(minutes);
        data[2] = std::to_string(count / minutes);
        // closedb
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}



// Function to execute the SQL query
int callback4(void *data, int argc, char **argv, char **azColName) {
    std::string &result = *static_cast<std::string *>(data);
    std::string rowString;

    // Check if the row has a non-NULL ID_PRODUTO and a non-zero QUANTIDADE
    if (argc >= 2 && argv[0] && argv[1] && atoi(argv[1]) != 0) {
        for (int i = 0; i < argc; i++) {
            rowString += azColName[i];
            rowString += ": ";
            rowString += argv[i] ? argv[i] : "NULL";
            rowString += " | ";
        }
        result += rowString + "\n";
    }

    return 0;
}

// Function to execute the SQL query
bool executeQuery4(const std::string &sql, const std::string &dbPath, std::string &result) {
    sqlite3 *db;
    char *errorMessage = nullptr;

    db = openDatabase(dbPath);
    int rc = sqlite3_exec(db, sql.c_str(), callback4, &result, &errorMessage);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
        sqlite3_close(db);
        return false;
    }
    sqlite3_close(db);
    return true;
}

// Worker function
void worker4(std::string &data, const std::string &dbPath, const std::string &sql) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    while (true) {
        executeQuery4(sql, dbPath, data);


        // close database
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}

// Function to execute the SQL query
int callback5(void *data, int argc, char **argv, char **azColName) {
    std::string &result = *static_cast<std::string *>(data);
    std::string rowString;

    // Check if the row has a non-NULL ID_PRODUTO and a non-zero QUANTIDADE
    if (argc >= 2 && argv[0] && argv[1] && atoi(argv[1]) != 0) {
        for (int i = 0; i < argc; i++) {
            rowString += azColName[i];
            rowString += ": ";
            rowString += argv[i] ? argv[i] : "NULL";
            rowString += " | ";
        }
        result += rowString + "\n";
    }

    return 0;
}

// Function to execute the SQL query
bool executeQuery5(const std::string &sql, const std::string &dbPath, std::string &result) {
    sqlite3 *db;
    char *errorMessage = nullptr;

    db = openDatabase(dbPath);
    int rc = sqlite3_exec(db, sql.c_str(), callback5, &result, &errorMessage);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
        sqlite3_close(db);
        return false;
    }
    sqlite3_close(db);
    return true;
}

// Worker function
void worker5(std::string &data, const std::string &dbPath, const std::string &sql) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    while (true) {
        executeQuery5(sql, dbPath, data);

        // close database
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}



void clearScreen()
{
// Clear the console depending on the operating system
#if defined(_WIN32) || defined(_WIN64)
    system("cls");
#else
    system("clear");
#endif
}

void dashboard(string *data1, string *data2, string *data4, string *data5, string *data7)
{

    while (true)
    {
        std::string dashboard =
            "  _____                _____   _    _   ____     ____               _____    _____    \n"
            " |  __ \\      /\\      / ____| | |  | | |  _ \\   / __ \\      /\\     |  __ \\  |  __ \\   \n"
            " | |  | |    /  \\    | (___   | |__| | | |_) | | |  | |    /  \\    | |__) | | |  | |  \n"
            " | |  | |   / /\\ \\    \\___ \\  |  __  | |  _ <  | |  | |   / /\\ \\   |  _  /  | |  | |  \n"
            " | |__| |  / ____ \\   ____) | | |  | | | |_) | | |__| |  / ____ \\  | | \\ \\  | |__| |  \n"
            " |_____/  /_/    \\_\\ |_____/  |_|  |_| |____/   \\____/  /_/    \\_\\ |_|  \\_\\ |_____/   \n"
            "                                                                                      \n"
            "                                                                                      \n";

        clearScreen();
        std::cout << dashboard << std::endl;

        // Print the results of the pipelines

        // Print the results of pipeline 1
        if (data1 != nullptr)
        {
            std::cout << "Question 1 - Número de produtos visualizados por minutos" << std::endl;
            std::cout << "Total number of records: " << data1[0] << std::endl;
            std::cout << "Difference in dates (minutes): " << data1[1] << std::endl;
            std::cout << "Ratio of records to time difference: " << data1[2] << std::endl;
        }

        // Print the results of pipeline 2
        if (data2 != nullptr)
        {
            std::cout << "Question 2 - Número de produtos comprados por minuto" << std::endl;
            std::cout << "Total number of records: " << data2[0] << std::endl;
            std::cout << "Difference in dates (minutes): " << data2[1] << std::endl;
            std::cout << "Ratio of records to time difference: " << data2[2] << std::endl;
        }

        // Print the results of pipeline 4
        if (data4 != nullptr)
        {
            std::cout << "Question 4 - Ranking dos produtos mais comprados" << std::endl;
            std::cout << *data4 << std::endl;
        }

        // Print the results of pipeline 5
        if (data5 != nullptr)
        {
            std::cout << "Question 5 - Ranking dos produtos mais visualizados" << std::endl;
            std::cout << *data5 << std::endl;
        }

        // Print the results of pipeline 7

        if (data7 != nullptr)
        {
            std::cout << "Question 7 - Número de usuários únicos visualizando cada produto por minuto" << std::endl;
            std::cout << "Total number of records: " << data7[0] << std::endl;
            std::cout << "Difference in dates (minutes): " << data7[1] << std::endl;
            std::cout << "Ratio of records to time difference: " << data7[2] << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}

int main()
{

    string *data1 = new string[3];
    string *data2 = new string[3];
    string data4  = "--------------------------------------------";
    string data5  = "--------------------------------------------";
    string *data7 = new string[3];

    data1[0] = "100";
    data1[1] = "10";
    data1[2] = "10";

    data2[0] = "100";
    data2[1] = "10";
    data2[2] = "70";

    data7[0] = "1430";
    data7[1] = "60";
    data7[2] = "23.83";

    std::vector<std::unique_ptr<ConsumerProducerQueue<std::string>>> queue_files;
    for (int i = 0; i < 6; i++)
    {
        queue_files.push_back(std::make_unique<ConsumerProducerQueue<std::string>>(100));
    }

    mock_files();

    // vector of threads
    std::vector<std::thread> threads;
    EventBasedTrigger eventTrigger1;
    EventBasedTrigger eventTrigger2;
    for (int i = 0; i < 1; i++)
    
    {
        threads.emplace_back([i, &eventTrigger1, &queue_files]
                             { eventTrigger1.triggerOnApperanceOfNewLogFile("./logs", queue_files); });
        threads.emplace_back([i, &eventTrigger1, &queue_files]
                             { eventTrigger1.triggerOnApperanceOfNewLogFile("./data", queue_files); });
    }

    ConsumerProducerQueue<DataFrame *> queue_reader(100);
    ConsumerProducerQueue<DataFrame *> queue_select(100);
    ConsumerProducerQueue<DataFrame *> queue_filter(100);

     //PIPELINE 1 --------------------------------------------------------------------------------------------
     FileReader csvReader1;
     int end = 0;
     for (int i = 0; i < 1; i++)
     {
         (threads).emplace_back([i, &csvReader1, &queue_files, &queue_reader, &end]
                                { csvReader1.read(',', 0, end, queue_reader, *queue_files[0], true, 40, "user_behavior_logs"); });
     }

     SelectHandler selectHandler(&queue_reader, &queue_select);
     for (int i = 0; i < 1; i++)
     {
         (threads).emplace_back([i, &selectHandler]
                                { selectHandler.select({"Button Product Id", "Date"}); });
     }

     FilterHandler filterHandler(&queue_select, &queue_filter);
     for (int i = 0; i < 1; i++)
     {
         (threads).emplace_back([i, &filterHandler]
                                { filterHandler.filter("Button Product Id", "!=", "0"); });
     }

     // ao inves de salvar no database esse poderia ir só para o repositório e eu pego do repositório o df para terminar a pergunta
     string dbPath1 = "./mydatabase.db";
     string tableName = "Table1";
     FinalHandler finalHandler(&queue_filter, nullptr);
     for (int i = 0; i < 1; i++)
     {
         (threads).emplace_back([i, &finalHandler, &dbPath1, &tableName]
                                { finalHandler.aggregate(dbPath1, tableName); });
     }

     int count = 12;
     double minutes = 13;
     threads.emplace_back([&data1, &dbPath1, &count, &minutes]
                          {
                              std::string sql = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
                              worker1(data1, dbPath1, count, minutes, sql); });
    
        // PIPELINE 2 --------------------------------------------------------------------------------------------

        ConsumerProducerQueue<DataFrame *> queue_reader2(100);
        ConsumerProducerQueue<DataFrame *> queue_select2(100);

        FileReader csvReader2;
        int end2 = 0;
        for (int i = 0; i < 2; i++)
        {
            (threads).emplace_back([i, &csvReader2, &queue_files, &queue_reader2, &end2]
                                   { csvReader2.read(',', 0, end2, queue_reader2, *queue_files[1], true, 40, "order"); });
        }

        SelectHandler selectHandler2(&queue_reader2, &queue_select2);
        for (int i = 0; i < 2; i++)
        {
            (threads).emplace_back([i, &selectHandler2]
                                   { selectHandler2.select({"QUANTIDADE", "DATA DE COMPRA"}); });
        }

        string dbPath2 = "./mydatabase2.db";
        string tableName2 = "Table2";
        FinalHandler finalHandler2(&queue_select2, nullptr);
        for (int i = 0; i < 1; i++)
        {
            (threads).emplace_back([i, &finalHandler2, &dbPath2, &tableName2]
                                   { finalHandler2.aggregate(dbPath2, tableName2); });
        }

        int count2 = 12;
        double minutes2 = 13;

        threads.emplace_back([&data2, &dbPath2, &count2, &minutes2]
                             {
                                 std::string sql = "SELECT SUM(QUANTIDADE), (strftime('%s', MAX(DATA_DE_COMPRA)) - strftime('%s', MIN(DATA_DE_COMPRA))) / 60.0 AS DateDiffInMinutes FROM Table2;";
                                 worker2(data2, dbPath2, count2, minutes2, sql); });


    //Question 4 - Ranking dos produtos mais comprados -----------------------------------------------------------

    ConsumerProducerQueue<DataFrame *> queue_reader4(100);
    ConsumerProducerQueue<DataFrame *> queue_select4(100);
    ConsumerProducerQueue<DataFrame *> queue_groupby4(100);

    FileReader csvReader4;
    int end4 = 0;
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &csvReader4, &queue_files, &queue_reader4, &end4]
                               { csvReader4.read(',', 0, end4, queue_reader4, *queue_files[2], false, true, "order"); });
    }

    SelectHandler selectHandler4(&queue_reader4, &queue_select4);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &selectHandler4]
                               { selectHandler4.select({ "ID PRODUTO", "QUANTIDADE"}); });
    }

    GroupByHandler groupByHandler4(&queue_select4, &queue_groupby4);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &groupByHandler4]
                               { groupByHandler4.group_by("ID PRODUTO", "sum"); });
    }

    string dbPath4 = "./mydatabase4.db";
    string tableName4 = "Table4";
    FinalHandler finalHandler4(&queue_groupby4, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler4, &dbPath4, &tableName4]
                               { finalHandler4.aggregate(dbPath4, tableName4, false, true, "", "ID PRODUTO", "sum", ""); });
    }



   threads.emplace_back([&data4, &dbPath4]
                          {
                              std::string sql = "SELECT * FROM Table4 ORDER BY QUANTIDADE DESC;";
                              worker4(data4, dbPath4, sql); });

      //Question 5 - Ranking dos produtos mais visualizados -----------------------------------------------------------

    ConsumerProducerQueue<DataFrame *> queue_reader5(100);
    ConsumerProducerQueue<DataFrame *> queue_select5(100);
    ConsumerProducerQueue<DataFrame *> queue_groupby5(100);
    ConsumerProducerQueue<DataFrame *> queue_filter5(100);
    ConsumerProducerQueue<DataFrame *> queue_aggregate5(100);

    FileReader csvReader5;
    int end5 = 0;
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &csvReader5, &queue_files, &queue_reader5, &end5]
                               { csvReader5.read(',', 0, end5, queue_reader5, *queue_files[3], false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler5(&queue_reader5, &queue_select5);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &selectHandler5]
                               { selectHandler5.select({"Button Product Id"}); });
    }

    FilterHandler filterHandler5(&queue_select5, &queue_filter5);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &filterHandler5]
                               { filterHandler5.filter("Button Product Id", "!=", "0"); });
    }


    GroupByHandler groupByHandler5(&queue_filter5, &queue_groupby5);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &groupByHandler5]
                               { groupByHandler5.group_by("Button Product Id", "count"); });
    }

    std::cout << "GroupBy Handler created" << std::endl;

    string dbPath5 = "./mydatabase5.db";
    string tableName5 = "Table5";
    FinalHandler finalHandler5(&queue_groupby5, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler5, &dbPath5, &tableName5]
                                { finalHandler5.aggregate(dbPath5, tableName5, false, true, "", "Button Product Id", "count"); });
    }

    threads.emplace_back([&data5, &dbPath5]
                          {
                              std::string sql = "SELECT * FROM Table5 ORDER BY count DESC;";
                              worker5(data5, dbPath5, sql); });


    // Question 7 - Numero de produtos sem estoque ---------------------------------

    ConsumerProducerQueue<DataFrame *> queue_reader7(100);
    ConsumerProducerQueue<DataFrame *> queue_readerdfvm7(100);
    ConsumerProducerQueue<DataFrame *> queue_select7(100);
    ConsumerProducerQueue<DataFrame *> queue_groupby7(100);
    ConsumerProducerQueue<DataFrame *> queue_aggregate7(100);
    ConsumerProducerQueue<DataFrame *> queue_join7(100);

    FileReader csvReader7;
    int end7 = 0;
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &csvReader7, &queue_files, &queue_reader7, &end7]
                               { csvReader7.read(',', 0, end7, queue_reader7, *queue_files[4], false, true, "order"); });
    }

    SelectHandler selectHandler7(&queue_reader7, &queue_select7);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &selectHandler7]
                               { selectHandler7.select({"ID PRODUTO", "QUANTIDADE"}); });
    }

    GroupByHandler groupByHandler7(&queue_select7, &queue_groupby7);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &groupByHandler7]
                               { groupByHandler7.group_by("ID PRODUTO", "sum"); });
    }

    FileReader dfvm_reader7;
    int endr7 = 0;
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &dfvm_reader7, &queue_files, &queue_readerdfvm7, &endr7]
                               { dfvm_reader7.read(',', 0, endr7, queue_readerdfvm7, *queue_files[5], false, true, "stock"); });
    }

    DataFrameVersionManager dfvm7(&queue_readerdfvm7);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &dfvm7]
                               { dfvm7.storeFromQueue(); });
    }
    JoinHandler joinHandler7(&queue_groupby7, &queue_join7);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &joinHandler7, &dfvm7]
                               { joinHandler7.join(&dfvm7,"ID PRODUTO", "ID PRODUTO"); });
    }


    string dbPath7 = "./mydatabase7.db";
    string tableName7 = "Table7";
    FinalHandler finalHandler7(&queue_join7, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler7, &dbPath7, &tableName7]
                               { finalHandler7.aggregate(dbPath7, tableName7, false, true, "", "ID PRODUTO", "sum", ""); });
    }
    string dbPath = "./mydatabase.db";



    // std::cout << "Num threads: " << threads.size() << std::endl;
    int i = 1;
    const auto interval = std::chrono::milliseconds(2000);

    threads.emplace_back([&data1, &data2, &data4, &data5, &data7]
                         {
                             //  Call the dashboard function here
                             dashboard(data1, data2, &data4, &data5, data7); });

    // Open the database
    for (auto &t : threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    return 0;
}