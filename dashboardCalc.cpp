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
        mockCSV();
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
        std::cout << "Total number of records: " << *count << std::endl;
        std::cout << "Difference in dates (minutes): " << *minutes << std::endl;
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
    while (true)
    {
        executeQuery1(sql, count, minutes, &dbPath);
        data[0] = std::to_string(count);
        data[1] = std::to_string(minutes);
        data[2] = std::to_string(count / minutes);
        //closedb
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
    }
}


std::vector<std::thread> pipeline1(string *data, ConsumerProducerQueue<std::string> *queue_files)
{
    if (queue_files == nullptr)
        return {};
    std::vector<std::thread> threads;
    // Question 1 - Número de produtos visualizados por minutos
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);
    ConsumerProducerQueue<DataFrame *> queue_filter(15);

    // check the arguments of the read function
    /*
    ===========
    ===========
    ===========
    ===========
    ===========*/
    FileReader csvReader1;
    int end = 0;
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &csvReader1, &queue_files, &queue_reader, &end]
                               { csvReader1.read(',', 0, end, queue_reader, *queue_files, true, 40, "user_behavior_logs"); });
    }

    SelectHandler selectHandler(&queue_reader, &queue_select);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &selectHandler]
                               { selectHandler.select({"Button Product Id", "Date"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    FilterHandler filterHandler(&queue_select, &queue_filter);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &filterHandler]
                               { filterHandler.filter("Button Product Id", "!=", "0"); });
    }

    std::cout << "Filter Handler created" << std::endl;

    // ao inves de salvar no database esse poderia ir só para o repositório e eu pego do repositório o df para terminar a pergunta
    string dbPath = "./mydatabase.db";
    string tableName = "Table1";
    FinalHandler finalHandler(&queue_filter, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler, &dbPath, &tableName]
                               { finalHandler.aggregate(dbPath, tableName); });
    }

    return threads;

    // for(auto& t : *threads){
    //     t.join();
    //     std::cout<<"Thread joined"<<std::endl;
    // }
    // std::cout<<"Threads joined"<<std::endl;

    // // isso vai ser passado para o executeQuery e vai printar a resposta da pergunta
    // // possivelmente nesse novo modelo da main ter um while loop vamos guardar essa
    // // informação em uma variavel passada para a pipeline1 e depois printar no final
    // // da main a resposta de todas as perguntas, possivelmente em uma area completa que será o dashboard
    // // limpariamos o terminal antes de cada print.
    // auto handleResults = [](int rowCount, double dateDiffMinutes)
    // {
    //     double ratio = rowCount / dateDiffMinutes;
    //     std::cout << "Total number of records: " << rowCount << std::endl;
    //     std::cout << "Difference in dates (minutes): " << dateDiffMinutes << std::endl;
    //     std::cout << "Ratio of records to time difference: " << ratio << std::endl;
    // };

    // // Open the database
    // sqlite3 *db = openDatabase(dbPath);
    // if (db != nullptr)
    // {
    //     string query = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
    //     executeQuery(db, query, handleResults);

    //     //     // Close the database
    //     sqlite3_close(db);
    // }
}

void pipeline2(string *data, ConsumerProducerQueue<std::string> *queue_files, std::vector<std::thread> *threads)
{
    // Question 2 - Número de produtos comprados por minuto
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);

    FileReader csvReader;
    int end = 0;
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &csvReader, &queue_files, &queue_reader, &end]
                                { csvReader.read(',', 0, end, queue_reader, *queue_files, false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler(&queue_reader, &queue_select);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &selectHandler]
                                { selectHandler.select({"QUANTIDADE", "DATA DE COMPRA"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    string dbPath = "./mydatabase.db";
    string tableName = "Table2";
    FinalHandler finalHandler(&queue_select, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (*threads).emplace_back([i, &finalHandler, &dbPath, &tableName]
                                { finalHandler.aggregate(dbPath, tableName, false, false, "", "", "", ""); });
    }

    // for(auto& t : threads){
    //     t.join();
    //     std::cout<<"Thread joined"<<std::endl;
    // }
    // std::cout<<"Threads joined"<<std::endl;

    // ainda preciso criar o handle results para essa pipeline

    // ainda preicso criar o sql dessa pipeline
}

void pipeline4(string *data, ConsumerProducerQueue<std::string> *queue_files, std::vector<std::thread> *threads)
{
    // Question 4 - Ranking dos produtos mais comprados
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);
    ConsumerProducerQueue<DataFrame *> queue_groupby(15);
    ConsumerProducerQueue<DataFrame *> queue_aggregate(15);

    FileReader csvReader;
    int end = 0;
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &csvReader, &queue_files, &queue_reader, &end]
                                { csvReader.read(',', 0, end, queue_reader, *queue_files, false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler(&queue_reader, &queue_select);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &selectHandler]
                                { selectHandler.select({"Button Product Id"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    GroupByHandler groupByHandler(&queue_select, &queue_groupby);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &groupByHandler]
                                { groupByHandler.group_by("Button Product Id", "count"); });
    }

    std::cout << "GroupBy Handler created" << std::endl;

    string dbPath = "./mydatabase.db";
    string tableName = "Table4";
    FinalHandler finalHandler(&queue_groupby, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (*threads).emplace_back([i, &finalHandler, &dbPath, &tableName]
                                { finalHandler.aggregate(dbPath, tableName, true, false, "count", "", "", "DESC"); });
    }

    // for(auto& t : threads){
    //     t.join();
    //     std::cout<<"Thread joined"<<std::endl;
    // }
    // std::cout<<"Threads joined"<<std::endl;

    // ainda preciso criar o sql para essa pipeline
}

void pipeline5(string *data, ConsumerProducerQueue<std::string> *queue_files, std::vector<std::thread> *threads)
{
    // Question 5 - Ranking dos produtos mais visualizados
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);
    ConsumerProducerQueue<DataFrame *> queue_groupby(15);
    ConsumerProducerQueue<DataFrame *> queue_aggregate(15);

    FileReader csvReader;
    int end = 0;
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &csvReader, &queue_files, &queue_reader, &end]
                                { csvReader.read(',', 0, end, queue_reader, *queue_files, false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler(&queue_reader, &queue_select);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &selectHandler]
                                { selectHandler.select({"Button Product Id"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    GroupByHandler groupByHandler(&queue_select, &queue_groupby);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &groupByHandler]
                                { groupByHandler.group_by("Button Product Id", "count"); });
    }

    std::cout << "GroupBy Handler created" << std::endl;

    string dbPath = "./mydatabase.db";
    string tableName = "Table5";
    FinalHandler finalHandler(&queue_groupby, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (*threads).emplace_back([i, &finalHandler, &dbPath, &tableName]
                                { finalHandler.aggregate(dbPath, tableName, true, false, "count", "", "", "DESC"); });
    }

    // for(auto& t : threads){
    //     t.join();
    //     std::cout<<"Thread joined"<<std::endl;
    // }
    // std::cout<<"Threads joined"<<std::endl;

    // ainda preciso criar o sql para essa pipeline
}

void pipeline7(string *data, ConsumerProducerQueue<std::string> *queue_files, std::vector<std::thread> *threads)
{
    // Question 7 - Número de usuários únicos visualizando cada produto por minuto
    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);
    ConsumerProducerQueue<DataFrame *> queue_groupby(15);
    ConsumerProducerQueue<DataFrame *> queue_aggregate(15);

    FileReader csvReader;
    int end = 0;
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &csvReader, &queue_files, &queue_reader, &end]
                                { csvReader.read(',', 0, end, queue_reader, *queue_files, false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler(&queue_reader, &queue_select);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &selectHandler]
                                { selectHandler.select({"Button Product Id", "User Id"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    GroupByHandler groupByHandler(&queue_select, &queue_groupby);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &groupByHandler]
                                { groupByHandler.group_by("Button Product Id", "count"); });
    }

    std::cout << "GroupBy Handler created" << std::endl;

    string dbPath = "./mydatabase.db";
    string tableName = "Table7";
    FinalHandler finalHandler(&queue_groupby, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (*threads).emplace_back([i, &finalHandler, &dbPath, &tableName]
                                { finalHandler.aggregate(dbPath, tableName, true, false, "count", "", "", "DESC"); });
    }

    // for(auto& t : threads){
    //     t.join();
    //     std::cout<<"Thread joined"<<std::endl;
    // }
    // std::cout<<"Threads joined"<<std::endl;

    // ainda preciso criar o sql para essa pipeline
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
            std::cout << "Total number of records: " << data4[0] << std::endl;
            std::cout << "Difference in dates (minutes): " << data4[1] << std::endl;
            std::cout << "Ratio of records to time difference: " << data4[2] << std::endl;
        }

        // Print the results of pipeline 5
        if (data5 != nullptr)
        {
            std::cout << "Question 5 - Ranking dos produtos mais visualizados" << std::endl;
            std::cout << "Total number of records: " << data5[0] << std::endl;
            std::cout << "Difference in dates (minutes): " << data5[1] << std::endl;
            std::cout << "Ratio of records to time difference: " << data5[2] << std::endl;
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
    string *data4 = new string[3];
    string *data5 = new string[3];
    string *data7 = new string[3];

    data1[0] = "100";
    data1[1] = "10";
    data1[2] = "10";

    data2[0] = "100";
    data2[1] = "10";
    data2[2] = "70";

    data4[0] = "100";
    data4[1] = "10";
    data4[2] = "20";

    data5[0] = "100";
    data5[1] = "40";
    data5[2] = "30";

    data7[0] = "1430";
    data7[1] = "60";
    data7[2] = "23.83";

    ConsumerProducerQueue<std::string> queue_files1(15);
    ConsumerProducerQueue<std::string> queue_files2(15);
    ConsumerProducerQueue<std::string> queue_files4(15);
    ConsumerProducerQueue<std::string> queue_files5(15);
    ConsumerProducerQueue<std::string> queue_files7(15);

    mock_files();

    // vector of threads
    std::vector<std::thread> threads;
    EventBasedTrigger eventTrigger1;
    for (int i = 0; i < 1; i++)
    {
        threads.emplace_back([i, &eventTrigger1, &queue_files1]
                             { eventTrigger1.triggerOnApperanceOfNewLogFile("./logs", queue_files1); });
    }

    // // EventBasedTrigger eventTrigger2;
    // // for (int i = 0; i < 1; i++)
    // // {
    // //     threads.emplace_back([i, &eventTrigger2, &queue_files2]
    // //                          { eventTrigger2.triggerOnApperanceOfNewLogFile("./logs", queue_files2); });
    // // }
    // // EventBasedTrigger eventTrigger4;
    // // for (int i = 0; i < 1; i++)
    // // {
    // //     threads.emplace_back([i, &eventTrigger4, &queue_files4]
    // //                          { eventTrigger4.triggerOnApperanceOfNewLogFile("./logs", queue_files4); });
    // // }
    // // EventBasedTrigger eventTrigger5;
    // // for (int i = 0; i < 1; i++)
    // // {
    // //     threads.emplace_back([i, &eventTrigger5, &queue_files5]
    // //                          { eventTrigger5.triggerOnApperanceOfNewLogFile("./logs", queue_files5); });
    // // }
    // // EventBasedTrigger eventTrigger7;
    // // for (int i = 0; i < 1; i++)
    // // {
    // //     threads.emplace_back([i, &eventTrigger7, &queue_files7]
    // //                          { eventTrigger7.triggerOnApperanceOfNewLogFile("./logs", queue_files7); });
    // // }

    ConsumerProducerQueue<DataFrame *> queue_reader(15);
    ConsumerProducerQueue<DataFrame *> queue_select(15);
    ConsumerProducerQueue<DataFrame *> queue_filter(15);

    // check the arguments of the read function
    /*
    ===========
    ===========
    ===========
    ===========
    ===========*/
    FileReader csvReader1;
    int end = 0;
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &csvReader1, &queue_files1, &queue_reader, &end]
                               { csvReader1.read(',', 0, end, queue_reader, queue_files1, true, 40, "user_behavior_logs"); });
    }

    SelectHandler selectHandler(&queue_reader, &queue_select);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &selectHandler]
                               { selectHandler.select({"Button Product Id", "Date"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    FilterHandler filterHandler(&queue_select, &queue_filter);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &filterHandler]
                               { filterHandler.filter("Button Product Id", "!=", "0"); });
    }

    std::cout << "Filter Handler created" << std::endl;

    // ao inves de salvar no database esse poderia ir só para o repositório e eu pego do repositório o df para terminar a pergunta
    string dbPath1 = "./mydatabase.db";
    string tableName = "Table1";
    FinalHandler finalHandler(&queue_filter, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler, &dbPath1, &tableName]
                               { finalHandler.aggregate(dbPath1, tableName); });
    }

    string dbPath = "./mydatabase.db";
    int count = 12;
    double minutes = 13;
    threads.emplace_back([&data1, &dbPath, &count, &minutes]
                         {
                             std::string sql = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
                             worker1(data1, dbPath, count, minutes, sql);
                         });

    threads.emplace_back([&data1, &data2, &data4, &data5, &data7]{
    //  Call the dashboard function here
        dashboard(data1, data2, data4, data5, data7);
    });

    // std::cout << "Num threads: " << threads.size() << std::endl;
    int i = 1;
    const auto interval = std::chrono::milliseconds(2000);

    // Open the database
    for (auto &t : threads)
    {
        // std::cout << "joining thread " << i << std::endl;
        if (t.joinable())
        {
            t.join();
        }
        i++;
    }

    return 0;
}
