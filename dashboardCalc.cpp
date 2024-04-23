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
void executeQuery(sqlite3 *db, const string &sql, std::function<void(int, double)> callback)
{
    char *errorMessage = nullptr;

    int rc = sqlite3_exec(
        db, sql.c_str(), [](void *cb, int argc, char **argv, char **azColName) -> int
        {
        if (argc == 2) {
            int count = argv[0] ? atoi(argv[0]) : 0;
            double seconds = argv[1] ? atof(argv[1]) : 0.0;
            double minutes = seconds / 60.0;
            auto func = *static_cast<std::function<void(int, double)>*>(cb);
            func(count, minutes);
        }
        return 0; },
        &callback, &errorMessage);

    if (rc != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
    }
}

std::vector<std::thread> pipeline1(string *data, ConsumerProducerQueue<std::string> *queue_files)
{
    if (queue_files == nullptr) return {};
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

    FileReader csvReader2;
    int end = 0;
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &csvReader2, &queue_files, &queue_reader, &end]
                                { csvReader2.read(',', 0, end, queue_reader, *queue_files, false, true, "user_behavior_logs"); });
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

void dashboard(string *data1, string *data2, string *data4, string *data5, string *data7)
{
    
    while(true)
    {
        //prints necessários
        //sleep for n seconds
    } 
}

int main()
{

    std::vector<std::unique_ptr<ConsumerProducerQueue<std::string>>> queue_files;
    for (int i = 0; i < 5; i++)
    {
        queue_files.push_back(std::make_unique<ConsumerProducerQueue<std::string>>(100));
    }


    mock_files();

    // vector of threads
    std::vector<std::thread> threads;
    EventBasedTrigger eventTrigger1;
    for (int i = 0; i < 1; i++)
    {
        threads.emplace_back([i, &eventTrigger1, &queue_files]
                             { eventTrigger1.triggerOnApperanceOfNewLogFile("./logs", queue_files); });
    }


    string *data1;
    ConsumerProducerQueue<DataFrame *> queue_reader(100);
    ConsumerProducerQueue<DataFrame *> queue_select(100);
    ConsumerProducerQueue<DataFrame *> queue_filter(100);

    // PIPELINE 1 --------------------------------------------------------------------------------------------
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



    // PIPELINE 2 --------------------------------------------------------------------------------------------

    ConsumerProducerQueue<DataFrame *> queue_reader2(100);
    ConsumerProducerQueue<DataFrame *> queue_select2(100);

    FileReader csvReader2;
    int end2 = 0;
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &csvReader2, &queue_files, &queue_reader2, &end2]
                                { csvReader2.read(',', 0, end2, queue_reader2, *queue_files[1], false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler2(&queue_reader2, &queue_select2);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &selectHandler2]
                                { selectHandler2.select({"QUANTIDADE", "DATA DE COMPRA"}); });
    }


    string dbPath2 = "./mydatabase2.db";
    string tableName2 = "Table2";
    FinalHandler finalHandler2(&queue_select, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler2, &dbPath2, &tableName2]
                                { finalHandler2.aggregate(dbPath2, tableName2); });
    }


    // Question 4 - Ranking dos produtos mais comprados -----------------------------------------------------------

    ConsumerProducerQueue<DataFrame *> queue_reader4(100);
    ConsumerProducerQueue<DataFrame *> queue_select4(100);
    ConsumerProducerQueue<DataFrame *> queue_groupby4(100);

    FileReader csvReader4;
    int end4 = 0;
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &csvReader4, &queue_files, &queue_reader4, &end4]
                                { csvReader4.read(',', 0, end4, queue_reader4, *queue_files[2], false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler4(&queue_reader4, &queue_select4);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &selectHandler4]
                                { selectHandler4.select({"Button Product Id"}); });
    }

    GroupByHandler groupByHandler4(&queue_select4, &queue_groupby4);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &groupByHandler4]
                                { groupByHandler4.group_by("Button Product Id", "count"); });
    }


    string dbPath4 = "./mydatabase4.db";
    string tableName4 = "Table4";
    FinalHandler finalHandler4(&queue_groupby4, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler4, &dbPath4, &tableName4]
                                { finalHandler4.aggregate(dbPath4, tableName4, true, false, "count", "", "", "DESC"); });
    }

    std::cout<<"got here"<<std::endl;
    for (auto &t : threads)
    {
        if(t.joinable()){
            t.join();
        }
    }
    return 0;

    // Question 5 - Ranking dos produtos mais visualizados -----------------------------------------------------------

    ConsumerProducerQueue<DataFrame *> queue_reader5(100);
    ConsumerProducerQueue<DataFrame *> queue_select5(100);
    ConsumerProducerQueue<DataFrame *> queue_groupby5(100);
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

    std::cout << "Select Handler created" << std::endl;

    GroupByHandler groupByHandler5(&queue_select5, &queue_groupby5);
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
                                { finalHandler5.aggregate(dbPath5, tableName5, true, false, "count", "", "", "DESC"); });
    }

    // Question 7 - Número de usuários únicos visualizando cada produto por minuto ---------------------------------
    ConsumerProducerQueue<DataFrame *> queue_reader7(100);
    ConsumerProducerQueue<DataFrame *> queue_select7(100);
    ConsumerProducerQueue<DataFrame *> queue_groupby7(100);

    FileReader csvReader7;
    int end7 = 0;
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &csvReader7, &queue_files, &queue_reader, &end]
                                { csvReader7.read(',', 0, end, queue_reader, *queue_files[4], false, true, "user_behavior_logs"); });
    }

    SelectHandler selectHandler7(&queue_reader7, &queue_select7);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &selectHandler7]
                                { selectHandler7.select({"Button Product Id", "User Id"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    GroupByHandler groupByHandler7(&queue_select7, &queue_groupby7);
    for (int i = 0; i < 2; i++)
    {
        (threads).emplace_back([i, &groupByHandler7]
                                { groupByHandler7.group_by("Button Product Id", "count"); });
    }

    std::cout << "GroupBy Handler created" << std::endl;

    string dbPath7 = "./mydatabase.db";
    string tableName7 = "Table7";
    FinalHandler finalHandler7(&queue_groupby7, nullptr);
    for (int i = 0; i < 1; i++)
    {
        (threads).emplace_back([i, &finalHandler7, &dbPath7, &tableName7]
                                { finalHandler7.aggregate(dbPath7, tableName7, true, false, "count", "", "", "DESC"); });
    }

    for (auto &t : threads)
    {
        if(t.joinable()){
            t.join();
        }
    }



    auto handleResults = [](int rowCount, double dateDiffMinutes)
    {
        double ratio = rowCount / dateDiffMinutes;
        std::cout << "Total number of records: " << rowCount << std::endl;
        std::cout << "Difference in dates (minutes): " << dateDiffMinutes << std::endl;
        std::cout << "Ratio of records to time difference: " << ratio << std::endl;
    };

    std::cout << "got here" << std::endl;
    string dbPath = "./mydatabase.db";
    // Open the database
    sqlite3 *db = openDatabase(dbPath);
    if (db != nullptr)
    {
        string query = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
        executeQuery(db, query, handleResults);

        //     // Close the database
        sqlite3_close(db);
    }

    std::vector<std::thread> loop_threads;
    auto init_time = std::chrono::system_clock::now();
    string *data2;
    string *data4;
    string *data5;
    string *data7;
    threads.emplace_back([&data1, &data2, &data4, &data5, &data7]{
        // Call the dashboard function here
        dashboard(data1, data2, data4, data5, data7);
    });
     return 0;
}

