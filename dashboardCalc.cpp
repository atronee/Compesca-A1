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
        mockSqliteTable(80);
        std::this_thread::sleep_for(std::chrono::seconds(10));
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

void pipeline1(string *data, ConsumerProducerQueue<std::string> *queue_files, std::vector<std::thread> *threads)
{
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
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &csvReader1, &queue_files, &queue_reader, &end]
                                { csvReader1.read(',', 0, end, queue_reader, *queue_files, true, 3, "user_behavior_logs"); });
    }

    SelectHandler selectHandler(&queue_reader, &queue_select);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &selectHandler]
                                { selectHandler.select({"Button Product Id", "Date"}); });
    }

    std::cout << "Select Handler created" << std::endl;

    FilterHandler filterHandler(&queue_select, &queue_filter);
    for (int i = 0; i < 2; i++)
    {
        (*threads).emplace_back([i, &filterHandler]
                                { filterHandler.filter("Button Product Id", "!=", "0"); });
    }

    std::cout << "Filter Handler created" << std::endl;

    // ao inves de salvar no database esse poderia ir só para o repositório e eu pego do repositório o df para terminar a pergunta
    string dbPath = "./mydatabase.db";
    string tableName = "Table1";
    FinalHandler finalHandler(&queue_filter, nullptr);
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

    // isso vai ser passado para o executeQuery e vai printar a resposta da pergunta
    // possivelmente nesse novo modelo da main ter um while loop vamos guardar essa
    // informação em uma variavel passada para a pipeline1 e depois printar no final
    // da main a resposta de todas as perguntas, possivelmente em uma area completa que será o dashboard
    // limpariamos o terminal antes de cada print.
    auto handleResults = [](int rowCount, double dateDiffMinutes)
    {
        double ratio = rowCount / dateDiffMinutes;
        std::cout << "Total number of records: " << rowCount << std::endl;
        std::cout << "Difference in dates (minutes): " << dateDiffMinutes << std::endl;
        std::cout << "Ratio of records to time difference: " << ratio << std::endl;
    };

    // Open the database
    sqlite3 *db = openDatabase(dbPath);
    if (db != nullptr)
    {
        string query = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
        executeQuery(db, query, handleResults);

        // Close the database
        sqlite3_close(db);
    }
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

void dashboard(string *data1, string *data2, string *data4, string *data5, string *data7)
{
    // use datas to print the dashboard with the results of the pipelines
    // everytime some of the variables are updated we should clean the terminal and print the new dashboard
}

int main()
{

    ConsumerProducerQueue<std::string> queue_files1(15);
    ConsumerProducerQueue<std::string> queue_files2(15);
    ConsumerProducerQueue<std::string> queue_files4(15);
    ConsumerProducerQueue<std::string> queue_files5(15);
    ConsumerProducerQueue<std::string> queue_files7(15);

    mock_files();

    // vector of threads
    std::vector<std::thread> threads;
    EventBasedTrigger eventTrigger1;
    for (int i = 0; i < 5; i++)
    {
        threads.emplace_back([i, &eventTrigger1, &queue_files1]
                             { eventTrigger1.triggerOnApperanceOfNewLogFile("./logs", queue_files1); });
    }
    EventBasedTrigger eventTrigger2;
    for (int i = 0; i < 5; i++)
    {
        threads.emplace_back([i, &eventTrigger2, &queue_files2]
                             { eventTrigger2.triggerOnApperanceOfNewLogFile("./logs", queue_files2); });
    }
    EventBasedTrigger eventTrigger4;
    for (int i = 0; i < 5; i++)
    {
        threads.emplace_back([i, &eventTrigger4, &queue_files4]
                             { eventTrigger4.triggerOnApperanceOfNewLogFile("./logs", queue_files4); });
    }
    EventBasedTrigger eventTrigger5;
    for (int i = 0; i < 5; i++)
    {
        threads.emplace_back([i, &eventTrigger5, &queue_files5]
                             { eventTrigger5.triggerOnApperanceOfNewLogFile("./logs", queue_files5); });
    }
    EventBasedTrigger eventTrigger7;
    for (int i = 0; i < 5; i++)
    {
        threads.emplace_back([i, &eventTrigger7, &queue_files7]
                             { eventTrigger7.triggerOnApperanceOfNewLogFile("./logs", queue_files7); });
    }

    string *data1;
    pipeline1(data1, &queue_files1, &threads); // this should be associated with a trigger to run the pipeline 1 only when a trigger is activated

    // pipeline2
    string *data2;
    pipeline2(data2, &queue_files2, &threads); // this should be associated with a trigger to run the pipeline 2 only when a trigger is activated

    // pipeline4
    string *data4;
    pipeline4(data4, &queue_files4, &threads); // this should be associated with a trigger to run the pipeline 4 only when a trigger is activated

    // pipeline5
    string *data5;
    pipeline5(data5, &queue_files5, &threads); // this should be associated with a trigger to run the pipeline 5 only when a trigger is activated

    // pipeline7
    string *data7;
    pipeline7(data7, &queue_files7, &threads); // this should be associated with a trigger to run the pipeline 7 only when a trigger is activated

    // use datas to print the dashboard with the results of the pipelines
    // everytime some of the variables are updated we should clean the terminal and print the new dashboard
    dashboard(data1, data2, data4, data5, data7);

    auto init_time = std::chrono::system_clock::now();

    for (auto &t : threads)
    {
        t.join();
    }

    std::vector<std::thread> loop_threads;

    while (true)
    {
        // new data
        // generate new data with mock following a trigger possibly wait 30 seconds and then add 100 new lines should be sufficient

        // pipeline1
        EventBasedTrigger eventTrigger1;
        for (int i = 0; i < 5; i++)
        {
            loop_threads.emplace_back([i, &eventTrigger1, &queue_files1]
                                      { eventTrigger1.triggerOnApperanceOfNewLogFile("./logs", queue_files1); });
        }

        EventBasedTrigger eventTrigger2;
        for (int i = 0; i < 5; i++)
        {
            loop_threads.emplace_back([i, &eventTrigger2, &queue_files2]
                                      { eventTrigger2.triggerOnApperanceOfNewLogFile("./logs", queue_files2); });
        }

        EventBasedTrigger eventTrigger4;
        for (int i = 0; i < 5; i++)
        {
            loop_threads.emplace_back([i, &eventTrigger4, &queue_files4]
                                      { eventTrigger4.triggerOnApperanceOfNewLogFile("./logs", queue_files4); });
        }

        EventBasedTrigger eventTrigger5;
        for (int i = 0; i < 5; i++)
        {
            loop_threads.emplace_back([i, &eventTrigger5, &queue_files5]
                                      { eventTrigger5.triggerOnApperanceOfNewLogFile("./logs", queue_files5); });
        }

        EventBasedTrigger eventTrigger7;
        for (int i = 0; i < 5; i++)
        {
            loop_threads.emplace_back([i, &eventTrigger7, &queue_files7]
                                      { eventTrigger7.triggerOnApperanceOfNewLogFile("./logs", queue_files7); });
        }

        // check if the time is greater than 10 seconds
        auto current_time = std::chrono::system_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::seconds>(current_time - init_time).count();
        if (diff > 90)
        {
            string *data1;
            pipeline1(data1, &queue_files1, &loop_threads); // this should be associated with a trigger to run the pipeline 1 only when a trigger is activated

            // pipeline2
            string *data2;
            pipeline2(data2, &queue_files2, &loop_threads); // this should be associated with a trigger to run the pipeline 2 only when a trigger is activated

            // pipeline4
            string *data4;
            pipeline4(data4, &queue_files4, &loop_threads); // this should be associated with a trigger to run the pipeline 4 only when a trigger is activated

            // pipeline5
            string *data5;
            pipeline5(data5, &queue_files5, &loop_threads); // this should be associated with a trigger to run the pipeline 5 only when a trigger is activated

            // pipeline7
            string *data7;
            pipeline7(data7, &queue_files7, &loop_threads); // this should be associated with a trigger to run the pipeline 7 only when a trigger is activated

            // use datas to print the dashboard with the results of the pipelines

            for (auto &thread : loop_threads)
            {
                thread.join();
            }

            // everytime some of the variables are updated we should clean the terminal and print the new dashboard
            dashboard(data1, data2, data4, data5, data7);

            init_time = std::chrono::system_clock::now();
        }
    }
    return 0;
}

// std::cout<<"Mock files created"<<std::endl;
// //Question 1 - Número de produtos visualizados por minuto
// ConsumerProducerQueue<std::string> queue_files1(15);
// ConsumerProducerQueue<DataFrame *> queue_reader1(15);
// ConsumerProducerQueue<DataFrame *> queue_select1(15);
// ConsumerProducerQueue<DataFrame *> queue_filter1(15);
// ConsumerProducerQueue<DataFrame *> queue_print1(15);

// //vector of threads
// std::vector<std::thread> threads;

// std::cout<<"Queues created"<<std::endl;

// std::this_thread::sleep_for(std::chrono::seconds(5));
// queue_files1.push("STOP");

// EventBasedTrigger eventTrigger;
// for (int i = 0; i < 1; i++){
//     threads.emplace_back([i, &eventTrigger, &queue_files1] {
//         eventTrigger.triggerOnApperanceOfNewLogFile("./logs", queue_files1);
//     });
// }

// std::cout<<"Event Trigger created"<<std::endl;

// FileReader csvReader1;
// int end1 = 0;
// for (int i = 0; i < 2; i++){
//     threads.emplace_back([i, &csvReader1, &queue_files1, &queue_reader1, &end1] {
//         csvReader1.read(',', 0, end1, queue_reader1, queue_files1, false, true, "user_behavior_logs");
//     });
// }

// std::cout<<"Reader created"<<std::endl;

// SelectHandler selectHandler1(&queue_reader1, &queue_select1);
// for (int i = 0; i < 2; i++){
//     threads.emplace_back([i, &selectHandler1] {
//         selectHandler1.select({"Button Product Id","Date"});
//     });
// }

// std::cout<<"Select Handler created"<<std::endl;

// FilterHandler filterHandler1(&queue_select1, &queue_filter1);
// for (int i = 0; i < 2; i++){
//     threads.emplace_back([i, &filterHandler1] {
//         filterHandler1.filter("Button Product Id", "!=", "0");
//     });
// }

// std::cout<<"Filter Handler created"<<std::endl;

// string dbPath = "./mydatabase.db";
// string tableName = "Table1";
// FinalHandler finalHandler1(&queue_filter1, &queue_print1);
// for (int i = 0; i < 1; i++){
//     threads.emplace_back([i, &finalHandler1, &dbPath, &tableName] {
//         finalHandler1.aggregate(dbPath, tableName, false,false,"", "", "", "");
//     });
// }

// std::cout<<"Final Handler created"<<std::endl;

// //ler a table com sqlite
// //encontra o número de linhas
// //encontrar o maior e o menor valor na coluna date
// //calcular a razão entre o número de linhas e a diferença entre o maior e o menor valor na coluna date em minutos

// auto handleResults = [](int rowCount, double dateDiffMinutes) {
//     double ratio = rowCount / dateDiffMinutes;
//     std::cout << "Total number of records: " << rowCount << std::endl;
//     std::cout << "Difference in dates (minutes): " << dateDiffMinutes << std::endl;
//     std::cout << "Ratio of records to time difference: " << ratio << std::endl;
// };

// for(auto& t : threads){
//     t.join();
//     std::cout<<"Thread joined"<<std::endl;
// }
// std::cout<<"Threads joined"<<std::endl;

// // Open the database
// sqlite3* db = openDatabase(dbPath);
// if (db != nullptr) {
//     string query = "SELECT COUNT(*), (strftime('%s', MAX(Date)) - strftime('%s', MIN(Date))) / 60.0 AS DateDiffInMinutes FROM Table1;";
//     executeQuery(db, query, handleResults);

//     // Close the database
//     sqlite3_close(db);
// }

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
// });e

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
// //sum FALTA Column
