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
#include <mutex>
#include <unistd.h>
#include <sys/file.h>
#include <atomic>
#include <functional>
#include <random>

std::mutex mtx4;

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
    return db;
}

void mock_files()
{
    for (int i = 0; i < 100; i += 10)
    {
        mockCSV(i/10, 1000);
        mockLogFiles(10, 100, i);
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
    int fd = open(dbPath.c_str(), O_RDWR| O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        return false;
    }
    if (flock(fd, LOCK_SH) != 0) {
        close(fd);
        return false;
    }
    db = openDatabase(dbPath);

    int rc = sqlite3_exec(db, sql.c_str(), callback4, &result, &errorMessage);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
        sqlite3_close(db);
        flock(fd, LOCK_UN);
        close(fd);
        return false;
    }
    sqlite3_close(db);
    flock(fd, LOCK_UN);
    close(fd);
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

void dashboard(string &data4)
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
        std::cout << dashboard << std::endl;

        // Print the results of the pipelines

        // Print the results of pipeline 4
        mtx4.lock();
        {
            std::cout << "Question 4 - Ranking dos produtos mais comprados" << std::endl;
            std::cout << data4 << std::endl;
            data4="";
        }
        mtx4.unlock();

        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}


int main()
{


    string data4  = "";


    std::vector<std::unique_ptr<ConsumerProducerQueue<std::string>>> queue_files;
    for (int i = 0; i < 9; i++)
    {
        queue_files.push_back(std::make_unique<ConsumerProducerQueue<std::string>>(100));
    }

    mockCSV(0, 1000);
    mockLogFiles(10, 100, 0);

    // vector of threads
    std::vector<std::thread> threads;

    threads.emplace_back(mock_files);

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

    //Question 4 - Ranking dos produtos mais comprados -----------------------------------------------------------

    ConsumerProducerQueue<DataFrame *> queue_reader4(100);
    ConsumerProducerQueue<DataFrame *> queue_select4(100);
    ConsumerProducerQueue<DataFrame *> queue_groupby4(100);

    FileReader csvReader4;
    int end4 = 0;
    for (int i = 0; i < 12; i++)
    {
        (threads).emplace_back([i, &csvReader4, &queue_files, &queue_reader4, &end4]
                               { csvReader4.read(',', 0, end4, queue_reader4, *queue_files[4], false, true, "order"); });
    }

    SelectHandler selectHandler4(&queue_reader4, &queue_select4);
    for (int i = 0; i < 12; i++)
    {
        (threads).emplace_back([i, &selectHandler4]
                               { selectHandler4.select({ "ID_PRODUTO", "QUANTIDADE"}); });
    }

    GroupByHandler groupByHandler4(&queue_select4, &queue_groupby4);
    for (int i = 0; i < 12; i++)
    {
        (threads).emplace_back([i, &groupByHandler4]
                               { groupByHandler4.group_by("ID_PRODUTO", "sum"); });
    }

    string dbPath4 = "./mydatabase4.db";
    string tableName4 = "Table4";
    FinalHandler finalHandler4(&queue_groupby4, nullptr);
    for (int i = 0; i < 11; i++)
    {
        (threads).emplace_back([i, &finalHandler4, &dbPath4, &tableName4]
                               { finalHandler4.aggregate(dbPath4, tableName4, false, true, "", "ID_PRODUTO", "sum", ""); });
    }



   threads.emplace_back([&data4, &dbPath4]
                          {
                              std::string sql = "SELECT * FROM Table4 ORDER BY QUANTIDADE DESC;";
                              worker4(std::ref(data4), dbPath4, sql); });


    threads.emplace_back([&data4]
                         {
                             //  Call the dashboard function here
                             dashboard(std::ref(data4)); });

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