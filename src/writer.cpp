#include "writer.h"
#include <sqlite3.h>
#include "DataFrame.h"
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <sys/file.h>
#include <fcntl.h>
#include <sstream>  // for std::stringstream
#include <iomanip>  // for std::put_time

void write_to_sqlite(DataFrame* fileDF, const std::string& filePath, const std::string& table, bool deleteFlag)
{
    {
        // creates a sqlite db
        sqlite3 *db;
        sqlite3_stmt *stmt;
        int fd = open(filePath.c_str(), O_RDWR| O_CREAT, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            return;
        }
        if (flock(fd, LOCK_EX) != 0) {
            close(fd);
            return;
        }
        // Open the SQLite database
        if (sqlite3_open(filePath.c_str(), &db) != SQLITE_OK) {
            std::cerr << "Error opening SQLite database: " << sqlite3_errmsg(db) << "\n";
            return;
        }
        std::string sql = "INSERT INTO " + table + " VALUES (";
        for (size_t i = 0; i < fileDF->get_column_order().size(); ++i) {
            sql += (i > 0 ? ", ?" : "?");
        }
        sql += ")";

        // Get the file descriptor and apply a lock


        std::vector<std::string> column_order = fileDF->get_column_order();
        std::unordered_map<std::string, size_t> column_types = fileDF->get_column_types();

        // Create table if it doesn't exist
        std::string sql_create = "CREATE TABLE IF NOT EXISTS " + table + " (";
        for (const auto &col: column_order) {
            if (column_types[col] == type_to_index[std::type_index(typeid(int))]) {
                sql_create += col + " INTEGER,";
            } else if (column_types[col] == type_to_index[std::type_index(typeid(float))]) {
                sql_create += col + " REAL,";
            } else if (column_types[col] == type_to_index[std::type_index(typeid(std::string))]) {
                sql_create += col + " TEXT,";
            } else {
                // For other data types, you can store them as BLOB or TEXT,
                // depending on how you want to handle them.
                sql_create += col + " TEXT,";

            }
        }
        sql_create.pop_back();  // Remove the last comma
        sql_create += ");";



            char *errMsg;
            if (sqlite3_exec(db, sql_create.c_str(), nullptr, nullptr, &errMsg) != SQLITE_OK) {
                std::cerr << "SQL error: " << errMsg << "\n";
                sqlite3_free(errMsg);
                sqlite3_close(db);
                return;
            }

        if (deleteFlag) {
            // Clear the table
            std::string sql_clear = "DELETE FROM " + table;
            char *errmsg;
            if (sqlite3_exec(db, sql_clear.c_str(), nullptr, nullptr, &errmsg) != SQLITE_OK) {
                std::cerr << "Error clearing table: " << errmsg << "\n";
                sqlite3_free(errmsg);
                sqlite3_close(db);
                return;
            }
        }

        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            std::cerr << "Error preparing SQL statement: " << sqlite3_errmsg(db) << "\n";
            sqlite3_close(db);
            return;
        }

        // Loop over the rows of the DataFrame
        for (size_t i = 0; i < fileDF->get_number_of_rows(); ++i) {
            // Bind the values to the SQL statement
            for (size_t j = 0; j < fileDF->get_column_order().size(); ++j) {
                if (column_types[column_order[j]] == type_to_index[std::type_index(typeid(int))]) {
                    sqlite3_bind_int(stmt, j + 1, fileDF->get_value<int>(column_order[j], i));
                } else if (column_types[column_order[j]] == type_to_index[std::type_index(typeid(float))]) {
                    sqlite3_bind_double(stmt, j + 1, fileDF->get_value<float>(column_order[j], i));
                } else if (column_types[column_order[j]] == type_to_index[std::type_index(typeid(std::string))]) {
                    sqlite3_bind_text(stmt, j + 1, fileDF->get_value<std::string>(column_order[j], i).c_str(), -1, SQLITE_STATIC);
                } else if (column_types[column_order[j]] == type_to_index[std::type_index(typeid(std::tm))]) {
                    std::tm time = fileDF->get_value<std::tm>(column_order[j], i);
                    std::stringstream ss;
                    ss << std::put_time(&time, "%Y-%m-%d %H:%M:%S");
                    std::string time_str = ss.str();
                    sqlite3_bind_text(stmt, j + 1, time_str.c_str(), -1, SQLITE_STATIC);
                }
                else{
                    std::cout<<"Wrong data type"<<std::endl;
                }
            }

            // Execute the SQL statement
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                std::cerr << "Error executing SQL statement: " << sqlite3_errmsg(db) << "\n";
            }

            // Reset the SQL statement for the next row
            sqlite3_reset(stmt);
        }

        // Finalize the SQL statement
        sqlite3_finalize(stmt);

        // Release the lock and close the file
        flock(fd, LOCK_UN);
        close(fd);
        sqlite3_close(db);
    }
}