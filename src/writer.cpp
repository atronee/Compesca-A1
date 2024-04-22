#include "writer.h"
#include <sqlite3.h>
#include "DataFrame.h"
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <sys/file.h>
#include <fcntl.h>

void write_to_sqlite(DataFrame* fileDF, const std::string& filePath, const std::string& table, bool deleteFlag)
{
    {
        // creates a sqlite db
        sqlite3 *db;
        sqlite3_stmt *stmt;
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
                const DataVariant& value = fileDF->get_row(i)[j];
                if (auto intPtr = std::get_if<int>(&value)) {
                    sqlite3_bind_int(stmt, j + 1, *intPtr);
                } else if (auto floatPtr = std::get_if<float>(&value)) {
                    sqlite3_bind_double(stmt, j + 1, *floatPtr);
                } else if (auto strPtr = std::get_if<std::string>(&value)) {
                    sqlite3_bind_text(stmt, j + 1, strPtr->c_str(), -1, SQLITE_TRANSIENT);
                } else if (auto tmPtr = std::get_if<std::tm>(&value)) {
                    std::string timeStr = std::asctime(tmPtr);
                    sqlite3_bind_text(stmt, j + 1, timeStr.c_str(), -1, SQLITE_TRANSIENT);
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