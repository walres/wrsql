#ifndef WR_SQL_SQLITE3API_H
#define WR_SQL_SQLITE3API_H

#ifdef _WIN32
#       define SQLITE_API __declspec(dllimport)
#endif

#include <sqlite3.h>

#endif // !WR_SQL_SQLITE3API_H
