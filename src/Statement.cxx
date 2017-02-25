/**
 * \file Statement.cxx
 *
 * \brief Implementation of class wr::sql::Statement, class wr::sql::Row and
 *      statement registration functions
 *
 * \copyright
 * \parblock
 *
 *   Copyright 2012-2017 James S. Waller
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * \endparblock
 */
#include <wrsql/Config.h>
#include <stdint.h>
#include <limits.h>
#include <string.h>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <wrutil/CityHash.h>
#include <wrutil/codecvt.h>
#include <wrutil/filesystem.h>
#include <wrutil/Format.h>
#include <wrutil/numeric_cast.h>
#include <wrutil/u8string_view.h>
#include <wrutil/string_view.h>

#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/Statement.h>

#include "sqlite3api.h"
#include "SessionPrivate.h"


namespace wr {
namespace sql {


namespace {


struct RegistrationData
{
        using Statements = std::unordered_map<std::string, size_t, CityHash>;

        Statements                        stmts_by_sql;
        std::vector<Statements::iterator> stmts_by_index;
        std::mutex                        lock;
};

//--------------------------------------

static RegistrationData &
registrationData()
{
        static RegistrationData data;
        return data;
}


} // anonymous namespace

//--------------------------------------

WRSQL_API size_t
registerStatement(
        const u8string_view &sql
)
{
        auto &regdata = registrationData();
        std::string sql_copy = sql.to_string();
        std::lock_guard<std::mutex> guard(regdata.lock);
        auto id  = regdata.stmts_by_index.size();
        auto ins = regdata.stmts_by_sql.insert({ std::move(sql_copy), id });

        if (ins.second) {
                regdata.stmts_by_index.push_back(ins.first);
        } else {
                id = ins.first->second;
        }

        return id;
}

//--------------------------------------

WRSQL_API size_t
numRegisteredStatements()
{
        auto                        &regdata = registrationData();
        std::lock_guard<std::mutex>  guard  (regdata.lock);
        return regdata.stmts_by_index.size();
}

//--------------------------------------

WRSQL_API u8string_view
registeredStatement(
        size_t id
)
{
        auto                        &regdata = registrationData();
        std::lock_guard<std::mutex>  guard  (regdata.lock);
        u8string_view                sql;

        if (id < regdata.stmts_by_index.size()) {
                sql = regdata.stmts_by_index[id]->first;
        } else {
                throw std::invalid_argument("index out of bounds");
        }

        return sql;
}

//--------------------------------------

WRSQL_API
Statement::Statement() :
        stmt_   (nullptr),
        session_(nullptr)
{
}

//--------------------------------------

WRSQL_API
Statement::Statement(
        const this_t &other
) :
        this_t()
{
        if (other.isPrepared()) {
                prepare(*other.session_, other.sql());
        }
}

//--------------------------------------

WRSQL_API
Statement::Statement(
        this_t &&other
) :
        this_t()
{
        operator=(std::move(other));
}

//--------------------------------------

WRSQL_API
Statement::Statement(
        const Session       &session,
        const u8string_view &sql
) :
        this_t()
{
        prepare(session, sql);
}

//--------------------------------------

WRSQL_API
Statement::Statement(
        const Session       &session,
        const u8string_view &sql,
        u8string_view       &tail
) :
        this_t()
{
        prepare(session, sql, tail);
}

//--------------------------------------

WRSQL_API
Statement::~Statement()
{
        finalize();
}

//--------------------------------------

WRSQL_API auto
Statement::prepare(
        const Session       &session,
        const u8string_view &sql
) -> this_t &
{
        u8string_view tail;
        return prepare(session, sql, tail);
}

//--------------------------------------

WRSQL_API auto
Statement::prepare(
        const Session       &session,
        const u8string_view &sql,
        u8string_view       &tail
) -> this_t &
{
        finalize();
        session_ = &session;

        int status;

        do {
                sqlite3_stmt *stmt;
                const char   *end;

                status = sqlite3_prepare_v2(session.body_->db(),
                                            sql.char_data(),
                                            numeric_cast<int>(sql.bytes()),
                                            &stmt, &end);
                switch (status) {
                case SQLITE_OK:
                        stmt_ = stmt;
                        tail = u8string_view(end,
                                sql.bytes() - numeric_cast<size_t>(
                                                        end - sql.char_data())).
                                trim_left();
                        break;
                case SQLITE_LOCKED:
                        if (session_->body_->waitForUnlock()) {
                                break;
                        }
                        // possible deadlock, fall through
                case SQLITE_BUSY:
                        throw Busy();
                default:
                        throw Error(*this, status, sql);
                }
        } while (status != SQLITE_OK);

        return *this;
}

//--------------------------------------

WRSQL_API void
Statement::finalize()
{
        if (isPrepared()) {
                reset();
                sqlite3_finalize(static_cast<sqlite3_stmt *>(stmt_));
                stmt_ = nullptr;
                stmt_.tag(false);
        }
        session_ = nullptr;
}

//--------------------------------------

WRSQL_API u8string_view
Statement::sql() const
{
        if (!isPrepared()) {
                return {};
        }
        return sqlite3_sql(static_cast<sqlite3_stmt *>(stmt_));
}

//--------------------------------------

WRSQL_API auto
Statement::reset() -> this_t &
{
        if (isPrepared()) {
                sqlite3_reset(static_cast<sqlite3_stmt *>(stmt_));
        }
        stmt_.tag(false);
        return *this;
}

//--------------------------------------

WRSQL_API auto
Statement::clearBindings() -> this_t &
{
        if (isActive()) {
                reset();
        }

        sqlite3_clear_bindings(static_cast<sqlite3_stmt *>(stmt_));
        return *this;
}

//--------------------------------------

WRSQL_API auto
Statement::bindNull(
        int param_no
) -> this_t &
{
        if (isActive()) {
                reset();
        }

        auto status = sqlite3_bind_null(static_cast<sqlite3_stmt *>(stmt_),
                                        param_no);
        if (status != SQLITE_OK) {
                throwBindError(param_no, status);
        }
        return *this;
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int  param_no,
        char val
) -> this_t &
{
        return bind(param_no, static_cast<int>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int           param_no,
        unsigned char val
) -> this_t &
{
        return bind(param_no, static_cast<unsigned int>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int   param_no,
        short val
) -> this_t &
{
        return bind(param_no, static_cast<int>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int            param_no,
        unsigned short val
) -> this_t &
{
        return bind(param_no, static_cast<unsigned int>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int param_no,
        int val
) -> this_t &
{
        if (isActive()) {
                reset();
        }

        auto status = sqlite3_bind_int(static_cast<sqlite3_stmt *>(stmt_),
                                       param_no, val);
        if (status != SQLITE_OK) {
                throwBindError(param_no, status);
        }

        return *this;
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int          param_no,
        unsigned int val
) -> this_t &
{
        return bind(param_no, static_cast<long long>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int  param_no,
        long val
) -> this_t &
{
#if (LONG_MAX == INT_MAX)
        return bind(param_no, static_cast<int>(val));
#else
        return bind(param_no, static_cast<long long>(val));
#endif
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int           param_no,
        unsigned long val
) -> this_t &
{
        return bind(param_no, static_cast<long long>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int       param_no,
        long long val
) -> this_t &
{
        if (isActive()) {
                reset();
        }

        auto status = sqlite3_bind_int64(static_cast<sqlite3_stmt *>(stmt_),
                                         param_no, val);
        if (status != SQLITE_OK) {
                throwBindError(param_no, status);
        }
        return *this;
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int                param_no,
        unsigned long long val
) -> this_t &
{
        return bind(param_no, static_cast<long long>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int   param_no,
        float val
) -> this_t &
{
        return bind(param_no, static_cast<double>(val));
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int    param_no,
        double val
) -> this_t &
{
        if (isActive()) {
                reset();
        }

        auto status = sqlite3_bind_double(static_cast<sqlite3_stmt *>(stmt_),
                                          param_no, val);
        if (status != SQLITE_OK) {
                throwBindError(param_no, status);
        }
        return *this;
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int         param_no,
        const char *text
) -> this_t &
{
        if (isActive()) {
                reset();
        }

        auto status = sqlite3_bind_text64(static_cast<sqlite3_stmt *>(stmt_),
                                          param_no, text, strlen(text),
                                          SQLITE_STATIC, SQLITE_UTF8);
        if (status != SQLITE_OK) {
                throwBindError(param_no, status);
        }
        return *this;
}

//--------------------------------------

static std::map<const void *, Statement::FreeBlobFn> blob_free_fn_map;
std::mutex                                           blob_free_fn_map_mutex;

//--------------------------------------

static void
freeBlob(
        void *data
)
{
        std::unique_lock<std::mutex> lock(blob_free_fn_map_mutex);

        auto i = blob_free_fn_map.find(data);

        if (i != blob_free_fn_map.end()) {
                i->second(data);
                blob_free_fn_map.erase(i);
        }
}

//--------------------------------------

WRSQL_API auto
Statement::bind(
        int         param_no,
        const void *data,
        size_t      bytes,
        FreeBlobFn  free_blob
) -> this_t &
{
        if (!data) {
                bindNull(param_no);
        } else {
                if (isActive()) {
                        reset();
                }

                int status;

                if (free_blob) {
                        bool ok;

                        {
                                std::unique_lock<std::mutex> lock(
                                                        blob_free_fn_map_mutex);
                                ok = blob_free_fn_map.
                                            insert({ data, free_blob }).second;
                        }

                        if (!ok) {
                                throw Error(printStr("bind(%d): destructor already registered for blob %p",
                                                     param_no, data));
                        }

                        status = sqlite3_bind_blob64(
                                            static_cast<sqlite3_stmt *>(stmt_),
                                            param_no, data, bytes, &freeBlob);
                } else {
                        status = sqlite3_bind_blob64(
                                            static_cast<sqlite3_stmt *>(stmt_),
                                            param_no, data, bytes,
                                            SQLITE_STATIC);
                }

                if (status != SQLITE_OK) {
                        throwBindError(param_no, status);
                }
        }
        return *this;
}

//--------------------------------------

template <> WRSQL_API auto
Statement::bind(
        int                  param_no,
        const u8string_view &text
) -> this_t &
{
        if (isActive()) {
                reset();
        }

        auto status = sqlite3_bind_text64(static_cast<sqlite3_stmt *>(stmt_),
                                          param_no, text.char_data(),
                                          text.bytes(), SQLITE_STATIC,
                                          SQLITE_UTF8);
        if (status != SQLITE_OK) {
                throwBindError(param_no, status);
        }
        return *this;
}

//--------------------------------------

template <> WRSQL_API auto
Statement::bind(
        int                param_no,
        const string_view &text
) -> this_t &
{
        return bind(param_no, u8string_view(text));
}

//--------------------------------------

template <> WRSQL_API auto
Statement::bind(
        int                param_no,
        const std::string &text
) -> this_t &
{
        return bind(param_no, u8string_view(text));
}

//--------------------------------------

#if WR_HAVE_STD_STRING_VIEW

template <> WRSQL_API auto
Statement::bind(
        int                     param_no,
        const std::string_view &text
) -> this_t &
{
        return bind(param_no, u8string_view(text));
}

#endif // WR_HAVE_STD_STRING_VIEW

//--------------------------------------

template <> WRSQL_API auto
Statement::bind(
        int         param_no,
        const path &p
) -> this_t &
{
        return bind(param_no, to_generic_u8string(p));
}

//--------------------------------------

template <> WRSQL_API auto
Statement::bind(
        int                   col_no,
        const file_time_type &val
) -> this_t &
{
        return bind(col_no, file_time_type::clock::to_time_t(val));
}

//--------------------------------------

void
Statement::throwBindError(
        int param_no,
        int status
) const
{
        switch (status) {
        case SQLITE_RANGE:
                throw std::invalid_argument(
                        printStr("parameter index %d out of range (SQL: %s)",
                                 param_no, sql()));
        case SQLITE_TOOBIG:
                throw std::length_error(
                        utf8_narrow_cvt().from_utf8(
                                        Session::message(session(), status)));
        case SQLITE_NOMEM:
                throw std::bad_alloc();
        default:
                throw Error(*this, status);
        }
}

//--------------------------------------

WRSQL_API auto
Statement::begin() -> Row
{
        if (!isPrepared()) {
                return end();
        } else if (isActive()) {
                reset();
        }
        stmt_.tag(true);
        return next();
}

//--------------------------------------

WRSQL_API auto
Statement::currentRow() -> Row
{
        return Row(*this);
}

//--------------------------------------

WRSQL_API auto
Statement::next() -> Row
{
        if (!isPrepared() || !isActive()) {
                return end();
        }

        int status;

        do {
                status = sqlite3_step(static_cast<sqlite3_stmt *>(stmt_));

                switch (status) {
                case SQLITE_ROW:
                        break;
                case SQLITE_OK: case SQLITE_DONE:
                        reset();
                        break;
                case SQLITE_INTERRUPT:
                        reset();
                        throw Interrupt();
                case SQLITE_LOCKED:
                        if (session_->body_->waitForUnlock()) {
                                break;
                        }
                        // possible deadlock, fall through
                case SQLITE_BUSY:
                        reset();
                        throw Busy();
                default:
                        reset();
                        throw Error(*this, status);
                }
        } while (status == SQLITE_LOCKED);

        return Row(*this);
}

//--------------------------------------

WRSQL_API auto
Statement::end() -> Row
{
        return {};
}

//--------------------------------------

WRSQL_API auto
Statement::operator=(
        const this_t &other
) -> this_t &
{
        if (&other != this) {
                if (other.isPrepared()) {
                        prepare(*other.session_, other.sql());
                } else {
                        finalize();
                }
        }

        return *this;
}

//--------------------------------------

WRSQL_API auto
Statement::operator=(
        this_t &&other
) -> this_t &
{
        if (&other != this) {
                finalize();
                session_ = other.session_;
                stmt_ = other.stmt_;
                other.stmt_ = nullptr;
        }

        return *this;
}

//--------------------------------------

WRSQL_API
Row::Row() :
        stmt_(nullptr)
{
}

//--------------------------------------

WRSQL_API
Row::Row(
        Statement &stmt
) :
        stmt_(&stmt)
{
}

//--------------------------------------

WRSQL_API bool
Row::next()
{
        if (empty()) {
                return false;
        }
        stmt_->next();
        return stmt_->isActive();
}

//--------------------------------------

WRSQL_API bool
Row::isNull(
        int col_no
) const
{
        return sqlite3_column_type(static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                   col_no) == SQLITE_NULL;
}

//--------------------------------------

WRSQL_API int
Row::colSize(
        int col_no
) const
{
        return sqlite3_column_bytes(static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                    col_no);
}

//--------------------------------------

template <> WRSQL_API int
Row::get<int>(
        int col_no
) const
{
        return sqlite3_column_int(static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                  col_no);
}

//--------------------------------------

template <> WRSQL_API unsigned int
Row::get<unsigned int>(
        int col_no
) const
{
        return static_cast<unsigned int>(get<int>(col_no));
}

//--------------------------------------

template <> WRSQL_API bool
Row::get<bool>(
        int col_no
) const
{
        return get<int>(col_no) != 0;
}

//--------------------------------------

template <> WRSQL_API char
Row::get<char>(
        int col_no
) const
{
        return numeric_cast<char>(get<int>(col_no));
}

//--------------------------------------

template <> WRSQL_API unsigned char
Row::get<unsigned char>(
        int col_no
) const
{
        return numeric_cast<unsigned char>(get<unsigned int>(col_no));
}

//--------------------------------------

template <> WRSQL_API char16_t
Row::get<char16_t>(
        int col_no
) const
{
        return numeric_cast<char16_t>(get<unsigned int>(col_no));
}

//--------------------------------------

template <> WRSQL_API char32_t
Row::get<char32_t>(
        int col_no
) const
{
        return numeric_cast<char32_t>(get<unsigned int>(col_no));
}

//--------------------------------------

template <> WRSQL_API wchar_t
Row::get<wchar_t>(
        int col_no
) const
{
        return numeric_cast<wchar_t>(get<unsigned int>(col_no));
}

//--------------------------------------

template <> WRSQL_API short
Row::get<short>(
        int col_no
) const
{
        return numeric_cast<short>(get<int>(col_no));
}

//--------------------------------------

template <> WRSQL_API unsigned short
Row::get<unsigned short>(
        int col_no
) const
{
        return numeric_cast<unsigned short>(get<unsigned int>(col_no));
}

//--------------------------------------

template <> WRSQL_API long
Row::get<long>(
        int col_no
) const
{
#if (LONG_MAX < INT64_MAX)
        return sqlite3_column_int(static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                  col_no);
#else
        return sqlite3_column_int64(static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                    col_no);
#endif
}

//--------------------------------------

template <> WRSQL_API unsigned long
Row::get<unsigned long>(
        int col_no
) const
{
        return static_cast<unsigned long>(get<long>(col_no));
}

//--------------------------------------

template <> WRSQL_API long long
Row::get<long long>(
        int col_no
) const
{
        return sqlite3_column_int64(static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                    col_no);
}

//--------------------------------------

template <> WRSQL_API unsigned long long
Row::get<unsigned long long>(
        int col_no
) const
{
        return static_cast<unsigned long long>(get<long long>(col_no));
}

//--------------------------------------

template <> WRSQL_API double
Row::get<double>(
        int col_no
) const
{
        if (isNull(col_no)) {
                return std::numeric_limits<double>::quiet_NaN();
        } else {
                return sqlite3_column_double(
                        static_cast<sqlite3_stmt *>(stmt_->stmt_), col_no);
        }
}

//--------------------------------------

template <> WRSQL_API float
Row::get<float>(
        int col_no
) const
{
        double value = get<double>(col_no);

        if (std::isfinite(value)) {
                return numeric_cast<float>(value);
        } else {
                // avoid spurious overflow/underflow exceptions when value is +/-infinity
                return static_cast<float>(value);
        }
}

//--------------------------------------

template <> WRSQL_API const char *
Row::get<const char *>(
        int col_no
) const
{
        if (isNull(col_no)) {
                return nullptr;
        } else {
                return reinterpret_cast<const char *>(sqlite3_column_text(
                        static_cast<sqlite3_stmt *>(stmt_->stmt_), col_no));
        }
}

//--------------------------------------

template <> WRSQL_API string_view
Row::get<string_view>(
        int col_no
) const
{
        if (isNull(col_no)) {
                return {};
        } else {
                /*
                 * According to the SQLite documentation:
                 * "The safest policy is to invoke these routines in one of
                 * the following ways:
                 * - sqlite3_column_text() followed by sqlite3_column_bytes()
                 * - sqlite3_column_blob() followed by sqlite3_column_bytes()
                 * - sqlite3_column_text16() followed by
                 *   sqlite3_column_bytes16()"
                 *
                 * Therefore make absolutely sure that sqlite3_column_text()
                 * gets called first
                 */
                auto text = sqlite3_column_text(
                        static_cast<sqlite3_stmt *>(stmt_->stmt_), col_no);

                return { reinterpret_cast<const char *>(text),
                         static_cast<size_t>(sqlite3_column_bytes(
                                static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                col_no)) };
        }
}

//--------------------------------------

template <> WRSQL_API u8string_view
Row::get<u8string_view>(
        int col_no
) const
{
        return get<string_view>(col_no);
}

//--------------------------------------

template <> WRSQL_API std::string
Row::get<std::string>(
        int col_no
) const
{
        return get<string_view>(col_no).to_string();
}

//--------------------------------------

template <> WRSQL_API const void *
Row::get<const void *>(
        int col_no
) const
{
        if (isNull(col_no)) {
                return nullptr;
        } else {
                return sqlite3_column_blob(
                            static_cast<sqlite3_stmt *>(stmt_->stmt_), col_no);
        }
}

//--------------------------------------

template <> WRSQL_API wr::path
Row::get<wr::path>(
        int col_no
) const
{
        return wr::u8path(get<u8string_view>(col_no));
}

//--------------------------------------

template <> WRSQL_API file_time_type
Row::get(
        int col_no
) const
{
        return file_time_type::clock::from_time_t(get<time_t>(col_no));
}

//--------------------------------------

WRSQL_API int
Row::numCols() const
{
        return sqlite3_column_count(static_cast<sqlite3_stmt *>(stmt_->stmt_));
}

//--------------------------------------

WRSQL_API u8string_view
Row::colName(
        int col_no
) const
{
        return sqlite3_column_name(static_cast<sqlite3_stmt *>(stmt_->stmt_),
                                   col_no);
}

//--------------------------------------

WRSQL_API ValueType
Row::colType(
        int col_no
) const
{
        int type = sqlite3_column_type(
                        static_cast<sqlite3_stmt *>(stmt_->stmt_), col_no);

        switch (type) {
        case SQLITE_INTEGER:
                return INT_TYPE;
        case SQLITE_FLOAT:
                return FLOAT_TYPE;
        case SQLITE_TEXT:
                return TEXT_TYPE;
        case SQLITE_BLOB:
                return BLOB_TYPE;
        case SQLITE_NULL:
                return NULL_TYPE;
        default:
                throw std::runtime_error(
                                printStr("unknown column type %d", type));
        }
}

//--------------------------------------

WRSQL_API int
Row::colNo(
        const u8string_view &col_name
) const
{
        int col_no = -1;

        for (int i = 0, n = numCols(); i < n && col_no < 0; ++i) {
                if (colName(i) == col_name) {
                        col_no = i;
                }
        }

        return col_no;
}

//--------------------------------------

WRSQL_API int
Row::colNo_throw(
        const u8string_view &col_name
) const
{
        int col_no = colNo(col_name);

        if (col_no < 0) {
                throw std::invalid_argument(printStr(
                        "no such column '%s' in result set", col_name));
        }

        return col_no;
}


} // namespace sql
} // namespace wr
