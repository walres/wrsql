/**
 * \file IDSet.cxx
 *
 * \brief wr::sql::IDSet class implementation
 *
 * \copyright
 * \parblock
 *
 *   Copyright 2014-2017 James S. Waller
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
#include <string.h>
#include <algorithm>
#include <iomanip>
#include <limits>
#include <set>

#include <wrutil/Format.h>
#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/IDSet.h>

#include "sqlite3api.h"
#include "SessionPrivate.h"
#include "IDSetPrivate.h"


namespace wr {
namespace sql {


IDSet::SQLInterface idset_sql_iface;

//--------------------------------------

WRSQL_API
IDSet::IDSet() :
        body_(new Body)
{
}

//--------------------------------------

WRSQL_API
IDSet::IDSet(
        std::initializer_list<ID> ids
) :
        this_t()
{
        insert(ids);
}

//--------------------------------------

WRSQL_API
IDSet::IDSet(
        Session &db
) :
        this_t()
{
        attach(db);
}

//--------------------------------------

WRSQL_API
IDSet::IDSet(
        Session                   &db,
        std::initializer_list<ID>  ids
) :
        this_t()
{
        attach(db);
        insert(ids);
}

//--------------------------------------

WRSQL_API
IDSet::IDSet(
        Session      &db,
        const this_t &other
) :
        this_t()
{
        attach(db);
        insert(other);
}

//--------------------------------------

WRSQL_API
IDSet::IDSet(
        const this_t &other
) :
        this_t()
{
        *this = other;
}

//--------------------------------------

WRSQL_API
IDSet::IDSet(
        this_t &&other
) :
        this_t()
{
        *this = std::move(other);
}

//--------------------------------------

WRSQL_API
IDSet::~IDSet()
{
        detach();
        delete body_;
}

//--------------------------------------

WRSQL_API auto
IDSet::operator=(
        const this_t &other
) -> this_t &
{
        if (&other != this) {
                if (!db() && other.db()) {
                        attach(*other.db());
                }
                body_->storage_ = other.body_->storage_;
        }
        return *this;        
}

//--------------------------------------

WRSQL_API auto
IDSet::operator=(
        this_t &&other
) -> this_t &
{
        if (&other != this) {
                detach();
                std::swap(body_, other.body_);
        }
        return *this;        
}

//--------------------------------------

WRSQL_API auto
IDSet::operator=(
        std::initializer_list<ID> ids
) -> this_t &
{
        clear();
        insert(ids);
        return *this;
}

//--------------------------------------

WRSQL_API auto
IDSet::attach(
        Session &db
) -> this_t &
{
        if (&db == body_->db_) {
                return *this;
        } else if (body_->db_) {
                detach();
        }

        body_->db_ = &db;

        if (db.isOpen()) {
                char buf[128];

                print(buf, "CREATE VIRTUAL TABLE temp.idset_%p USING sdig_idset(%p)",
                      body_, this);

                db.exec(buf);
        }

        return *this;
}

//--------------------------------------

WRSQL_API auto
IDSet::detach() -> this_t &
{
        if (body_->db_) {
                if (body_->db_->isOpen()) {
                        char buf[32 + (sizeof(void *) * 2)];
                        print(buf, "DROP TABLE idset_%p", body_);
                        body_->db_->exec(buf);
                }
                body_->db_ = nullptr;
        }
        return *this;
}

//--------------------------------------

WRSQL_API auto IDSet::insert(ID id) -> std::pair<iterator, bool>
        { return body_->insert(id); }

auto
IDSet::Body::insert(
        ID id
) -> std::pair<iterator, bool>
{
        auto pos = std::equal_range(storage_.begin(), storage_.end(), id);

        std::pair<iterator, bool> result;

        if (pos.first == pos.second) {
                result.first = storage_.insert(pos.first, id);
                result.second = true;
        } else {
                result.first = pos.first;
                result.second = false;
        }

        return result;
}

//--------------------------------------

WRSQL_API auto
IDSet::insert(
        const this_t &other
) -> size_type
{
        if (other.empty() || (&other == this)) {
                return 0;
        }

        if (empty()) {
                body_->storage_ = other.body_->storage_;
                return size();
        }

        auto      dst = body_->storage_.begin();
        auto      src = other.begin(), src_end = other.end();
        size_type n   = 0;

        while (src != src_end) {
                if (dst == body_->storage_.end()) {
                        body_->storage_.insert(dst, src, src_end);
                        n += src_end - src;
                        src = src_end;
                        dst = body_->storage_.end();
                        continue;
                } else if (*src == *dst) {
                        ++src;
                        ++dst;
                } else if (*src < *dst) {
                        auto src2 = std::lower_bound(next(src), src_end, *dst);

                        /* range-based vector::insert() returns nothing
                           in some older C++ standard libraries */
                        size_t offset = dst - body_->storage_.begin();
                        body_->storage_.insert(dst, src, src2);
                        n += src2 - src;
                        src = src2;
                        dst = body_->storage_.begin() + offset + (src2 - src);
                } else {  // *src > *dst
                        dst = std::lower_bound(next(dst),
                                               body_->storage_.end(), *src);
                }       
        }

        return n;
}

//--------------------------------------

WRSQL_API auto
IDSet::insert(
        std::initializer_list<ID> ids
) -> size_type
{
        return insert(ids.begin(), ids.end());
}

//--------------------------------------

WRSQL_API auto
IDSet::insert(
        Statement &stmt,
        int        col_no
) -> size_type
{
        size_type n = 0;

        for (Row row: stmt) {
                if (insert(row.get<ID>(col_no)).second) {
                        ++n;
                }
        }

        return n;
}

//--------------------------------------

WRSQL_API auto IDSet::erase(ID id) -> size_type { return body_->erase(id); }

auto
IDSet::Body::erase(
        ID id
) -> size_type
{
        auto pos = std::equal_range(storage_.begin(), storage_.end(), id);

        if (pos.first != pos.second) {
                pos.first = storage_.erase(pos.first);
                return 1;
        } else {
                return 0;
        }
}

//--------------------------------------

WRSQL_API auto
IDSet::erase(
        const_iterator pos
) -> iterator
{
        /* vector::erase() doesn't take const_iterator with
           some older C++ standard libraries */
        auto i = body_->storage_.begin();
        i += pos - i;
        return body_->storage_.erase(i);
}

//--------------------------------------

WRSQL_API auto
IDSet::erase(
        const_iterator first,
        const_iterator last
) -> iterator
{
        /* vector::erase() doesn't take const_iterator with
           some older C++ standard libraries */
        auto first_nc = body_->storage_.begin(), last_nc = first_nc;
        first_nc += first - first_nc;
        last_nc += last - last_nc;
        return body_->storage_.erase(first_nc, last_nc);
}

//--------------------------------------

WRSQL_API auto
IDSet::erase(
        const this_t &other
) -> size_type
{
        if (empty() || other.empty()) {
                return 0;
        } else if (&other == this) {
                size_type result = size();
                clear();
                return result;
        }

        auto          &storage = body_->storage_;
        auto           dst     = storage.begin();
        const_iterator src     = other.begin(), src_end = other.end();
        size_t         n       = 0;

        while ((src != src_end) && (dst != storage.end())) {
                if (*src == *dst) {
                        auto dst2 = dst;
                        do {
                                ++src;
                                ++dst2;
                        } while ((src != src_end) && (dst2 != storage.end())
                                                  && (*src == *dst2));
                        n += dst2 - dst;
                        dst = storage.erase(dst, dst2);
                } else if (*src < *dst) {
                        src = std::lower_bound(src, src_end, *dst);
                } else {  // *src > *dst
                        dst = std::lower_bound(dst, storage.end(), *src);
                }
        }

        return n;
}

//--------------------------------------

WRSQL_API auto
IDSet::erase(
        std::initializer_list<ID> ids
) -> size_type
{
        size_type n = 0;

        for (ID id: ids) {
                n += erase(id);
        }

        return n;
}

//--------------------------------------

WRSQL_API auto
IDSet::erase(
        Statement &stmt,
        int        col_no
) -> size_type
{
        size_type n = 0;

        for (Row row: stmt) {
                n += erase(row.get<ID>(col_no));
        }

        return n;
}

//--------------------------------------

WRSQL_API auto
IDSet::intersect(
        const this_t &other
) -> size_type
{
        if (empty() || (&other == this)) {
                return 0;
        }

        size_type n;

        if (other.empty()) {
                n = size();
                clear();
                return n;
        }

        auto &storage = body_->storage_;
        auto  dst     = storage.begin();
        auto  src     = other.begin(), src_end = other.end();

        n = 0;

        while ((src != src_end) && (dst != storage.end())) {
                if (*src == *dst) {
                        ++src, ++dst;
                } else if (*src < *dst) {
                        src = std::lower_bound(src, src_end, *dst);
                } else {  // *src > *dst
                        auto dst2 = std::lower_bound(dst, storage.end(), *src);
                        n += dst2 - dst;
                        dst = storage.erase(dst, dst2);
                }
        }

        if (dst != storage.end()) {
                n += storage.end() - dst;
                storage.erase(dst, storage.end());
        }

        return n;
}

//--------------------------------------

WRSQL_API auto
IDSet::intersect(
        Statement &stmt,
        int        col_no
) -> size_type
{
        if (empty()) {
                return 0;
        }

        size_type n;
        Row       src = stmt.begin();

        if (!src) {
                n = size();
                clear();
                return n;
        }

        auto &storage = body_->storage_;
        auto  dst     = storage.begin();

        n = 0;

        while (src && (dst != storage.end())) {
                auto src_val = src.get<ID>(col_no);
                if (src_val == *dst) {
                        ++src, ++dst;
                } else if (src_val < *dst) {
                        do {
                                if (++src) {
                                        src_val = src.get<ID>(col_no);
                                } else {
                                        break;
                                }
                        } while (src_val < *dst);
                } else {  // src_val > *dst
                        auto dst2 = std::lower_bound(dst, storage.end(),
                                                     src_val);
                        n += dst2 - dst;
                        dst = storage.erase(dst, dst2);
                }
        }

        if (dst != storage.end()) {
                n += storage.end() - dst;
                storage.erase(dst, storage.end());
        }

        return n;
}

//--------------------------------------

WRSQL_API auto
IDSet::intersect(
        std::initializer_list<ID> ids
) -> size_type
{
        return intersect(ids.begin(), ids.end());
}

//--------------------------------------

WRSQL_API auto
IDSet::symmetric_difference(
        const this_t &other
) -> this_t &
{
        if (&other == this) {
                clear();
                return *this;
        } else if (other.empty()) {
                return *this;
        }

        auto &storage = body_->storage_;
        auto  dst     = storage.begin();

        for (auto src(other.begin()), src_end(other.end()); src != src_end;) {
                if (dst == storage.end()) {
                        storage.insert(dst, src, src_end);
                        break;
                } else if (*src == *dst) {  // delete runs of equal values
                        auto dst2 = dst;
                        do {
                                ++src;
                                ++dst2;
                        } while ((src != src_end) && (dst2 != storage.end())
                                                  && (*src == *dst2));
                        dst = storage.erase(dst, dst2);
                } else if (*src < *dst) {
                        auto src2 = std::lower_bound(std::next(src), src_end,
                                                     *dst);

                        /* range-based vector::insert() returns nothing
                           in some older C++ standard libraries */
                        size_t offset = dst - storage.begin();
                        storage.insert(dst, src, src2);
                        src = src2;
                        dst = storage.begin() + offset + (src2 - src);
                } else {  // *src > *dst
                        dst = std::lower_bound(std::next(dst), storage.end(),
                                               *src);
                }
        }

        return *this;
}

//--------------------------------------

WRSQL_API auto
IDSet::symmetric_difference(
        Statement &stmt,
        int        col_no
) -> this_t &
{
        auto &storage = body_->storage_;
        auto  dst     = storage.begin();

        for (auto src = stmt.begin(); src;) {
                if (dst == storage.end()) {
                        do {
                                storage.push_back(src.get<ID>(col_no));
                        } while (++src);
                        break;
                }

                auto src_val = src.get<ID>(col_no);

                if (src_val == *dst) {
                        auto dst2 = dst;
                        do {
                                ++dst2;
                                auto next_src_val = src_val;

                                // skip duplicate source values
                                while (next_src_val == src_val && (++src)) {
                                        next_src_val = src.get<ID>(col_no);
                                }

                                src_val = next_src_val;
                        } while (src && (dst2 != storage.end())
                                     && (src_val == *dst2));

                        dst = storage.erase(dst, dst2);
                } else if (src_val < *dst) {
                        dst = std::next(storage.insert(dst, src_val));
                        // skip duplicate values in source result set
                        auto old_src_val = src_val;
                        while ((src_val == old_src_val) && (++src)) {
                                src_val = src.get<ID>(col_no);
                        }
                } else {  // src_val > *dst
                        dst = std::lower_bound(std::next(dst), storage.end(),
                                               src_val);
                }
        }

        return *this;
}

//--------------------------------------

WRSQL_API auto
IDSet::symmetric_difference(
        std::initializer_list<ID> ids
) -> this_t &
{
        return symmetric_difference(ids.begin(), ids.end());
}

//--------------------------------------

WRSQL_API auto
IDSet::swap(
        IDSet &other
) -> this_t &
{
        if (&other != this) {
                // don't just swap the body pointers!
                /* swapping the body pointers risks SQL queries accessing the
                   wrong sets after return; to avoid that situation all
                   affected queries would have to re-prepare with the swapped
                   table names */
                body_->storage_.swap(other.body_->storage_);

                Session *db = this->db(), *other_db = other.db();

                if (db != other_db) {
                        other.detach();
                        if (db) {
                                other.attach(*db);
                        }
                        detach();
                        if (other_db) {
                                attach(*other_db);
                        }
                }
        }

        return *this;
}

//--------------------------------------

WRSQL_API std::string
IDSet::sql_name() const
{
        return printStr("idset_%p", body_);
}

//--------------------------------------

WRSQL_API auto IDSet::count(ID id) const -> size_type
        { return body_->count(id); }

auto
IDSet::Body::count(
        ID id
) const -> size_type
{
        auto pos = std::equal_range(storage_.begin(), storage_.end(), id);

        if (pos.first == pos.second) {
                return 0;
        } else {
                return 1;
        }
}

//--------------------------------------

WRSQL_API auto
IDSet::find(
        ID id
) const -> iterator
{
        auto i = lower_bound(id), end = this->end();

        if ((i == end) || (*i != id)) {
                i = end;
        }

        return i;
}

//--------------------------------------

WRSQL_API auto
IDSet::lower_bound(
        ID id
) const -> iterator
{
        return std::lower_bound(begin(), end(), id);
}

//--------------------------------------

WRSQL_API auto
IDSet::upper_bound(
        ID id
) const -> iterator
{
        return std::upper_bound(begin(), end(), id);
}

//--------------------------------------

WRSQL_API auto
IDSet::equal_range(
        ID id
) const -> std::pair<iterator, iterator>
{
        return std::equal_range(begin(), end(), id);
}

//--------------------------------------

WRSQL_API Session *IDSet::db() const           { return body_->db_; }
WRSQL_API ID IDSet::operator[](size_t i) const { return body_->storage_[i]; }

WRSQL_API bool IDSet::empty() const { return body_->storage_.empty(); }

WRSQL_API auto IDSet::size() const -> size_type
        { return body_->storage_.size(); }

WRSQL_API auto IDSet::max_size() const -> size_type
        { return body_->storage_.max_size(); }

WRSQL_API auto IDSet::capacity() const -> size_type
        { return body_->storage_.capacity(); }

WRSQL_API auto IDSet::cbegin() const -> const_iterator
        { return body_->storage_.cbegin(); }

WRSQL_API auto IDSet::cend() const -> const_iterator
        { return body_->storage_.cend(); }

WRSQL_API auto IDSet::crbegin() const -> const_reverse_iterator
        { return body_->storage_.crbegin(); }

WRSQL_API auto IDSet::crend() const -> const_reverse_iterator
        { return body_->storage_.crend(); }

WRSQL_API auto IDSet::clear() -> this_t &
        { body_->storage_.clear(); return *this; }

WRSQL_API auto IDSet::reserve(size_t n) -> this_t &
        { body_->storage_.reserve(n); return *this; }

WRSQL_API auto IDSet::shrink_to_fit() -> this_t &
        { body_->storage_.shrink_to_fit(); return *this; }

WRSQL_API bool operator==(const IDSet &a, const IDSet &b)
        { return (&a == &b) || (a.body_->storage_ == b.body_->storage_); }

WRSQL_API bool operator!=(const IDSet &a, const IDSet &b)
        { return (&a != &b) && (a.body_->storage_ != b.body_->storage_); }

WRSQL_API bool operator<(const IDSet &a, const IDSet &b)
        { return (&a != &b) && (a.body_->storage_ < b.body_->storage_); }

WRSQL_API bool operator<=(const IDSet &a, const IDSet &b)
        { return (&a == &b) || (a.body_->storage_ <= b.body_->storage_); }

WRSQL_API bool operator>(const IDSet &a, const IDSet &b)
        { return (&a != &b) && (a.body_->storage_ > b.body_->storage_); }

WRSQL_API bool operator>=(const IDSet &a, const IDSet &b)
        { return (&a == &b) || (a.body_->storage_ >= b.body_->storage_); }

//--------------------------------------

void
IDSet::checkAttached(
        const char *context
) const
{
        if (!body_->db_) {
                throw std::runtime_error(
                        printStr("%s: IDSet %p not attached to any database",
                                 context, this));
        }
}

//--------------------------------------

struct IDSet::SQLInterface::Cursor :
        public sqlite3_vtab_cursor
{
        IDSet::Body  *set_body;  ///< body of target IDSet
        size_t        pos;       ///< index of current position
        optional<ID>  id;        /**< if null, cursor is either yet to be
                                      positioned by filter() or is at the end
                                      of the result set */
        bool sync();
        void next();
};

//--------------------------------------

IDSet::SQLInterface::SQLInterface()
{
        iVersion = 1;
        xCreate = &attach;
        xConnect = &attach;
        xBestIndex = &getBestIndex;
        xDisconnect = &detach;
        xDestroy = &detach;
        xOpen = &openCursor;
        xClose = &closeCursor;
        xFilter = &filter;
        xNext = &next;
        xEof = &isEOF;
        xColumn = &getColumnValue;
        xRowid = &getRowID;
        xUpdate = &update;
        xFindFunction = nullptr;
        xBegin = nullptr;
        xSync = nullptr;
        xCommit = nullptr;
        xRollback = nullptr;
        xRename = &rename;
        xSavepoint = nullptr;
        xRelease = nullptr;
        xRollbackTo = nullptr;

        /*
         * quirk of SQLite API: sqlite3_auto_extension() specifies a function of
         * this type, but really expects it to be:
         *     int (sqlite3 *, const char **,
         *          const struct sqlite3_api_routines *)
         */
        union {
                int (*a)(sqlite3 *, const char **,
                         const struct sqlite3_api_routines *);

                void (*b)();
        };

        a = &registerWithSession;
        sqlite3_auto_extension(b);
}

//--------------------------------------

int
IDSet::SQLInterface::registerWithSession(
        sqlite3                            *db,
        const char                        **out_err_msg,
        const struct sqlite3_api_routines  * /* thunk */
)
{
        int status = sqlite3_create_module_v2(
                db, "sdig_idset", &idset_sql_iface, &idset_sql_iface, nullptr);

        if (status != SQLITE_OK) {
                *out_err_msg = sqlite3_mprintf("%s", sqlite3_errmsg(db));
        }

        return status;
}

//--------------------------------------

int
IDSet::SQLInterface::attach(
        sqlite3             *db,
        void                *aux,
        int                  argc,
        const char * const  *argv,
        sqlite3_vtab       **vtab,
        char               **error
)
{
        if (argc < 4) {
                *error = sqlite3_mprintf("IDSet::SQLInterface::attach(): "
                                         "missing IDSet object pointer");
                return SQLITE_ERROR;
        }

        union
        {
                uintptr_t  u;
                IDSet     *set;
        } args;

        args.u = static_cast<uintptr_t>(strtoull(argv[3], nullptr, 0));

        if (!args.u) {
                *error = sqlite3_mprintf("IDSet::SQLInterface::attach(): "
                                         "null IDSet object pointer");
                return SQLITE_ERROR;
        }

        sqlite3_declare_vtab(
                db, "CREATE TABLE idset (id INTEGER PRIMARY KEY);");
                        // table name ignored by SQLite library in this case

        sqlite3_vtab_config(db, SQLITE_VTAB_CONSTRAINT_SUPPORT, true);
                                        // allow conflict handling to work
        *vtab = args.set->body_;
        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::detach(
        sqlite3_vtab *vtab
)
{
        auto &body = static_cast<Body &>(*vtab);
        body.db_ = nullptr;
        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::getBestIndex(
        sqlite3_vtab       *vtab,
        sqlite3_index_info *iinfo
)
{
        int arg_no = 0;

        iinfo->idxNum = 0;

        if (iinfo->nConstraint) {
                iinfo->idxStr
                      = static_cast<char *>(sqlite3_malloc(iinfo->nConstraint));

                if (!iinfo->idxStr) {
                        return SQLITE_NOMEM;
                }

                iinfo->needToFreeIdxStr = true;
        }

        for (int i = 0; i < iinfo->nConstraint; ++i) {
                const auto &constraint = iinfo->aConstraint[i];
                auto       &usage      = iinfo->aConstraintUsage[i];

                if (!constraint.usable) {
                        usage.argvIndex = 0;
                                // 1-based index, 0 = not used
                        usage.omit = true;
                        continue;
                }

                if ((constraint.iColumn != 0) && (constraint.iColumn != -1)) {
                        return SQLITE_ERROR;
                }

                switch (constraint.op) {
                case SQLITE_INDEX_CONSTRAINT_EQ:
                case SQLITE_INDEX_CONSTRAINT_GT:
                case SQLITE_INDEX_CONSTRAINT_LE:
                case SQLITE_INDEX_CONSTRAINT_LT:
                case SQLITE_INDEX_CONSTRAINT_GE:
                        iinfo->idxStr[arg_no]
                                = static_cast<char>(constraint.op);
                        usage.argvIndex = ++arg_no;
                        usage.omit = false;
                        break;
                default:
                        usage.argvIndex = 0;
                        usage.omit = true;
                        continue;
                }
        }

        iinfo->orderByConsumed = true;  // unless proven otherwise

        for (int i = 0; i < iinfo->nOrderBy; ++i) {
                const auto &order_by = iinfo->aOrderBy[i];

                if ((order_by.iColumn != 0) && (order_by.iColumn != -1)) {
                        return SQLITE_ERROR;
                }
                if (order_by.desc) {
                        iinfo->orderByConsumed = false;
                        break;
                }
        }

        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::openCursor(
        sqlite3_vtab         *vtab,
        sqlite3_vtab_cursor **vcursor
)
{
        auto *cursor = new Cursor;
        cursor->set_body = static_cast<Body *>(vtab);
        *vcursor = cursor;
        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::closeCursor(
        sqlite3_vtab_cursor *vcursor
)
{
        delete static_cast<Cursor *>(vcursor);
        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::filter(
        sqlite3_vtab_cursor *vcursor,
        int                  /* idx_num (always 0) */,
        const char          *idx_str,
        int                  argc,
        sqlite3_value      **argv
)
{
        auto &cursor = static_cast<Cursor &>(*vcursor);

        if (cursor.set_body->storage_.empty()) {
                return SQLITE_OK;
        }
        
        cursor.pos = 0;
        cursor.id = *cursor.set_body->storage_.begin();
        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::next(
        sqlite3_vtab_cursor *vcursor
)
{
        static_cast<Cursor &>(*vcursor).next();
        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::isEOF(
        sqlite3_vtab_cursor *vcursor
)
{
        return !static_cast<Cursor &>(*vcursor).id.has_value();
}

//--------------------------------------

int
IDSet::SQLInterface::getColumnValue(
        sqlite3_vtab_cursor *vcursor,
        sqlite3_context     *ctx,
        int                  col_idx
)
{
        if (col_idx > 0) {
                return SQLITE_RANGE;
        }

        auto &cursor = static_cast<Cursor &>(*vcursor);

        if (cursor.sync()) {
                sqlite3_result_int64(ctx, cursor.id.value());
                return SQLITE_OK;
        } else {
                return SQLITE_DONE;
        }
}

//--------------------------------------

int
IDSet::SQLInterface::getRowID(
        sqlite3_vtab_cursor *vcursor,
        sqlite_int64        *rowid
)
{
        auto &cursor = static_cast<Cursor &>(*vcursor);

        if (cursor.sync()) {
                *rowid = cursor.id.value();
                return SQLITE_OK;
        } else {
                return SQLITE_DONE;
        }
}

//--------------------------------------

int
IDSet::SQLInterface::update(
        sqlite3_vtab   *vtab,
        int             argc,
        sqlite3_value **argv,
        sqlite_int64   *out_rowid
)
{
        auto *set_body = static_cast<Body *>(vtab);
        int   conflict_action
                        = sqlite3_vtab_on_conflict(set_body->db_->body_->db());

        optional<ID> rowid;
        
        if (sqlite3_value_type(argv[0]) != SQLITE_NULL) {
                rowid = sqlite3_value_int64(argv[0]);
        }

        if (argc == 1) {  // DELETE
                set_body->erase(rowid.value());
        } else if (argc < 1) {
                ;  // unexpected; treat as no-op
        } else if (!rowid.has_value()) {  // INSERT
                optional<ID> insert_rowid;

                if (sqlite3_value_type(argv[1]) != SQLITE_NULL) {
                        insert_rowid = sqlite3_value_int64(argv[1]);
                }

                if (sqlite3_value_type(argv[2]) == SQLITE_NULL) {
                        if (conflict_action != SQLITE_IGNORE) {
                                if (vtab->zErrMsg) {
                                        sqlite3_free(vtab->zErrMsg);
                                }

                                vtab->zErrMsg = sqlite3_mprintf(
                                    "illegal INSERT INTO idset_%p with id=NULL",
                                    vtab);
                        }
                        return SQLITE_CONSTRAINT_NOTNULL;
                }

                ID id = sqlite3_value_int64(argv[2]);

                if (!insert_rowid.has_value() || (insert_rowid == id)) {
                        if (!set_body->insert(id).second) {
                                switch (conflict_action) {
                                case SQLITE_REPLACE:
                                        *out_rowid = id;
                                        break;
                                default:
                                        if (vtab->zErrMsg) {
                                                sqlite3_free(vtab->zErrMsg);
                                        }
                                        vtab->zErrMsg = sqlite3_mprintf(
                                              "illegal INSERT INTO idset_%p: "
                                                "ID %lld not unique",
                                              vtab, static_cast<long long>(id));
                                        // fall through
                                case SQLITE_IGNORE:
                                        return SQLITE_CONSTRAINT_UNIQUE;
                                }
                        }
                } else {
                        // illegal INSERT at explicit rowid where rowid != id
                        if (vtab->zErrMsg) {
                                sqlite3_free(vtab->zErrMsg);
                        }
                        vtab->zErrMsg = sqlite3_mprintf("illegal INSERT INTO "
                                  "idset_%p with rowid=%lld, id=%lld: rowid "
                                  "cannot differ from id", vtab,
                                static_cast<long long>(insert_rowid.value()),
                                static_cast<long long>(id));

                        return SQLITE_CONSTRAINT_VTAB;
                }
        } else if (sqlite3_value_int64(argv[1]) == *rowid) {
                // UPDATE without explicit rowid change
                if (argc < 3) {
                        return SQLITE_OK;
                }
                if (sqlite3_value_type(argv[2]) == SQLITE_NULL) {
                        if (conflict_action != SQLITE_IGNORE) {
                                if (vtab->zErrMsg) {
                                        sqlite3_free(vtab->zErrMsg);
                                }
                                vtab->zErrMsg = sqlite3_mprintf(
                                        "illegal UPDATE idset_%p with id=NULL "
                                          "where rowid=%lld", vtab,
                                        static_cast<long long>(rowid.value()));
                        }
                        return SQLITE_CONSTRAINT_NOTNULL;
                }

                ID id = sqlite3_value_int64(argv[2]);

                if (id == rowid) {
                        return SQLITE_OK;
                }

                if (set_body->count(id)) switch (conflict_action) {
                case SQLITE_REPLACE:
                        set_body->erase(rowid.value());
                        return SQLITE_OK;
                default:
                        if (vtab->zErrMsg) {
                                sqlite3_free(vtab->zErrMsg);
                        }
                        vtab->zErrMsg = sqlite3_mprintf(
                                "illegal UPDATE idset_%p on rowid=%lld:"
                                  " ID %lld not unique", vtab,
                                static_cast<long long>(rowid.value()),
                                static_cast<long long>(id));
                        // fall through
                case SQLITE_IGNORE:
                        return SQLITE_CONSTRAINT_UNIQUE;
                }

                set_body->erase(rowid.value());
                set_body->insert(id);
        } else {
                if (conflict_action != SQLITE_IGNORE) {
                        if (vtab->zErrMsg) {
                                sqlite3_free(vtab->zErrMsg);
                        }

                        long long update_rowid = sqlite3_value_int64(argv[1]);

                        vtab->zErrMsg = sqlite3_mprintf("illegal UPDATE "
                             "idset_%p attempting to modify rowid %lld to %lld",
                             vtab, static_cast<long long>(rowid.value()),
                             update_rowid);
                }
                return SQLITE_CONSTRAINT_VTAB;
        }

        return SQLITE_OK;
}

//--------------------------------------

int
IDSet::SQLInterface::rename(
        sqlite3_vtab *vtab,
        const char   *new_name
)
{
        char orig_name[8 + (sizeof(vtab) * 2)];

        print(orig_name, "idset_%p", vtab);

        /* renaming only allowed internally via IDSet move constructor /
           assignment operator */
        if (strcmp(new_name, orig_name)) {
                if (vtab->zErrMsg) {
                        sqlite3_free(vtab->zErrMsg);
                }
                vtab->zErrMsg = sqlite3_mprintf(
                        "illegal rename of %s to \"%s\"", orig_name, new_name);

                return SQLITE_MISUSE;
        }

        return SQLITE_OK;
}

//--------------------------------------

bool
IDSet::SQLInterface::Cursor::sync()
{
        if (!id.has_value()) {
                return false;
        }

        if (pos < set_body->storage_.size()) {
                if (id == set_body->storage_[pos]) {
                        return true;  // no changes to set under cursor
                }
        }

        // set has been changed so cursor no longer points where it thinks
        auto begin = set_body->storage_.begin(),
             end   = set_body->storage_.end(),
             i     = std::lower_bound(begin, end, id.value());

        if (i == end) {
                id = {};
        } else if ((id == *i) && (++i == end)) {
                id = {};
        } else {
                id = *i, pos = i - begin;
        }

        return id.has_value();
}

//--------------------------------------

void
IDSet::SQLInterface::Cursor::next()
{
        if (!id.has_value()) {
                return;
        }

        ID orig_id = id.value();

        if (sync() && (id == orig_id)) {
                if (++pos < set_body->storage_.size()) {
                        id = set_body->storage_[pos];
                } else {
                        id = {};  // no more values
                }
        }
}

} // namespace sql
} // namespace wr

//--------------------------------------

WRSQL_API void
wr::fmt::TypeHandler<wr::sql::IDSet>::set(
        Arg                  &arg,
        const wr::sql::IDSet &val
)
{
        arg.type = Arg::OTHER_T;
        arg.other = &val;
        arg.fmt_fn = &TypeHandler<wr::sql::IDSet>::format;
}

//--------------------------------------

WRSQL_API bool
wr::fmt::TypeHandler<wr::sql::IDSet>::format(
        const Params &parms
)
{
        if (parms.conv != 's') {
                errno = EINVAL;
                return false;
        }

        std::string buf = static_cast<const wr::sql::IDSet *>(
                                                parms.arg->other)->sql_name();
        Arg arg2;
        arg2.type = Arg::STR_T;
        arg2.s = { buf.data(), buf.size() };

        return parms.target.format(parms, &arg2);
}
