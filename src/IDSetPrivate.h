/**
 * \file IDSetPrivate.h
 *
 * \brief Internal declarations relating to class wr::sql::IDSet
 *
 * \warning The declarations within this file are not part of the wrSQL API
 *      and are only intended for use by the wrSQL library source code itself
 *      (e.g. unit tests). These declarations are subject to change without
 *      notice.
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
#ifndef WRSQL_ID_SET_PRIVATE_H
#define WRSQL_ID_SET_PRIVATE_H


namespace wr {
namespace sql {


class IDSet::SQLInterface :
        private sqlite3_module
{
public:
        using this_t = SQLInterface;

        SQLInterface();

private:
        friend class IDSetTests;
        struct Cursor;

        static int registerWithSession(sqlite3 *db, const char **out_err_msg,
                                      const struct sqlite3_api_routines *thunk);

        static int attach(sqlite3 *db, void *aux, int argc,
                          const char * const *argv, sqlite3_vtab **vtab,
                          char **error);

        static int detach(sqlite3_vtab *vtab);
        static int getBestIndex(sqlite3_vtab *vtab, sqlite3_index_info *iinfo);

        static int openCursor(sqlite3_vtab *vtab,
                              sqlite3_vtab_cursor **vcursor);

        static int closeCursor(sqlite3_vtab_cursor *vcursor);

        static int filter(sqlite3_vtab_cursor *vcursor, int idx_num,
                          const char *idx_str, int argc, sqlite3_value **argv);

        static int next(sqlite3_vtab_cursor *vcursor);
        static int isEOF(sqlite3_vtab_cursor *vcursor);

        static int getColumnValue(sqlite3_vtab_cursor *vcursor,
                                  sqlite3_context *ctx, int col_idx);

        static int getRowID(sqlite3_vtab_cursor *vcursor, sqlite_int64 *rowid);

        static int update(sqlite3_vtab *vtab, int argc, sqlite3_value **argv,
                          sqlite_int64 *out_rowid);

        static int rename(sqlite3_vtab *vtab, const char *new_name);
};


extern IDSet::SQLInterface idset_sql_iface;

//--------------------------------------

struct IDSet::Body :
        public sqlite3_vtab
{
        using this_t = Body;

        // make these functions available to IDSet::SQLInterface
        std::pair<iterator, bool> insert(ID id);
        size_type erase(ID id);
        size_type count(ID id) const;

        storage_type   storage_;
        const Session *db_ = nullptr;
};


} // namespace sql
} // namespace wr


#endif // !WRSQL_ID_SET_PRIVATE_H
