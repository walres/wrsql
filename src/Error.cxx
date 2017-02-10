/**
 * \file Error.cxx
 *
 * \brief Method implementations for wr::sql::SQLException, wr::sql::Error,
 *      wr::sql::Interrupt and wr::sql::Busy
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
#include <iostream>

#include <wrutil/codecvt.h>
#include <wrutil/Format.h>
#include <wrsql/Error.h>
#include <wrsql/Session.h>

#include "sqlite3api.h"


namespace wr {
namespace sql {


WRSQL_API
SQLException::SQLException(
        const string_view &what
) :
        base_t(what)
{
}

//--------------------------------------

WRSQL_API
SQLException::SQLException(
        const string_view   &what,
        const u8string_view &sql
) :
        this_t(printStr(sql.empty() ? "%s" : "%s [SQL: %s]",
                        what, utf8_narrow_cvt().from_utf8(sql)))
{
}

//--------------------------------------

WRSQL_API
SQLException::SQLException(
        const Session *session,
        int            status
) :
        this_t(utf8_narrow_cvt().from_utf8(Session::message(session, status)))
{
}

//--------------------------------------

WRSQL_API
SQLException::SQLException(
        const Session       *session,
        int                  status,
        const u8string_view &sql
) :
        this_t(utf8_narrow_cvt().from_utf8(Session::message(session, status)),
               sql)
{
}

//--------------------------------------

WRSQL_API
SQLException::SQLException(
        const Statement &stmt,
        int              status
) :
        this_t(stmt.session(), status, stmt.sql())
{
}

//--------------------------------------

WRSQL_API
SQLException::SQLException(
        const Statement     &stmt,
        int                  status,
        const u8string_view &sql
) :
        this_t(stmt.session(), status, sql)
{
}

//--------------------------------------

WRSQL_API
Interrupt::Interrupt() : base_t("Statement interrupted")
{
}

//--------------------------------------

WRSQL_API
Busy::Busy() : base_t("Cannot obtain write lock due to existing read locks")
{
}


} // namespace sql
} // namespace wr
