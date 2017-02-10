/**
 * \file wrsql/Error.h
 *
 * \brief Declaration of exception classes thrown by the \c wrSQL library
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
#ifndef WRSQL_ERROR_H
#define WRSQL_ERROR_H

#include <stdexcept>
#include <wrutil/string_view.h>
#include <wrutil/u8string_view.h>
#include <wrsql/Config.h>


namespace wr {
namespace sql {


class Session;
class Statement;


/**
 * \class wr::sql::SQLException
 * \brief base class for all \c wrSQL library exception objects
 * \see \c wr::sql::Error, \c wr::sql::Interrupt, \c wr::sql::Busy
 */
class WRSQL_API SQLException : public std::runtime_error
{
public:
        using this_t = SQLException;
        using base_t = std::runtime_error;

        ///@{
        /**
         * \brief constructor
         *
         * \param [in] what
         *      locally-encoded descriptive text
         * \param [in] sql
         *      UTF-8-encoded SQL statement
         * \param [in] session
         *      an open database connection object
         * \param [in] stmt
         *      a prepared statement object
         * \param [in] status
         *      error or status code specific to the underlying database
         *      implementation
         */
        SQLException(const string_view &what);
        SQLException(const string_view &what, const u8string_view &sql);
        SQLException(const Session *session, int status);
        SQLException(const Session *session, int status,
                     const u8string_view &sql);
        SQLException(const Statement &stmt, int status);
        SQLException(const Statement &stmt, int status,
                     const u8string_view &sql);
        ///@}
};

//--------------------------------------
/**
 * \class wr::sql::Error
 * \brief exception thrown upon discovery of a syntatic, semantic or run-time
 *      error while preparing or executing an SQL statement
 *
 * Constructors are inherited from class \c wr::sql::SQLException.
 */
class WRSQL_API Error : public SQLException
{
public:
        using this_t = Error;
        using base_t = SQLException;
        using base_t::base_t;
};

//--------------------------------------
/**
 * \class wr::sql::Interrupt
 * \brief exception thrown upon an explicitly requested interruption of an
 *      executing SQL statement
 *
 * Constructors are inherited from class \c wr::sql::SQLException.
 */
class WRSQL_API Interrupt : public SQLException
{
public:
        using this_t = Interrupt;
        using base_t = SQLException;
        Interrupt();
};

//--------------------------------------
/**
 * \class wr::sql::Busy
 * \brief exception thrown when the underlying database detects deadlock or
 *      excessive contention during concurrent access
 *
 * Constructors are inherited from class \c wr::sql::SQLException.
 *
 * \c Busy exceptions are handled automatically by \c Transaction contexts.
 */
class WRSQL_API Busy : public SQLException
{
public:
        using this_t = Busy;
        using base_t = SQLException;
        Busy();
};


} // namespace sql
} // namespace wr


#endif // !WRSQL_ERROR_H
