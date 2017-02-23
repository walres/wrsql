/**
 * \file wrsql/Statement.h
 *
 * \brief Declaration of class \c wr::sql::Statement, class \c wr::sql::Row
 *      and statement registration functions
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
#ifndef WRSQL_STATEMENT_H
#define WRSQL_STATEMENT_H

#include <functional>
#include <utility>
#include <wrutil/optional.h>
#include <wrutil/tagged_ptr.h>
#include <wrsql/Config.h>


namespace wr {


class u8string_view;


namespace sql {


/// \brief row ID type
using ID = int64_t;

/// \brief generalised column value type
enum ValueType
{
        NULL_TYPE = 0,
        INT_TYPE,
        FLOAT_TYPE,
        TEXT_TYPE,
        BLOB_TYPE
};


class Session;
class Row;  // defined below


/**
 * \class wr::sql::Statement
 * \brief compiled SQL statement
 */
class WRSQL_API Statement
{
public:
        using this_t = Statement;

        ///@{
        /**
         * \brief object constructor
         *
         * The default constructor initialises a null (unprepared)
         * \c Statement object. Such objects must be prepared by a call to
         * \c prepare() before they can be used.
         *
         * Constructors which accept an \c sql parameter create a prepared
         * statement, compiling the SQL statement through an implicit call to
         * \c prepare().
         *
         * \param other
         *      \c Statement object to be copied or transferred
         * \param [in] session
         *      an open database connection
         * \param [in] sql
         *      UTF-8-encoded SQL statement text for compilation
         * \param [out] tail
         *      if \c sql contains multiple statements then the text of the
         *      second statement onwards is output here
         *
         * \throw wr::sql::Error
         *      A syntactic or semantic error was found in \c sql
         */
        Statement();
        Statement(const this_t &other);
        Statement(this_t &&other);
        Statement(const Session &session, const u8string_view &sql);
        Statement(const Session &session, const u8string_view &sql,
                  u8string_view &tail);
        ///@}

        /**
         * \brief object destructor
         *
         * Implicitly calls \c finalize().
         */
        ~Statement();

        ///@{
        /**
         * \brief assignment operator
         *
         * This function has no effect if <code>(&other == this)</code>.
         *
         * \param other  \c Statement object to be copied or transferred
         * \return reference to \c *this
         */
        this_t &operator=(const this_t &other);
        this_t &operator=(this_t &&other);
        ///@}

        ///@{
        /**
         * \brief compile SQL statement
         *
         * Compiles the SQL statement given by \c sql for the open database
         * connection, \c session, ready for parameter binding using the
         * \c bind() family of methods and execution using the \c begin()
         * method.
         *
         * \param [in] session
         *      an open database connection
         * \param [in] sql
         *      UTF-8-encoded SQL statement text for compilation
         * \param [out] tail
         *      if \c sql contains multiple statements then the text of the
         *      second statement onwards is output here
         *
         * \return reference to \c *this
         *
         * \throw wr::sql::Error
         *      A syntactic or semantic error was found in \c sql
         */
        this_t &prepare(const Session &session, const u8string_view &sql);
        this_t &prepare(const Session &session, const u8string_view &sql,
                        u8string_view &tail);
        ///@}

        /**
         * \brief dispose of a compiled \c Statement
         *
         * Releases a compiled \c Statement object's resources and returns
         * the object to a null (unprepared) state. If the object is already
         * unprepared then this method has no effect.
         */
        void finalize();

        ///@{
        /**
         * \brief get status information
         * \return
         *      \c isPrepared() and <code>operator bool()</code> return
         *      \c true if the \c Statement object is a compiled (prepared)
         *      SQL statement or \c false otherwise
         * \return
         *      \c isActive() returns \c true if the \c Statement object is
         *      being executed and not all rows have yet been fetched or
         *      \c false otherwise
         * \return
         *      \c isFinalized() returns \c true if the \c Statement object
         *      is \e not prepared or \c false otherwise
         * \return
         *      \c session() returns the database connection associated with a
         *      prepared \c Statement object or \c nullptr for an unprepared
         *      \c Statement object
         * \return
         *      \c sql() returns the original SQL text associated with a
         *      prepared \c Statement object or an empty string for an
         *      unprepared \c Statement object
         */
        bool isPrepared() const        { return stmt_ != nullptr; }
        bool isActive() const          { return stmt_ && (stmt_.tag() != 0); }
        bool isFinalized() const       { return !stmt_; }
        const Session *session() const { return session_; }
        explicit operator bool() const { return isPrepared(); }
        u8string_view sql() const;
        ///@}

        /**
         * \brief reset a prepared \c Statement object to an inactive state
         *
         * \c reset() places an active prepared \c Statement object back into
         * an inactive state, cancelling any execution in progress such that
         * \c current() will return a null row after this method returns.
         *
         * This method has no effect on an inactive or unprepared \c Statement
         * object.
         *
         * \note
         *      Bound parameter values are unaffected.
         *
         * \return reference to \c *this
         */
        this_t &reset();

        /**
         * \brief clear any bound parameter values
         * \return reference to \c *this
         */
        this_t &clearBindings();

        ///@{
        /**
         * \brief bind value to statement parameter
         *
         * Type conversions are performed automatically where necessary.
         * When \c bind() is invoked on an active statement, that statement
         * is implicitly reset.
         *
         * The following template overload of \c bind can be specialized for
         * user-defined types:
         *
         * <code>template \<typename T\> this_t \&bind(int param_no,
         *                                             const T &val);</code>
         *
         * For example:
         *
         * \verbatim
         * enum Fruit { APPLE, ORANGE, PEAR, STRAWBERRY };
         *
         * template <> auto
         * wr::sql::Statement::bind(int param_no,
         *                          const Fruit &val) -> this_t &
         * {
         *         return bind(param_no, static_cast<int>(val));
         * }
         * \endverbatim
         *
         * \param [in] param_no
         *      1-based index of parameter to bind
         * \param [in] val
         *      the value to be bound
         *
         * \return reference to \c *this
         *
         * \throw std::invalid_argument
         *      \c param_no referred to a nonexistent parameter number
         * \throw std::length_error
         *      size of \c val exceeds limits imposed by underlying database
         *      implementation
         * \throw std::bad_alloc
         *      memory allocation failed
         * \throw wr::sql::Error
         *      other run-time statement error occurred (dependent on
         *      underlying database implementation)
         */
        this_t &bindNull(int param_no);

        this_t &bind(int param_no, std::nullptr_t val)
                { (void) val; return bindNull(param_no); }

        this_t &bind(int param_no, char val);
        this_t &bind(int param_no, unsigned char val);
        this_t &bind(int param_no, short val);
        this_t &bind(int param_no, unsigned short val);
        this_t &bind(int param_no, int val);
        this_t &bind(int param_no, unsigned int val);
        this_t &bind(int param_no, long val);
        this_t &bind(int param_no, unsigned long val);
        this_t &bind(int param_no, long long val);
        this_t &bind(int param_no, unsigned long long val);
        this_t &bind(int param_no, float val);
        this_t &bind(int param_no, double val);
        this_t &bind(int param_no, const char *val);
        template <typename T> this_t &bind(int param_no,
                                           const optional<T> &val);

        // for specialization on external classes
        template <typename T> this_t &bind(int param_no, const T &val);
                /* specialized as standard for std::string,
                   wr::u8string_view, wr::string_view, wr::path
                   and std::string_view (when available) */

        template <size_t N> this_t &bind(int param_no, const char (&val)[N])
                { return bind(param_no, string_view(val, N)); }
        ///@}

        /// \brief destructor delegate type for releasing bound blob data
        using FreeBlobFn = std::function<void (void *)>;

        /**
         * \brief bind binary data to statement parameter
         *
         * \param [in] param_no
         *      1-based index of parameter to bind
         * \param [in] data
         *      pointer to start of data; if <code>(data == nullptr)</code>
         *      then a NULL value is bound as per \c bindNull(param_no)
         * \param [in] bytes
         *      data size
         * \param [in] free_blob
         *      optional destructor function to release memory allocated for
         *      \c data, invoked upon the next call to \c ~Statement(),
         *      \c finalize() or \c clearBindings(); if \c free_blob is not
         *      specified then no action is taken
         *
         * \return reference to \c *this
         *
         * \throw std::invalid_argument
         *      \c param_no referred to a nonexistent parameter number
         * \throw std::length_error
         *      data size exceeds limits imposed by underlying database
         *      implementation
         * \throw std::bad_alloc
         *      memory allocation failed
         * \throw wr::sql::Error
         *      \c data has already been bound with a non-default \c free_blob
         *      function, or another run-time statement error occurred
         *      (dependent on underlying database implementation)
         */
        this_t &bind(int param_no, const void *data, size_t bytes,
                     FreeBlobFn free_blob = {});

        ///@{
        /**
         * \brief bind values to multiple statement parameters
         *
         * \c bindAll() invokes \c bind() for each of the specified values
         * (\c args) to bind each parameter of the statement in order.
         * If fewer arguments are specified than there are parameters then
         * the remaining bound parameters are cleared to null values.
         *
         * \param [in] args...
         *      zero or more values to bind
         *
         * \return reference to \c *this
         *
         * \throw std::length_error
         *      size of a value exceeds limits imposed by underlying database
         *      implementation
         * \throw std::bad_alloc
         *      memory allocation failed
         * \throw wr::sql::Error
         *      other run-time statement error occurred (dependent on
         *      underlying database implementation)
         */
        template <typename ...Args> this_t &bindAll(Args &&...args)
               { return clearBindings().bind_(1, std::forward<Args>(args)...); }

        template <typename ...Args> this_t &bindAll()
                { return clearBindings(); }
        ///@}

        ///@{
        /**
         * \brief execute statement and fetch first result row
         *
         * As an option, arguments may be passed to \c begin() which are bound
         * to each statement parameter, in order, by an implicit call to
         * \c bindAll().
         *
         * Invoking \c begin() in the middle of fetching a result set causes
         * statement execution to be restarted, so the first result row will
         * be visited again.
         *
         * \param [in] bind_args...
         *      optional value(s) to bind to statement parameters
         *
         * \return
         *      \c Row object representing first row fetched (if any)
         *      or a null \c Row object if the statement returns no results
         *
         * \throw std::length_error
         *      size of a value specified by \c bind_args exceeded limits
         *      imposed by underlying database implementation
         * \throw std::bad_alloc
         *      memory allocation failed
         * \throw wr::sql::Error
         *      a miscellaneous error such as a constraint violation,
         *      unknown name or some other run-time statement execution error
         *      occurred (exact nature depends on underlying database
         *      implementation)
         * \throw wr::sql::Interrupt
         *      \c Session::interrupt() was invoked by a progress handler or
         *      another thread
         * \throw wr::sql::Busy
         *      a deadlock or excessive contention was detected between
         *      this and other connections concurrently accessing the database
         *      (handled automatically by \c Transaction::begin())
         */
        Row begin();
        template <typename ...Args> Row begin(Args &&...bind_args);
        ///@}

        /**
         * \brief get the most recently-fetched row
         * \return
         *      \c Row object representing current row or a null \c Row
         *      object if the \c Statement object is unprepared or inactive
         */
        Row currentRow();

        /**
         * \brief fetch next row
         *
         * If the last row has been fetched, the \c Statement object returns
         * to an inactive state and a null \c Row object is returned.
         *
         * \return
         *      \c Row object representing next row or a null \c Row object
         *      (as returned by \c end()) if the \c Statement object is
         *      unprepared, is inactive or is currently at the last result row
         *
         * \throw wr::sql::Error
         *      a run-time statement execution error occurred (exact nature
         *      depends on underlying database implementation)
         * \throw wr::sql::Interrupt
         *      \c Session::interrupt() was invoked by a progress handler or
         *      another thread for the same database connection
         * \throw wr::sql::Busy
         *      a deadlock or excessive contention was detected between
         *      this and other connections concurrently accessing the database
         *      (handled automatically by \c Transaction::begin())
         */
        Row next();

        /**
         * \brief get \c Row object representing the one-past-last row
         * \return null \c Row object
         */
        Row end();

private:
        friend Row;

        template <typename Arg1> this_t &bind_(int n, Arg1 &&arg);

        template <typename Arg1, typename ...ArgN>
        this_t &bind_(int n, Arg1 &&first, ArgN &&...rest);

        void throwBindError(int param_no, int status) const;

        tagged_ptr<void, 1>  stmt_;
        const Session       *session_;
};

//--------------------------------------
/**
 * \class wr::sql::Row
 * \brief a row of a result set
 */
class WRSQL_API Row
{
public:
        using this_t = Row;

        ///@{
        /**
         * \brief object constructor
         *
         * \param [in] stmt
         *      \c Statement object to fetch values from
         * \param [in] other
         *      \c Row object to be copied
         */
        Row();
        Row(Statement &stmt);
        Row(const this_t &other) = default;
        ///@}

        /**
         * \brief assignment operator
         *
         * \param [in] other
         *      \c Row object to be copied
         */
        this_t &operator=(const this_t &other) = default;

        ///@{
        /**
         * \brief range-based \c for support
         *
         * Provide support for use of range-based \c for statements to extract
         * data from \c Statement and \c Row objects
         *
         * \return
         *      <code>operator*()</code> returns a reference to \c *this
         * \return
         *      <code>operator->()</code> returns \c this
         * \return
         *      <code>operator++()</code> fetches the next row (by invoking
         *      \c next()) and returns \c *this
         *
         * \throw wr::sql::Error      (<code>operator++()</code> only)
         *      a run-time statement execution error occurred (exact nature
         *      depends on underlying database implementation)
         * \throw wr::sql::Interrupt  (<code>operator++()</code> only)
         *      \c Session::interrupt() was invoked by a progress handler or
         *      another thread for the same database connection
         * \throw wr::sql::Busy       (<code>operator++()</code> only)
         *      a deadlock or excessive contention was detected between
         *      this and other connections concurrently accessing the database
         *      (handled automatically by \c Transaction::begin())
         */
        this_t &operator*()  { return *this; }
        this_t *operator->() { return this; }
        this_t &operator++() { next(); return *this; }
                // postfix operator++(int) not possible
        ///@}

        /**
         * \brief fetch next row
         *
         * \return
         *      \c true if another row was available or \c false otherwise
         *
         * \throw wr::sql::Error
         *      a run-time statement execution error occurred (exact nature
         *      depends on underlying database implementation)
         * \throw wr::sql::Interrupt
         *      \c Session::interrupt() was invoked by a progress handler or
         *      another thread for the same database connection
         * \throw wr::sql::Busy
         *      a deadlock or excessive contention was detected between
         *      this and other connections concurrently accessing the database
         *      (handled automatically by \c Transaction::begin())
         */
        bool next();

        ///@{
        /**
         * \brief get general state information
         * \return
         *      \c statement() returns a pointer to the associated
         *      \c Statement object or \c nullptr for a default-constructed
         *      \c Row object
         * \return
         *      \c empty() returns \c true if the \c Row object is not
         *      associated with an active \c Statement object or \c false
         *      otherwise
         * \return
         *      <code>operator bool()</code> returns \c true if the \c Row
         *      object is associated with an active \c Statement object or
         *      \c false otherwise
         */
        const Statement *statement() const { return stmt_; }
        bool empty() const { return !stmt_ || !stmt_->isActive(); }
        explicit operator bool() const { return !empty(); }
        ///@}

        ///@{
        /**
         * \brief retrieve column value
         *
         * \c get() and \c getNullable() retrieve a single column's value
         * from a result row as a given type, performing conversion where
         * necessary. \c getNullable() returns the result as a \c wr::optional
         * object instantiated with the target type and should be used for
         * columns whose value may be <code>NULL</code>; if the result column
         * contains a \c NULL value then the returned object will not contain
         * a value.
         *
         * The two-argument \c get() methods provide an alternative form
         * where the retrieved column value is returned through the second
         * parameter \c value_out. These methods simply delegate to the
         * corresponding single-argument \c get() but allow template argument
         * deduction by the compiler so that the caller can omit template
         * arguments.
         *
         * The single-argument \c get(int) is specialized for the following
         * types as standard:
         *      * \c bool
         *      * \c char
         *      * <code>unsigned char</code>
         *      * \c char16_t
         *      * \c char32_t
         *      * \c wchar_t
         *      * \c short
         *      * <code>unsigned short</code>
         *      * \c int
         *      * <code>unsigned int</code>
         *      * \c long
         *      * <code>unsigned long</code>
         *      * <code>long long</code>
         *      * <code>unsigned long long</code>
         *      * \c float
         *      * \c double
         *      * <code>const char *</code>
         *      * \c std::string
         *      * \c wr::string_view
         *      * \c wr::u8string_view
         *      * <code>const void *</code>
         *      * \c wr::path
         *
         * \c get(int) may also be specialized for any type not listed above
         * allowing its use with any other overload of \c get() as well as
         * \c getNullable().
         *
         * \param [in] col_no
         *      zero-based column number
         * \param [in] col_name
         *      column name
         * \param [out] value_out
         *      the retrieved column value
         *
         * \return
         *      the single-argument \c get() and \c getNullable() methods
         *      return the retrieved column value
         * \return
         *      the two-argument \c get() methods return a reference to
         *      \c *this; the retrieved column value is returned via the
         *      parameter \c value_out
         */
        template <typename T> T get(int col_no) const;
        template <typename T> T get(const u8string_view &col_name) const
                { return get<T>(colNo_throw(col_name)); }

        template <typename T> optional<T> getNullable(int col_no) const
                { return isNull(col_no) ? optional<T>()
                                        : optional<T>(get<T>(col_no)); }

        template <typename T>
        optional<T> getNullable(const u8string_view &col_name) const
                { return getNullable<T>(colNo_throw(col_name)); }

        template <typename T> const this_t &get(int col_no, T &value_out) const
                { value_out = get<T>(col_no); return *this; }

        template <typename T> const this_t &get(int col_no,
                                                optional<T> &value_out) const
                { value_out = getNullable<T>(col_no); return *this; }

        template <typename T> const this_t &get(const u8string_view &col_name,
                                                T &value_out) const
                { value_out = get<T>(col_name); return *this; }

        template <typename T> const this_t &get(const u8string_view &col_name,
                                                optional<T> &value_out) const
                { value_out = getNullable<T>(col_name); return *this; }
        ///@}

        ///@{
        /**
         * \brief get column information
         *
         * \param [in] col_no
         *      zero-based column number
         * \param [in] col_name
         *      UTF-8-encoded column name
         *
         * \return
         *      \c numCols() returns the number of columns available
         * \return
         *      \c isNull() returns \c true if the specified column contains
         *      a NULL value or \c false otherwise
         * \return
         *      \c colSize() returns the size of the specified column's data
         *      in bytes
         * \return
         *      \c colName() returns the UTF-8-encoded name of the specified
         *      column given its zero-based index or an empty string if not
         *      available
         * \return
         *      \c colType() returns a \c ValueType constant representing the
         *      type category of the specified column's value
         * \return
         *      \c colNo() and \c colNo_throw() return the zero-based index of
         *      the specified column given its name; \c colNo() returns -1 if
         *      \c col_name did not match any column's name
         *
         * \throw std::invalid_argument
         *      an unknown column name was passed to \c colNo_throw()
         */
        int numCols() const;
        bool isNull(int col_no) const;
        int colSize(int col_no) const;
        u8string_view colName(int col_no) const;
        ValueType colType(int col_no) const;
        int colNo(const u8string_view &col_name) const;
        int colNo_throw(const u8string_view &col_name) const;
        ///@}

        ///@{
        /**
         * \brief comparison operator
         *
         * \param [in] other  \c Row object to compare with
         *
         * \return
         *      \c operator==() returns \c true if \c *this and \c other are
         *      both associated with the same \c Statement object or are
         *      both associated with either an inactive or no \c Statement
         *      object
         * \return
         *      \c operator!=() returns \c true if \c *this and \c other are
         *      associated with different \c Statement objects and at least
         *      one of those objects is active
         */
        bool operator==(const this_t &other) const
                { return (stmt_ == other.stmt_)
                        || (!stmt_ && !other.stmt_->isActive())
                        || (!stmt_->isActive() && !other.stmt_); }

        bool operator!=(const this_t &other) const
                { return !operator==(other); }
        ///@}

private:
        Statement *stmt_;
};

//--------------------------------------
/**
 * \brief register an SQL statement for precompilation
 *
 * \c registerStatement() helps facilitate convenient pre-compilation of
 * repetitively-executed SQL statements for each database connection on which
 * they are used, to improve performance.
 *
 * When an application invokes \c registerStatement() with the text of an SQL
 * statement, an integer uniquely identifying the statement is returned to the
 * application. This integer can then be passed to the \c exec() method of an
 * open \c Session object which compiles the statement the first time it is
 * used with that \c Session object, producing a \c Statement object that is
 * reused on all subsequent calls to \c Session::exec() for the same
 * connection and statement. The precompiled \c Statement object itself can be
 * retrieved using \c Session::statement().
 *
 * \note
 *      \c sql is not compiled by this function and as such is not checked
 *      for correctness; any syntactic or semantic errors will only be
 *      detected and reported upon a call to \c Session::statement() or
 *      \c Session::exec()
 *
 * If the SQL statement text was already registered then the same value is
 *      returned again.
 *
 * This function is thread-safe.
 *
 * \param [in] sql
 *      UTF-8-encoded SQL statement text
 *
 * \return an integer uniquely identifying the statement
 *
 * \see \c Session::exec(), \c Session::statement(),
 *      \c numRegisteredStatements(), \c registeredStatement()
 */
WRSQL_API size_t registerStatement(const u8string_view &sql);

/**
 * \brief query the number of pre-registered SQL statements
 *
 * This function is thread-safe.
 *
 * \return the number of pre-registered SQL statements
 */
WRSQL_API size_t numRegisteredStatements();

/**
 * \brief query the contents of a pre-registered statement
 *
 * \c registeredStatement() is used to retrieve the original text for an SQL
 * statement registered by a prior call to \c wr::sql::registerStatement().
 *
 * This function is thread-safe.
 *
 * \param [in] id  identifier returned by prior call to \c registerStatement()
 *
 * \return the SQL text of the SQL statement with the registered \c id
 *
 * \throw std::invalid_argument
 *      \c id was not recognised
 */
WRSQL_API u8string_view registeredStatement(size_t id);

//--------------------------------------

template <typename T> inline auto
Statement::bind(
        int                param_no,
        const optional<T> &val
) -> this_t &
{
        if (val.has_value()) {
                bind(param_no, val.value());
        } else {
                bindNull(param_no);
        }

        return *this;
}

//--------------------------------------

template <typename Arg1> inline auto
Statement::bind_(
        int    n,
        Arg1 &&arg
) -> this_t &
{
        return bind(n, std::forward<Arg1>(arg));
}

//--------------------------------------

template <typename Arg1, typename ...ArgN> inline auto
Statement::bind_(
        int    n,
        Arg1 &&first,
        ArgN &&...rest
) -> this_t &
{
        bind(n, std::forward<Arg1>(first));
        return bind_(n + 1, std::forward<ArgN>(rest)...);
}

//--------------------------------------

template <typename ...Args> inline auto
Statement::begin(
        Args &&...bind_args
) -> Row
{
        return std::move(bindAll(std::forward<Args>(bind_args)...).begin());
}


} // namespace sql
} // namespace wr


#endif // !WRSQL_STATEMENT_H
