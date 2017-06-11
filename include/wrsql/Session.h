/**
 * \file wrsql/Session.h
 *
 * \brief Declaration of class \c wr::sql::Session
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
#ifndef WRSQL_SESSION_H
#define WRSQL_SESSION_H

#include <functional>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <wrutil/u8string_view.h>
#include <wrsql/Config.h>
#include <wrsql/Statement.h>


namespace wr {
namespace sql {


class IDSet;
class Transaction;
class SessionTests;


/**
 * \class wr::sql::Session
 * \brief database connection
 *
 * An application may open any number of Session objects for a given database,
 * subject to available memory and any restrictions placed on the number of
 * simultaneous connection allowed by the underlying database implementation.
 * Accessing a given database via distinct Session objects is guaranteed to be
 * thread safe. For a given Session instance method calls are not thread safe.
 * To maintain thread safety it is recommended that each thread uses its own
 * dedicated Session object(s).
 */
class WRSQL_API Session : public boost::intrusive_ref_counter<Session>
{
public:
        using this_t = Session;
        using Ptr = boost::intrusive_ptr<this_t>;
        using ConstPtr = boost::intrusive_ptr<const this_t>;

        ///@{
        /**
         * \brief object constructor
         *
         * \param other  \c Session object to be copied or moved
         * \param uri    URI describing type and location of database to open
         */
        Session();
        Session(const this_t &other);
        Session(this_t &&other);
        Session(const u8string_view &uri);
        ///@}

        /**
         * \brief object destructor
         *
         * Closes the connection, if open, and frees associated resources.
         */
        ~Session();

        ///@{
        /**
         * \brief assignment operator
         *
         * Both the copy and move assignment operators close any connection
         * currently opened by the invoking \c Session object. The copy
         * assignment operator opens a new connection to the same database
         * URI referred to by \c other. The move assignment operator causes
         * the invoking \c Session object to take over the state of the
         * \c other \c Session object leaving \c other in a valid but
         * indeterminate state (\c other is assumed to be no longer required).
         *
         * Both operators have no effect if <code>(&other == this)</code>.
         *
         * \param other  \c Session object to be copied or moved
         *
         * \return reference to \c *this
         */
        this_t &operator=(const this_t &other);
        this_t &operator=(this_t &&other);
        ///@}

        /**
         * \brief open connection to a database
         *
         * If another database connection is open by the Session object upon
         * calling this function then it is closed first by an implicit call
         * to \c close(), regardless of the outcome.
         *
         * \param [in] uri
         *      URI describing the type and location of the target database
         *
         * \throw wr::sql::Error
         *      invalid \c uri, permission denied or memory exhausted
         */
        void open(const u8string_view &uri);

        /**
         * \brief close connection if open
         *
         * \throw wr::sql::Error
         *      queries still in progress on this connection
         */
        void close();

        /**
         * \brief compile and begin executing SQL statement
         *
         * This method is intended for convenient execution of
         * non-performance-critical statements, or statements whose text is
         * computed on an ad-hoc basis. The SQL is (re-)compiled on each call.
         *
         * Invoking this method is equivalent to:
         * <code>
         * // 'session', 'sql' and 'args' refer respectively to the pertinent
         * // Session object, SQL statement and bound arguments (if any)
         * wr::sql::Statement stmt(session, sql);
         * stmt.begin(sql, <i>args...</i>);
         * </code>
         * where \c stmt is the object returned by this method.
         *
         * For greater performance on statements to be executed repeatedly,
         * potentially across different threads via other open Session objects,
         * an SQL statement can be registered by a call to
         * \c wr::sql::registerStatement() then executed by calling the
         * overload \c exec(size_t). In this use case the statement is compiled
         * only once then retained by the invoking \c Session object for future
         * calls. The compiled \c Statement object can also be retrieved by a
         * call to \c statement() ).
         *
         * \param [in] sql
         *      the SQL statement
         * \param [in] ...args
         *      argument value(s) to be bound to parameters given in the
         *      statement, in order of appearance (optional depending on
         *      statement)
         *
         * \return the compiled \c Statement object being executed
         *
         * \throw wr::sql::Error
         *      the statement could not be compiled or executed due to a
         *      syntax error or a semantic error in \c sql
         * \throw wr::sql::Error
         *      a bound parameter value was invalid within the context
         *      of the statement
         * \throw wr::sql::Busy
         *      contention occurred with locks held by other database
         *      connections, or a potential deadlock was detected; handled
         *      automatically when called inside the context of a
         *      \c Transaction
         * \throw wr::sql::Interrupt
         *      a call to \c Session::interrupt() was issued by another thread
         *      (or possibly a progress handler invoked by the calling
         *      thread)
         *
         * \see wr::sql::Statement(), \c wr::sql::registerStatement()
         */
        template <typename ...Args> Statement exec(const u8string_view &sql,
                                                   Args &&...args) const;

        /**
         * `Statement` object wrapper returned by `Session::exec(size_t, ...)`
         * supporting use in C++11 range-based `for` and effecting automatic
         * reset of the underlying `Statement` upon exiting the scope of the
         * call to `Session::exec()`.
         */
        class WRSQL_API ExecResult
        {
        public:
                using this_t = ExecResult;

                ///@{
                /// \brief constructor
                ExecResult(Statement::Ptr stmt) : stmt_(stmt) {}

                ExecResult(const this_t &) = delete;
                ExecResult(this_t &&) = default;
                ///@}

                /// \brief destructor
                ~ExecResult()
                {
                        if (*this) {
                                stmt_->reset();
                        }
                }

                ///@{
                /// \brief assignment operator
                this_t &operator=(const this_t &) = delete;
                this_t &operator=(this_t &&) = default;
                ///@}

                ///@{
                /// \brief range-based `for` support
                Row begin() { return stmt_->currentRow(); }
                Row end()   { return stmt_->end(); }
                ///@}

                ///@{
                /// \brief provide access to underlying `Statement` object
                Statement *operator->() { return stmt_.get(); }
                Statement &operator*() { return *stmt_; }
                explicit operator Statement::Ptr() const { return stmt_; }
                ///@}

                /**
                 * \brief take control of the underlying `Statement` object
                 *
                 * Return a reference-counted pointer to the underlying
                 * `Statement` object, leaving `this` no longer referencing
                 * any `Statement` object.
                 *
                 * \return underlying `Statement` object
                 */
                Statement::Ptr release();

                /**
                 * \brief determine if `this` references a row
                 */
                explicit operator bool() const
                        { return stmt_ && stmt_->isActive(); }

        private:
                Statement::Ptr stmt_;
        };

        /**
         * \brief execute precompiled statement
         *
         * \param [in] stmt_id
         *      ID of statement returned by a prior call to
         *      \c wr::sql::registerStatement()
         * \param [in] ...args
         *      argument value(s) to be bound to parameters given in the
         *      registered statement, in order of appearance (optional
         *      depending on statement)
         *
         * \return executed `Statement` object
         *
         * \throw std::invalid_argument
         *      \c stmt_id was not recognised
         * \throw wr::sql::Error
         *      the statement could not be compiled or executed due to a
         *      syntax error in the original SQL or a semantic error in the
         *      statement
         * \throw wr::sql::Error
         *      a bound parameter value was invalid within the context
         *      of the statement
         * \throw wr::sql::Busy
         *      contention occurred with locks held by other database
         *      connections, or a potential deadlock was detected; handled
         *      automatically when called inside the context of a
         *      \c Transaction
         * \throw wr::sql::Interrupt
         *      a call to \c Session::interrupt() was issued by another thread
         *      (or possibly a progress handler invoked by the calling
         *      thread) 
         */
        template <typename ...Args> ExecResult exec(size_t stmt_id,
                                                    Args &&...args) const;

        /**
         * \brief search the database for a table, view or other named object
         *
         * \param [in] type  the object's type ("table", "view", "index")
         * \param [in] name  the object's name
         *
         * \return \c true if the named object was found, \c false otherwise
         */
        bool hasObject(const u8string_view &type, const u8string_view &name);

        /**
         * \brief interrupt any statement being executed on a connection
         *
         * \c interrupt() may be invoked within the same thread executing a
         * statement (via a progress handler) or by any other thread. If a
         * statement is being executed at the time then the associated call
         * to \c wr::sql::Session::exec(), \c wr::sql::Statement::begin() or
         * \c wr::sql::Statement::next() will throw a \c wr::sql::Interrupt
         * exception.
         *
         * \c interrupt() has no effect if no statement is being executed at
         * the exact time of invocation, or if it is invoked just after the
         * last row has been retrieved by a statement in progress.
         */
        void interrupt();

        /**
         * \brief determine whether a connection is open
         * \return
         *      \c true if a connection has been opened by this \c Session
         *      object, \c false otherwise
         */
        bool isOpen() const;

        /**
         * \brief retrieve the URI of an open connection
         * \return
         *      the connection's URI if a connection is open, otherwise an
         *      empty string 
         */
        u8string_view uri() const;

        /**
         * \brief obtain the ID of the last row inserted
         *
         * \c lastInsertRowID() returns the integer ID of the row most
         * recently inserted by a successful \c INSERT statement via the
         * invoking \c Session. The ID of that row is returned even if that
         * \c INSERT statement was rolled back afterwards.
         *
         * \return
         *      the ID of the row most recently inserted, or 0 if no \c INSERT
         *      statements have been executed on this connection
         */
        ID lastInsertRowID() const;

        /**
         * \brief obtain the number of rows changed or deleted by the last
         *      executed statement
         */
        int rowsAffected() const;

        /// \brief obtain the most recent status code
        int lastStatusCode() const;

        /// \brief obtain a message describing the most recent status
        u8string_view lastMessage() const;

        /**
         * \brief obtain a message describing a given status code
         *
         * \param [in] session
         *      if not NULL, the pointee \c Session object is used to obtain
         *      a more context-specific message if available; otherwise a
         *      generic message is obtained
         * \param [in] status
         *      status code specific to the underlying database type
         *
         * \return
         *      the corresponding message text
         *
         * \note The returned message may reside in a buffer modified by
         *      other API calls, and should be copied (e.g. using
         *      \c u8string_view::to_string() ) if it is intended to be used
         *      away from the original call site.
         */
        static u8string_view message(const Session *session, int status);

        /**
         * \brief free any spare memory previously allocated for the
         *      database connection
         */
        void releaseMemory();

        /**
         * \brief instruct the database to perform a garbage-collection cycle
         * \note If the specific database implementation does not support this
         *      operation, this function is a no-op.
         */
        void vacuum();

        /**
         * \brief return precompiled registered statement
         *
         * For greater efficiency the wrSQL library has a mechanism for
         * registering SQL statements for repeated usage without having to
         * recreate `Statement` objects every time (avoiding memory allocations,
         * reparsing SQL etc). The function `wr::sql::registerStatement()` is
         * invoked with the desired SQL text, returning an ID number
         * corresponding to that statement. This is later passed to
         * `Session::statement()` to retrieve a `Statement` object corresponding
         * to it. For every registered ID `Session` caches a `Statement` object
         * for every thread that has invoked `Session::statement()` with that
         * ID. To support reentrant usage of a given statement
         * `Session::statement()` returns a copy of the cached `Statement`
         * object if the cached object is already active.
         *
         * \param [in] stmt_id
         *      ID of statement as returned by a prior call to
         *      \c wr::sql::registerStatement()
         *
         * \return reference-counted pointer to corresponding precompiled
         *      `Statement` object; if the default `Statement` object for the
         *      current thread is already active, a private copy of that object
         *      is returned instead.
         *
         * \throw std::invalid_argument
         *      \c stmt_id was not recognised
         * \throw wr::sql::Error
         *      the statement could not be compiled due to an error in the
         *      original SQL
         */
        Statement::Ptr statement(size_t ix) const;

        /**
         * \brief finalize all precompiled registered statements
         */
        void finalizeRegisteredStatements();

        /**
         * \brief reset all precompiled registered statements
         */
        void resetRegisteredStatements();

        /**
         * \brief callback function type for statement execution progress
         *      handlers
         *
         * These functions are accepted by \c setProgressHandler and the
         * \c ScopedProgressHandler constructor, which cause the callable
         * object passed to be invoked periodically during execution of SQL
         * statements. When the callable object returns \c false the
         * statement being executed continues as normal; if the callable
         * object returns \c true then the statement's execution is
         * halted and a \c wr::sql::Interrupt exception is thrown in the
         * thread executing the statement.
         *
         * This mechanism can be used to check for user input and refresh
         * the screen to prevent an application from appearing to freeze
         * during long-running statements, and to provide a way for users to
         * cancel such statements.
         *
         * \see \c setProgressHandler(), \c ScopedProgressHandler
         */
        using ProgressHandler = std::function<bool ()>;

        /**
         * \brief set callback function to be periodically invoked during
         *      statement execution
         *
         * The function should return \c true
         *
         * \param [in] handler
         *      the callable object to be periodically invoked
         *
         * \return reference to \c *this
         */
        this_t &setProgressHandler(ProgressHandler handler);

        /**
         * \brief RAII class for setting and automatically removing a progress
         *      handler upon exiting a given scope
         */
        class ScopedProgressHandler
        {
        public:
                using this_t = ScopedProgressHandler;

                ScopedProgressHandler(Session &db,
                                      const ProgressHandler &handler) : db_(db)
                        { db_.setProgressHandler(handler); }

                ~ScopedProgressHandler() { db_.setProgressHandler({}); }

        private:
                Session &db_;
        };


        /**
         * \brief execute a transaction
         *
         * This is a convenience function which creates a
         * \c wr::sql::Transaction object and invokes its \c begin() method
         * with the specified \c code.
         *
         * For further information refer to \c Transaction::begin().
         *
         * \param [in] code
         *      callable object invoked within the transaction's context
         *      with one argument: a reference to the created transaction
         *
         * \return \c wr::sql::Transaction object representing the transaction
         *
         * \throw wr::sql::Interrupt
         *      a call to \c Session::interrupt() was issued by another thread
         *      (or possibly a progress handler invoked by the calling
         *      thread) 
         *
         * \see \c wr::sql::Transaction::begin()
         */
        Transaction beginTransaction(std::function<void (Transaction &)> code);

        /** \brief callback function invoked upon completion of the outermost
                transaction */
        using CommitAction = std::function<void ()>;

        /**
         * \brief take specific action when the outermost active transaction
         *      commits
         *
         * A queue of \c CommitAction objects is maintained by \c Session for
         * the outermost active transaction. \c onFinalCommit() adds \c action
         * to the back of this queue. When the outermost active transaction
         * commits, the actions are invoked in registration order. If the
         * transaction rolls back then the actions are not invoked.
         *
         * If \c onFinalCommit() is invoked after the outermost transaction
         * has committed, or when no transaction is active, then \c action is
         * invoked immediately.
         *
         * Actions registered using \c onFinalCommit() are forgotten after
         * invocation, or if the active transaction is rolled back.
         *
         * \param [in] action
         *      callable object to be invoked upon commit
         *
         * \see \c CommitAction, \c onRollback()
         */
        void onFinalCommit(CommitAction action);


        /// \brief callback function invoked upon transaction rollback
        using RollbackAction = std::function<void ()>;

        /**
         * \brief take specific action if the active transaction rolls back
         *
         * A stack of \c RollbackAction objects is maintained by \c Session
         * for the outermost active transaction. \c onRollback() pushes
         * \c action onto this stack. If the outermost active transaction
         * rolls back, the actions are popped off the stack and invoked in
         * the reverse of the order in which they were registered. If the
         * outermost transaction commits then the actions will not be invoked.
         *
         * \c onRollback() is ignored if no transaction is active or if the
         * outermost active transaction has already committed.
         *
         * Actions registered using \c onRollback() are forgotten after
         * invocation, or once the outermost active transaction commits.
         *
         * \param [in] action
         *      callable object to be invoked upon rollback
         *
         * \see \c RollbackAction, \c onFinalCommit()
         */
        void onRollback(RollbackAction action);

private:
        friend IDSet;
        friend Statement;
        friend Transaction;
        friend SessionTests;

        struct Body;

        Body *body_;
};

//--------------------------------------

template <typename ...Args> inline Statement
Session::exec(
        const u8string_view     &sql,
        Args                &&...args
) const
{
        Statement q(*this, sql);
        q.begin(std::forward<Args>(args)...);
        return q;
}

//--------------------------------------

template <typename ...Args> inline auto
Session::exec(
        size_t      stmt_id,
        Args   &&...args
) const -> ExecResult
{
        Statement::Ptr q = statement(stmt_id);
        q->begin(std::forward<Args>(args)...);
        return q;
}


} // namespace sql
} // namespace wr


#endif // !WRSQL_SESSION_H
