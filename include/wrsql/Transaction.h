/**
 * \file wrsql/Transaction.h
 *
 * \brief Declaration of class \c wr::sql::Transaction
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
#ifndef WRSQL_TRANSACTION_H
#define WRSQL_TRANSACTION_H

#include <wrsql/Config.h>
#include <functional>
#include <wrutil/tagged_ptr.h>


namespace wr {
namespace sql {


class Transaction;

/// \brief Transaction function type
using TransactionFn = std::function<void (Transaction &)>;


class Session;


/**
 * \class wr::sql::Transaction
 * \brief transaction context
 */
class WRSQL_API Transaction
{
public:
        using this_t = Transaction;

        //@{
        /**
         * \brief object constructor
         *
         * The default constructor initialises an inactive \c Transaction
         * object; active \c Transaction objects can only be created by
         * invoking \c begin(), \c Session::beginTransaction() or the
         * move constructor with an object created by a call to one of the
         * other two aforementioned methods.
         *
         * \param [in,out] other  object to be moved
         */
        Transaction();
        Transaction(const this_t &) = delete;
        Transaction(this_t &&other);
        //@}
        
        /**
         * \brief object destructor
         *
         * Automatically rolls back the transaction if it is active on entry.
         */
        ~Transaction();

        this_t &operator=(const this_t &) = delete;

        /**
         * \brief move assignment operator
         *
         * Transfers the state of \c other to \c *this. If \c *this
         * represents a transaction in progress, i.e. it is not in the
         * default-constructed state, it is rolled back. \c other is left
         * in the default-constructed state. The method has no effect if
         * <code>(&other == this)</code>.
         *
         * \param [in,out] other  Transaction object to be moved
         *
         * \return reference to *this
         */
        this_t &operator=(this_t &&other);

        /**
         * \brief create new transaction context and execute code inside it
         *
         * \c begin() creates a Transaction object representing a new
         * transaction context for the given \c session, invoking the
         * callable object \c code (typically a lambda function specified
         * inline) with the new Transaction object as its sole parameter.
         * Within this context all database operations executed by \c code are
         * guaranteed to either be performed in their their entireity (the
         * transaction is <em>committed</em>), or not at all (the transaction
         * is aborted or <em>rolled back</em>).
         *
         * If \c code does not explicitly invoke \c commit() or \c rollback()
         * on the Transaction object passed to it then the transaction is
         * committed automatically on exit from \c code unless an exception
         * is thrown inside \c code.
         *
         * When invoked within the context of an existing transaction on
         * \c session, a \e nested transaction is begun. Invoking \c commit()
         * on a nested transaction only affects the nested transaction itself,
         * but invoking \c rollback() on a nested transaction causes all
         * outlying transaction contexts to be rolled back as well. The
         * outermost transaction context must commit before nested
         * transactions' changes can take effect globally.
         *
         * If \c code throws an exception \e other than \c wr::sql::Busy then
         * that exception will be propagated back to the caller. Unless the
         * transaction already committed or rolled back back before the
         * exception was thrown, the transaction is rolled back automatically.
         *
         * If a \c wr::sql::Busy exception is thrown within \c code due to
         * contention with other active connections concurrently accessing the
         * database (typically due to a potential deadlock) then by default
         * the transaction is rolled back and all transaction code is
         * re-executed from the beginning of the outermost transaction in
         * effect when the \c Busy exception was thrown. Therefore the author
         * of a transaction's code should be prepared for \c code to be
         * executed multiple times. \c Session::onFinalCommit() arranges for a
         * callable object to be invoked automatically when the outermost
         * transaction context commits. Likewise, \c Session::onRollback()
         * arranges for code to be executed automatically upon any part of the
         * outermost transaction context being rolled back.
         *
         * If a \c wr::sql::Busy exception is thrown due to execution of
         * statements after the transaction has become inactive due to commit
         * or rollback (this practice is discouraged) and the transaction is
         * not nested then the \c Busy exception is propagated back to the
         * caller.
         *
         * \c Session::beginTransaction() provides a convenient wrapper for
         * this function.
         *
         * \param [in,out] session  the database connection
         * \param [in]     code     the transactional code to execute
         *
         * \return the executed transaction - will have been committed or
         *      rolled back on exit
         *
         * \see \c Session::beginTransaction(), \c commit(), \c rollback(),
         *      \c Session::onFinalCommit(), \c Session::onRollback()
         */
        static this_t begin(Session &session, TransactionFn code);

        /**
         * \brief commit the active transaction
         *
         * \c commit() marks the completion of the active transaction, and
         * makes it inactive. If \c commit() is invoked within the context of
         * a nested transaction then any changes made by the committed
         * transaction do not become globally visible until the outermost
         * transaction commits. If the outermost transaction later rolls back
         * then any changes made by committed nested transactions are
         * discarded.
         *
         * Transaction code should not execute any further INSERT, UPDATE or
         * DELETE statements after issuing a \c commit() or \c rollback() as
         * these will not be executed as part of the transaction.
         *
         * Invoking \c commit() on an inactive \c Transaction object has no
         * effect.
         *
         * \return reference to \c *this
         *
         * \throw wr::sql::Busy
         *      a potential deadlock or excessive contention occurred between
         *      connections concurrently accessing the database; this is
         *      handled automatically by the implementation of \c begin()
         *
         * \see \c begin(), \c rollback(), \c Session::onFinalCommit()
         */
        this_t &commit();

        /**
         * \brief cancel the active transaction
         *
         * \c rollback() cancels the active transaction, making it inactive
         * and undoing all changes made under its context including any
         * changes made by committed nested transactions. If \c rollback() is
         * invoked within the context of a nested transaction context then all
         * outlying transactions are also rolled back.
         *
         * Transaction code should not execute any further INSERT, UPDATE or
         * DELETE statements after issuing a \c rollback() or \c commit() as
         * these will not be executed as part of the transaction.
         *
         * Invoking \c rollback() on an inactive \c Transaction object has no
         * effect.
         *
         * \return reference to \c *this
         *
         * \see \c begin(), \c commit()
         */
        this_t &rollback();

        /**
         * \brief inquire whether a transaction is running in a nested context
         * \return
         *      \c true if the transaction is active and was started within
         *      another transaction's context on the same connection; \c false
         *      otherwise
         */
        bool nested() const;

        ///@{
        /**
         * \brief query transaction state
         * \return
         *      \c active() returns \c true if the \c Transaction object is
         *      in effect but no call to \c commit() or \c rollback() has
         *      been made, or any nested transaction has rolled back;
         *      otherwise \c active() returns \c false
         * \return
         *      \c committed() returns \c true if \c commit() has been
         *      invoked, \c false otherwise
         * \return
         *      \c rolledBack() returns \c true if \c rollback() has been
         *      invoked by this or any nested \c Transaction, \c false
         *      otherwise
         */
        bool active() const;
        bool committed() const;
        bool rolledBack() const;
        ///@}

private:
        friend class Session;

        void begin_(Session *session, ...);
        this_t *onRollback();

        enum: uint8_t { DEFAULT = 0, COMMITTED, ROLLED_BACK };

        tagged_ptr<Session, 2>  session_; // tag contains value from above enum
        this_t                 *outer_;  
};


} // namespace sql
} // namespace wr


#endif // !WRSQL_TRANSACTION_H
