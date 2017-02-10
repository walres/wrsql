/**
 * \file Transaction.cxx
 *
 * \brief Implementation of class wr::sql::Transaction
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
#include <assert.h>
#include <stdarg.h>

#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/Statement.h>
#include <wrsql/Transaction.h>

#include "sqlite3api.h"
#include "SessionPrivate.h"


namespace wr {
namespace sql {


WRSQL_API
Transaction::Transaction() :
        session_(nullptr),
        outer_  (nullptr)
{
        session_.tag(DEFAULT);
}

//--------------------------------------

WRSQL_API
Transaction::Transaction(
        this_t &&other
) :
        this_t()
{
        *this = std::move(other);
}

//--------------------------------------

WRSQL_API
Transaction::~Transaction()
{
        if (session_) {
                rollback();
        }
}

//--------------------------------------

WRSQL_API auto
Transaction::operator=(
        this_t &&other
) -> this_t &
{
        if (&other != this) {
                if (session_) {
                        rollback();
                }
                std::swap(session_, other.session_);
                std::swap(outer_, other.outer_);
                if (session_) {
                        session_.ptr()->body_->replaceTransaction(&other, this);
                }
        }
        return *this;
}

//--------------------------------------

enum { DEFERRED, IMMEDIATE, EXCLUSIVE };

inline int upgrade(int mode)
        { return mode == DEFERRED ? IMMEDIATE : EXCLUSIVE; }

WRSQL_API auto
Transaction::begin(
        Session       &session,
        TransactionFn  code
) -> this_t  // static
{
        this_t txn;
        int    mode = DEFERRED;

        do try {
                txn.begin_(&session, mode);
                code(txn);
                txn.commit();
                break;
        } catch (Busy &) {
                if (txn.nested() || !txn.active()) {
                        throw;
                } else {
                        txn.rollback();
                        mode = upgrade(mode);
                }
        } while (true);

        return txn;
}

//--------------------------------------

WRSQL_API void
Transaction::begin_(
        Session *session,
        ...
)
{
        int mode;

        va_list args;
        va_start(args, session);
        mode = va_arg(args, int);
        va_end(args);

        static const size_t
                        BEGIN_DEFERRED  = registerStatement("BEGIN"),
                        BEGIN_IMMEDIATE = registerStatement("BEGIN IMMEDIATE"),
                        BEGIN_EXCLUSIVE = registerStatement("BEGIN EXCLUSIVE");

        outer_ = session->body_->addTransaction(this);

        if (!outer_) {
                size_t stmt_id;

                switch (mode) {
                default:
                case DEFERRED: stmt_id = BEGIN_DEFERRED; break;
                case IMMEDIATE: stmt_id = BEGIN_IMMEDIATE; break;
                case EXCLUSIVE: stmt_id = BEGIN_EXCLUSIVE; break;
                }

                session->exec(stmt_id);
        }

        session_ = session;
}

//--------------------------------------

WRSQL_API auto
Transaction::commit() -> this_t &
{
        static const size_t COMMIT = sql::registerStatement("COMMIT");

        if (session_) {
                auto &session = *session_;

                if (!outer_) {  // this is the outermost transaction
                        session.exec(COMMIT);
                        session.body_->transactionCommitted();
                }

                session_ = nullptr;
                session_.tag(COMMITTED);

                /* otherwise, only allow the outermost transaction
                   of the chain to perform the final commit */

                session.body_->removeTransaction(this);
        }

        return *this;
}

//--------------------------------------

WRSQL_API auto
Transaction::rollback() -> this_t &
{
        static const size_t ROLLBACK = sql::registerStatement("ROLLBACK");

        if (session_) {
                auto &session = *session_;
                session_ = nullptr;

                if (!sqlite3_get_autocommit(session.body_->db_)) {
                        session.exec(ROLLBACK);
                } /* else no transaction active, probably rolled back
                     automatically by SQLite error */

                session.body_->transactionRolledBack();
        }

        return *this;
}

//--------------------------------------

auto
Transaction::onRollback() -> this_t *
{
        this_t *outer = outer_;
        session_ = nullptr;
        session_.tag(ROLLED_BACK);
        outer_ = nullptr;
        return outer;
}

//--------------------------------------

bool
Transaction::nested() const
{
        return outer_ != nullptr;
}

//--------------------------------------

bool
Transaction::active() const
{
        return session_.ptr() != nullptr;
}

//--------------------------------------

bool
Transaction::committed() const
{
        return session_.tag() == COMMITTED;
}

//--------------------------------------

bool
Transaction::rolledBack() const
{
        return session_.tag() == ROLLED_BACK;
}


} // namespace sql
} // namespace wr
