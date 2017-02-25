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
        if (active()) {
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
                if (active()) {
                        rollback();
                }
                std::swap(session_, other.session_);
                std::swap(outer_, other.outer_);
                if (active()) {
                        session_.ptr()->body_->replaceTransaction(&other, this);
                }
        }
        return *this;
}

//--------------------------------------

WRSQL_API auto
Transaction::begin(
        Session       &session,
        TransactionFn  code
) -> this_t  // static
{
        this_t txn;
        bool   nested = session.body_->innerTransaction() != nullptr;

        do {
                try {
                        txn.begin_(&session);
                        code(txn);
                        txn.commit();
                        break;
                } catch (Busy &) {
                        if (txn.nested()) {
                                throw;
                        } else {
                                txn.rollback();
                        }
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

        if (!session->body_->innerTransaction()) {  // will not be nested
                static const size_t BEGIN_TXN = registerStatement("BEGIN");
                session->exec(BEGIN_TXN);  // may throw
        }

        outer_ = session->body_->addTransaction(this);
        session_ = session;
}

//--------------------------------------

WRSQL_API auto
Transaction::commit() -> this_t &
{
        static const size_t COMMIT = sql::registerStatement("COMMIT");

        if (active()) {
                auto &session = *session_;

                if (!nested()) {  // this is the outermost transaction
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

        if (active()) {
                auto &session = *session_;
                session_ = nullptr;

                if (!sqlite3_get_autocommit(session.body_->db())) {
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
