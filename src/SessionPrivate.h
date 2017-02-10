/**
 * \file SessionPrivate.h
 *
 * \brief Internal declarations relating to class wr::sql::Session
 *
 * \warning The declarations within this file are not part of the wrSQL API
 *      and are only intended for use by the wrSQL library source code itself
 *      (e.g. Transaction.cxx and unit tests). These declarations are subject
 *      to change without notice.
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
#ifndef WRSQL_SESSION_PRIVATE_H
#define WRSQL_SESSION_PRIVATE_H

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>


namespace wr {
namespace sql {


class Transaction;

using RegisteredStmts = std::vector<std::unique_ptr<Statement>>;
using CommitActions   = std::list<Session::CommitAction>;
using RollbackActions = std::list<Session::RollbackAction>;

//--------------------------------------

struct Session::Body
{
        using this_t = Body;

        Session                 &me_;
        sqlite3                 *db_;
        int                      flags_;
        std::string              uri_;
        Transaction             *inner_txn_;
        mutable RegisteredStmts  statements_;
        std::condition_variable  unlock_notifier_;
        std::mutex               wait_lock_;
        std::atomic<bool>        waiting_;
        ProgressHandler          progress_handler_;
        CommitActions            commit_actions_;
        RollbackActions          rollback_actions_;


        Body(Session &me);
        ~Body();

        static int callProgressHandler(void *me);

        bool waitForUnlock();
        static void onUnlock(void **blocked, int num_blocked);
                                        // sqlite3 unlock notification callback

        Transaction *addTransaction(Transaction *txn);
        void removeTransaction(Transaction *txn);
        void replaceTransaction(Transaction *before, Transaction *after);

        void transactionCommitted();
        void transactionRolledBack();
};


} // namespace sql
} // namespace wr


#endif // !WRSQL_SESSION_PRIVATE_H
