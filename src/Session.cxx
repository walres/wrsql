/**
 * \file Session.cxx
 *
 * \brief Implementation of class wr::sql::Session and the ALPHANUM
 *      collation sequence
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
#include <wrutil/ctype.h>
#include <wrutil/Format.h>

#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/Statement.h>
#include <wrsql/Transaction.h>

#include "sqlite3api.h"
#include "SessionPrivate.h"


namespace wr {
namespace sql {


static int collateAlphaNum(void *context, int a_len, const void *a,
                           int b_len, const void *b);

//--------------------------------------

Session::Body::Body(
        Session &me
) :
        me_       (me),
        db_       (nullptr),
        inner_txn_(nullptr),
        waiting_  (false)
{
}

//--------------------------------------

WRSQL_API Session::Session() : body_(new Body(*this)) {}

WRSQL_API Session::Session(const this_t &other) : this_t(other.uri()) {}

WRSQL_API Session::Session(this_t &&other) : this_t()
        { std::swap(body_, other.body_); }

//--------------------------------------

WRSQL_API
Session::Session(
        const u8string_view &uri
) :
        this_t()
{
        open(uri);
}

//--------------------------------------

WRSQL_API
Session::~Session()
{
        try {
                close();
        } catch (const Error &err) {
                print(std::cerr, "problem closing Session %p on \"%s\": %s\n",
                      this, utf8_narrow_cvt().from_utf8(uri()), err.what());
        }
}

//--------------------------------------

WRSQL_API auto
Session::operator=(
        const this_t &other
) -> this_t &
{
        if (&other != this) {
                close();
                open(other.uri());
        }
        return *this;
}

//--------------------------------------

WRSQL_API auto
Session::operator=(
        this_t &&other
) -> this_t &
{
        if (&other != this) {
                close();
                std::swap(body_, other.body_);
        }
        return *this;
}

//--------------------------------------

WRSQL_API void
Session::open(
        const u8string_view &uri
)
{
        auto        scheme = uri.split('\x3a');
        std::string body_uri = uri.to_string(),  // moved to body_->uri later
                    uri_copy;

        if (!scheme.second.empty()) {
                // currently only SQLite3 is supported
                if ((scheme.first.compare_nocase(u8"sqlite3") == 0)
                            || (scheme.first.compare_nocase(u8"file") == 0)) {
                        uri_copy += u8"file:";
                        uri_copy += scheme.second.to_string();
                } else {
                        throw Error(printStr("unrecognised database type \"%s\" in URI \"%s\"",
                                utf8_narrow_cvt().from_utf8(scheme.first),
                                utf8_narrow_cvt().from_utf8(uri)));
                }
        } else {
                uri_copy = "file://" + uri.to_string();
        }

        sqlite3 *db;

        int status = sqlite3_open_v2(uri_copy.c_str(), &db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                nullptr);

        if (status == SQLITE_OK) {
                if (isOpen()) try {
                        close();
                } catch (...) {
                        sqlite3_close(db);
                        throw;
                }
                body_->db_ = db;
                sqlite3_create_collation_v2(body_->db_, "ALPHANUM", SQLITE_UTF8,
                                            nullptr, &collateAlphaNum, nullptr);
                body_->uri_ = std::move(body_uri);
        } else if (body_->db_) {
                Error err(this, lastStatusCode());
                try {
                        close();
                } catch (const Error &) {
                        abort();  // unexpected
                }
                throw err;
        } else {
                throw Error(this, status);
        }
}

//--------------------------------------

WRSQL_API void
Session::close()
{
        if (isOpen()) {
                body_->statements_.clear();

                int status = sqlite3_close(body_->db_);

                if (status != SQLITE_OK) {
                        throw Error(this, status);
                }

                body_->db_ = nullptr;
                body_->uri_ = {};
        }
}

//--------------------------------------

WRSQL_API bool
Session::hasObject(
        const u8string_view &type,
        const u8string_view &name
)
{
        if (!isOpen()) {
                return false;
        }

        static const size_t HAS_OBJECT = registerStatement(
                "SELECT rootpage FROM sqlite_master WHERE type=? AND name=?");

        return !statement(HAS_OBJECT).begin(type, name).empty();
}

//--------------------------------------

WRSQL_API ID
Session::lastInsertRowID() const
{
        return sqlite3_last_insert_rowid(body_->db_);
}

//--------------------------------------

WRSQL_API void
Session::interrupt()
{
        if (isOpen()) {
                sqlite3_interrupt(body_->db_);
        }
}

//--------------------------------------

WRSQL_API bool
Session::isOpen() const
{
        return body_->db_ != nullptr;
}

//--------------------------------------

WRSQL_API u8string_view
Session::uri() const
{
        return body_->uri_;
}

//--------------------------------------

WRSQL_API int
Session::rowsAffected() const
{
        return sqlite3_changes(body_->db_);
}

//--------------------------------------

WRSQL_API int
Session::lastStatusCode() const
{
        return sqlite3_errcode(body_->db_);
}

//--------------------------------------

WRSQL_API u8string_view
Session::lastMessage() const
{
        return sqlite3_errmsg(body_->db_);
}

//--------------------------------------

WRSQL_API u8string_view
Session::message(
        const Session *session,
        int            status
) // static
{
        if (session && (status == session->lastStatusCode())) {
                return session->lastMessage();
        } else {
                return sqlite3_errstr(status);
        }
}

//--------------------------------------

WRSQL_API void
Session::releaseMemory()
{
        sqlite3_db_release_memory(body_->db_);
        body_->statements_.clear();
}

//--------------------------------------

WRSQL_API void
Session::vacuum()
{
        for (auto &stmt: body_->statements_) {
                stmt.reset();
        }

        exec("VACUUM");
}

//--------------------------------------

WRSQL_API Statement &
Session::statement(
        size_t id
) const
{
        if (id >= body_->statements_.size()) {
                if (id >= numRegisteredStatements()) {
                        throw std::invalid_argument(
                                printStr("invalid statement ID %u given", id));
                }
                body_->statements_.resize(id + 1);
        }

        auto &stmt = body_->statements_[id];

        if (!stmt) {
                stmt.reset(new Statement);
        }
        if (!stmt->isPrepared()) {
                stmt->prepare(*this, registeredStatement(id));
        }

        return *stmt;
}

//--------------------------------------

WRSQL_API void
Session::finalizeStatements()
{
        for (auto &stmt: body_->statements_) {
                if (stmt) {
                        stmt->finalize();
                }
        }
}

//--------------------------------------

WRSQL_API auto
Session::setProgressHandler(
        ProgressHandler handler
) -> this_t &
{
        int    period;
        int  (*sqlite_handler)(void *);
        void  *context;

        if (handler) {
                period = 10000;
                sqlite_handler = &Body::callProgressHandler;
                context = body_;
        } else {
                period = 0;
                sqlite_handler = nullptr;
                context = nullptr;
        }

        body_->progress_handler_ = std::move(handler);
        sqlite3_progress_handler(body_->db_, period, sqlite_handler, context);
        return *this;
}

//--------------------------------------

int
Session::Body::callProgressHandler(
        void *me
) // static
{
        return static_cast<this_t *>(me)->progress_handler_();
}

//--------------------------------------

WRSQL_API Transaction
Session::beginTransaction(
        std::function<void (Transaction &)> code
)
{
        return Transaction::begin(*this, code);
}

//--------------------------------------

WRSQL_API void
Session::onFinalCommit(
        CommitAction action
)
{
        if (body_->inner_txn_) {
                body_->commit_actions_.emplace_back(std::move(action));
        } else {
                action();
        }
}

//--------------------------------------

WRSQL_API void
Session::onRollback(
        RollbackAction action
)
{
        if (body_->inner_txn_) {
                body_->rollback_actions_.emplace_back(std::move(action));
        }
}

//--------------------------------------

bool
Session::Body::waitForUnlock()
{
        waiting_ = true;

        bool result = sqlite3_unlock_notify(db_, &onUnlock, this) == SQLITE_OK;

        if (result) {
                std::unique_lock<std::mutex> guard(wait_lock_);
                while (waiting_) {
                        unlock_notifier_.wait(guard);
                }
                sqlite3_unlock_notify(db_, nullptr, nullptr);
        } else {
                waiting_ = false;
        }

        return result;
}

//--------------------------------------

void
Session::Body::onUnlock(
        void **blocked,
        int    num_blocked
) // static
{
        for (int i = 0; i < num_blocked; ++i) {
                auto s = static_cast<this_t *>(blocked[i]);
                std::unique_lock<std::mutex> guard(s->wait_lock_);
                s->waiting_ = false;
                s->unlock_notifier_.notify_all();
        }
}

//--------------------------------------

Transaction *
Session::Body::addTransaction(
        Transaction *txn
)
{
        Transaction *prev = inner_txn_;
        inner_txn_ = txn;
        return prev;
}

//--------------------------------------

void
Session::Body::removeTransaction(
        Transaction *txn
)
{
        if (txn == inner_txn_) {
                inner_txn_ = txn->outer_;
        } else {
                for (Transaction *i = inner_txn_; i; i = i->outer_) {
                        if (i->outer_ == txn) {
                                i->outer_ = txn->outer_;
                                break;
                        }
                }
        }
}

//--------------------------------------

void
Session::Body::replaceTransaction(
        Transaction *before,
        Transaction *after
)
{
        if (inner_txn_ == before) {
                inner_txn_ = after;
        } else {
                for (Transaction *i = inner_txn_; i; i = i->outer_) {
                        if (i->outer_ == before) {
                                i->outer_ = after;
                                break;
                        }
                }
        }
}

//--------------------------------------

void
Session::Body::transactionCommitted()
{
        rollback_actions_.clear();

        while (!commit_actions_.empty()) {
                commit_actions_.front()();
                commit_actions_.pop_front();
        }
}

//--------------------------------------

void
Session::Body::transactionRolledBack()
{
        while (inner_txn_) {
                inner_txn_ = inner_txn_->onRollback();
        }

        commit_actions_.clear();

        while (!rollback_actions_.empty()) {
                rollback_actions_.back()();
                rollback_actions_.pop_back();
        }
}

//--------------------------------------

static int
collateAlphaNum(
        void       * /* context */,
        int         a_len,
        const void *a,
        int         b_len,
        const void *b
)
{
        int           diff = 0;
        u8string_view a1  (static_cast<const char *>(a),
                           static_cast<size_t>(a_len)),
                      b1  (static_cast<const char *>(b),
                           static_cast<size_t>(b_len));

        auto ia = a1.begin(), ib = b1.begin();

        while (!diff && (ia < a1.end()) && (ib < b1.end())) {
                if (!isualnum(*ia)) {
                        ++ia;
                        if (!isualnum(*ib)) {
                                ++ib;
                        }
                } else if (!isualnum(*ib)) {
                        ++ib;
                } else {
                        diff = touupper(*ia) - touupper(*ib);
                        ++ia;
                        ++ib;
                }
        }

        if (!diff) {
                if (ia >= a1.end()) {
                        if (ib >= b1.end()) {
                                return 0;
                        } else {
                                return -1;
                        }
                } else {
                        return 1;
                }
        }

        return diff;
}


} // namespace sql
} // namespace wr
