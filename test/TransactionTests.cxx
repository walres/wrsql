/**
 * \file TransactionTests.cxx
 *
 * \brief Unit test module for class \c wr::sql::Transaction
 *
 * \copyright
 * \parblock
 *
 *   Copyright 2017 James S. Waller
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
#include <condition_variable>
#include <mutex>
#include <thread>

#include <wrutil/uiostream.h>

#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/Transaction.h>

#include "SampleDB.h"
#include "SQLTestManager.h"


namespace wr {
namespace sql {


class TransactionTests : public SQLTestManager
{
public:
        using base_t = SQLTestManager;

        TransactionTests(int argc, const char **argv) :
                base_t("Transaction", argc, argv) { db_.init(defaultURI()); }

        virtual ~TransactionTests() { db_.close(); }

        int runAll();

        static void defaultConstruct(),
                    begin(),
                    beginNested(),
                    rollback(),
                    busyHandling(),
                    nestedBusyHandling();

private:
        static SampleDB db_;
};


SampleDB TransactionTests::db_;


} // namespace sql
} // namespace wr

//--------------------------------------

int
main(
        int          argc,
        const char **argv
)
{
        return wr::sql::TransactionTests(argc, argv).runAll();
}

//--------------------------------------

int
wr::sql::TransactionTests::runAll()
{
        run("defaultConstruct", 1, &defaultConstruct);
        run("begin", 1, &begin);
        run("begin", 2, &beginNested);
        run("rollback", 1, &rollback);
        run("busyHandling", 1, &busyHandling);
        run("busyHandling", 2, &nestedBusyHandling);

        return failed() ? EXIT_FAILURE : EXIT_SUCCESS;
}

//--------------------------------------

void
wr::sql::TransactionTests::defaultConstruct() // static
{
        Transaction txn;

        if (txn.active()) {
                throw TestFailure("txn.active() returned true, expected false");
        }
        if (txn.nested()) {
                throw TestFailure("txn.nested() returned true, expected false");
        }
        if (txn.committed()) {
                throw TestFailure("txn.committed() returned true, expected false");
        }
        if (txn.rolledBack()) {
                throw TestFailure("txn.rolledBack() returned true, expected false");
        }
}

//--------------------------------------

void
wr::sql::TransactionTests::begin() // static
{
        db_.exec("CREATE TEMP TABLE foo (id INTEGER PRIMARY KEY)");

        auto txn = db_.beginTransaction([](Transaction &txn) {
                if (!txn.active()) {
                        throw TestFailure("txn.active() returned false inside body, expected true");
                }
                if (txn.nested()) {
                        throw TestFailure("txn.nested() returned true, expected false");
                }
                if (txn.committed()) {
                        throw TestFailure("txn.committed() returned true inside body, expected false");
                }
                if (txn.rolledBack()) {
                        throw TestFailure("txn.rolledBack() returned true inside body, expected false");
                }

                db_.exec("INSERT INTO foo (id) VALUES (1)");
        });

        if (!txn.committed()) {
                throw TestFailure("txn.committed() returned false after completion, expected true");
        }
        if (txn.rolledBack()) {
                throw TestFailure("txn.rolledBack() returned true after completion, expected false");
        }
        if (txn.active()) {
                throw TestFailure("txn.active() returned true after completion, expected false");
        }
        if (db_.exec("SELECT id FROM foo").currentRow().get<int64_t>(0) != 1) {
                throw TestFailure("inserted row had value %d, expected 1");
        }
}

//--------------------------------------

void
wr::sql::TransactionTests::beginNested() // static
{
        auto txn1 = db_.beginTransaction([&](Transaction &txn1) {
                auto txn2 = db_.beginTransaction([&](Transaction &txn2) {
                        if (!txn2.nested()) {
                                throw TestFailure("txn2.nested() returned false, expected true");
                        }
                });

                if (!txn2.committed()) {
                        throw TestFailure("txn2.committed() returned false after completion, expected true");
                }
                if (txn1.committed()) {
                        throw TestFailure("txn1.committed() returned true before completion, expected false");
                }
                if (txn2.rolledBack()) {
                        throw TestFailure("txn2.rolledBack() returned true after completion, expected false");
                }
        });
}

//--------------------------------------

void
wr::sql::TransactionTests::rollback() // static
{
        db_.exec("CREATE TEMP TABLE foo (id INTEGER PRIMARY KEY)");

        auto txn = db_.beginTransaction([](Transaction &txn) {
                db_.exec("INSERT INTO foo (id) VALUES (1)");
                txn.rollback();
        });

        if (!txn.rolledBack()) {
                throw TestFailure("txn.rolledBack() returned false after completion, expected true");
        }
        if (txn.committed()) {
                throw TestFailure("txn.committed() returned true after completion, expected false");
        }
        if (db_.exec("SELECT id FROM foo").currentRow()) {
                throw TestFailure("query returned row(s); expected none");
        }
}

//--------------------------------------

void
wr::sql::TransactionTests::busyHandling() // static
{
        std::mutex                   mutex;
        std::condition_variable      writer_proceed_cv, reader_proceed_cv;
        bool                         writer_proceed = false,
                                     reader_proceed = false;
        std::unique_lock<std::mutex> writer_lock(mutex);

        /*
         * The reader thread
         */
        std::thread reader([&]{
                Session db2(db_);
                for (Row r: db2.exec("SELECT * FROM employees")) {
                        // wait until writer starts waiting for writer_proceed
                        std::unique_lock<std::mutex> reader_lock(mutex);
                        writer_proceed = true;
                        writer_proceed_cv.notify_all();
                        reader_proceed_cv.wait(reader_lock,
                                               [&]{ return reader_proceed; } );
                        break;
                }
        });

        /*
         * The writer thread (mutex locked on entry)
         */
        // wait until reader has started executing its SELECT statement
        writer_proceed_cv.wait(writer_lock, [&]{ return writer_proceed; });

        int retry_count = -1;

        db_.beginTransaction([&](Transaction &) {
                if (++retry_count > 0) {  // 2nd+ time around
                        /* allow the reader to complete so the second INSERT
                           attempt should succeed */
                        reader_proceed = true;
                        writer_lock.unlock();
                        reader_proceed_cv.notify_all();
                        reader.join();
                }

                db_.exec("INSERT INTO employees (number, surname, forename, "
                        "extension, email, office_code, reports_to, job_title) "
                        "VALUES (9999, 'Smith', 'Jane', 'x4321', "
                          "'jsmith@classicmodelcars.com', 7, 1102, 'Payroll')");
        });

        if (retry_count != 1) {
                throw TestFailure("expected busy condition on first transaction attempt");
        }
}

//--------------------------------------

void
wr::sql::TransactionTests::nestedBusyHandling() // static
{
        std::mutex              mutex;
        std::condition_variable parent_proceed_cv, child_proceed_cv;
        bool                    parent_proceed = false,
                                child_proceed = false;
        /*
         * The child thread
         */
        std::thread child([&]{
                std::unique_lock<std::mutex> child_lock(mutex);

                // wait until parent enters nested transaction
                child_proceed_cv.wait(child_lock, [&]{ return child_proceed; });
                child_proceed = false;

                Session db2(db_);

                db2.beginTransaction([&](Transaction &txn) {
                        db2.exec("INSERT INTO offices (code, city, phone, "
                                 "address_line_1, address_line_2, state, "
                                 "country, postal_code, territory) "
                                 "VALUES ('8', 'Toronto', '+1 416 123 4567', "
                                 "'2476 Wellington Street', NULL, 'Ontario', "
                                 "'Canada', 'M9C 3J5', 'NA')");

                        // wait until parent starts waiting for parent_proceed
                        parent_proceed = true;
                        parent_proceed_cv.notify_all();
                        child_proceed_cv.wait(child_lock,
                                              [&]{ return child_proceed; } );
                });
        });

        /*
         * The parent thread
         */
        int retry_count = -1;

        db_.beginTransaction([&](Transaction &) {
                std::unique_lock<std::mutex> parent_lock(mutex);

                if (++retry_count > 0) {  // 2nd+ time around
                        /* allow the child to complete so the nested INSERT
                           attempt should succeed */
                        child_proceed = true;
                        parent_lock.unlock();
                        child_proceed_cv.notify_all();
                        child.join();
                }

                try {  // do not expect this to fail
                        for (Row row: db_.exec("SELECT * FROM employees")) {
                                (void) row;
                                break;
                        }
                } catch (SQLException &e) {  // includes Busy, Error & Interrupt
                        throw TestFailure("failed to query employees (%s)",
                                          e.what());
                }

                db_.beginTransaction([&](Transaction &txn) {
                        // if child exists, tell it to continue
                        child_proceed = true;
                        child_proceed_cv.notify_all();

                        // wait for child to begin its SELECT
                        parent_proceed_cv.wait(parent_lock,
                                               [&]{ return parent_proceed; });

                        db_.exec("INSERT INTO employees (number, surname, "
                                "forename, extension, email, office_code, "
                                "reports_to, job_title) VALUES (9876, 'Doe', "
                                "'John', 'x9999', 'jdoe@classicmodelcars.com',"
                                "7, 1143, 'Sales Rep')");

                        // should not get here on first try
                        if (retry_count == 0) {
                                child.detach();
                                throw TestFailure("nested transaction did not throw sql::Busy as expected");
                        }
                });
        });

        if (retry_count != 1) {
                throw TestFailure("expected busy condition on first transaction attempt");
        }
}
