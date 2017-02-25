/**
 * \file SessionTests.cxx
 *
 * \brief Unit test module for class \c wr::sql::Session
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
#include <iostream>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/Transaction.h>

#include "SampleDB.h"
#include "SQLTestManager.h"


namespace wr {
namespace sql {


class SessionTests : public SQLTestManager
{
public:
        using base_t = SQLTestManager;

        SessionTests(int argc, const char **argv) :
                base_t("Session", argc, argv) {}

        int runAll();

        static void defaultConstruct(),
                    constructWithOpen(),
                    destructWithError(),
                    openNonExistentFile(),
                    openUnrecognisedDatabaseType(),
                    reOpen(),
                    getURI(),
                    execSimple(),
                    createSampleDBSchema(),
                    statement1(),
                    statement2(),
                    statement3(),
                    finalizeRegisteredStatements(),
                    resetRegisteredStatements(),
                    hasObject1(),
                    hasObject2(),
                    copyConstruct(),
                    moveConstruct(),
                    serialisedInterrupt(),
                    concurrentInterrupt(),
                    lastInsertRowID(),
                    rowsAffected1(),
                    rowsAffected2(),
                    rowsAffected3(),
                    releaseMemory(),
                    vacuum(),
                    copyAssign(),
                    copyAssignThis(),
                    moveAssign(),
                    moveAssignThis(),
                    setProgressHandler(),
                    clearProgressHandler(),
                    onFinalCommit(),
                    onRollback();
};


} // namespace sql
} // namespace wr

//--------------------------------------

int
main(
        int          argc,
        const char **argv
)
{
        return wr::sql::SessionTests(argc, argv).runAll();
}

//--------------------------------------

int
wr::sql::SessionTests::runAll()
{
        run("construct", 1, &defaultConstruct);
        run("construct", 2, &constructWithOpen);
        run("construct", 3, &copyConstruct);
        run("construct", 4, &moveConstruct);
        run("destruct", 1, &destructWithError);
        run("open", 1, &openNonExistentFile);
        run("open", 2, &openUnrecognisedDatabaseType);
        run("open", 3, &reOpen);
        run("getURI", 1, &getURI);
        run("exec", 1, &execSimple);
        run("exec", 2, &createSampleDBSchema);
        run("statement", 1, &statement1);
        run("statement", 2, &statement2);
        run("statement", 3, &statement3);
        run("finalizeRegisteredStatements", 1, &finalizeRegisteredStatements);
        run("resetRegisteredStatements", 1, &resetRegisteredStatements);
        run("hasObject", 1, &hasObject1);
        run("hasObject", 2, &hasObject2);
        run("interrupt", 1, &serialisedInterrupt);
        run("interrupt", 2, &concurrentInterrupt);
        run("lastInsertRowID", 1, &lastInsertRowID);
        run("rowsAffected", 1, &rowsAffected1);
        run("rowsAffected", 2, &rowsAffected2);
        run("rowsAffected", 3, &rowsAffected3);
        run("releaseMemory", 1, &releaseMemory);
        run("vacuum", 1, &vacuum);
        run("copyAssign", 1, &copyAssign);
        run("copyAssignThis", 1, &copyAssignThis);
        run("moveAssign", 1, &moveAssign);
        run("moveAssignThis", 1, &moveAssignThis);
        run("setProgressHandler", 1, &setProgressHandler);
        run("clearProgressHandler", 1, &clearProgressHandler);
        run("onFinalCommit", 1, &onFinalCommit);
        run("onRollback", 1, &onRollback);

        return failed() ? EXIT_FAILURE : EXIT_SUCCESS;
}

//--------------------------------------

void
wr::sql::SessionTests::defaultConstruct() // static
{
        Session s;

        if (!s.body_) {
                throw TestFailure("s.body_ is null, expected non-null");
        } else if (s.isOpen()) {
                throw TestFailure("s.isOpen() returned true, expected false");
        }

        try {
                s.exec("SELECT * FROM sqlite_master");  // should throw
                throw TestFailure("s.exec() did not throw exception with no database open");
        } catch (const Error &) {
                ;  // OK, as expected
        }
}

//--------------------------------------

void
wr::sql::SessionTests::constructWithOpen() // static
{
        Session db(defaultURI());

        if (!db.isOpen()) {
                throw TestFailure("db.isOpen() returned false, expected true");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::destructWithError() // static
{
        Statement stmt;
        {
                Session db(":memory:");
                stmt.prepare(db, "SELECT * FROM sqlite_master");  // throws
        }
}

//--------------------------------------

void
wr::sql::SessionTests::openNonExistentFile() // static
{
        fs_error_code err;
        remove(defaultPath(), err);
        /* possible race condition where someone else could recreate the file;
           can't do much about it? */
        try {
                Session db;
                db.open(defaultURI().to_string() + "?mode=ro");
                if (exists(defaultPath())) {
                        throw TestFailure("Another process recreated the file");
                } else {
                        throw TestFailure("db.open() did not throw exception when given non-existent file");
                }
        } catch (Error &) {
                // OK
        }
}

//--------------------------------------

void
wr::sql::SessionTests::openUnrecognisedDatabaseType() // static
try {
        Session db;
        db.open("dummy://foo.darkstar.org:43210");  // expect exception
        throw TestFailure("db.open() did not throw exception for unrecognised database type");
} catch (Error &e) {
        string_view what = e.what();
        if (what.find("unrecognised database type") == what.npos) {
                throw;
        } // else OK
}

//--------------------------------------

void
wr::sql::SessionTests::reOpen() // static
{
        Session db;

        db.open(defaultURI());
        SampleDB::createSchema(db);

        db.open(":memory:");  // closes file opened above

        try {
                db.exec("SELECT * FROM customers");
                throw TestFailure("statement executed without error; expected error");
        } catch (Error &e) {
                string_view msg = e.what();
                if (msg.find("customers") == msg.npos) {
                        throw;
                } // otherwise OK: table 'customers' does not exist, as expected
        }
}

//--------------------------------------

void
wr::sql::SessionTests::getURI() // static
{
        Session db;

        if (!db.uri().empty()) {
                throw TestFailure("db.uri() returned \"%s\", expected blank",
                                  db.uri());
        }

        db.open(defaultURI());

        if (db.uri() != defaultURI()) {
                throw TestFailure("db.uri() returned \"%s\", expected \"%s\"",
                                  db.uri(), defaultURI());
        }
}

//--------------------------------------

void
wr::sql::SessionTests::execSimple() // static
{
        Session db(":memory:");
        SampleDB::createSchema(db);
        size_t  n = 0;

        for (Row row: db.exec("SELECT * FROM sqlite_master")) {
                ++n;
        }

        // above query should be finalised by this point
        static const size_t N_ROWS_EXPECTED = 12;

        if (n != N_ROWS_EXPECTED) {
                throw TestFailure("query returned %u row(s), expected %u rows",
                                  n, N_ROWS_EXPECTED);
        }
}

//--------------------------------------

void
wr::sql::SessionTests::createSampleDBSchema() // static
{
        SampleDB db(defaultURI());
        db.dropSchema();
        db.createSchema();
        db.populateAllTables();
}

//--------------------------------------

void
wr::sql::SessionTests::hasObject1() // static
{
        SampleDB db(defaultURI());
        if (!db.hasObject("table", "customers")) {
                throw TestFailure("db.hasObject(\"table\", \"customers\") returned false, expected true");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::hasObject2() // static
{
        SampleDB db(defaultURI());
        if (db.hasObject("table", "foo")) {
                throw TestFailure("db.hasObject(\"table\", \"foo\") returned true, expected false");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::copyConstruct() // static
{
        SampleDB db1(defaultURI()), db2(db1);

        if (!db2.isOpen()) {
                throw TestFailure("db2.isOpen() returned false, expected true");
        }
        if (db2.uri() != db1.uri()) {
                throw TestFailure("db2.uri() returned \"%s\", expected \"%s\"",
                                  db2.uri(), db1.uri());
        }
}

//--------------------------------------

void
wr::sql::SessionTests::moveConstruct() // static
{
        Session db1(":memory:");
        SampleDB::createSchema(db1);
        Session db2(std::move(db1));

        if (db1.isOpen()) {
                throw TestFailure("db1.isOpen() returned true, expected false");
        }
        if (db1.hasObject("table", "customers")) {
                throw TestFailure("db1.hasObject(\"table\", \"customers\" returned true, expected false");
        }
        if (!db2.isOpen()) {
                throw TestFailure("db2.isOpen() returned false, expected true");
        }
        if (!db2.hasObject("table", "customers")) {
                throw TestFailure("db2.hasObject(\"table\", \"customers\" returned false, expected true");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::statement1() // static
try {
        SampleDB db(defaultURI());
        db.statement(999);  // should throw
        throw TestFailure("db.statement() with invalid ID did not throw exception");
} catch (std::invalid_argument &) {
        // OK, expected
} // other exception will be trapped and logged by TestManager

//--------------------------------------

void
wr::sql::SessionTests::statement2() // static
{
        static size_t GET_LONDON_PHONE_NO = registerStatement(
                        "SELECT phone FROM offices WHERE city = 'London'");

        SampleDB    db(defaultURI());
        size_t      n = 0;
        std::string phone_no;

        static const char *EXPECTED_PHONE_NO = "+44 20 7877 2041";

        for (Row row: db.exec(GET_LONDON_PHONE_NO)) {
                row.get(0, phone_no);
                ++n;
        }

        if (n != 1) {
                throw TestFailure("db.exec() returned %u rows, expected %u", n, 1);
        } else if (phone_no != EXPECTED_PHONE_NO) {
                throw TestFailure("db.exec() returned phone number \"%s\", expected \"%s\"",
                                  phone_no, EXPECTED_PHONE_NO);
        }
}

//--------------------------------------

void
wr::sql::SessionTests::statement3() // static
{
        static size_t GET_LONDON_PHONE_NO = registerStatement(
                        "SELECT phone FROM offices WHERE city = 'London'");

        SampleDB    db(defaultURI());
        size_t      n = 0;
        std::string phone_no;

        db.statement(GET_LONDON_PHONE_NO).finalize();

        static const char *EXPECTED_PHONE_NO = "+44 20 7877 2041";

        for (Row row: db.exec(GET_LONDON_PHONE_NO)) {
                row.get(0, phone_no);
                ++n;
        }

        if (n != 1) {
                throw TestFailure("db.exec() returned %u rows, expected %u", n, 1);
        } else if (phone_no != EXPECTED_PHONE_NO) {
                throw TestFailure("db.exec() returned phone number \"%s\", expected \"%s\"",
                                  phone_no, EXPECTED_PHONE_NO);
        }
}

//--------------------------------------

void
wr::sql::SessionTests::finalizeRegisteredStatements() // static
{
        static size_t GET_EMPLOYEES
                        = registerStatement("SELECT * FROM employees");

        SampleDB   db(defaultURI());
        Statement &get_employees = db.statement(GET_EMPLOYEES);

        if (!get_employees.isPrepared()) {
                throw TestFailure("statement not prepared after call to db.statement()");
        }

        db.finalizeRegisteredStatements();

        if (!get_employees.isFinalized()) {
                throw TestFailure("statement not finalized after call to db.finalizeRegisteredStatements()");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::resetRegisteredStatements() // static
{
        static size_t GET_EMPLOYEES
                        = registerStatement("SELECT * FROM employees");

        SampleDB db(defaultURI());
        Row      row = db.exec(GET_EMPLOYEES);

        if (!row) {
                throw TestFailure("query returned no results");
        }

        db.resetRegisteredStatements();

        if (row || row.statement()->isActive()) {
                throw TestFailure("statement not reset by call to db.resetRegisteredStatements()");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::serialisedInterrupt() // static
try {
        SampleDB db(defaultURI());
        size_t   n = 0;

        for (Row &row: db.exec("SELECT * FROM order_details")) {
                (void) row;
                if (++n > 5) {
                        db.interrupt();
                }
        }

        throw TestFailure("db.interrupt() had no effect");
} catch (sql::Interrupt &) {
        ; // OK
}

//--------------------------------------

void
wr::sql::SessionTests::concurrentInterrupt() // static
{
        SampleDB                     db(defaultURI());
        std::mutex                   m;
        std::condition_variable      cv;
        std::unique_lock<std::mutex> lock(m);
        size_t                       n = 0;

        auto task = std::async(std::launch::async,
                [](SampleDB &db, std::mutex &m, std::condition_variable &cv,
                   size_t &n) -> bool {
                        for (Row row: db.exec("SELECT * FROM order_details")) {
                                (void) row;
                                std::unique_lock<std::mutex> lock(m);
                                ++n;
                                cv.notify_all();
                        }
                        return false;
                },
                std::ref(db), std::ref(m), std::ref(cv), std::ref(n));

        cv.wait(lock, [&n]{ return n > 5; });
        db.interrupt();
        lock.unlock();

        try {
                task.get();
                throw TestFailure("db.interrupt() had no effect");
        } catch (sql::Interrupt &) {
                ;  // OK
        }
}

//---------------------------------------

void
wr::sql::SessionTests::lastInsertRowID() // static
{
        SampleDB db(defaultURI());

        db.exec("INSERT INTO employees (number, surname, forename, extension, "
                "email, office_code, reports_to, job_title) "
                "VALUES (9999, 'Smith', 'Jane', 'x4321', "
                          "'jsmith@classicmodelcars.com', 7, 1102, 'Payroll'), "
                       "(9998, 'Bloggs', 'Fred', 'x4320', "
                          "'fbloggs@classicmodelcars.com', 7, 1102, "
                          "'Software Engineer')");

        auto rowid = db.lastInsertRowID();

        if (rowid != 9998) {
                throw TestFailure("db.lastInsertRowID() returned %d, expected 9998",
                                  rowid);
        }
}

//--------------------------------------

void
wr::sql::SessionTests::rowsAffected1() // static
{
        SampleDB db(defaultURI());

        Statement q = db.exec("DELETE FROM payments WHERE customer_no=103");

        auto rows_affected = db.rowsAffected();

        if (rows_affected != 3) {
                throw TestFailure("db.rowsAffected() returned %d, expected 3",
                                  rows_affected);
        }
}

//--------------------------------------

void
wr::sql::SessionTests::rowsAffected2() // static
{
        SampleDB db(defaultURI());

        db.exec("DELETE FROM payments WHERE customer_no=999");

        auto rows_affected = db.rowsAffected();

        if (rows_affected != 0) {
                throw TestFailure("db.rowsAffected() returned %d, expected 0",
                                  rows_affected);
        }
}

//--------------------------------------

void
wr::sql::SessionTests::rowsAffected3() // static
{
        SampleDB db(defaultURI());

        db.exec("DELETE FROM customers WHERE number=103");

        auto rows_affected = db.rowsAffected();

        if (rows_affected != 1) {
                throw TestFailure("db.rowsAffected() returned %d, expected 1",
                                  rows_affected);
        }
}

//--------------------------------------

void
wr::sql::SessionTests::releaseMemory() // static
{
        SampleDB db;
        db.init(":memory:");
        db.exec("DELETE FROM customers WHERE number=103");
        db.releaseMemory();
}

//--------------------------------------

void
wr::sql::SessionTests::vacuum() // static
{
        SampleDB db;
        db.init(":memory:");
        db.exec("DELETE FROM customers WHERE number=103");
        db.vacuum();
}

//--------------------------------------

void
wr::sql::SessionTests::copyAssign() // static
{
        SampleDB db1(defaultURI()), db2;

        db2 = db1;

        if (!db2.isOpen()) {
                throw TestFailure("db2.isOpen() returned false, expected true");
        }
        if (db2.uri() != db1.uri()) {
                throw TestFailure("db2.uri() returned \"%s\", expected \"%s\"",
                                  db2.uri(), db1.uri());
        }
}

//--------------------------------------

void
wr::sql::SessionTests::copyAssignThis() // static
{
        SampleDB db(defaultURI());
        db = db;
        if (!db.isOpen()) {
                throw TestFailure("db.isOpen() returned false, expected true");
        }
        if (db.uri() != defaultURI()) {
                throw TestFailure("db.uri() returned \"%s\", expected \"%s\"",
                                  db.uri(), defaultURI());
        }
}

//--------------------------------------

void
wr::sql::SessionTests::moveAssign() // static
{
        SampleDB db1(defaultURI()), db2(":memory:");

        db2 = std::move(db1);

        if (db1.isOpen()) {
                throw TestFailure("db1.isOpen() returned true, expected false");
        }
        if (db1.uri() == defaultURI()) {
                throw TestFailure("db1.uri() returned \"%s\", expected \"%s\"",
                                  db1.uri(), defaultURI());
        }
        if (!db2.isOpen()) {
                throw TestFailure("db2.isOpen() returned false, expected true");
        }
        if (db2.uri() != defaultURI()) {
                throw TestFailure("db2.uri() returned \"%s\", expected \"%s\"",
                                  db2.uri(), defaultURI());
        }
}

//--------------------------------------

void
wr::sql::SessionTests::moveAssignThis() // static
{
        SampleDB db(defaultURI());
        db = std::move(db);
        if (!db.isOpen()) {
                throw TestFailure("db.isOpen() returned false, expected true");
        }
        if (db.uri() != defaultURI()) {
                throw TestFailure("db.uri() returned \"%s\", expected \"%s\"",
                                  db.uri(), defaultURI());
        }
}

//--------------------------------------

void
wr::sql::SessionTests::setProgressHandler() // static
{
        SampleDB db(defaultURI());
        bool     called = false;

        db.setProgressHandler([&called] { called = true; return false; });

        for (Row r: db.exec("SELECT * FROM order_details det "
                            "JOIN orders ord ON ord.number = det.order_no "
                            "JOIN customers cus ON cus.number = ord.number")) {
                ;
        }

        if (!called) {
                throw TestFailure("progress handler not called");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::clearProgressHandler() // static
{
        SampleDB db(defaultURI());
        bool     called = false;

        db.setProgressHandler([&called] { called = true; return false; });
        db.setProgressHandler({});

        for (Row r: db.exec("SELECT * FROM order_details det "
                            "JOIN orders ord ON ord.number = det.order_no "
                            "JOIN customers cus ON cus.number = ord.number")) {
                ;
        }

        if (called) {
                throw TestFailure("progress handler called in error");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::onFinalCommit() // static
{
        SampleDB db(defaultURI());
        bool     called = false;

        db.beginTransaction([&](Transaction &) {
                db.beginTransaction([&](Transaction &) {
                        db.onFinalCommit([&] { called = true; });
                });

                if (called) {
                        throw TestFailure("commit hook function called in error");
                }
        });

        if (!called) {
                throw TestFailure("commit hook function not called");
        }
}

//--------------------------------------

void
wr::sql::SessionTests::onRollback() // static
{
        SampleDB db(defaultURI());
        bool     called = false;

        db.beginTransaction([&](Transaction &txn) {
                db.onRollback([&] { called = true; });
                txn.rollback();
        });

        if (!called) {
                throw TestFailure("rollback hook function not called");
        }
}
