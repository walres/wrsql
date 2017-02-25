/**
 * \file StatementTests.cxx
 *
 * \brief Unit test module for class \c wr::sql::Statement
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
#include <cmath>
#include <iostream>
#include <limits>

#include <wrutil/codecvt.h>
#include <wrutil/optional.h>
#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/Statement.h>

#include "SampleDB.h"
#include "SQLTestManager.h"


namespace wr {
namespace sql {


class StatementTests : public SQLTestManager
{
public:
        using base_t = SQLTestManager;

        StatementTests(int argc, const char **argv) :
                base_t("Statement", argc, argv)
                { db_.init(defaultURI()); setTimeout(10000); }

        virtual ~StatementTests() { db_.close(); }

        int runAll();

        static void defaultConstruct(),
                    prepValid(),
                    prepBlank(),
                    prepInvalidSQL(),
                    prepInvalidSession(),
                    prepWithTail(),
                    constructPrepped(),
                    constructPreppedInvalidSQL(),
                    constructPreppedInvalidSession(),
                    constructPreppedWithTail(),
                    isPrepared(),
                    toBool(),
                    end(),
                    beginUnprepared(),
                    beginPrepared(),
                    beginEmpty(),
                    beginDuringFetch(),
                    beginAfterFetch(),
                    nextUnprepared(),
                    nextBeforeFetch(),
                    nextDuringFetch(),
                    nextIsEnd(),
                    nextAfterFetch(),
                    currentRowUnprepared(),
                    currentRowBeforeFetch(),
                    currentRowDuringFetch(),
                    currentRowAfterFetch(),
                    isActive(),
                    isFinalized(),
                    retrieveSession(),
                    bindInvalidIndex(),
                    bindNull(),
                    bindNullPtr(),
                    bindNullOpt();

        template <typename NumericType> static void bindMinMax();
        template <typename FloatType> static void bindNaN();
        template <typename FloatType> static void bindInfinity();
        template <typename StrType>
                static void bindString(StrType city, StrType expected_result);

        static void bindBlob(),
                    bindBlobWithFree(),
                    bindBlobDupFree(),
                    bindOptional(),
                    bindAfterFetch(),
                    bindUserType(),
                    variadicBind(),
                    bindDuringActiveStatement1(),
                    bindDuringActiveStatement2(),
                    resetUnpreppedStatement(),
                    resetPreppedStatement(),
                    resetPreservesBindings(),
                    resetBeginFetch(),
                    resetDuringFetch(),
                    resetAfterFetch(),
                    resetMakesInactive(),
                    copyConstruct(),
                    moveConstruct(),
                    copyAssign(),
                    moveAssign(),
                    retrieveSQLUnprepared(),
                    retrieveSQL(),
                    registerStatement(),
                    reRegisterStatement(),
                    getNumRegisteredStatements(),
                    retrieveRegisteredStatementSQL();

private:
        static SampleDB db_;
};


SampleDB StatementTests::db_;


} // namespace sql
} // namespace wr

//--------------------------------------

int
main(
        int          argc,
        const char **argv
)
{
        return wr::sql::StatementTests(argc, argv).runAll();
}

//--------------------------------------

int
wr::sql::StatementTests::runAll()
{
        run("defaultConstruct", 1, &defaultConstruct);
        run("prepare", 1, &prepValid);
        run("prepare", 2, &prepBlank);
        run("prepare", 3, &prepInvalidSQL);
        run("prepare", 4, &prepInvalidSession);
        run("prepare", 5, &prepWithTail);
        run("constructPrepped", 1, &constructPrepped);
        run("constructPrepped", 2, &constructPreppedInvalidSQL);
        run("constructPrepped", 3, &constructPreppedInvalidSession);
        run("constructPrepped", 4, &constructPreppedWithTail);
        run("isPrepared", 1, &isPrepared);
        run("toBool", 1, &toBool);
        run("end", 1, &end);
        run("begin", 1, &beginUnprepared);
        run("begin", 2, &beginPrepared);
        run("begin", 3, &beginEmpty);
        run("begin", 4, &beginDuringFetch);
        run("begin", 5, &beginAfterFetch);
        run("next", 1, &nextUnprepared);
        run("next", 2, &nextBeforeFetch);
        run("next", 3, &nextDuringFetch);
        run("next", 4, &nextIsEnd);
        run("next", 5, &nextAfterFetch);
        run("currentRow", 1, &currentRowUnprepared);
        run("currentRow", 2, &currentRowBeforeFetch);
        run("currentRow", 3, &currentRowDuringFetch);
        run("currentRow", 4, &currentRowAfterFetch);
        run("isActive", 1, &isActive);
        run("isFinalized", 1, &isFinalized);
        run("retrieveSession", 1, &retrieveSession);
        run("bindInvalidIndex", 1, &bindInvalidIndex);
        run("bindNull", 1, &bindNull);
        run("bindNullPtr", 1, &bindNullPtr);
        run("bindNullOpt", 1, &bindNullOpt);
        run("bindChar", 1, &bindMinMax<char>);
        run("bindUChar", 1, &bindMinMax<unsigned char>);
        run("bindShort", 1, &bindMinMax<short>);
        run("bindUShort", 1, &bindMinMax<unsigned short>);
        run("bindInt", 1, &bindMinMax<int>);
        run("bindUInt", 1, &bindMinMax<unsigned int>);
        run("bindLong", 1, &bindMinMax<long>);
        run("bindULong", 1, &bindMinMax<unsigned long>);
        run("bindLongLong", 1, &bindMinMax<long long>);
        run("bindULongLong", 1, &bindMinMax<unsigned long long>);
        run("bindFloat", 1, &bindMinMax<float>);
        run("bindFloat", 2, &bindNaN<float>);
        run("bindFloat", 3, &bindInfinity<float>);
        run("bindDouble", 1, &bindMinMax<double>);
        run("bindDouble", 2, &bindNaN<double>);
        run("bindDouble", 3, &bindInfinity<double>);
        run("bindRawString", 1, &bindString<const char *>,
                u8"Montr\u00e9al", u8"Qu\u00e9bec Home Shopping Network");
        run("bindStdString", 1, &bindString<std::string>,
                u8"M\u00fcnchen", u8"Franken Gifts, Co");
        run("bindStringView", 1, &bindString<string_view>,
                u8"Lule\u00e5", u8"Volvo Model Replicas, Co");
        run("bindU8StringView", 1, &bindString<u8string_view>,
                u8"Br\u00e4cke", u8"Scandinavian Gift Ideas");
        run("bindBlob", 1, &bindBlob);
        run("bindBlob", 2, &bindBlobWithFree);
        run("bindBlob", 3, &bindBlobDupFree);
        run("bindOptional", 1, &bindOptional);
        run("bindAfterFetch", 1, &bindAfterFetch);
        run("bindUserType", 1, &bindUserType);
        run("variadicBind", 1, &variadicBind);
        run("bindDuringActiveStatement", 1, &bindDuringActiveStatement1);
        run("bindDuringActiveStatement", 2, &bindDuringActiveStatement2);
        run("reset", 1, &resetUnpreppedStatement);
        run("reset", 2, &resetPreppedStatement);
        run("reset", 3, &resetPreservesBindings);
        run("reset", 4, &resetBeginFetch);
        run("reset", 5, &resetDuringFetch);
        run("reset", 6, &resetAfterFetch);
        run("reset", 7, &resetMakesInactive);
        run("copyConstruct", 1, &copyConstruct);
        run("moveConstruct", 1, &moveConstruct);
        run("copyAssign", 1, &copyAssign);
        run("moveAssign", 1, &moveAssign);
        run("sql", 1, &retrieveSQLUnprepared);
        run("sql", 2, &retrieveSQL);
        run("registerStatement", 1, &registerStatement);
        run("registerStatement", 2, &reRegisterStatement);
        run("numRegisteredStatements", 1, &getNumRegisteredStatements);
        run("registeredStatement", 1, &retrieveRegisteredStatementSQL);
        
        return failed() ? EXIT_FAILURE : EXIT_SUCCESS;
}

//--------------------------------------

void
wr::sql::StatementTests::defaultConstruct() // static
{
        Statement null_stmt;
        (void) null_stmt;  // mark as unused to silence warnings
}

//--------------------------------------

void
wr::sql::StatementTests::prepValid() // static
{
        Statement query;
        query.prepare(db_, "SELECT * FROM customers");
}

//--------------------------------------

void
wr::sql::StatementTests::prepBlank() // static
{
        Statement blank;
        blank.prepare(db_, "");
}

//--------------------------------------

void
wr::sql::StatementTests::prepInvalidSQL() // static
{
        Statement invalid;
        try {
                invalid.prepare(db_, "FIND foo IN nonsense");
                throw TestFailure("prepare did not throw exception for invalid statement");
        } catch (Error &) {
                ;  // OK, expected
        }
}

//--------------------------------------

void
wr::sql::StatementTests::prepInvalidSession() // static
{
        Statement query;
        Session duff_db;
        try {
                query.prepare(duff_db, "SELECT * from customers");
                throw TestFailure("prepare did not throw exception for invalid Session");
        } catch (Error &) {
                ;  // OK, expected
        }
}

//--------------------------------------

void
wr::sql::StatementTests::prepWithTail() // static
{
        static const std::string HEAD = "SELECT * FROM orders;",
                                 TAIL = "SELECT * FROM payments;",
                                 SQL  = HEAD + '\n' + TAIL;
        Statement query;
        u8string_view returned_tail;
        query.prepare(db_, SQL, returned_tail);

        if (returned_tail != TAIL) {
                throw TestFailure("prepare() returned tail \"%s\", expected \"%s\"",
                                  returned_tail, TAIL);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::constructPrepped() // static
{
        Statement query(db_, "SELECT * FROM payments");
        (void) query;  // mark as unused to silence compiler warning
}

//--------------------------------------

void
wr::sql::StatementTests::constructPreppedInvalidSQL() // static
try {
        Statement query(db_, "GIMME * FROM *");
        (void) query;  // mark as unused to silence compiler warning
        throw TestFailure("constructor did not throw exception for invalid statement");
} catch (Error &) {
        // OK, expected
}

//--------------------------------------

void
wr::sql::StatementTests::constructPreppedInvalidSession() // static
{
        Session duff_db;

        try {
                Statement query(duff_db, "SELECT * FROM products;");
                (void) query;  // mark as unused to silence compiler warning
                throw TestFailure("constructor did not throw exception for invalid Session");
        } catch (Error &) {
                // OK, expected
        }
}

//--------------------------------------

void
wr::sql::StatementTests::constructPreppedWithTail() // static
{
        static const std::string HEAD = "SELECT * FROM orders;",
                                 TAIL = "SELECT * FROM payments;",
                                 SQL  = HEAD + '\n' + TAIL;

        u8string_view returned_tail;
        Statement query(db_, SQL, returned_tail);
        (void) query;  // mark as unused to silence compiler warning

        if (returned_tail != TAIL) {
                throw TestFailure("prepare() returned tail \"%s\", expected \"%s\"",
                                  returned_tail, TAIL);
        }

}

//--------------------------------------

void
wr::sql::StatementTests::isPrepared() // static
{
        Statement query;

        if (query.isPrepared()) {
                throw TestFailure("query.isPrepared() returned true before call to prepare()");
        }

        try {
                query.prepare(db_, "duh");
        } catch (Error &) {
                // OK, expected
        }

        if (query.isPrepared()) {
                throw TestFailure("query.isPrepared() returned true after calling prepare() with invalid statement");
        }

        query.prepare(db_, "SELECT * FROM offices WHERE city='New York'");

        if (!query.isPrepared()) {
                throw TestFailure("query.isPrepared() returned false after calling prepare() with valid statement");
        }

        query.finalize();

        if (query.isPrepared()) {
                throw TestFailure("query.isPrepared() returned true after call to query.finalize()");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::toBool() // static
{
        Statement query;

        if (query) {
                throw TestFailure("query.operator bool() returned true before call to prepare()");
        }

        try {
                query.prepare(db_, "duh");
        } catch (Error &) {
                // OK, expected
        }

        if (query) {
                throw TestFailure("query.operator bool() returned true after calling prepare() with invalid statement");
        }

        query.prepare(db_, "SELECT * FROM offices WHERE city='New York'");

        if (!query) {
                throw TestFailure("query.operator bool() returned false after calling prepare() with valid statement");
        }

        query.finalize();

        if (query) {
                throw TestFailure("query.operator bool() returned true after call to query.finalize()");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::end() // static
{
        Statement query;
        Row       end1 = query.end();

        query.prepare(db_, "SELECT * FROM offices WHERE city = 'Paris'");

        Row end2 = query.end();
        if (end2 != end1) {
                throw TestFailure("end2 != end1, should be (end2 == end1)");
        }

        query.begin();

        Row end3 = query.end();
        if (end3 != end1) {
                throw TestFailure("end3 != end1, should be (end3 == end1)");
        }

        query.next();

        if (query.isActive()) {
                throw TestFailure("query.isActive() returned true after final row");
        }

        Row end4 = query.end();
        if (end4 != end1) {
                throw TestFailure("end4 != end1, should be (end4 == end1)");
        }

        query.finalize();

        Row end5 = query.end();
        if (end5 != end1) {
                throw TestFailure("end5 != end1, should be (end5 == end1)");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::beginUnprepared() // static
{
        Statement stmt;
        if (stmt.begin() != stmt.end()) {
                throw TestFailure("(stmt.begin() != stmt.end()) for unprepared statement");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::beginPrepared() // static
{
        Statement query(db_, "SELECT name FROM customers WHERE number=103");
        Row   row = query.begin();

        if (row) {
                auto name = row.get<u8string_view>(0);
                static auto EXPECTED_NAME = u8"Atelier graphique";

                if (name != EXPECTED_NAME) {
                        throw TestFailure("row(0) returned \"%s\", expected \"%s\"",
                                          name, EXPECTED_NAME);
                }
        } else {
                throw TestFailure("begin() did not return a row for valid query of sample data");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::beginEmpty() // static
{
        Statement query(db_,
                        "SELECT number FROM customers WHERE name='nobody'");
        Row       row = query.begin();

        if (row != query.end()) {
                throw TestFailure("query.begin() did not return row equal to query.end() for query with no results");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::beginDuringFetch() // static
{
        Statement query(db_, "SELECT name FROM customers ORDER BY number DESC");
        Row       row = query.begin();

        ++row;
        ++row;
        row = query.begin();

        auto name = row.get<u8string_view>(0);
        static auto EXPECTED_NAME = u8"Kelly's Gift Shop";

        if (name != EXPECTED_NAME) {
                throw TestFailure("row(0) after second call to begin() returned \"%s\", expected \"%s\"",
                                  name, EXPECTED_NAME);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::beginAfterFetch() // static
{
        Statement query(db_, "SELECT name FROM product_lines ORDER BY name");

        for (Row row: query) {
                (void) row;  // mark as unused to silence compiler warning
        }

        if (query.isActive()) {
                throw TestFailure("query.isActive() returned true after initial fetch");
        }

        Row row = query.begin();
        auto name = row.get<u8string_view>(0);
        static auto EXPECTED_NAME = u8"Classic Cars";

        if (name != EXPECTED_NAME) {
                throw TestFailure("row(0) after second call to begin() returned \"%s\", expected \"%s\"",
                                  name, EXPECTED_NAME);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::nextUnprepared() // static
{
        Statement query;
        if (query.next() != query.end()) {
                throw TestFailure("(query.next() != query.end()) for unprepared Statement object");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::nextBeforeFetch() // static
{
        Statement query(db_, "SELECT name FROM customers ORDER BY number");
        Row       result = query.next();

        if (result) {
                throw TestFailure("query.next() returned result \"%s\" for inactive query, expected no result",
                                  result.get<string_view>("name"));
        }
}

//--------------------------------------

void
wr::sql::StatementTests::nextDuringFetch() // static
{
        Statement query(db_, "SELECT forename FROM employees WHERE surname='Patterson' ORDER BY forename");
        Row row(query.begin());

        static auto EXPECTED_1ST_FORENAME = "Mary";
        auto forename = row.get<u8string_view>(0);

        if (forename != EXPECTED_1ST_FORENAME) {
                throw TestFailure("first row returned is \"%s\", expected \"%s\"",
                                  forename, EXPECTED_1ST_FORENAME);
        }

        row = query.next();
        static auto EXPECTED_2ND_FORENAME = "Steve";
        forename = row.get<u8string_view>(0);

        if (forename != EXPECTED_2ND_FORENAME) {
                throw TestFailure("second row returned is \"%s\", expected \"%s\"",
                                  forename, EXPECTED_2ND_FORENAME);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::nextIsEnd() // static
{
        Statement query(db_,
                "SELECT forename FROM employees WHERE surname='Firrelli'");
        Row   row = query.begin();

        static auto EXPECTED_1ST_FORENAME = "Jeff";
        auto forename = row.get<u8string_view>(0);

        if (forename != EXPECTED_1ST_FORENAME) {
                throw TestFailure("first row returned is \"%s\", expected \"%s\"",
                                  forename, EXPECTED_1ST_FORENAME);
        }

        row = query.next();
        static auto EXPECTED_2ND_FORENAME = "Julie";
        forename = row.get<u8string_view>(0);

        if (forename != EXPECTED_2ND_FORENAME) {
                throw TestFailure("second row returned is \"%s\", expected \"%s\"",
                                  forename, EXPECTED_2ND_FORENAME);
        }

        if (query.next() != query.end()) {
                throw TestFailure("(query.next() != query.end()) after last row");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::nextAfterFetch() // static
{
        Statement query(db_, "SELECT code FROM offices WHERE city='London'");

        Row row = query.begin();

        if (!row) {
                throw TestFailure("query returned no results");
        }

        int code = row.get<int>(0);
        static const int EXPECTED = 7;

        if (code != EXPECTED) {
                throw TestFailure("query returned result %d, expected %d",
                                  code, EXPECTED);
        }
        if (query.next() != query.end()) {
                throw TestFailure("first call to query.next() did not return query.end()");
        }
        if (query.next() != query.end()) {
                throw TestFailure("second call to query.next() did not return query.end()");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::currentRowUnprepared() // static
{
        Statement stmt;
        if (stmt.currentRow() != stmt.end()) {
                throw TestFailure("(stmt.currentRow() != stmt.end()) for unprepared statement");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::currentRowBeforeFetch() // static
{
        Statement query(db_, "SELECT * FROM customers");
        if (query.currentRow() != query.end()) {
                throw TestFailure("(query.currentRow() != query.end()) before call to begin()");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::currentRowDuringFetch() // static
{
        Statement query(db_,
                "SELECT forename FROM employees WHERE surname='Patterson' "
                        "ORDER BY forename");
        Row row(query.begin());

        auto expected_forename = row.get<u8string_view>(0);
        auto current_forename = query.currentRow().get<u8string_view>(0);

        if (current_forename != expected_forename) {
                throw TestFailure("first call to query.currentRow() returned \"%s\", expected \"%s\"",
                                  current_forename, expected_forename);
        }

        row = query.next();
        expected_forename = row.get<u8string_view>(0);
        current_forename = query.currentRow().get<u8string_view>(0);

        if (current_forename != expected_forename) {
                throw TestFailure("second row returned is \"%s\", expected \"%s\"",
                                  current_forename, expected_forename);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::currentRowAfterFetch() // static
{
        Statement query(db_, "SELECT * FROM offices WHERE city='Tokyo'");
        query.begin();  // one and only row
        query.next();
        if (query.currentRow() != query.end()) {
                throw TestFailure("(query.next() != query.end()) after last row");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::isActive() // static
{
        Statement stmt;

        if (stmt.isActive()) {
               throw TestFailure("stmt.isActive() returned true for unprepared statement");
        }

        stmt.prepare(db_, "SELECT * FROM customers");

        if (stmt.isActive()) {
                throw TestFailure("stmt.isActive() returned true before call to begin()");
        }

        stmt.begin();

        if (!stmt.isActive()) {
                throw TestFailure("stmt.isActive() returned false during fetch");
        }

        while (stmt.currentRow()) {
                ++stmt.currentRow();
        }

        if (stmt.isActive()) {
                throw TestFailure("stmt.isActive() returned true after fetch completed");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::isFinalized() // static
{
        Statement stmt;

        if (!stmt.isFinalized()) {
                throw TestFailure("stmt.isFinalized() returned false for default-constructed Statement object");
        }

        try {
                stmt.prepare(db_, "duh");
        } catch (Error &) {
                // OK, expected
        }

        if (!stmt.isFinalized()) {
                throw TestFailure("stmt.isFinalized() returned false after call to prepare() threw exception");
        }

        stmt.prepare(db_, "SELECT * FROM customers");

        if (stmt.isFinalized()) {
                throw TestFailure("stmt.isFinalized() returned true for prepared statement");
        }

        stmt.begin();
        stmt.finalize();

        if (!stmt.isFinalized()) {
                throw TestFailure("stmt.isFinalized() returned false for explicitly finalized statement");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::retrieveSession() // static
{
        Statement stmt;

        if (stmt.session() != nullptr) {
                throw TestFailure("stmt.session() returned non-null value for default-constructed Statement object");
        }

        stmt.prepare(db_, "SELECT * FROM customers");

        if (stmt.session() != &db_) {
                throw TestFailure("stmt.session() returned unexpected value after call to prepare()");
        }

        stmt.finalize();

        if (stmt.session() != nullptr) {
                throw TestFailure("stmt.session() returned non-null value for explicitly finalized Statement object");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindInvalidIndex() // static
{
        Statement query(db_, "SELECT * FROM employees WHERE surname LIKE ?1");

        try {
                query.bindNull(2);
                throw TestFailure("bind to invalid index did not throw exception");
        } catch (std::invalid_argument &) {
                // OK, expected
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindNull() // static
{
        Statement query(db_, "SELECT ?1");
        query.bindNull(1);
        if (!query.begin().isNull(0)) {
                throw TestFailure("bound null value not returned in result set");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindNullPtr() // static
{
        Statement query(db_, "SELECT ?1");
        query.bind(1, nullptr);
        if (!query.begin().isNull(0)) {
                throw TestFailure("bound null value not returned in result set");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindNullOpt() // static
{
        Statement query(db_, "SELECT ?1");
        query.bind(1, nullopt);
        if (!query.begin().isNull(0)) {
                throw TestFailure("bound null value not returned in result set");
        }
}

//--------------------------------------

template <typename NumericType> void
wr::sql::StatementTests::bindMinMax() // static
{
        Statement query(db_, "SELECT ?1");

        static const auto min = std::numeric_limits<NumericType>::min();
        query.bind(1, min);
        auto value = query.begin().get<NumericType>(0);
        if (value != min) {
                throw TestFailure("bound value %s returned as %s", min, value);
        }

        static const auto max = std::numeric_limits<NumericType>::max();
        query.reset();
        query.bind(1, max);
        query.begin().get(0, value);
        if (value != max) {
                throw TestFailure("bound value %s returned as %s", max, value);
        }
}

//--------------------------------------

template <typename FloatType> void
wr::sql::StatementTests::bindNaN() // static
{
        Statement query(db_, "SELECT ?1");
        query.bind(1, std::numeric_limits<FloatType>::quiet_NaN());
        auto value = query.begin().get<FloatType>(0);
        if (!std::isnan(value)) {
                throw TestFailure("bound NaN value returned as %f", value);
        }
}

//--------------------------------------

template <typename FloatType> void
wr::sql::StatementTests::bindInfinity() // static
{
        Statement query(db_, "SELECT ?1");
        query.bind(1, std::numeric_limits<FloatType>::infinity());
        auto value = query.begin().get<FloatType>(0);
        if (!std::isinf(value) || !(value > 0)) {
                throw TestFailure("bound positive infinite value returned as %f",
                                  value);
        }

        query.reset().bind(1, -std::numeric_limits<FloatType>::infinity());
        query.begin().get(0, value);
        if (!std::isinf(value) || !(value < 0)) {
                throw TestFailure("bound negative infinite value returned as %f",
                                  value);
        }
}

//--------------------------------------

template <typename StrType> void
wr::sql::StatementTests::bindString(
        StrType city,
        StrType expected_result
)
{
        Statement query(db_, "SELECT ?1");
        StrType arg = "";
        query.bind(1, arg);
        auto text = query.begin().get<u8string_view>(0);
        if (text != arg) {
                throw TestFailure("bound empty string returned as \"%s\"",
                                  text);
        }

        Statement query2(db_, "SELECT name FROM customers WHERE city=?1");
        Row row = query2.bind(1, city).begin();

        if (!row) {
                throw TestFailure("no result for city \"%s\", expected \"%s\"",
                                  utf8_narrow_cvt().from_utf8(city),
                                  utf8_narrow_cvt().from_utf8(expected_result));
        } else {
                row.get(0, text);
                if (text != expected_result) {
                        throw TestFailure("got customer \"%s\" for city \"%s\", expected \"%s\"",
                                utf8_narrow_cvt().from_utf8(text),
                                utf8_narrow_cvt().from_utf8(city),
                                utf8_narrow_cvt().from_utf8(expected_result));
                }
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindBlob() // static
{
        db_.exec(u8"CREATE TEMP TABLE blob_test(id INTEGER PRIMARY KEY, "
                                               "data BLOB)");

        Statement ins(db_, "REPLACE INTO blob_test (id, data) VALUES (?1, ?2)");

        static const int64_t ID = 1;
        static const auto STR = u8"The quick brown fox jumps over the lazy dog";

        ins.bind(1, ID);
        ins.bind(2, STR, strlen(STR));  // bind string as blob
        ins.begin();

        auto inserted_id = db_.lastInsertRowID();

        if (inserted_id != 1) {
                throw TestFailure("db_.lastInsertRowID() returned %d, expected %d",
                                  inserted_id, ID);
        }

        Statement get(db_, "SELECT data FROM blob_test WHERE id=?1");

        auto got = get.bind(1, ID).begin().get<u8string_view>(0);

        if (got != STR) {
                throw TestFailure("retrieved blob value \"%s\", expected \"%s\"",
                                  got, STR);
        }

        // finalize statements to prevent DROP TABLE blocking below
        get.finalize();
        ins.finalize();
        db_.finalizeRegisteredStatements();

        db_.exec("DROP TABLE blob_test");
}

//--------------------------------------

void
wr::sql::StatementTests::bindBlobWithFree() // static
{
        db_.exec(u8"CREATE TEMP TABLE blob_test(id INTEGER PRIMARY KEY, "
                                               "data BLOB)");

        Statement ins(db_, "REPLACE INTO blob_test (id, data) VALUES (?1, ?2)");

        static const int64_t ID = 1;
        static const auto STR = u8"The quick brown fox jumps over the lazy dog";

        auto len = strlen(STR);
        auto buf = new char[len];
        std::copy_n(STR, len, buf);

        ins.bind(1, ID);
        ins.bind(2, buf, len, [&buf](void *) { delete [] buf; buf = nullptr; });
                                                        // bind string as blob
        ins.begin();
        ins.clearBindings();

        if (buf) {
                throw TestFailure("free function not called");
        }

        auto inserted_id = db_.lastInsertRowID();

        if (inserted_id != 1) {
                throw TestFailure("db_.lastInsertRowID() returned %d, expected %d",
                                  inserted_id, ID);
        }

        Statement     get     (db_, "SELECT data FROM blob_test WHERE id=?1");
        auto          result   = get.bind(1, ID).begin();
        auto          raw_data = result.get<const void *>(0);
        u8string_view data    (static_cast<const char *>(raw_data),
                               result.colSize(0));
        if (data != STR) {
                throw TestFailure("retrieved blob value \"%s\", expected \"%s\"",
                                  data, STR);
        }

        // finalize statements to prevent DROP TABLE blocking below
        get.finalize();
        ins.finalize();
        db_.finalizeRegisteredStatements();

        db_.exec("DROP TABLE blob_test");
}

//--------------------------------------

void
wr::sql::StatementTests::bindBlobDupFree() // static
{
        Statement query(db_, "SELECT ?1, ?2");

        static const auto STR = u8"The quick brown fox jumps over the lazy dog";

        query.bind(1, STR, strlen(STR), [](void *) {});

        try {
                query.bind(2, STR, strlen(STR), [](void *) {});
                throw TestFailure("exception not thrown on duplicate registration of blob data");
        } catch (Error &) {
                // OK, expected
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindOptional() // static
{
        optional<int64_t> reports_to;

        Statement query(db_, "SELECT number FROM employees WHERE "
                                "(reports_to IS NULL AND ?1 IS NULL) "
                                "OR (reports_to = ?1)");
        reports_to = {};
        Row result = query.bind(1, reports_to).begin();
        
        int64_t expected_id = 1002;

        if (result.get<int64_t>(0) != expected_id) {
                throw TestFailure("Employee query for NULL reports_to column returned ID %d, expected %d",
                                  result.get<int64_t>(0), expected_id);
        }

        reports_to = 1621;
        result = query.reset().bind(1, reports_to).begin();
        expected_id = 1625;

        if (result.get<int64_t>(0) != expected_id) {
                throw TestFailure("Employee query for reports_to column value 1621 returned ID %d, expected %d",
                                  result.get<int64_t>(0), expected_id);
        }
}

//--------------------------------------
/*
 * ensure bind() no longer throws after last row fetched
 */
void
wr::sql::StatementTests::bindAfterFetch() // static
{
        Statement query(db_, "SELECT ?1");
        query.bindNull(1);
        Row result(query.begin());
        ++result;
        query.bind(1, 123);  // should not throw
        result = query.begin();
}

//--------------------------------------

enum OrderStatus
{
        IN_PROCESS,
        SHIPPED,
        ON_HOLD,
        DISPUTED,
        RESOLVED,
        CANCELLED
};

//--------------------------------------

namespace wr {
namespace sql {


template <> auto
wr::sql::Statement::bind(
        int                param_no,
        const OrderStatus &status
) -> this_t &
{
        const char *text;

        switch (status) {
        case IN_PROCESS: text = "In Process"; break;
        case SHIPPED: text = "Shipped"; break;
        case ON_HOLD: text = "On Hold"; break;
        case DISPUTED: text = "Disputed"; break;
        case RESOLVED: text = "Resolved"; break;
        case CANCELLED: text = "Cancelled"; break;
        default:
                throw std::invalid_argument(printStr(
                        "unknown status code %d", static_cast<int>(status)));
        }

        return bind(param_no, text);
}

//--------------------------------------

template <> OrderStatus
Row::get<OrderStatus>(
        int col_no
) const
{
        auto value = get<u8string_view>(col_no);

        if (value == "In Process") {
                return IN_PROCESS;
        } else if (value == "Shipped") {
                return SHIPPED;
        } else if (value == "On Hold") {
                return ON_HOLD;
        } else if (value == "Disputed") {
                return DISPUTED;
        } else if (value == "Resolved") {
                return RESOLVED;
        } else if (value == "Cancelled") {
                return CANCELLED;
        }

        throw SQLException(printStr("'%s' is not a valid order status",
                                    utf8_narrow_cvt().from_utf8(value)));
}


} // namespace sql
} // namespace wr

//--------------------------------------

void
wr::sql::StatementTests::bindUserType() // static
{
        static const std::set<int64_t> DISPUTED_IDS = { 10406, 10415, 10417 },
                                       ON_HOLD_IDS  = { 10334, 10401,
                                                        10407, 10414 };
        Statement get_orders_with_status(db_,
                "SELECT number FROM orders WHERE status=?1 ORDER BY number");
        std::set<int64_t> ids;

        get_orders_with_status.bind(1, DISPUTED);

        for (Row row: get_orders_with_status) {
                ids.insert(row.get<int64_t>(0));
        }

        if (get_orders_with_status.isActive()) {
                throw TestFailure("query still active after first fetch");
        }

        if (ids != DISPUTED_IDS) {
                std::string id_list;
                const char *sep = "";
                for (auto id: ids) {
                        id_list += sep + std::to_string(id);
                        sep = ", ";
                }
                throw TestFailure("Query of disputed orders returned {%s}, expected {10406, 10415, 10417}",
                                  id_list);
        }

        ids.clear();

        for (Row row: get_orders_with_status.bind(1, ON_HOLD)) {
                ids.insert(row.get<int64_t>(0));
        }

        if (ids != ON_HOLD_IDS) {
                std::string id_list;
                const char *sep = "";
                for (auto id: ids) {
                        id_list += sep + std::to_string(id);
                        sep = ", ";
                }
                throw TestFailure("Query of on-hold orders returned {%s}, expected {10334, 10401, 10407, 10414}",
                                  id_list);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::variadicBind() // static
{
        Statement get_customer_orders(db_, "SELECT number FROM orders "
                                           "WHERE orders.customer_no=?1 "
                                           "AND status=?2 ORDER BY date");

        get_customer_orders.bindAll(496, CANCELLED);
        Row result = get_customer_orders.begin();
        int64_t expected_id = 10179;

        if (result.get<int64_t>(0) != expected_id) {
                throw TestFailure("query returned order number %d, expected %d",
                                  result.get<int64_t>(0), expected_id);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindDuringActiveStatement1() // static
{
        static const char * const CITY[2] = { "NYC", "Sydney" };

        Statement stmt(db_, "SELECT * FROM offices WHERE city=?1");
        Row row = stmt.begin(CITY[0]);
                /* statement remains active until an attempt to fetch beyond
                   the last row, which we never do in this test */

        if (!row) {
                throw TestFailure("no office rows returned for city \"%s\"",
                                  CITY[0]);
        }

        static const int EXPECTED_OFFICE_CODE[2] = { 3, 6 };
        auto office_code = row.get<int>("code");

        if (office_code != EXPECTED_OFFICE_CODE[0]) {
                throw TestFailure("got office code %d for city \"%s\", expected %d",
                                  office_code, CITY[0],
                                  EXPECTED_OFFICE_CODE[0]);
        }

        row = stmt.begin(CITY[1]);  // should not throw

        if (!row) {
                throw TestFailure("no office rows returned for city \"%s\"",
                                  CITY[1]);
        }

        office_code = row.get<int>("code");

        if (office_code != EXPECTED_OFFICE_CODE[1]) {
                throw TestFailure("got office code %d for city \"%s\", expected %d",
                                  office_code, CITY[1],
                                  EXPECTED_OFFICE_CODE[1]);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::bindDuringActiveStatement2() // static
{
        Statement stmt1(db_, "SELECT * FROM offices WHERE city=?1");
        stmt1.begin("NYC");
                /* statement remains active until an attempt to fetch beyond
                   the last row, which we never do in this test */

        static const char * const SURNAME = "Thompson";

        Statement stmt2(db_, "SELECT * FROM employees WHERE surname=?1");
        Row       row  = stmt2.begin(SURNAME); // should not throw

        if (!row) {
                throw TestFailure("no employee rows returned for surname \"%s\"",
                                  SURNAME);
        }

        static const ID EXPECTED_EMPLOYEE_NO = 1166;

        auto employee_no = row.get<ID>("number");

        if (employee_no != EXPECTED_EMPLOYEE_NO) {
                throw TestFailure("got employee number %d for surname \"%s\", expected %d",
                                  employee_no, SURNAME, EXPECTED_EMPLOYEE_NO);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::resetUnpreppedStatement() // static
{
        Statement stmt;
        stmt.reset();
}

//--------------------------------------

void
wr::sql::StatementTests::resetPreppedStatement() // static
{
        Statement stmt(db_, "SELECT * FROM customers");
        stmt.reset();
}

//--------------------------------------

void
wr::sql::StatementTests::resetPreservesBindings() // static
{
        Statement query(db_, "SELECT code FROM offices WHERE city=?1");
        query.bind(1, "San Francisco");
        query.reset();
        auto result = query.begin().get<int>(0);
        static const int EXPECTED = 1;
        if (result != EXPECTED) {
                throw TestFailure("result was %d, expected %d",
                                  result, EXPECTED);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::resetBeginFetch() // static
{
        Statement query(db_,
                        "SELECT cheque_no FROM payments WHERE customer_no=?"
                                "ORDER BY date");

        Row r = query.bind(1, 103).begin();
        u8string_view expected = u8"JM555205";

        if (r.get<u8string_view>(0) != expected) {
                throw TestFailure("cheque number returned before reset was \"%s\", expected \"%s\"",
                                  r.get<u8string_view>(0), expected);
        }

        query.reset();
        r = query.begin();

        if (r.get<u8string_view>(0) != expected) {
                throw TestFailure("cheque number returned after reset was \"%s\", expected \"%s\"",
                                  r.get<u8string_view>(0), expected);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::resetDuringFetch() // static
{
        Statement query(db_,
                        "SELECT cheque_no FROM payments WHERE customer_no=?"
                                "ORDER BY date");

        Row r = query.bind(1, 103).begin();
        ++r;
        u8string_view expected = u8"HQ336336";

        if (r.get<u8string_view>(0) != expected) {
                throw TestFailure("cheque number returned before reset was \"%s\", expected \"%s\"",
                                  r.get<u8string_view>(0), expected);
        }

        query.reset();
        r = query.begin();
        expected = u8"JM555205";

        if (r.get<u8string_view>(0) != expected) {
                throw TestFailure("cheque number returned after reset was \"%s\", expected \"%s\"",
                                  r.get<u8string_view>(0), expected);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::resetAfterFetch() // static
{
        Statement query(db_, "SELECT number FROM employees WHERE job_title=?1");

        Row r = query.bind(1, "VP Sales").begin();
        int64_t expected_id = 1056;

        if (r.get<int64_t>(0) != expected_id) {
                throw TestFailure("returned employee ID for VP Sales was %d, expected %d",
                                  r.get<int64_t>(0), expected_id);
        }

        if (++r != query.end()) {
                throw TestFailure("multiple rows returned, expected only one");
        }

        query.reset();  // should not throw or cause any other side effects

        r = query.bind(1, "VP Marketing").begin();
        expected_id = 1076;

        if (r.get<int64_t>(0) != expected_id) {
                throw TestFailure("returned employee ID for VP Marketing was %d, expected %d",
                                  r.get<int64_t>(0), expected_id);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::resetMakesInactive() // static
{
        Statement query(db_, "SELECT number FROM customers");

        if (!query.begin()) {
                throw TestFailure("statement returned no results");
        } else if (!query.isActive()) {
                throw TestFailure("isActive() returned false upon executing statement, expected true");
        }

        query.reset();

        if (query.isActive()) {
                throw TestFailure("isActive() returned true after reset(), expected false");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::copyConstruct() // static
{
        Statement query1(db_, "SELECT name FROM product_lines");
        Row       r1 = query1.begin();
        ++r1;
        ++r1;

        Statement query2(query1);
        Row       r2 = query2.begin();
        ++r2;

        u8string_view expected = u8"Planes", returned;

        r1.get(0, returned);

        if (returned != expected) {
                throw TestFailure("r1.get(0) returned \"%s\", expected \"%s\"",
                                  returned, expected);
        }

        expected = u8"Motorcycles";
        r2.get(0, returned);

        if (returned != expected) {
                throw TestFailure("r2.get(0) returned \"%s\", expected \"%s\"",
                                  returned, expected);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::moveConstruct() // static
{
        Statement query1(db_, "SELECT name FROM product_lines");
        Row       r1 = query1.begin();
        ++r1;

        Statement query2(std::move(query1));

        if (query1.isActive()) {
                throw TestFailure("query1.isActive() returned true after move, expected false");
        }
        if (query1.isPrepared()) {
                throw TestFailure("query1.isPrepared() returned true after move, expected false");
        }

        r1 = query1.begin();

        u8string_view returned;

        if (r1 != query1.end()) {
                r1.get(0, returned);
                throw TestFailure("r1 != query.end(), r1.get(0) = \"%s\"",
                                  returned);
        }

        Row r2 = query2.currentRow();
        ++r2;
        r2.get(0, returned);

        u8string_view expected = u8"Planes";

        if (returned != expected) {
                throw TestFailure("r2.get(0) returned \"%s\", expected \"%s\"",
                                  returned, expected);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::copyAssign() // static
{
        Statement query1(db_, "SELECT name FROM product_lines");
        Row       r1 = query1.begin();
        ++r1;
        ++r1;

        Statement query2(db_, "SELECT * FROM products");
        Row       r2 = query2.begin();

        query2 = query1;
        r2 = query2.begin();
        ++r2;

        u8string_view expected = u8"Planes", returned;

        r1.get(0, returned);

        if (returned != expected) {
                throw TestFailure("r1.get(0) returned \"%s\", expected \"%s\"",
                                  returned, expected);
        }

        expected = u8"Motorcycles";
        r2.get(0, returned);

        if (returned != expected) {
                throw TestFailure("r2.get(0) returned \"%s\", expected \"%s\"",
                                  returned, expected);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::moveAssign() // static
{
        Statement query1(db_, "SELECT name FROM product_lines");
        Row       r1 = query1.begin();
        ++r1;

        Statement query2(db_, "SELECT * FROM products");
        Row       r2 = query2.begin();
        ++r2;

        query2 = std::move(query1);

        if (query1.isActive()) {
                throw TestFailure("query1.isActive() returned true after move, expected false");
        }
        if (query1.isPrepared()) {
                throw TestFailure("query1.isPrepared() returned true after move, expected false");
        }

        r1 = query1.begin();

        u8string_view returned;

        if (r1 != query1.end()) {
                r1.get(0, returned);
                throw TestFailure("r1 != query.end(), r1.get(0) = \"%s\"",
                                  returned);
        }

        r2 = query2.currentRow();
        ++r2;
        r2.get(0, returned);

        u8string_view expected = u8"Planes";

        if (returned != expected) {
                throw TestFailure("r2.get(0) returned \"%s\", expected \"%s\"",
                                  returned, expected);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::retrieveSQLUnprepared() // static
{
        Statement stmt;
        if (!stmt.sql().empty()) {
                throw TestFailure("sql() returned \"%s\" for unprepared statement, expected empty string",
                                  stmt.sql());
        }
}

//--------------------------------------

void
wr::sql::StatementTests::retrieveSQL() // static
{
        static const auto SQL = "SELECT code FROM offices WHERE city='Tokyo'";
        Statement query(db_, SQL);
        if (query.sql() != SQL) {
                throw TestFailure("query.sql() returned \"%s\", expected \"%s\"",
                                  query.sql(), SQL);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::registerStatement() // static
{
        static const auto FIND_OFFICE_AT =
                wr::sql::registerStatement(
                                "SELECT code FROM offices WHERE city=?");

        Row r = db_.exec(FIND_OFFICE_AT, "Tokyo");

        if (!r) {
                throw TestFailure("no row returned");
        } else {
                static const int TOKYO_CODE = 5;
                if (r.get<int>(0) != TOKYO_CODE) {
                        throw TestFailure("code returned was %d, expected %d",
                                          r.get<int>(0), TOKYO_CODE);
                }
        }
}

//--------------------------------------

void
wr::sql::StatementTests::reRegisterStatement() // static
{
        static const auto SQL = "SELECT code FROM offices WHERE city=?";

        static const auto FIND_OFFICE_AT_1 = wr::sql::registerStatement(SQL),
                          FIND_OFFICE_AT_2 = wr::sql::registerStatement(SQL);

        if (FIND_OFFICE_AT_1 != FIND_OFFICE_AT_2) {
                throw TestFailure("same statement re-registered at different index");
        }
}

//--------------------------------------

void
wr::sql::StatementTests::getNumRegisteredStatements() // static
{
        auto n_before = numRegisteredStatements();

        static const auto FIND_OFFICE_AT = wr::sql::registerStatement(
                                      "SELECT code FROM offices WHERE city=?");

        auto n_after = numRegisteredStatements();

        if (n_after != (n_before + 1)) {
                throw TestFailure("numRegisteredStatements() returned %u after statement registration, expected %u",
                                  n_after, n_before + 1);
        }
}

//--------------------------------------

void
wr::sql::StatementTests::retrieveRegisteredStatementSQL() // static
{
        static const auto SQL = "SELECT code FROM offices WHERE city=?";
        static const auto FIND_OFFICE_AT = wr::sql::registerStatement(SQL);

        if (registeredStatement(FIND_OFFICE_AT) != SQL) {
                throw TestFailure("retrieved SQL \"%s\", expected \"%s\"",
                                  registeredStatement(FIND_OFFICE_AT), SQL);
        }
}
