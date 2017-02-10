/**
 * \file IDSetTests.cxx
 *
 * \brief Unit test module for class wr::sql::IDSet
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
#include <limits>
#include <list>
#include <sstream>
#include <vector>
#include <sqlite3.h>

#include <wrsql/Error.h>
#include <wrsql/Session.h>
#include <wrsql/IDSet.h>

#include "SampleDB.h"
#include "SQLTestManager.h"
#include "../src/IDSetPrivate.h"


namespace wr {
namespace sql {


class IDSetTests : public SQLTestManager
{
public:
        using base_t = SQLTestManager;

        IDSetTests(int argc, const char **argv) : base_t("IDSet", argc, argv)
                { db_.init(defaultURI()); }

        virtual ~IDSetTests() { db_.close(); }

        int runAll();

        static void checkBodyNotNull_(const IDSet &set, const char *set_name),
                    checkSessionIsNull_(const IDSet &set, const char *set_name,
                                        bool expect_null),
                    checkContents_(const IDSet &set, const char *set_name,
                                   std::initializer_list<ID> expected_contents),
                    defaultConstruct(),
                    constructFromInitializerList(),
                    constructFromSession(),
                    constructFromSessionAndOtherSet(),
                    constructFromSessionAndInitializerList(),
                    copyConstructUnattached(),
                    copyConstructAttached(),
                    moveConstruct(),
                    copyAssignUnattachedToUnattached(),
                    copyAssignAttachedToUnattached(),
                    copyAssignUnattachedToAttached(),
                    copyAssignAttachedToAttached(),
                    copyAssignThis(),
                    moveAssign(),
                    moveAssignThis(),
                    assignInitializerList(),
                    attach(),
                    reattachToOtherSession(),
                    reattachToSameSession(),
                    attachToClosedSession(),
                    detach(),
                    detachClosedSession(),
                    detachNoSession(),
                    db(),
                    insertSingleIntoEmpty(),
                    insertSingleExisting(),
                    insertSingleAtStart(),
                    insertSingleAtEnd(),
                    insertSingleInMiddle(),
                    insertThis(),
                    insertIDSetIntoEmpty(),
                    insertIDSetAtStart(),
                    insertIDSetAtEnd(),
                    insertIDSetIntermingled(),
                    insertIDSetOverlapping(),
                    insertRange(),
                    insertInitializerList(),
                    insertStatementDefaultColumn(),
                    insertStatementNonDefaultColumn(),
                    insertSQLNoBinding(),
                    insertSQLWithBinding(),
                    sqlInsert(),
                    eraseNonExistentID(),
                    eraseByIDSingle(),
                    eraseByIDFirst(),
                    eraseByIDLast(),
                    eraseByIDMiddle(),
                    eraseByIteratorFirst(),
                    eraseByIteratorLast(),
                    eraseByIteratorMiddle(),
                    eraseFullRange(),
                    eraseEmptyRange(),
                    eraseRangeStart(),
                    eraseRangeEnd(),
                    eraseRangeMiddle(),
                    eraseThis(),
                    eraseIDSetEmptySet(),
                    eraseIDSetEqualSet(),
                    eraseIDSetSuperset(),
                    eraseIDSetSubset(),
                    eraseIDSetDisjoint(),
                    eraseIDSetOnEmpty(),
                    eraseStatementDefaultColumn(),
                    eraseStatementNonDefaultColumn(),
                    eraseEmptyInitializerList(),
                    eraseInitializerListAll(),
                    eraseInitializerListStart(),
                    eraseInitializerListEnd(),
                    eraseInitializerListMiddle(),
                    eraseInitializerListStaggered(),
                    eraseSQLNoBinding(),
                    eraseSQLWithBinding(),
                    sqlDelete(),
                    intersectThis(),
                    intersectIDSetEmptySet(),
                    intersectIDSetWithEmpty(),
                    intersectIDSetEqualSet(),
                    intersectIDSetSuperset(),
                    intersectIDSetSubset(),
                    intersectIDSetDisjoint(),
                    intersectIDSetMixed(),
                    intersectStatementEmptySet(),
                    intersectStatementWithEmpty(),
                    intersectStatementEqualSet(),
                    intersectStatementSuperset(),
                    intersectStatementSubset(),
                    intersectStatementDisjoint(),
                    intersectStatementMixed(),
                    intersectRangeEmpty(),
                    intersectRangeMixed(),
                    intersectRangeEqual(),
                    intersectInitializerListEmpty(),
                    intersectInitializerListMixed(),
                    intersectInitializerListEqual(),
                    intersectSQLNoBinding(),
                    intersectSQLWithBinding(),
                    symmetricDifferenceThis(),
                    symmetricDifferenceIDSetEmptySet(),
                    symmetricDifferenceIDSetWithEmpty(),
                    symmetricDifferenceIDSetEqualSet(),
                    symmetricDifferenceIDSetSuperset(),
                    symmetricDifferenceIDSetSubset(),
                    symmetricDifferenceIDSetDisjoint(),
                    symmetricDifferenceIDSetMixed(),
                    symmetricDifferenceStatementEmptySet(),
                    symmetricDifferenceStatementWithEmpty(),
                    symmetricDifferenceStatementEqualSet(),
                    symmetricDifferenceStatementSuperset(),
                    symmetricDifferenceStatementSubset(),
                    symmetricDifferenceStatementDisjoint(),
                    symmetricDifferenceStatementMixed(),
                    symmetricDifferenceRangeEmpty(),
                    symmetricDifferenceRangeMixed(),
                    symmetricDifferenceRangeEqual(),
                    symmetricDifferenceInitializerListEmpty(),
                    symmetricDifferenceInitializerListMixed(),
                    symmetricDifferenceInitializerListEqual(),
                    symmetricDifferenceSQLNoBinding(),
                    symmetricDifferenceSQLWithBinding(),
                    clear(),
                    swapBothEmptyUnattached(),
                    swapEmptyWithNonEmpty(),
                    swapEmptyWithNonEmptyAttached(),
                    swapBothNonEmptyAttachedSameDB(),
                    swapBothNonEmptyAttachedDiffDB(),
                    begin(),
                    end(),
                    iterate(),
                    rbegin(),
                    rend(),
                    reverseIterate(),
                    empty(),
                    size(),
                    maxSize(),
                    capacity(),
                    count(),
                    find(),
                    lowerBound(),
                    upperBound(),
                    equalRange(),
                    indexOperator(),
                    reserve(),
                    shrinkToFit(),
                    compareEqual(),
                    compareNotEqual(),
                    compareLess(),
                    compareLessOrEqual(),
                    compareGreater(),
                    compareGreaterOrEqual();

private:
        static SampleDB db_;
};


SampleDB IDSetTests::db_;


} // namespace sql
} // namespace wr

//--------------------------------------

int
main(
        int          argc,
        const char **argv
)
{
        return wr::sql::IDSetTests(argc, argv).runAll();
}

//--------------------------------------

int
wr::sql::IDSetTests::runAll()
{
        run("construct", 1, &defaultConstruct);
        run("construct", 2, &constructFromInitializerList);
        run("construct", 3, &constructFromSession);
        run("construct", 4, &constructFromSessionAndOtherSet);
        run("construct", 5, &constructFromSessionAndInitializerList);
        run("construct", 6, &copyConstructUnattached);
        run("construct", 7, &copyConstructAttached);
        run("construct", 8, &moveConstruct);

        run("assign", 1, &copyAssignUnattachedToUnattached),
        run("assign", 2, &copyAssignAttachedToUnattached),
        run("assign", 3, &copyAssignUnattachedToAttached),
        run("assign", 4, &copyAssignAttachedToAttached),
        run("assign", 5, &copyAssignThis),
        run("assign", 6, &moveAssign),
        run("assign", 7, &moveAssignThis),
        run("assign", 8, &assignInitializerList);

        run("attach", 1, &attach);
        run("attach", 2, &reattachToOtherSession);
        run("attach", 3, &reattachToSameSession);
        run("attach", 4, &attachToClosedSession);

        run("detach", 1, &detach);
        run("detach", 2, &detachClosedSession);
        run("detach", 3, &detachNoSession);

        run("db", 1, &db);

        run("insert", 1, &insertSingleIntoEmpty);
        run("insert", 2, &insertSingleExisting);
        run("insert", 3, &insertSingleAtStart);
        run("insert", 4, &insertSingleAtEnd);
        run("insert", 5, &insertSingleInMiddle);
        run("insert", 6, &insertThis);
        run("insert", 7, &insertIDSetIntoEmpty);
        run("insert", 8, &insertIDSetAtStart);
        run("insert", 9, &insertIDSetAtEnd);
        run("insert", 10, &insertIDSetIntermingled);
        run("insert", 11, &insertIDSetOverlapping);
        run("insert", 12, &insertRange);
        run("insert", 13, &insertInitializerList);
        run("insert", 14, &insertStatementDefaultColumn);
        run("insert", 15, &insertStatementNonDefaultColumn);
        run("insert", 16, &insertSQLNoBinding);
        run("insert", 17, &insertSQLWithBinding);
        run("sqlInsert", 1, &sqlInsert);

        run("erase", 1, &eraseNonExistentID);
        run("erase", 2, &eraseByIDSingle);
        run("erase", 3, &eraseByIDFirst);
        run("erase", 4, &eraseByIDLast);
        run("erase", 5, &eraseByIDMiddle);
        run("erase", 6, &eraseByIteratorFirst);
        run("erase", 7, &eraseByIteratorLast);
        run("erase", 8, &eraseByIteratorMiddle);
        run("erase", 9, &eraseFullRange);
        run("erase", 10, &eraseEmptyRange);
        run("erase", 11, &eraseRangeStart);
        run("erase", 12, &eraseRangeEnd);
        run("erase", 13, &eraseRangeMiddle);
        run("erase", 14, &eraseThis);
        run("erase", 15, &eraseIDSetEmptySet);
        run("erase", 16, &eraseIDSetEqualSet);
        run("erase", 17, &eraseIDSetSuperset);
        run("erase", 18, &eraseIDSetSubset);
        run("erase", 19, &eraseIDSetDisjoint);
        run("erase", 20, &eraseIDSetOnEmpty);
        run("erase", 21, &eraseStatementDefaultColumn);
        run("erase", 22, &eraseStatementNonDefaultColumn);
        run("erase", 23, &eraseEmptyInitializerList);
        run("erase", 24, &eraseInitializerListAll);
        run("erase", 25, &eraseInitializerListStart);
        run("erase", 26, &eraseInitializerListEnd);
        run("erase", 27, &eraseInitializerListMiddle);
        run("erase", 28, &eraseInitializerListStaggered);
        run("erase", 29, &eraseSQLNoBinding);
        run("erase", 30, &eraseSQLWithBinding);
        run("sqlDelete", 1, &sqlDelete);

        run("intersect", 1, &intersectThis);
        run("intersect", 2, &intersectIDSetEmptySet);
        run("intersect", 3, &intersectIDSetWithEmpty);
        run("intersect", 4, &intersectIDSetEqualSet);
        run("intersect", 5, &intersectIDSetSuperset);
        run("intersect", 6, &intersectIDSetSubset);
        run("intersect", 7, &intersectIDSetDisjoint);
        run("intersect", 8, &intersectIDSetMixed);
        run("intersect", 9, &intersectStatementEmptySet);
        run("intersect", 10, &intersectStatementWithEmpty);
        run("intersect", 11, &intersectStatementEqualSet);
        run("intersect", 12, &intersectStatementSuperset);
        run("intersect", 13, &intersectStatementSubset);
        run("intersect", 14, &intersectStatementDisjoint);
        run("intersect", 15, &intersectStatementMixed);
        run("intersect", 16, &intersectRangeEmpty);
        run("intersect", 17, &intersectRangeMixed);
        run("intersect", 18, &intersectRangeEqual);
        run("intersect", 19, &intersectInitializerListEmpty);
        run("intersect", 20, &intersectInitializerListMixed);
        run("intersect", 21, &intersectInitializerListEqual);
        run("intersect", 22, &intersectSQLNoBinding);
        run("intersect", 23, &intersectSQLWithBinding);

        run("symmetricDifference", 1, &symmetricDifferenceThis);
        run("symmetricDifference", 2, &symmetricDifferenceIDSetEmptySet);
        run("symmetricDifference", 3, &symmetricDifferenceIDSetWithEmpty);
        run("symmetricDifference", 4, &symmetricDifferenceIDSetEqualSet);
        run("symmetricDifference", 5, &symmetricDifferenceIDSetSuperset);
        run("symmetricDifference", 6, &symmetricDifferenceIDSetSubset);
        run("symmetricDifference", 7, &symmetricDifferenceIDSetDisjoint);
        run("symmetricDifference", 8, &symmetricDifferenceIDSetMixed);
        run("symmetricDifference", 9, &symmetricDifferenceStatementEmptySet);
        run("symmetricDifference", 10, &symmetricDifferenceStatementWithEmpty);
        run("symmetricDifference", 11, &symmetricDifferenceStatementEqualSet);
        run("symmetricDifference", 12, &symmetricDifferenceStatementSuperset);
        run("symmetricDifference", 13, &symmetricDifferenceStatementSubset);
        run("symmetricDifference", 14, &symmetricDifferenceStatementDisjoint);
        run("symmetricDifference", 15, &symmetricDifferenceStatementMixed);
        run("symmetricDifference", 16, &symmetricDifferenceRangeEmpty);
        run("symmetricDifference", 17, &symmetricDifferenceRangeMixed);
        run("symmetricDifference", 18, &symmetricDifferenceRangeEqual);
        run("symmetricDifference", 19,
                &symmetricDifferenceInitializerListEmpty);
        run("symmetricDifference", 20,
                &symmetricDifferenceInitializerListMixed);
        run("symmetricDifference", 21,
                &symmetricDifferenceInitializerListEqual);
        run("symmetricDifference", 22, &symmetricDifferenceSQLNoBinding);
        run("symmetricDifference", 23, &symmetricDifferenceSQLWithBinding);

        run("clear", 1, &clear);

        run("swap", 1, &swapBothEmptyUnattached);
        run("swap", 2, &swapEmptyWithNonEmpty);
        run("swap", 3, &swapEmptyWithNonEmptyAttached);
        run("swap", 4, &swapBothNonEmptyAttachedSameDB);
        run("swap", 5, &swapBothNonEmptyAttachedDiffDB);

        run("begin", 1, &begin);
        run("end", 1, &end);
        run("iterate", 1, &iterate);
        run("rbegin", 1, &rbegin);
        run("rend", 1, &rend);
        run("reverseIterate", 1, &reverseIterate);

        run("empty", 1, &empty);
        run("size", 1, &size);
        run("maxSize", 1, &maxSize);
        run("capacity", 1, &capacity);

        run("count", 1, &count);
        run("find", 1, &find);
        run("lowerBound", 1, &lowerBound);
        run("upperBound", 1, &upperBound);
        run("equalRange", 1, &equalRange);

        run("indexOperator", 1, &indexOperator);
        run("reserve", 1, &reserve);
        run("shrinkToFit", 1, &shrinkToFit);

        run("compareEqual", 1, &compareEqual);
        run("compareNotEqual", 1, &compareNotEqual);
        run("compareLess", 1, &compareLess);
        run("compareLessOrEqual", 1, &compareLessOrEqual);
        run("compareGreater", 1, &compareGreater);
        run("compareGreaterOrEqual", 1, &compareGreaterOrEqual);

        return failed() ? EXIT_FAILURE : EXIT_SUCCESS;
}

//--------------------------------------

void
wr::sql::IDSetTests::checkBodyNotNull_(
        const IDSet &set,
        const char  *set_name
) // static
{
        if (!set.body_) {
                throw TestFailure("%s.body_ is NULL", set_name);
        }
}

#define checkBodyNotNull(set) checkBodyNotNull_(set, #set)

//--------------------------------------

void
wr::sql::IDSetTests::checkSessionIsNull_(
        const IDSet &set,
        const char  *set_name,
        bool         expect_null
) // static
{
        if ((set.body_->db_ == nullptr) != expect_null) {
                throw TestFailure("%s.body_->db_ is %s NULL, expected %sNULL",
                                  set_name, expect_null ? "not " : "",
                                  expect_null ? "" : "non-");
        }
}

#define checkSessionIsNull(set, expect_null) \
        checkSessionIsNull_(set, #set, expect_null)

//--------------------------------------

void
wr::sql::IDSetTests::checkContents_(
        const IDSet               &set,
        const char                *set_name,
        std::initializer_list<ID>  expected_contents
) // static
{
        if (set.body_->storage_.size() != expected_contents.size()) {
                throw TestFailure("%s contains %u element(s), expected %u",
                                  set_name, set.body_->storage_.size(),
                                  expected_contents.size());
        }

        Statement query;

        if (set.body_->db_ && set.body_->db_->isOpen()) {
                query = set.body_->db_->exec(
                                printStr("SELECT id FROM idset_%p", set.body_));
        }

        auto j = expected_contents.begin();

        for (IDSet::size_type i = 0; i < expected_contents.size(); ++i, ++j) {
                if (set.body_->storage_[i] != *j) {
                        throw TestFailure("%s.body_->storage_[%u] is %d, expected %d",
                                          set_name, i,
                                          set.body_->storage_[i], *j);
                }
                if (query) {
                        if (!query.current()) {
                                throw TestFailure("SQL access of %s returned %u value(s), expected %u",
                                                  set_name, i + 1,
                                                  expected_contents.size());
                        } else if (query.current().get<ID>(0) != *j) {
                                throw TestFailure("value %u returned by SQL access of %s is %d, expected %d",
                                                  i, set_name,
                                                  query.current().get<ID>(0),
                                                  *j);
                        }
                        query.next();
                }
        }
}

#define checkContents(set, ...) checkContents_(set, #set, {__VA_ARGS__})

//--------------------------------------

void
wr::sql::IDSetTests::defaultConstruct() // static
{
        IDSet set;
        checkBodyNotNull(set);
        checkSessionIsNull(set, true);
        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::constructFromInitializerList() // static
{
        IDSet set = { 3, 1, 2, 1 };
        checkBodyNotNull(set);
        checkSessionIsNull(set, true);
        checkContents(set, 1, 2, 3);
}

//--------------------------------------

void
wr::sql::IDSetTests::constructFromSession() // static
{
        IDSet set(db_);
        checkBodyNotNull(set);
        checkSessionIsNull(set, false);
        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::constructFromSessionAndOtherSet() // static
{
        IDSet set1 = { 999, 123, 456, 0, 999, 0, 222 },
              set2(db_, set1);

        checkBodyNotNull(set1);
        checkSessionIsNull(set1, true);
        checkBodyNotNull(set2);
        checkSessionIsNull(set2, false);
        checkContents(set1, 0, 123, 222, 456, 999);
        checkContents(set2, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::constructFromSessionAndInitializerList() // static
{
        IDSet set(db_, { 999, 123, 456, 0, 999, 0, 222 });
        checkBodyNotNull(set);
        checkSessionIsNull(set, false);
        checkContents(set, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::copyConstructUnattached() // static
{
        IDSet set1({ 999, 123, 456, 0, 999, 0, 222 }),
              set2(set1);

        if (set2.body_ == set1.body_) {
                throw TestFailure("set1 and set2 point at same body");
        }
        if (set2.body_->db_ != nullptr) {
                throw TestFailure("set2.body_->db_ is %p, should be NULL",
                                  set2.body_->db_);
        }

        checkContents(set1, 0, 123, 222, 456, 999);
        checkContents(set2, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::copyConstructAttached() // static
{
        IDSet set1(db_, { 999, 123, 456, 0, 999, 0, 222 }),
              set2(set1);

        if (set2.body_ == set1.body_) {
                throw TestFailure("set1 and set2 point at same body");
        }
        if (set2.body_->db_ != set1.body_->db_) {
                throw TestFailure("set2.body_->db_ is %p, expected %p (== set1.body_->db_)",
                                  set2.body_->db_, set1.body_->db_);
        }
        checkContents(set1, 0, 123, 222, 456, 999);
        checkContents(set2, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::moveConstruct() // static
{
        IDSet set1(db_, { 999, 123, 456, 0, 999, 0, 222 }),
              set2(std::move(set1));

        checkBodyNotNull(set1);
        checkBodyNotNull(set2);
        checkSessionIsNull(set1, true);
        checkSessionIsNull(set2, false);
        checkContents(set1);
        checkContents(set2, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::attach() // static
{
        IDSet set({ 999, 123, 456, 0, 999, 0, 222 });
        IDSet &set_ref = set.attach(db_);

        if (&set_ref != &set) {
                throw TestFailure("set.attach() should return reference to set");
        }

        if (set.body_->db_ != &db_) {
                throw TestFailure("set.body_->db_ is %p after invoking set.attach(), expected %p",
                                  set.body_->db_, &db_);
        }

        checkContents(set, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::reattachToOtherSession() // static
{
        IDSet set(db_, { 999, 123, 456, 0, 999, 0, 222 });
        checkContents(set, 0, 123, 222, 456, 999);

        Session  db2(db_);
        IDSet   &set_ref = set.attach(db2);

        if (&set_ref != &set) {
                throw TestFailure("set.attach() should return reference to set");
        }

        try {
                for (Row r: db_.exec(printStr("SELECT id FROM idset_%p",
                                              set.body_))) {
                        throw TestFailure("table representing re-attached IDSet still exists in original database connection");
                }
        } catch (Error &e) {
                string_view msg = e.what();
                if (msg.find(printStr("idset_%p", set.body_)) == msg.npos) {
                        throw;  // some other error
                } // otherwise OK, expected
        }

        checkContents(set, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::reattachToSameSession() // static
{
        IDSet set({ 999, 123, 456, 0, 999, 0, 222 });
        set.attach(db_);
        checkContents(set, 0, 123, 222, 456, 999);

        IDSet &set_ref = set.attach(db_);

        if (&set_ref != &set) {
                throw TestFailure("set.attach() should return reference to set");
        }

        checkContents(set, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::attachToClosedSession() // static
{
        IDSet    set({ 999, 123, 456, 0, 999, 0, 222 });
        Session  dummy_db;
        IDSet   &set_ref = set.attach(dummy_db);

        if (&set_ref != &set) {
                throw TestFailure("set.attach() should return reference to set");
        }

        checkContents(set, 0, 123, 222, 456, 999);
}

//--------------------------------------

void
wr::sql::IDSetTests::detach() // static
{
        IDSet set(db_);
        checkSessionIsNull(set, false);
        IDSet &set_ref = set.detach();
        if (&set_ref != &set) {
                throw TestFailure("set.detach() should return reference to set");
        }
        checkSessionIsNull(set, true);
}

//--------------------------------------

void
wr::sql::IDSetTests::detachClosedSession() // static
{
        Session dummy_db;
        IDSet set(dummy_db);
        checkSessionIsNull(set, false);
        IDSet &set_ref = set.detach();
        if (&set_ref != &set) {
                throw TestFailure("set.detach() should return reference to set");
        }
        checkSessionIsNull(set, true);
}

//--------------------------------------

void
wr::sql::IDSetTests::detachNoSession() // static
{
        IDSet set, &set_ref = set.detach();
        if (&set_ref != &set) {
                throw TestFailure("set.detach() should return reference to set");
        }
        checkSessionIsNull(set, true);
}

//--------------------------------------

void
wr::sql::IDSetTests::db() // static
{
        IDSet set1;

        if (set1.db()) {
                throw TestFailure("[1] set1.db() returned %p, expected NULL",
                                  set1.db());
        }

        set1.attach(db_);

        if (set1.db() != &db_) {
                throw TestFailure("[2] set1.db() returned %p, expected %p",
                                  set1.db(), &db_);
        }

        set1.detach();

        if (set1.db() != nullptr) {
                throw TestFailure("[3] set1.db() returned %p, expected NULL",
                                  set1.db());
        }

        IDSet set2(db_);

        if (set2.db() != &db_) {
                throw TestFailure("[4] set2.db() returned %p, expected %p",
                                  set2.db(), &db_);
        }

        set1 = set2;

        if (set1.db() != &db_) {
                throw TestFailure("[5] set1.db() returned %p, expected %p",
                                  set1.db(), &db_);
        }

        if (set2.db() != &db_) {
                throw TestFailure("[6] set2.db() returned %p, expected %p",
                                  set2.db(), &db_);
        }

        IDSet set3(std::move(set2));

        if (set3.db() != set1.db()) {
                throw TestFailure("[7] set3.db() returned %p, expected %p",
                                  set3.db(), set1.db());
        }

        if (set2.db()) {
                throw TestFailure("[8] set2.db() returned %p, expected NULL",
                                  set2.db());
        }

        set2 = std::move(set3);

        if (set2.db() != set1.db()) {
                throw TestFailure("[9] set2.db() returned %p, expected %p",
                                  set2.db(), set1.db());
        }

        if (set3.db()) {
                throw TestFailure("[10] set3.db() returned %p, expected NULL",
                                  set3.db());
        }

        IDSet set4(set2);

        if (set4.db() != set2.db()) {
                throw TestFailure("[11] set4.db() returned %p, expected %p",
                                  set4.db(), set2.db());
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::copyAssignUnattachedToUnattached() // static
{
        IDSet set1, set2({ 1, 5, 21, 13, 1, 8, 3, 2 });

        checkContents(set1);
        checkSessionIsNull(set1, true);
        checkContents(set2, 1, 2, 3, 5, 8, 13, 21);
        checkSessionIsNull(set2, true);

        set1 = set2;

        checkContents(set1, 1, 2, 3, 5, 8, 13, 21);
        checkSessionIsNull(set1, true);
}

//--------------------------------------

void
wr::sql::IDSetTests::copyAssignAttachedToUnattached() // static
{
        IDSet set1({ 1, 5, 21, 13, 1, 8, 3, 2 }), set2(db_);

        checkContents(set1, 1, 2, 3, 5, 8, 13, 21);
        checkSessionIsNull(set1, true);
        checkContents(set2);
        checkSessionIsNull(set2, false);

        set1 = set2;

        checkContents(set1);

        if (set1.body_->db_ != set2.body_->db_) {
                throw TestFailure("set1.body_->db is %p, should be %p",
                                  set1.body_->db_, set2.body_->db_);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::copyAssignUnattachedToAttached() // static
{
        IDSet set1(db_, { 1, 5, 21, 13, 1, 8, 3, 2 }), set2({ 2, 10, 6, 8, 4 });

        checkContents(set1, 1, 2, 3, 5, 8, 13, 21);
        checkSessionIsNull(set1, false);
        checkContents(set2, 2, 4, 6, 8, 10);
        checkSessionIsNull(set2, true);

        set1 = set2;

        checkContents(set1, 2, 4, 6, 8, 10);

        if (set1.body_->db_ != &db_) {
                throw TestFailure("set1.body_->db is %p, should be %p",
                                  set1.body_->db_, &db_);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::copyAssignAttachedToAttached() // static
{
        Session tmp(":memory:");

        IDSet set1(db_, { 1, 5, 21, 13, 1, 8, 3, 2 }),
              set2(tmp, { 2, 10, 6, 8, 4 });

        checkContents(set1, 1, 2, 3, 5, 8, 13, 21);
        checkSessionIsNull(set1, false);
        checkContents(set2, 2, 4, 6, 8, 10);
        checkSessionIsNull(set2, false);

        set1 = set2;

        checkContents(set1, 2, 4, 6, 8, 10);

        if (set1.body_->db_ != &db_) {
                throw TestFailure("set1.body_->db is %p, should be %p",
                                  set1.body_->db_, &db_);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::copyAssignThis() // static
{
        IDSet set(db_, { 1, 5, 21, 13, 1, 8, 3, 2 });

        set = set;

        checkContents(set, 1, 2, 3, 5, 8, 13, 21);

        if (set.body_->db_ != &db_) {
                throw TestFailure("set.body_->db is %p, should be %p",
                                  set.body_->db_, &db_);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::moveAssign() // static
{
        Session tmp(":memory:");

        IDSet set1,
              set2(tmp, { 1, 5, 21, 13, 1, 8, 3, 2}),
              set3(db_, { 2, 10, 6, 8, 4 });

        set1 = std::move(set2);

        checkContents(set1, 1, 2, 3, 5, 8, 13, 21);

        if (set1.body_->db_ != &tmp) {
                throw TestFailure("[1] set1.body_->db is %p, should be %p",
                                  set1.body_->db_, &tmp);
        }

        set1 = std::move(set3);

        tmp.close();  // should be OK, attachment to tmp broken above

        checkContents(set1, 2, 4, 6, 8, 10);

        if (set1.body_->db_ != &db_) {
                throw TestFailure("[2] set1.body_->db is %p, should be %p",
                                  set1.body_->db_, &db_);
        }

}

//--------------------------------------

void
wr::sql::IDSetTests::moveAssignThis() // static
{
        IDSet set(db_, { 1, 5, 21, 13, 1, 8, 3, 2 });

        set = std::move(set);

        checkContents(set, 1, 2, 3, 5, 8, 13, 21);

        if (set.body_->db_ != &db_) {
                throw TestFailure("set.body_->db is %p, should be %p",
                                  set.body_->db_, &db_);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::assignInitializerList() // static
{
        IDSet set(db_);

        set = { 1, 5, 21, 13, 1, 8, 3, 2 };

        checkContents(set, 1, 2, 3, 5, 8, 13, 21);

        if (set.body_->db_ != &db_) {
                throw TestFailure("set.body_->db is %p, should be %p",
                                  set.body_->db_, &db_);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::insertSingleIntoEmpty() // static
{
        IDSet set(db_);
        auto  ins = set.insert(1);

        if (ins.first != set.body_->storage_.begin()) {
                throw TestFailure("returned iterator invalid, expected iterator to beginning of set");
        }
        if (!ins.second) {
                throw TestFailure("returned insertion flag is false, expected true");
        }

        checkContents(set, 1);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertSingleExisting() // static
{
        IDSet set(db_, { 1 });
        auto  ins = set.insert(1);

        if (ins.first != set.body_->storage_.begin()) {
                throw TestFailure("returned iterator invalid, expected iterator to beginning of set");
        }
        if (ins.second) {
                throw TestFailure("returned insertion flag is true, expected false");
        }

        checkContents(set, 1);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertSingleAtStart() // static
{
        IDSet set(db_, { 1, 2, 3 });
        auto  ins = set.insert(0);

        if (ins.first != set.body_->storage_.begin()) {
                throw TestFailure("returned iterator invalid, expected iterator to beginning of set");
        }
        if (!ins.second) {
                throw TestFailure("returned insertion flag is false, expected true");
        }

        checkContents(set, 0, 1, 2, 3);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertSingleAtEnd() // static
{
        IDSet set(db_, { 1, 2, 3 });
        auto  ins = set.insert(4);

        if (ins.first != std::prev(set.body_->storage_.end())) {
                throw TestFailure("returned iterator invalid, expected iterator to last element of set");
        }
        if (!ins.second) {
                throw TestFailure("returned insertion flag is false, expected true");
        }

        checkContents(set, 1, 2, 3, 4);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertSingleInMiddle() // static
{
        IDSet set(db_, { 0, 2 });
        auto  ins = set.insert(1);

        if (ins.first != std::next(set.body_->storage_.begin())) {
                throw TestFailure("returned iterator invalid, expected iterator to second element of set");
        }
        if (!ins.second) {
                throw TestFailure("returned insertion flag is false, expected true");
        }

        checkContents(set, 0, 1, 2);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertThis() // static
{
        IDSet set(db_, { 1, 2, 3 });
        auto  n  = set.insert(set);

        if (n != 0) {
                throw TestFailure("insert() returned %u, expected 0", n);
        }

        checkContents(set, 1, 2, 3);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertIDSetIntoEmpty() // static
{
        IDSet set1(db_), set2(db_, { 1, 2, 3 });
        auto  n   = set1.insert(set2);

        if (n != 3) {
                throw TestFailure("insert() returned %u, expected 3", n);
        }

        checkContents(set1, 1, 2, 3);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertIDSetAtStart() // static
{
        IDSet set1(db_, { 4, 5, 6 }), set2(db_, { 3, 2, 1 });
        auto  n   = set1.insert(set2);

        if (n != 3) {
                throw TestFailure("insert() returned %u, expected 3", n);
        }

        checkContents(set1, 1, 2, 3, 4, 5, 6);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertIDSetAtEnd() // static
{
        IDSet set1(db_, { 1, 2, 3 }), set2(db_, { 4, 5, 6 });
        auto  n   = set1.insert(set2);

        if (n != 3) {
                throw TestFailure("insert() returned %u, expected 3", n);
        }

        checkContents(set1, 1, 2, 3, 4, 5, 6);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertIDSetIntermingled() // static
{
        IDSet set1(db_, { 2, 4, 6, 8 }), set2(db_, { 0, 1, 3, 5, 7, 9, 10 });
        auto  n   = set1.insert(set2);

        if (n != 7) {
                throw TestFailure("insert() returned %u, expected 7", n);
        }

        checkContents(set1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertIDSetOverlapping() // static
{
        IDSet set1(db_, { 2, 4, 6, 8 }),
              set2(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        auto  n   = set1.insert(set2);

        if (n != 7) {
                throw TestFailure("insert() returned %u, expected 7", n);
        }

        checkContents(set1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertRange() // static
{
        IDSet        set1(db_, { 1, 5, 21, 13, 1, 8, 3, 2});
        std::set<ID> set2({ 2, 10, 6, 8, 4 });
        auto         n   = set1.insert(set2.begin(), set2.end());

        if (n != 3) {
                throw TestFailure("insert() returned %u, expected 3", n);
        }

        checkContents(set1, 1, 2, 3, 4, 5, 6, 8, 10, 13, 21);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertInitializerList() // static
{
        IDSet set1(db_, { 1, 5, 21, 13, 1, 8, 3, 2});
        auto  n   = set1.insert({ 2, 10, 6, 8, 4 });

        if (n != 3) {
                throw TestFailure("insert() returned %u, expected 3", n);
        }

        checkContents(set1, 1, 2, 3, 4, 5, 6, 8, 10, 13, 21);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertStatementDefaultColumn() // static
{
        IDSet     set (db_);
        Statement stmt(db_, "SELECT number FROM employees "
                                "WHERE surname='Patterson'");
        auto      n   = set.insert(stmt);

        if (n != 3) {
                throw TestFailure("insert() returned %u, expected 3", n);
        }

        checkContents(set, 1056, 1088, 1216);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertStatementNonDefaultColumn() // static
{
        IDSet     set (db_);
        Statement stmt(db_, "SELECT forename, surname, number FROM employees "
                                "WHERE surname='Patterson'");
        auto      n   = set.insert(stmt, 2);

        if (n != 3) {
                throw TestFailure("insert() returned %u, expected 3", n);
        }

        checkContents(set, 1056, 1088, 1216);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertSQLNoBinding() // static
{
        IDSet set(db_);
        auto  n  = set.insert_sql(
                        "SELECT number FROM employees WHERE office_code=1");

        if (n != 6) {
                throw TestFailure("insert() returned %u, expected 6", n);
        }

        checkContents(set, 1002, 1056, 1076, 1143, 1165, 1166);
}

//--------------------------------------

void
wr::sql::IDSetTests::insertSQLWithBinding() // static
{
        IDSet set(db_);
        auto  n  = set.insert_sql(
                        "SELECT number FROM customers WHERE country=?", "UK");

        if (n != 5) {
                throw TestFailure("insert() returned %u, expected 5", n);
        }

        checkContents(set, 187, 201, 240, 324, 489);
}

//--------------------------------------

void
wr::sql::IDSetTests::sqlInsert() // static
{
        IDSet set(db_);
        db_.exec(printStr("INSERT INTO %s SELECT number FROM employees "
                                "WHERE office_code IN (1, 2, 3)", set));
        checkContents(set, 1002, 1056, 1076, 1143, 1165, 1166, 1188, 1216,
                      1286, 1323);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseNonExistentID() // static
{
        IDSet  set(db_, { 2, 10, 6, 8, 4 });
        size_t n  = set.erase(5);

        if (n != 0) {
                throw TestFailure("set.erase(5) returned %u, expected 0", n);
        }

        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseByIDSingle() // static
{
        IDSet  set(db_, { 2 });
        size_t n  = set.erase(2);

        if (n != 1) {
                throw TestFailure("set.erase(5) returned %u, expected 0", n);
        }

        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseByIDFirst() // static
{
        IDSet  set(db_, { 2, 10, 6, 8, 4 });
        size_t n  = set.erase(2);

        if (n != 1) {
                throw TestFailure("set.erase(2) returned %u, expected 0", n);
        }

        checkContents(set, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseByIDLast() // static
{
        IDSet  set(db_, { 2, 10, 6, 8, 4 });
        size_t n  = set.erase(10);

        if (n != 1) {
                throw TestFailure("set.erase(10) returned %u, expected 0", n);
        }

        checkContents(set, 2, 4, 6, 8);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseByIDMiddle() // static
{
        IDSet  set(db_, { 2, 10, 6, 8, 4 });
        size_t n  = set.erase(6);

        if (n != 1) {
                throw TestFailure("set.erase(6) returned %u, expected 0", n);
        }

        checkContents(set, 2, 4, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseByIteratorFirst() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i  = set.erase(set.begin());

        if (*i != 4) {
                throw TestFailure("set.erase() on first element returned iterator to %d, expected 4",
                                  *i);
        }

        checkContents(set, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseByIteratorLast() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i  = set.erase(std::prev(set.end()));

        if (i != set.end()) {
                throw TestFailure("set.erase() on last element returned iterator to %d, expected set.end()",
                                  *i);
        }

        checkContents(set, 2, 4, 6, 8);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseByIteratorMiddle() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i  = set.erase(std::next(set.begin(), 2));

        if (*i != 8) {
                throw TestFailure("set.erase() on middle element returned iterator to %d, expected 8",
                                  *i);
        }

        checkContents(set, 2, 4, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseFullRange() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i  = set.erase(set.begin(), set.end());

        if (i != set.end()) {
                throw TestFailure("set.erase() on full range returned invalid iterator, expected set.end()");
        }

        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseEmptyRange() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i  = std::next(set.begin()),
              j  = set.erase(i, i);

        if (i != j) {
                throw TestFailure("set.erase() on empty range returned iterator to %d, expected %d",
                                  *j, *i);
        }

        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseRangeStart() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i = set.erase(set.begin(), std::next(set.begin(), 2));

        if (i != set.begin()) {
                throw TestFailure("set.erase() on first two elements returned iterator to %d, expected %d",
                                  *i, 6);
        }

        checkContents(set, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseRangeEnd() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i = std::prev(set.end(), 2);

        i = set.erase(i, set.end());

        if (i != set.end()) {
                throw TestFailure("set.erase() on last two elements returned iterator to %d, expected set.end()",
                                  *i);
        }

        checkContents(set, 2, 4, 6);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseRangeMiddle() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  i = std::next(set.begin(), 2);

        i = set.erase(i, std::next(i, 2));

        if (i != std::prev(set.end())) {
                throw TestFailure("set.erase() on 3rd & 4th elements returned iterator to %d, expected %d",
                                  *i, 10);
        }

        checkContents(set, 2, 4, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseThis() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n = set.erase(set);

        if (n != 5) {
                throw TestFailure("set.erase(set) returned %u, expected 5",
                                  n);
        }

        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseIDSetEmptySet() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }), set2(db_);
        auto  n = set1.erase(set2);

        if (n != 0) {
                throw TestFailure("set1.erase(set2) returned %u, expected 0",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseIDSetEqualSet() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }), set2(db_, { 8, 6, 4, 2, 10 });
        auto  n = set1.erase(set2);

        if (n != 5) {
                throw TestFailure("set1.erase(set2) returned %u, expected 5",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseIDSetSuperset() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }),
              set2(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        auto  n = set1.erase(set2);

        if (n != 5) {
                throw TestFailure("set1.erase(set2) returned %u, expected 5",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseIDSetSubset() // static
{
        IDSet set1(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }),
              set2(db_, { 2, 10, 6, 8, 4 });
        auto  n = set1.erase(set2);

        if (n != 5) {
                throw TestFailure("set1.erase(set2) returned %u, expected 5",
                                  n);
        }

        checkContents(set1, 0, 1, 3, 5, 7, 9);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseIDSetDisjoint() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }), set2(db_, { 5, 9, 1, 3, 7 });
        auto  n = set1.erase(set2);

        if (n != 0) {
                throw TestFailure("set1.erase(set2) returned %u, expected 0",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseIDSetOnEmpty() // static
{
        IDSet set1(db_), set2(db_, { 2, 10, 6, 8, 4 });
        auto  n = set1.erase(set2);

        if (n != 0) {
                throw TestFailure("set1.erase(set2) returned %u, expected 0",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseStatementDefaultColumn() // static
{
        IDSet set(db_);

        db_.exec(printStr("INSERT INTO %s SELECT number FROM employees", set));

        Statement stmt(db_, "SELECT number FROM employees WHERE job_title='Sales Rep'");

        auto n = set.erase(stmt);

        if (n != 17) {
                throw TestFailure("set.erase(stmt) returned %u, expected 17",
                                  n);
        }

        checkContents(set, 1002, 1056, 1076, 1088, 1102, 1143);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseStatementNonDefaultColumn() // static
{
        IDSet set(db_);

        db_.exec(printStr("INSERT INTO %s SELECT number FROM employees", set));

        Statement stmt(db_, "SELECT surname, forename, number FROM employees WHERE job_title='Sales Rep'");

        auto n = set.erase(stmt, 2);

        if (n != 17) {
                throw TestFailure("set.erase(stmt) returned %u, expected 17",
                                  n);
        }

        checkContents(set, 1002, 1056, 1076, 1088, 1102, 1143);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseEmptyInitializerList() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.erase(std::initializer_list<ID>());

        if (n != 0) {
                throw TestFailure("set.erase() returned %u, expected 0", n);
        }

        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseInitializerListAll() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.erase({ 2, 4, 6, 8, 10 });

        if (n != 5) {
                throw TestFailure("set.erase() returned %u, expected 5", n);
        }

        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseInitializerListStart() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.erase({ 2, 3, 4 });

        if (n != 2) {
                throw TestFailure("set.erase() returned %u, expected 2", n);
        }

        checkContents(set, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseInitializerListEnd() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.erase({ 8, 9, 10 });

        if (n != 2) {
                throw TestFailure("set.erase() returned %u, expected 2", n);
        }

        checkContents(set, 2, 4, 6);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseInitializerListMiddle() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.erase({ 4, 5, 6 });

        if (n != 2) {
                throw TestFailure("set.erase() returned %u, expected 2", n);
        }

        checkContents(set, 2, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseInitializerListStaggered() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.erase({ 2, 6, 10 });

        if (n != 3) {
                throw TestFailure("set.erase() returned %u, expected 3", n);
        }

        checkContents(set, 4, 8);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseSQLNoBinding() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM employees");

        auto n = set.erase_sql("SELECT number FROM employees "
                               "WHERE office_code NOT IN (5,6,7)");

        if (n != 15) {
                throw TestFailure("set.erase_sql() returned %u, expected 15",
                                  n);
        }

        checkContents(set, 1088, 1501, 1504, 1611, 1612, 1619, 1621, 1625);
}

//--------------------------------------

void
wr::sql::IDSetTests::eraseSQLWithBinding() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM employees");

        auto n = set.erase_sql("SELECT number FROM employees "
                               "WHERE reports_to=?", 1143);

        if (n != 6) {
                throw TestFailure("set.erase_sql() returned %u, expected 6", n);
        }

        checkContents(set, 1002, 1056, 1076, 1088, 1102, 1143, 1337, 1370,
                      1401, 1501, 1504, 1611, 1612, 1619, 1621, 1625, 1702);
}

//--------------------------------------

void
wr::sql::IDSetTests::sqlDelete() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM employees");

        db_.exec(printStr("DELETE FROM %s WHERE id IN ("
                                "SELECT number FROM employees "
                                  "WHERE office_code IN (1, 3, 5, 7))", set));

        checkContents(set, 1088, 1102, 1188, 1216, 1337, 1370, 1401, 1611,
                      1612, 1619, 1702);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectThis() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.intersect(set);

        if (n != 0) {
                throw TestFailure("set.intersect(set) returned %u, expected 0",
                                  n);
        }

        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectIDSetEmptySet() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 }), empty;
        auto  n  = set.intersect(empty);

        if (n != 5) {
                throw TestFailure("set.intersect(empty) returned %u, expected 5",
                                  n);
        }

        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectIDSetWithEmpty() // static
{
        IDSet set1(db_), set2(db_, { 2, 10, 6, 8, 4 });
        auto  n   = set1.intersect(set2);

        if (n != 0) {
                throw TestFailure("set1.intersect(set2) returned %u, expected 0",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectIDSetEqualSet() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }), set2(db_, { 2, 10, 6, 8, 4 });
        auto  n   = set1.intersect(set2);

        if (n != 0) {
                throw TestFailure("set1.intersect(set2) returned %u, expected 0",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectIDSetSuperset() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }),
              set2(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
        auto  n   = set1.intersect(set2);

        if (n != 0) {
                throw TestFailure("set1.intersect(set2) returned %u, expected 0",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectIDSetSubset() // static
{
        IDSet set1(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }),
              set2(db_, { 2, 10, 6, 8, 4 });
        auto  n   = set1.intersect(set2);

        if (n != 8) {
                throw TestFailure("set1.intersect(set2) returned %u, expected 8",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectIDSetDisjoint() // static
{
        IDSet set1(db_, { 2, 4, 6, 8, 10 }),
              set2(db_, { 1, 3, 5, 7, 9 });
        auto  n   = set1.intersect(set2);

        if (n != 5) {
                throw TestFailure("set1.intersect(set2) returned %u, expected 5",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectIDSetMixed() // static
{
        IDSet set1(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 }),
              set2(db_, { 1, 4, 7, 8, 3, 6, 9 });
        auto  n   = set1.intersect(set2);

        if (n != 5) {
                throw TestFailure("set1.intersect(set2) returned %u, expected 5",
                                  n);
        }

        checkContents(set1, 1, 3, 4, 9);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectStatementEmptySet() // static
{
        IDSet     set (db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, "SELECT 1 WHERE 0=1");  // yields no rows
        auto      n   = set.intersect(stmt);

        if (n != 5) {
                throw TestFailure("set.intersect(stmt) returned %u, expected 5",
                                  n);
        }

        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectStatementWithEmpty() // static
{
        IDSet     set1(db_), set2(db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        auto      n   = set1.intersect(stmt);

        if (n != 0) {
                throw TestFailure("set1.intersect(stmt) returned %u, expected 0",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectStatementEqualSet() // static
{
        IDSet     set1(db_, { 2, 10, 6, 8, 4 }), set2(db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        auto      n   = set1.intersect(stmt);

        if (n != 0) {
                throw TestFailure("set1.intersect(stmt) returned %u, expected 0",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectStatementSuperset() // static
{
        IDSet     set1(db_, { 2, 10, 6, 8, 4 }),
                  set2(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        auto      n   = set1.intersect(stmt);

        if (n != 0) {
                throw TestFailure("set1.intersect(stmt) returned %u, expected 0",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectStatementSubset() // static
{
        IDSet     set1(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }),
                  set2(db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        auto      n   = set1.intersect(stmt);

        if (n != 8) {
                throw TestFailure("set1.intersect(stmt) returned %u, expected 8",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectStatementDisjoint() // static
{
        IDSet     set1(db_, { 2, 4, 6, 8, 10 }),
                  set2(db_, { 1, 3, 5, 7, 9 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        auto      n   = set1.intersect(stmt);

        if (n != 5) {
                throw TestFailure("set1.intersect(stmt) returned %u, expected 5",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectStatementMixed() // static
{
        db_.exec(printStr("CREATE TEMP TABLE %s (value INT)", __func__));
        db_.exec(printStr("INSERT INTO %s (value) VALUES (1),(1),(3),(4),(4),(4),(6),(7),(7),(8),(9),(9)",
                          __func__));

        IDSet     set1(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 });
        Statement stmt(db_, printStr("SELECT 0, value FROM %s", __func__));
        auto      n   = set1.intersect(stmt, 1);

        if (n != 5) {
                throw TestFailure("set1.intersect(stmt) returned %u, expected 5",
                                  n);
        }

        checkContents(set1, 1, 3, 4, 9);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectRangeEmpty() // static
{
        IDSet           set1(db_, { 2, 10, 6, 8, 4 });
        std::vector<ID> set2 = {};
        auto            n    = set1.intersect(set2.begin(), set2.end());

        if (n != 5) {
                throw TestFailure("set1.intersect() returned %u, expected 5",
                                  n);
        }

        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectRangeMixed() // static
{
        IDSet         set1(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 });
        std::list<ID> set2 = { 3, 1, 1, 6, 3, 4, 4, 4, 7, 7, 4, 8, 9, 9 };
        auto          n    = set1.intersect(set2.begin(), set2.end());

        if (n != 5) {
                throw TestFailure("set1.intersect() returned %u, expected 5",
                                  n);
        }

        checkContents(set1, 1, 3, 4, 9);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectRangeEqual() // static
{
        IDSet           set1(db_, { 2, 10, 6, 8, 4 });
        std::vector<ID> set2 = { 6, 8, 2, 2, 4, 10, 10, 6, 6, 6 };
        auto            n    = set1.intersect(set2.begin(), set2.end());

        if (n != 0) {
                throw TestFailure("set1.intersect() returned %u, expected 0",
                                  n);
        }

        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectInitializerListEmpty() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.intersect(std::initializer_list<ID>());

        if (n != 5) {
                throw TestFailure("set.intersect() returned %u, expected 5", n);
        }

        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectInitializerListMixed() // static
{
        IDSet set(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 });
        auto  n  = set.intersect({ 3, 1, 1, 6, 3, 4, 4, 4, 7, 7, 4, 8, 9, 9 });

        if (n != 5) {
                throw TestFailure("set.intersect() returned %u, expected 5", n);
        }

        checkContents(set, 1, 3, 4, 9);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectInitializerListEqual() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        auto  n  = set.intersect({ 6, 8, 2, 2, 4, 10, 10, 6, 6, 6 });

        if (n != 0) {
                throw TestFailure("set.intersect() returned %u, expected 0", n);
        }

        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectSQLNoBinding() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM customers");
        auto n = set.intersect_sql("SELECT number FROM customers "
                                   "WHERE country='Canada' ORDER BY number");
        if (n != 119) {
                throw TestFailure("set.intersect_sql() returned %u, expected 120",
                                  n);
        }

        checkContents(set, 202, 233, 260);
}

//--------------------------------------

void
wr::sql::IDSetTests::intersectSQLWithBinding() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM customers");
        auto n = set.intersect_sql(
                "SELECT number FROM customers WHERE country=? ORDER BY number",
                "New Zealand");

        if (n != 118) {
                throw TestFailure("set.intersect_sql() returned %u, expected 120",
                                  n);
        }

        checkContents(set, 323, 357, 412, 496);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceThis() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        set.symmetric_difference(set);
        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceIDSetEmptySet() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 }), empty;
        set.symmetric_difference(empty);
        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceIDSetWithEmpty() // static
{
        IDSet set1(db_), set2(db_, { 2, 10, 6, 8, 4 });
        set1.symmetric_difference(set2);
        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceIDSetEqualSet() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }), set2(db_, { 2, 10, 6, 8, 4 });
        set1.symmetric_difference(set2);
        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceIDSetSuperset() // static
{
        IDSet set1(db_, { 2, 10, 6, 8, 4 }),
              set2(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
        set1.symmetric_difference(set2);
        checkContents(set1, 0, 1, 3, 5, 7, 9, 11, 12);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceIDSetSubset() // static
{
        IDSet set1(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }),
              set2(db_, { 2, 10, 6, 8, 4 });
        set1.symmetric_difference(set2);
        checkContents(set1, 0, 1, 3, 5, 7, 9, 11, 12);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceIDSetDisjoint() // static
{
        IDSet set1(db_, { 2, 4, 6, 8, 10 }),
              set2(db_, { 1, 3, 5, 7, 9 });
        set1.symmetric_difference(set2);
        checkContents(set1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceIDSetMixed() // static
{
        IDSet set1(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 }),
              set2(db_, { 1, 4, 7, 8, 3, 6, 9 });
        set1.symmetric_difference(set2);
        checkContents(set1, 0, 2, 5, 6, 7, 8, 10, 15);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceStatementEmptySet() // static
{
        IDSet     set (db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, "SELECT 1 WHERE 0=1");  // yields no rows
        set.symmetric_difference(stmt);
        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceStatementWithEmpty() // static
{
        IDSet     set1(db_), set2(db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        set1.symmetric_difference(stmt);
        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceStatementEqualSet() // static
{
        IDSet     set1(db_, { 2, 10, 6, 8, 4 }), set2(db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        set1.symmetric_difference(stmt);
        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceStatementSuperset() // static
{
        IDSet     set1(db_, { 2, 10, 6, 8, 4 }),
                  set2(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 });
        Statement stmt(db_, printStr("SELECT 0, id FROM %s", set2));
        set1.symmetric_difference(stmt, 1);
        checkContents(set1, 0, 1, 3, 5, 7, 9, 11, 12);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceStatementSubset() // static
{
        IDSet     set1(db_, { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }),
                  set2(db_, { 2, 10, 6, 8, 4 });
        Statement stmt(db_, printStr("SELECT id FROM %s", set2));
        set1.symmetric_difference(stmt);
        checkContents(set1, 0, 1, 3, 5, 7, 9, 11, 12);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceStatementDisjoint() // static
{
        IDSet     set1(db_, { 2, 4, 6, 8, 10 }),
                  set2(db_, { 1, 3, 5, 7, 9 });
        Statement stmt(db_, printStr("SELECT 0, 0, id FROM %s", set2));
        set1.symmetric_difference(stmt, 2);
        checkContents(set1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceStatementMixed() // static
{
        db_.exec(printStr("CREATE TEMP TABLE %s (value INT)", __func__));
        db_.exec(printStr("INSERT INTO %s (value) VALUES (1),(1),(3),(4),(4),(4),(6),(7),(7),(8),(9),(9)",
                          __func__));

        IDSet     set1(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 });
        Statement stmt(db_, printStr("SELECT value FROM %s", __func__));

        set1.symmetric_difference(stmt);
        checkContents(set1, 0, 2, 5, 6, 7, 8, 10, 15);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceRangeEmpty() // static
{
        IDSet           set1(db_, { 2, 10, 6, 8, 4 });
        std::vector<ID> set2 = {};
        set1.symmetric_difference(set2.begin(), set2.end());
        checkContents(set1, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceRangeMixed() // static
{
        IDSet         set1(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 });
        std::list<ID> set2 = { 3, 1, 1, 6, 3, 4, 4, 4, 7, 7, 4, 8, 9, 9 };

        set1.symmetric_difference(set2.begin(), set2.end());
        checkContents(set1, 0, 2, 5, 6, 7, 8, 10, 15);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceRangeEqual() // static
{
        IDSet           set1(db_, { 2, 10, 6, 8, 4 });
        std::vector<ID> set2 = { 6, 8, 2, 2, 4, 10, 10, 6, 6, 6 };

        set1.symmetric_difference(set2.begin(), set2.end());
        checkContents(set1);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceInitializerListEmpty() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        set.symmetric_difference(std::initializer_list<ID>());
        checkContents(set, 2, 4, 6, 8, 10);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceInitializerListMixed() // static
{
        IDSet set(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 });
        set.symmetric_difference({ 3, 1, 1, 6, 3, 4, 4, 4, 7, 7, 4, 8, 9, 9 });
        checkContents(set, 0, 2, 5, 6, 7, 8, 10, 15);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceInitializerListEqual() // static
{
        IDSet set(db_, { 2, 10, 6, 8, 4 });
        set.symmetric_difference({ 6, 8, 2, 2, 4, 10, 10, 6, 6, 6 });
        checkContents(set);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceSQLNoBinding() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM customers WHERE country='USA'");
        auto n = set.symmetric_difference_sql(
                "SELECT number FROM customers WHERE sales_rep_employee_no=1323 ORDER BY number");

        checkContents(set, 112, 124, 129, 151, 157, 161, 168, 173, 181, 198,
                      202, 204, 205, 219, 239, 260, 286, 320, 321, 339, 347,
                      362, 363, 379, 424, 450, 455, 456, 462, 475, 487, 495);
}

//--------------------------------------

void
wr::sql::IDSetTests::symmetricDifferenceSQLWithBinding() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM customers WHERE country='France'");
        set.symmetric_difference_sql(
                "SELECT number FROM customers WHERE sales_rep_employee_no=? ORDER BY number",
                1370);

        checkContents(set, 141, 146, 172, 250, 350, 353, 406);
}

//--------------------------------------

void
wr::sql::IDSetTests::clear() // static
{
        IDSet set(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 });
        set.clear();
        checkContents(set);
        set.clear();
        checkContents(set);
        if (set.db() != &db_) {
                throw TestFailure("set.db() returned %p after clear(), expected %p",
                                  set.db(), &db_);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::swapBothEmptyUnattached() // static
{
        IDSet set1, set2;
        std::swap(set1, set2);
        checkContents(set1);
        checkSessionIsNull(set1, true);
        checkContents(set2);
        checkSessionIsNull(set2, true);
}

//--------------------------------------

void
wr::sql::IDSetTests::swapEmptyWithNonEmpty() // static
{
        IDSet set1, set2 = { 1, 4, 7, 8, 3, 6, 9 };
        std::swap(set1, set2);
        checkContents(set1, 1, 3, 4, 6, 7, 8, 9);
        checkSessionIsNull(set1, true);
        checkContents(set2);
        checkSessionIsNull(set2, true);
}

//--------------------------------------

void
wr::sql::IDSetTests::swapEmptyWithNonEmptyAttached() // static
{
        IDSet set1, set2(db_, { 1, 4, 7, 8, 3, 6, 9 });
        std::swap(set1, set2);
        checkContents(set1, 1, 3, 4, 6, 7, 8, 9);
        if (set1.db() != &db_) {
                throw TestFailure("set1.db() returned %p after swap, expected %p (&db_)",
                                  set1.db(), &db_);
        }
        checkContents(set2);
        checkSessionIsNull(set2, true);
}

//--------------------------------------

void
wr::sql::IDSetTests::swapBothNonEmptyAttachedSameDB() // static
{
        IDSet       set1(db_, { 0, 9, 2, 5, 4, 3, 1, 10, 15 }),
                    set2(db_, { 1, 4, 7, 8, 3, 6, 9 });
        std::string orig_sql_name1 = set1.sql_name(),
                    orig_sql_name2 = set2.sql_name();
        Statement   get1(db_, printStr("SELECT id FROM %s", set1)),
                    get2(db_, printStr("SELECT id FROM %s", set2));

        std::swap(set1, set2);

        checkContents(set1, 1, 3, 4, 6, 7, 8, 9);

        // names on the SQL side must remain stable
        if (set1.sql_name() != orig_sql_name1) {
                throw TestFailure("set1.sql_name() returned \"%s\" after swap, expected \"%s\"",
                                  set1.sql_name(), orig_sql_name1);
        }

        /* original statements must still work if both sets were attached
           to the same database connection */
        std::vector<ID> queried, expected({ 1, 3, 4, 6, 7, 8, 9 });
        for (Row row: get1) {
                queried.push_back(row.get<ID>(0));
        }
        if (queried != expected) {
                throw TestFailure("statement 'get1' returned wrong results after swap");
        }

        checkContents(set2, 0, 1, 2, 3, 4, 5, 9, 10, 15);

        if (set2.sql_name() != orig_sql_name2) {
                throw TestFailure("set2.sql_name() returned \"%s\" after swap, expected \"%s\"",
                                  set2.sql_name(), orig_sql_name2);
        }

        queried.clear();
        expected = { 0, 1, 2, 3, 4, 5, 9, 10, 15 };
        for (Row row: get2) {
                queried.push_back(row.get<ID>(0));
        }
        if (queried != expected) {
                throw TestFailure("statement 'get2' returned wrong results after swap");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::swapBothNonEmptyAttachedDiffDB() // static
{
        Session     tmp(":memory:");
        IDSet       set1(tmp, { 0, 9, 2, 5, 4, 3, 1, 10, 15 }),
                    set2(db_, { 1, 4, 7, 8, 3, 6, 9 });
        std::string orig_sql_name1 = set1.sql_name(),
                    orig_sql_name2 = set2.sql_name();

        std::swap(set1, set2);

        checkContents(set1, 1, 3, 4, 6, 7, 8, 9);

        if (set1.db() != &db_) {
                throw TestFailure("set1.db() returned %p after swap, expected %p (&db_)",
                                  set1.db(), &db_);
        }

        // names on the SQL side must remain stable
        if (set1.sql_name() != orig_sql_name1) {
                throw TestFailure("set1.sql_name() returned \"%s\" after swap, expected \"%s\"",
                                  set1.sql_name(), orig_sql_name1);
        }

        checkContents(set2, 0, 1, 2, 3, 4, 5, 9, 10, 15);

        if (set2.db() != &tmp) {
                throw TestFailure("set2.tmp() returned %p after swap, expected %p (&tmp)",
                                  set2.db(), &tmp);
        }

        if (set2.sql_name() != orig_sql_name2) {
                throw TestFailure("set2.sql_name() returned \"%s\" after swap, expected \"%s\"",
                                  set2.sql_name(), orig_sql_name2);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::begin() // static
{
        IDSet set = { 1, 5, 21, 13, 8, 3, 2 };
        auto  i   = set.begin();

        if (*i != 1) {
                throw TestFailure("*set.begin() returned %d, expected 1", *i);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::end() // static
{
        IDSet set = { 1, 5, 21, 13, 8, 3, 2 };
        auto  beg = set.begin(), end = set.end();

        if ((end - beg) != 7) {
                throw TestFailure("set.end() returned iterator at offset %d from set.begin(), expected offset 7",
                                  end - beg);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::iterate() // static
{
        IDSet           set = { 1, 5, 21, 13, 8, 3, 2 };
        std::vector<ID> seen,
                        expected = { 1, 2, 3, 5, 8, 13, 21 };

        for (auto id: set) {
                seen.push_back(id);
        }

        if (seen != expected) {
                std::ostringstream msg;
                msg << "seen != expected; seen = {";
                const char *sep = "";
                for (auto id: seen) {
                        msg << sep << id;
                        sep = ", ";
                }
                throw TestFailure(msg.str());
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::rbegin() // static
{
        IDSet set = { 1, 5, 21, 13, 8, 3, 2 };
        auto  i   = set.rbegin();

        if (*i != 21) {
                throw TestFailure("*set.rbegin() returned %d, expected 21", *i);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::rend() // static
{
        IDSet set = { 1, 5, 21, 13, 8, 3, 2 };
        auto  beg = set.rbegin(), end = set.rend();

        if ((end - beg) != 7) {
                throw TestFailure("set.rend() returned iterator at offset %d from set.begin(), expected offset 7",
                                  end - beg);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::reverseIterate() // static
{
        IDSet           set = { 1, 5, 21, 13, 8, 3, 2 };
        std::vector<ID> seen,
                        expected = { 21, 13, 8, 5, 3, 2, 1 };

        for (auto i = set.rbegin(), j = set.rend(); i != j; ++i) {
                seen.push_back(*i);
        }

        if (seen != expected) {
                std::ostringstream msg;
                msg << "seen != expected; seen = {";
                const char *sep = "";
                for (auto id: seen) {
                        msg << sep << id;
                        sep = ", ";
                }
                throw TestFailure(msg.str());
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::empty() // static
{
        IDSet set(db_);

        if (!set.empty()) {
                throw TestFailure("first call to set.empty() returned false, expected true");
        }

        set.insert({ 1, 5, 21, 13, 8, 3, 2 });

        if (set.empty()) {
                throw TestFailure("second call to set.empty() returned true, expected false");
        }

        db_.exec(printStr("DELETE FROM %s", set));

        if (!set.empty()) {
                throw TestFailure("third call to set.empty() returned false, expected true");
        }

        set.insert({ 1, 5, 21, 13, 8, 3, 2 });
        set.erase(set.begin(), set.end());

        if (!set.empty()) {
                throw TestFailure("fourth call to set.empty() returned false, expected true");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::size() // static
{
        IDSet set(db_);
        auto  n = set.size();

        if (n != 0) {
                throw TestFailure("first call to set.size() returned %u, expected 0",
                                  n);
        }

        set.insert({ 1, 5, 21, 13, 8, 3, 2 });
        n = set.size();

        if (n != 7) {
                throw TestFailure("second call to set.size() returned %u, expected 7",
                                  n);
        }

        db_.exec(printStr("DELETE FROM %s WHERE id < 10", set));
        n = set.size();

        if (n != 2) {
                throw TestFailure("third call to set.size() returned %u, expected 2",
                                  n);
        }

        set.insert({ 1, 5, 21, 13, 8, 3, 2 });
        set.erase(set.begin(), set.end());
        n = set.size();

        if (n != 0) {
                throw TestFailure("fourth call to set.size() returned %u, expected 0",
                                  n);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::maxSize() // static
{
        IDSet               set;
        IDSet::storage_type dummy;
        auto                n        = set.max_size(),
                            expected = dummy.max_size();

        if (n != expected) {
                throw TestFailure("set.max_size() returned %u, expected %u",
                                  n, expected);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::capacity() // static
{
        IDSet set(db_);
        auto  n   = set.capacity();

        if (n != 0) {
                throw TestFailure("first call to set.capacity() returned %u, expected 0",
                                  set.capacity());
        }

        set.insert_sql("SELECT number FROM orders");

        n = set.capacity();
        auto n_min = set.size();

        if (n < n_min) {
                throw TestFailure("second call to set.capacity() returned %u, expected >= %u",
                                  n, n_min);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::count() // static
{
        IDSet set;
        auto  n = set.count(1);
        if (n != 0) {
                throw TestFailure("set.count(1) returned %u, expected %u", n);
        }

        set = { 1, 5, 21, 13, 8, 3, 2 };

        n = set.count(10);
        if (n != 0) {
                throw TestFailure("set.count(10) returned %u, expected %u", n);
        }

        n = set.count(1);
        if (n != 1) {
                throw TestFailure("set.count(1) returned %u, expected %u", n);
        }

        n = set.count(5);
        if (n != 1) {
                throw TestFailure("set.count(5) returned %u, expected %u", n);
        }

        n = set.count(21);
        if (n != 1) {
                throw TestFailure("set.count(21) returned %u, expected %u", n);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::find() // static
{
        IDSet set;
        auto  i = set.find(1);
        if (i != set.end()) {
                throw TestFailure("set.find(1) returned wrong position, expected set.end()");
        }

        set = { 1, 5, 21, 13, 8, 3, 2 };

        i = set.find(10);
        if (i != set.end()) {
                throw TestFailure("set.find(10) returned wrong position, expected set.end()");
        }

        i = set.find(1);
        if (i != set.begin()) {
                throw TestFailure("set.find(1) returned wrong position, expected set.begin()");
        }

        i = set.find(5);
        if (i != (set.begin() + 3)) {
                throw TestFailure("set.find(5) returned wrong position, expected (set.begin() + 3)");
        }

        i = set.find(21);
        if (i != (set.end() - 1)) {
                throw TestFailure("set.find(21) returned wrong position, expected (set.end() - 1)");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::lowerBound() // static
{
        IDSet set;
        auto  i = set.lower_bound(1);
        if (i != set.end()) {
                throw TestFailure("set.lower_bound(1) returned wrong position, expected set.end()");
        }

        set = { 1, 5, 21, 13, 8, 3, 2 };

        i = set.lower_bound(0);
        if (i != set.begin()) {
                throw TestFailure("set.lower_bound(0) returned wrong position, expected set.begin()");
        }

        i = set.lower_bound(1);
        if (i != set.begin()) {
                throw TestFailure("set.lower_bound(1) returned wrong position, expected set.begin()");
        }

        i = set.lower_bound(5);
        if (i != (set.begin() + 3)) {
                throw TestFailure("set.lower_bound(5) returned wrong position, expected (set.begin() + 3)");
        }

        i = set.lower_bound(10);
        if (i != set.begin() + 5) {
                throw TestFailure("set.lower_bound(10) returned wrong position, expected (set.begin() + 5)");
        }

        i = set.lower_bound(21);
        if (i != (set.end() - 1)) {
                throw TestFailure("set.lower_bound(21) returned wrong position, expected (set.end() - 1)");
        }

        i = set.lower_bound(22);
        if (i != set.end()) {
                throw TestFailure("set.lower_bound(22) returned wrong position, expected set.end()");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::upperBound() // static
{
        IDSet set;
        auto  i = set.upper_bound(0);
        if (i != set.end()) {
                throw TestFailure("set.upper_bound(1) returned wrong position, expected set.end()");
        }

        set = { 1, 5, 21, 13, 8, 3, 2 };

        i = set.upper_bound(0);
        if (i != set.begin()) {
                throw TestFailure("set.upper_bound(0) returned wrong position, expected set.begin()");
        }

        i = set.upper_bound(1);
        if (i != (set.begin() + 1)) {
                throw TestFailure("set.upper_bound(1) returned wrong position, expected (set.begin() + 1)");
        }

        i = set.upper_bound(5);
        if (i != (set.begin() + 4)) {
                throw TestFailure("set.upper_bound(5) returned wrong position, expected (set.begin() + 4)");
        }

        i = set.upper_bound(10);
        if (i != set.begin() + 5) {
                throw TestFailure("set.upper_bound(10) returned wrong position, expected (set.begin() + 5)");
        }

        i = set.upper_bound(21);
        if (i != set.end()) {
                throw TestFailure("set.upper_bound(21) returned wrong position, expected set.end()");
        }

        i = set.upper_bound(22);
        if (i != set.end()) {
                throw TestFailure("set.upper_bound(22) returned wrong position, expected set.end()");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::equalRange() // static
{
        IDSet set;
        auto  i = set.equal_range(0);
        if ((i.first != set.end()) || (i.second != set.end())) {
                throw TestFailure("set.equal_range(1) returned wrong range, expected { set.end(), set.end() }");
        }

        set = { 1, 5, 21, 13, 8, 3, 2 };

        i = set.equal_range(0);
        if ((i.first != set.begin()) || (i.second != set.begin())) {
                throw TestFailure("set.equal_range(0) returned wrong range, expected { set.begin(), set.begin() }");
        }

        i = set.equal_range(1);
        if ((i.first != set.begin()) || (i.second != (set.begin() + 1))) {
                throw TestFailure("set.equal_range(1) returned wrong range, expected { set.begin(), set.begin() + 1 }");
        }

        i = set.equal_range(5);
        if ((i.first != (set.begin() + 3)) || (i.second != (set.begin() + 4))) {
                throw TestFailure("set.equal_range(5) returned wrong range, expected { set.begin() + 3, set.begin() + 4 }");
        }

        i = set.equal_range(10);
        if ((i.first != set.begin() + 5) || (i.second != set.begin() + 5)) {
                throw TestFailure("set.equal_range(10) returned wrong range, expected { set.begin() + 5, set.begin() + 5 }");
        }

        i = set.equal_range(21);
        if ((i.first != (set.end() - 1)) || (i.second != set.end())) {
                throw TestFailure("set.equal_range(21) returned wrong range, expected { set.end() - 1, set.end() }");
        }

        i = set.equal_range(22);
        if ((i.first != set.end()) || (i.second != set.end())) {
                throw TestFailure("set.equal_range(22) returned wrong range, expected { set.end(), set.end() }");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::indexOperator() // static
{
        IDSet set = { 1, 5, 21, 13, 8, 3, 2 };

        auto id = set[0];
        if (id != 1) {
                throw TestFailure("set[0] returned %d, expected 1", id);
        }

        id = set[3];
        if (id != 5) {
                throw TestFailure("set[3] returned %d, expected 5", id);
        }
        
        id = set[6];
        if (id != 21) {
                throw TestFailure("set[6] returned %d, expected 21", id);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::reserve() // static
{
        IDSet set(db_);
        set.reserve(128);
        auto cap = set.capacity();

        if (cap < 128) {
                throw TestFailure("set.capacity() returned %u after set.reserve(121)",
                                  cap);
        }

        set.insert(103);
        auto orig_data_addr = set.body_->storage_.data();
        set.insert_sql("SELECT number FROM customers");

        if (set.capacity() != cap) {
                throw TestFailure("set.capacity() returned %u after inserting all customer IDs, expected %u",
                                  set.capacity(), cap);
        }

        if (set.body_->storage_.data() != orig_data_addr) {
                throw TestFailure("address of first element is %p after inserting all customer IDs, expected %p",
                                  set.body_->storage_.data(), orig_data_addr);
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::shrinkToFit() // static
{
        IDSet set(db_);
        set.insert_sql("SELECT number FROM customers");
        set.clear();
        set.shrink_to_fit();
        if (set.capacity() != 0) {
                throw TestFailure("set.capacity() returned %u after set.shrink_to_fit(); expected 0",
                                  set.capacity());
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::compareEqual() // static
{
        IDSet empty1, empty2(db_);

        // database attachments must not affect comparison
        if (!(empty1 == empty2)) {
                throw TestFailure("empty1 should compare equal to empty2");
        }

        IDSet fib1 = { 1, 5, 21, 13, 8, 3, 2 },
              fib2 = { 1, 2, 3, 5, 8, 13, 21 },
              fib3 = { 1, 2, 3, 5, 8, 13, 21, 34 };

        if (!(fib1 == fib2)) {
                throw TestFailure("fib1 should compare equal to fib2");
        }

        if (fib1 == fib3) {
                throw TestFailure("fib1 should not compare equal to fib3");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::compareNotEqual() // static
{
        IDSet empty1, empty2(db_);

        if (empty1 != empty2) {
                throw TestFailure("empty1 should compare equal to empty2");
        }

        IDSet fib1 = { 1, 5, 21, 13, 8, 3, 2 },
              fib2 = { 1, 2, 3, 5, 8, 13, 21 },
              fib3 = { 1, 2, 3, 5, 8, 13, 21, 34 };

        if (fib1 != fib2) {
                throw TestFailure("fib1 should compare equal to fib2");
        }

        if (!(fib1 != fib3)) {
                throw TestFailure("fib1 should not compare equal to fib3");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::compareLess() // static
{
        IDSet empty1, empty2(db_);

        if (empty1 < empty2) {
                throw TestFailure("empty1 should compare equal to empty2");
        }

        IDSet set1 = { 1 },
              set2 = { 1, 2, 3 },
              set3 = { 2 },
              set4 = { 3, 1, 2 },
              set5 = { 1, 2, 3, 4 };

        if (!(empty1 < set1)) {
                throw TestFailure("empty1 should compare less than set1");
        }

        if (!(set1 < set2)) {
                throw TestFailure("set1 should compare less than set2");
        }

        // by lexicographical comparison (set2 < set3)
        if (!(set2 < set3)) {
                throw TestFailure("set2 should compare less than set3");
        }

        if (!(set1 < set3)) {
                throw TestFailure("set1 should compare less than set3");
        }

        if (set3 < set1) {
                throw TestFailure("set3 should compare greater than set1");
        }

        if (set2 < set4) {
                throw TestFailure("set2 should compare equal to set4");
        }

        if (!(set4 < set5)) {
                throw TestFailure("set4 should compare less than set5");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::compareLessOrEqual() // static
{
        IDSet empty1, empty2(db_);

        if (!(empty1 <= empty2)) {
                throw TestFailure("empty1 should compare equal to empty2");
        }


        IDSet set1 = { 1 },
              set2 = { 1, 2, 3 },
              set3 = { 2 },
              set4 = { 3, 1, 2 },
              set5 = { 1, 2, 3, 4 };

        if (!(empty1 <= set1)) {
                throw TestFailure("empty1 should compare less than set1");
        }

        if (!(set1 <= set2)) {
                throw TestFailure("set1 should compare less than set2");
        }

        // by lexicographical comparison (set2 <= set3)
        if (!(set2 <= set3)) {
                throw TestFailure("set2 should compare less than set3");
        }

        if (!(set1 <= set3)) {
                throw TestFailure("set1 should compare less than set3");
        }

        if (set3 <= set1) {
                throw TestFailure("set3 should compare greater than set1");
        }

        if (!(set2 <= set4)) {
                throw TestFailure("set2 should compare equal to set4");
        }

        if (!(set4 <= set5)) {
                throw TestFailure("set4 should compare less than set5");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::compareGreater() // static
{
        IDSet empty1, empty2(db_);

        if (empty1 > empty2) {
                throw TestFailure("empty1 should compare equal to empty2");
        }

        IDSet set1 = { 1 },
              set2 = { 1, 2, 3 },
              set3 = { 2 },
              set4 = { 3, 1, 2 },
              set5 = { 1, 2, 3, 4 };

        if (!(set1 > empty1)) {
                throw TestFailure("set1 should compare greater than empty1");
        }

        if (!(set2 > set1)) {
                throw TestFailure("set2 should compare greater than set1");
        }

        // by lexicographical comparison (set3 > set2)
        if (!(set3 > set2)) {
                throw TestFailure("set3 should compare greater than set2");
        }

        if (set1 > set3) {
                throw TestFailure("set1 should compare less than set3");
        }

        if (!(set3 > set1)) {
                throw TestFailure("set3 should compare greater than set1");
        }

        if (set4 > set2) {
                throw TestFailure("set4 should compare equal to set2");
        }

        if (!(set5 > set4)) {
                throw TestFailure("set5 should compare greater than set4");
        }
}

//--------------------------------------

void
wr::sql::IDSetTests::compareGreaterOrEqual() // static
{
        IDSet empty1, empty2(db_);

        if (!(empty1 >= empty2)) {
                throw TestFailure("empty1 should compare equal to empty2");
        }

        IDSet set1 = { 1 },
              set2 = { 1, 2, 3 },
              set3 = { 2 },
              set4 = { 3, 1, 2 },
              set5 = { 1, 2, 3, 4 };

        if (!(set1 >= empty1)) {
                throw TestFailure("set1 should compare greater than empty1");
        }

        if (!(set2 >= set1)) {
                throw TestFailure("set2 should compare greater than set1");
        }

        // by lexicographical comparison (set3 > set2)
        if (!(set3 >= set2)) {
                throw TestFailure("set3 should compare greater than set2");
        }

        if (set1 >= set3) {
                throw TestFailure("set1 should compare less than set3");
        }

        if (!(set3 >= set1)) {
                throw TestFailure("set3 should compare greater than set1");
        }

        if (!(set4 >= set2)) {
                throw TestFailure("set4 should compare equal to set2");
        }

        if (!(set5 >= set4)) {
                throw TestFailure("set5 should compare greater than set4");
        }
}
