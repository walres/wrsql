/**
 * \file SampleDB.h
 *
 * \brief Mock-up company database class used by wrSQL unit tests
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
#ifndef WRSQL_SAMPLE_DB_H
#define WRSQL_SAMPLE_DB_H

#include <wrsql/Session.h>


class SampleDB : public wr::sql::Session
{
public:
        using this_t = SampleDB;
        using base_t = Session;

        using base_t::base_t;

        void init(const wr::u8string_view &uri);

        static void createSchema(Session &db);
        static void dropSchema(Session &db);
        static void populateCustomers(Session &db);
        static void populateEmployees(Session &db);
        static void populateOffices(Session &db);
        static void populateProductLines(Session &db);
        static void populateProducts(Session &db);
        static void populateOrders(Session &db);
        static void populateOrderDetails(Session &db);
        static void populatePayments(Session &db);
        static void populateAllTables(Session &db);

        void createSchema()         { createSchema(*this); }
        void dropSchema()           { dropSchema(*this); }
        void populateCustomers()    { populateCustomers(*this); }
        void populateEmployees()    { populateEmployees(*this); }
        void populateOffices()      { populateOffices(*this); }
        void populateProductLines() { populateProductLines(*this); }
        void populateProducts()     { populateProducts(*this); }
        void populateOrders()       { populateOrders(*this); }
        void populateOrderDetails() { populateOrderDetails(*this); }
        void populatePayments()     { populatePayments(*this); }
        void populateAllTables()    { populateAllTables(*this); }
};


#endif // !WRSQL_SAMPLE_DB_H
