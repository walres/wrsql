/**
 * \file SQLTestManager.cxx
 *
 * \brief Base class for wrSQL unit test modules
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
#ifndef WRSQL_TEST_MANAGER_H__
#define WRSQL_TEST_MANAGER_H__


#include <iostream>
#include <wrutil/filesystem.h>
#include <wrutil/string_view.h>
#include <wrutil/TestManager.h>


namespace wr {
namespace sql {


class SQLTestManager : public TestManager
{
public:
        using base_t = TestManager;

        SQLTestManager(const string_view &group, int argc, const char **argv) :
                base_t("sql::" + group.to_string(), argc, argv)
        {
                std::string &db_path_arg = (*this)["dbPath"];

                is_parent_process_ = db_path_arg.empty();

                if (is_parent_process_) {
                        db_path_ = temp_directory_path() / unique_path();
                        db_path_arg = to_generic_u8string(db_path_);
                } else {
                        db_path_ = db_path_arg;
                }

                db_uri_ = u8"sqlite3://";

                if (db_path_.has_root_name()) {
                        db_uri_ += u8"/";
                }

                db_uri_ += db_path_arg;
        }

        virtual ~SQLTestManager()
        {
                if (is_parent_process_) {
                        fs_error_code err;
                        remove(db_path_, err);
                        if (err) {
                                std::cerr << "*** Error deleting " << db_path_
                                          << ": " << err.message() << '\n';
                        }
                }
        }

        bool isParentProcess() const      { return is_parent_process_; }
        static const path &defaultPath()  { return db_path_; }
        static u8string_view defaultURI() { return db_uri_; }

private:
        bool               is_parent_process_;
        static path        db_path_;
        static std::string db_uri_;
};


path        SQLTestManager::db_path_;
std::string SQLTestManager::db_uri_;


} // namespace sql
} // namespace wr


#endif // !WRSQL_TEST_MANAGER_H__
