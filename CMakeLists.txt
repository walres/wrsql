#
# Copyright 2016-2017 James S. Waller
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

project(wrsql CXX C)
cmake_minimum_required(VERSION 2.6)

include(CheckCXXSourceRuns)

set(WRSQL_VERSION_MAJOR 0)
set(WRSQL_VERSION_MINOR 1)
set(WRSQL_VERSION_PATCH 0)

if (NOT TOP_BUILD_DIR)
        set(TOP_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR})
endif()

if (UNIX)
        list(INSERT WRSQL_SYS_LIBS 0 pthread)
endif()

#########################################
#
# Dependencies
#

#
# Check for wrUtil library
#
if (WRUTIL_ROOT)
        # fall through
elseif (IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/wrutil/)
        set(WRUTIL_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/wrutil)
        file(MAKE_DIRECTORY ${WRUTIL_BUILD_DIR})
        add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/wrutil ${WRUTIL_BUILD_DIR})

        include(${CMAKE_CURRENT_SOURCE_DIR}/wrutil/cmake/compiler-config.cmake)
        include(${WRUTIL_BUILD_DIR}/wrutil-config-internal.cmake)
endif()

if (NOT WRUTIL_BUILD_DIR)
        if (WRUTIL_ROOT)
                include(${WRUTIL_ROOT}/share/wrutil/compiler-config.cmake)
                include(${WRUTIL_ROOT}/share/wrutil/wrutil-config.cmake)
                include(${WRUTIL_ROOT}/share/wrutil/wrutil-targets.cmake)

                # for check_cxx_*() functions
                set(CMAKE_REQUIRED_LIBRARIES wrutil_static)

                set(CHECK_CXX_CODE "#include <wrutil/uiostream.h>\nint main() { wr::uout << \"Hello\" << std::endl\; return 0\; }\n")
                check_cxx_source_runs(${CHECK_CXX_CODE} HAVE_WRUTIL)
        endif()
        if (NOT HAVE_WRUTIL)
                message(SEND_ERROR "wrutil library not found. Please either specify -DWRUTIL_ROOT=<dir> where <dir> is the base directory of a pre-installed wrutil package, or place a copy of the wrutil source tree (with directory name \"wrutil\") under the top directory of wrsql.")
        endif()
endif()

#
# Check for SQLite3 library
#
set(CMAKE_REQUIRED_DEFINITIONS ${CXX_CHOSEN_STD_FLAG})

IF (SQLITE3_ROOT)
        file(TO_CMAKE_PATH "${SQLITE3_ROOT}" SQLITE3_ROOT)
endif()

if (SQLITE3_INCLUDE)
        file(TO_CMAKE_PATH "${SQLITE3_INCLUDE}" SQLITE3_INCLUDE)
else()
        if (SQLITE3_ROOT)
                set(SQLITE3_INCLUDE "${SQLITE3_ROOT}/include")
        endif()
endif()

if (SQLITE3_INCLUDE)
        include_directories(${SQLITE3_INCLUDE})
        set(CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS} -I${SQLITE3_INCLUDE}")
endif()

if (SQLITE3_LIB)
        file(TO_CMAKE_PATH "${SQLITE3_LIB}" SQLITE3_LIB)
else()
        if (SQLITE3_ROOT)
                set(SQLITE3_LIB "${SQLITE3_ROOT}/lib")
        endif()
endif()

if (SQLITE3_LIB)
        link_directories(${SQLITE3_LIB})
        find_library(SQLITE3_LIB_NAME sqlite3 PATHS "${SQLITE3_LIB}")
else()
        find_library(SQLITE3_LIB_NAME sqlite3)
endif()

if (SQLITE3_LIB_NAME)
        set(CMAKE_REQUIRED_LIBRARIES "${SQLITE3_LIB_NAME}")
        set(CHECK_CXX_CODE "#ifdef _WIN32\n#define SQLITE_API __declspec(dllimport)\n#endif\n#include <sqlite3.h>\nint main() { sqlite3 *db\; sqlite3_open(\":memory:\", &db)\; sqlite3_close(db)\; return 0\; }\n")

        check_cxx_source_runs(${CHECK_CXX_CODE} HAVE_SQLITE3)
                # use check_cxx_source_runs() to verify no link errors
else()
        message("sqlite3 library not found.")
endif()

if (NOT HAVE_SQLITE3)
        unset(HAVE_SQLITE3 CACHE)
        unset(SQLITE3_LIB_NAME CACHE)
        message(SEND_ERROR "sqlite3 dependency not satisfied.\nPlease either specify -DSQLITE3_ROOT=<dir> where <dir> is the base directory of your sqlite3 installation, or specify -DSQLITE3_INCLUDE=<inc_dir> and -DSQLITE3_LIB=<lib_dir> where <inc_dir> and <lib_dir> are the directories containing the sqlite3 headers and libraries respectively.\nIf using shared sqlite3 library (DLL) please ensure your environment is set to pick it up when executing a program linked to it.")
endif()

unset(CMAKE_REQUIRED_DEFINITIONS)
unset(CMAKE_REQUIRED_LIBRARIES)

########################################
#
# Target Definitions
#
include_directories(include)

set(WRSQL_SOURCES
        src/Error.cxx
        src/IDSet.cxx
        src/Session.cxx
        src/Statement.cxx
        src/Transaction.cxx
)

set(WRSQL_HEADERS
        include/wrsql/Config.h
        include/wrsql/Error.h
        include/wrsql/IDSet.h
        include/wrsql/Session.h
        include/wrsql/Statement.h
        src/SessionPrivate.h
        include/wrsql/Transaction.h
)

add_library(wrsql SHARED ${WRSQL_SOURCES} ${WRSQL_HEADERS})
add_library(wrsql_static STATIC ${WRSQL_SOURCES} ${WRSQL_HEADERS})

target_link_libraries(wrsql wrutil sqlite3)

set_target_properties(wrsql PROPERTIES
        COMPILE_FLAGS "-Dwrutil_IMPORTS ${WR_SOFLAGS}"
        SOVERSION ${WRSQL_VERSION_MAJOR}
        VERSION ${WRSQL_VERSION_MAJOR}.${WRSQL_VERSION_MINOR}.${WRSQL_VERSION_PATCH}
)

########################################
#
# Unit Tests
#
add_executable(SessionTests test/SessionTests.cxx
                test/SampleDB.cxx test/SampleDB.h)

add_executable(StatementTests test/StatementTests.cxx
                test/SampleDB.cxx test/SampleDB.h)

add_executable(TransactionTests test/TransactionTests.cxx
                test/SampleDB.cxx test/SampleDB.h)

add_executable(IDSetTests test/IDSetTests.cxx test/SampleDB.cxx test/SampleDB.h)

set(TESTS SessionTests StatementTests TransactionTests IDSetTests)

set_target_properties(${TESTS} PROPERTIES
        COMPILE_FLAGS "-Dwrutil_IMPORTS -Dwrsql_IMPORTS"
        RUNTIME_OUTPUT_DIRECTORY test
)

foreach(TEST ${TESTS})
        add_test(${TEST} test/${TEST})
        target_link_libraries(${TEST} wrsql wrutil wrdebug sqlite3
                              ${WRSQL_SYS_LIBS})
endforeach(TEST)

########################################
#
# Output Directories
#
set_target_properties(wrsql wrsql_static
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${TOP_BUILD_DIR}/bin
        ARCHIVE_OUTPUT_DIRECTORY ${TOP_BUILD_DIR}/lib
        LIBRARY_OUTPUT_DIRECTORY ${TOP_BUILD_DIR}/lib
)

########################################
#
# Installation
#
install(TARGETS wrsql wrsql_static
        EXPORT wrsql-targets
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
)

install(DIRECTORY include/wrsql
        DESTINATION include
        DIRECTORY_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                              GROUP_READ GROUP_EXECUTE
                              WORLD_READ WORLD_EXECUTE
        FILE_PERMISSIONS OWNER_READ OWNER_WRITE
                         GROUP_READ
                         WORLD_READ
)

install(EXPORT wrsql-targets DESTINATION share/wrsql)

configure_file(cmake/wrsql-config.cmake.in wrsql-config.cmake @ONLY)
configure_file(cmake/wrsql-config-internal.cmake.in
               wrsql-config-internal.cmake @ONLY)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/wrsql-config.cmake
        DESTINATION share/wrsql
        PERMISSIONS OWNER_READ OWNER_WRITE
                    GROUP_READ
                    WORLD_READ
)
