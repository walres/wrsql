/**
 * \file wrsql/Config.h
 *
 * \brief Platform-specific definitions for the wrSQL library
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
#ifndef WRSQL_CONFIG_H
#define WRSQL_CONFIG_H

#include <wrutil/Config.h>


#if WR_WINDOWS
#       ifdef wrsql_EXPORTS
#               define WRSQL_API __declspec(dllexport)
#       elif defined(wrsql_IMPORTS)
#               define WRSQL_API __declspec(dllimport)
#       else
#               define WRSQL_API
#       endif
#elif WR_HAVE_ELF_VISIBILITY_ATTR
#       ifdef wrsql_EXPORTS
#               define WRSQL_API [[gnu::visibility("default")]]
#       else
#               define WRSQL_API
#       endif
#else
#       define WRSQL_API
#endif


#endif // !WRSQL_CONFIG_H
