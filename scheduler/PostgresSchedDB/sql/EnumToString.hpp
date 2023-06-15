/**
 * @project        The CERN Tape Archive (CTA)
 * @description    Macros and templates to convert enums to/from strings and PostgreSQL
 *                 enum representations
 * @copyright      Copyright © 2021-2022 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <stdexcept>
#include <string>

namespace cta {

/**
 * cta::to_string() to convert enumerations to strings
 *
 * This template should never be instantiated, it is needed only to allow the definition of the
 * specialised type (below).
 */
template<typename T>
T from_string(const std::string& val) {
  throw std::runtime_error("In from_string(): This function should not be instantiated.");
}

/**
 * TO_STRING macro: defines to_string(T) and from_string<T>(string) for an enum class : uint8_t
 */
#define TO_STRING(T)                                                                  \
  constexpr const char* to_string(postgresscheddb::T e) {                             \
    using namespace postgresscheddb;                                                  \
    return Strings##T[static_cast<uint8_t>(e)];                                       \
  }                                                                                   \
  template<>                                                                          \
  inline postgresscheddb::T from_string<postgresscheddb::T>(const std::string& val) { \
    using namespace postgresscheddb;                                                  \
    uint8_t i = 0;                                                                    \
    for (auto& v : Strings##T) {                                                      \
      if (v == val) return static_cast<T>(i);                                         \
      ++i;                                                                            \
    }                                                                                 \
    throw std::runtime_error("In from_string(): Unexpected enum value " + val);       \
  }

}  // namespace cta
