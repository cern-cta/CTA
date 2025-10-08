/**
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <stdexcept>
#include <string>

namespace cta {

// Macros and templates to convert enums to/from strings and PostgreSQL enum representations

/**
 * cta::to_string() to convert enumerations to strings
 *
 * This template should never be instantiated, it is needed only to allow the definition of the
 * specialised type (below).
 */
  template<typename T>
  T from_string([[maybe_unused]] std::string_view val) {
    throw std::runtime_error("In from_string(): This function should not be instantiated."); \
}

/**
 * TO_STRING macro: defines to_string(T) and from_string<T>(string) for an enum class : uint8_t
 */
#define TO_STRING(T) \
constexpr const char* to_string(schedulerdb::T e) { \
  using namespace schedulerdb; \
  return Strings##T[static_cast<uint8_t>(e)]; \
} \
template<> \
inline schedulerdb::T from_string<schedulerdb::T>(std::string_view val) { \
  using namespace schedulerdb; \
  uint8_t i = 0; \
  for(auto &v : Strings##T) { \
    if(v == val) return static_cast<T>(i); \
    ++i; \
  } \
  throw std::runtime_error(std::string("In from_string(): Unexpected enum value ") + std::string(val)); \
}

} // namespace cta

