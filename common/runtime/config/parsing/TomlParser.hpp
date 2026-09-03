/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ParseResult.hpp"
#include "Reflection.hpp"
#include "common/exception/UserError.hpp"

#include <algorithm>
#include <chrono>
#include <concepts>
#include <map>
#include <toml++/toml.hpp>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace cta::runtime::parsing {

// Utility functions

std::chrono::system_clock::time_point dateTimeToTimePoint(const toml::date_time& dt);

// Constraints

template<class T>
struct is_std_optional : std::false_type {};

template<class U>
struct is_std_optional<std::optional<U>> : std::true_type {};

template<class T>
concept StdOptional = is_std_optional<std::remove_cvref_t<T>>::value;

template<class T>
struct is_std_chrono_time_point : std::false_type {};

template<class U>
struct is_std_chrono_time_point<std::chrono::time_point<U>> : std::true_type {};

template<class T>
concept StdChronoTimePoint = is_std_chrono_time_point<std::remove_cvref_t<T>>::value;

template<class T>
struct is_std_vector : std::false_type {};

template<class U, class A>
struct is_std_vector<std::vector<U, A>> : std::true_type {};

template<class T>
concept StdVector = is_std_vector<std::remove_cvref_t<T>>::value;

template<class T>
struct is_map_string_key : std::false_type {};

template<class V, class C, class A>
struct is_map_string_key<std::map<std::string, V, C, A>> : std::true_type {};

template<class V, class H, class E, class A>
struct is_map_string_key<std::unordered_map<std::string, V, H, E, A>> : std::true_type {};

template<class T>
concept MapStringKey = is_map_string_key<std::remove_cvref_t<T>>::value;

template<class T>
concept TomlValueConvertible =
  requires(toml::node_view<const toml::node> nv) { nv.template value<std::remove_cvref_t<T>>(); };

template<class T>
concept ScalarLike = TomlValueConvertible<T> && !StdOptional<T> && !reflection::Reflectable<T> && !StdVector<T>
                     && !MapStringKey<T> && !StdChronoTimePoint<T>;

// Forward declarations (needed because we have some recursive calls)

template<StdChronoTimePoint T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<StdOptional T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<StdVector T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<MapStringKey T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<ScalarLike T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<reflection::Reflectable T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<reflection::Reflectable T>
ParseResult parseTable(T& out, std::string_view fieldName, const toml::table& tbl, const bool strict);

// Implementations

/**
 * Parse a TOML date/time node into a std::chrono::system_clock::time_point.
 */
template<StdChronoTimePoint T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  const auto* dt_value = node.as_date_time();

  if (!dt_value) {
    return ParseResult::error("Field '" + std::string(fieldName) + "' must be a date/time.");
  }
  out = dateTimeToTimePoint(**dt_value);
  return ParseResult::success();
}

template<StdOptional T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  if (!node) {
    if (strict) {
      return ParseResult::error("Expected field '" + std::string(fieldName) + "' is missing.");
    }
    // In non-strict mode, a missing optional is fine
    return ParseResult::success();
  }
  using InnerType = typename std::remove_cvref_t<T>::value_type;
  InnerType tmp {};
  if (auto res = parseNode(tmp, fieldName, node, strict); !res.ok()) {
    return ParseResult::error(fieldName, res);
  }
  out = std::move(tmp);
  return ParseResult::success();
}

template<StdVector T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  const toml::array* arr = node.as_array();
  if (!arr) {
    return ParseResult::error("Field '" + std::string(fieldName) + "' must be an array.");
  }
  out.clear();
  out.reserve(arr->size());
  using ElemType = typename std::remove_cvref_t<T>::value_type;

  std::vector<ParseResult> errs;
  arr->for_each([&out, &fieldName, &errs, strict](auto& val) {
    ElemType elem {};

    if (auto res = parseNode(elem, fieldName, toml::node_view<const toml::node> {&val}, strict); !res.ok()) {
      errs.push_back(res);
      return;
    }
    out.push_back(std::move(elem));
  });
  if (errs.empty()) {
    return ParseResult::success();
  }
  return ParseResult::error(fieldName, errs);
}

template<MapStringKey T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  const toml::table* tbl = node.as_table();
  if (!tbl) {
    return ParseResult::error("Field '" + std::string(fieldName) + "' must be a table.");
  }
  out.clear();
  using ElemType = typename std::remove_cvref_t<T>::mapped_type;

  std::vector<ParseResult> errs;
  tbl->for_each([&out, &errs, strict](auto& key, auto& val) {
    ElemType elem {};
    if (auto res = parseNode(elem, key, toml::node_view<const toml::node> {&val}, strict); !res.ok()) {
      errs.push_back(res);
      return;
    }
    out.emplace(std::string(key.str()), std::move(elem));
  });
  if (errs.empty()) {
    return ParseResult::success();
  }
  return ParseResult::error(fieldName, errs);
}

template<ScalarLike T>
ParseResult parseNode(T& out,
                      std::string_view fieldName,
                      toml::node_view<const toml::node> node,
                      [[maybe_unused]] const bool strict) {
  using F = std::remove_cvref_t<T>;
  if constexpr (std::same_as<F, bool>) {
    if (!node.is_boolean()) {
      return ParseResult::error("Field '" + std::string(fieldName) + "' must be a boolean.");
    }
  } else if constexpr (std::integral<F>) {
    if (!node.is_integer()) {
      return ParseResult::error("Field '" + std::string(fieldName) + "' must be an integer.");
    }
  } else if constexpr (std::floating_point<F>) {
    if (!node.is_floating_point()) {
      return ParseResult::error("Field '" + std::string(fieldName) + "' must be a floating-point number.");
    }
  }
  auto val = node.value<F>();
  if (!val) {
    if constexpr (std::integral<F> || std::floating_point<F>) {
      return ParseResult::error("Field '" + std::string(fieldName) + "' is outside the supported range.");
    }
    return ParseResult::error("Field '" + std::string(fieldName) + "' has an invalid value or type.");
  }
  out = std::move(*val);
  return ParseResult::success();
}

template<reflection::Reflectable T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  const toml::table* tbl = node.as_table();
  if (!tbl) {
    return ParseResult::error("Field '" + std::string(fieldName) + "' must be a table.");
  }
  return parseTable(out, fieldName, *tbl, strict);
}

template<reflection::Reflectable T>
ParseResult parseTable(T& out, std::string_view fieldName, const toml::table& tbl, const bool strict) {
  std::unordered_set<std::string_view> seenFields;

  std::vector<ParseResult> errs;

  auto assignField = [&tbl, &errs, &seenFields, strict](std::string_view tableFieldName, auto& field) {
    const auto node = tbl[tableFieldName];

    if (!node) {
      if (strict) {
        errs.push_back(ParseResult::error("Expected field '" + std::string(tableFieldName) + "' is missing."));
      }
      return;
    }
    seenFields.insert(tableFieldName);
    auto res = parseNode(field, tableFieldName, node, strict);
    if (!res.ok()) {
      errs.push_back(res);
      return;
    }
  };

  // Do a pass over the fields of T and try to assign its members
  reflection::forEachMember(out, assignField);

  if (strict) {
    // In strict mode, we need to do a second pass to spot keys in the TOML but not in T
    for (const auto& [key, value] : tbl) {
      if (!seenFields.contains(key)) {
        errs.push_back(ParseResult::error("Unknown field '" + std::string(key) + "'."));
      }
    }
  }
  if (errs.empty()) {
    return ParseResult::success();
  }
  return ParseResult::error(fieldName, errs);
}

template<reflection::Reflectable T>
ParseResult parseTable(T& out, const toml::table& tbl, const bool strict) {
  return parseTable(out, "", tbl, strict);
}

}  // namespace cta::runtime::parsing
