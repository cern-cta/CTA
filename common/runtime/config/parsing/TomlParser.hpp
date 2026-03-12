/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

// TODO: change
#include "../ConfigMeta.hpp"
#include "ParseResult.hpp"
#include "ParserConstraints.hpp"
#include "common/exception/UserError.hpp"

#include <algorithm>
#include <concepts>
#include <map>
#include <toml++/toml.hpp>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace cta::runtime::parsing {

// Forward declarations

template<StdOptional T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<StdVector T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<StdVector T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<MapStringKey T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<ScalarLike T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<Reflectable T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict);

template<Reflectable T>
ParseResult parseTable(T& out, std::string_view fieldName, const toml::table& tbl, const bool strict);

// Implementations

template<StdOptional T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  if (!node) {
    if (strict) {
      return ParseResult::error("Field named '" + std::string(fieldName) + "' not found.");
    }
    // In non-strict mode, a missing optional is fine
    return ParseResult::success();
  }
  using InnerType = typename std::remove_cvref_t<T>::value_type;
  InnerType tmp;
  auto res = parseNode(tmp, fieldName, node, strict);
  if (!res.ok()) {
    return ParseResult::error(fieldName, res);
  }
  out = std::move(tmp);
  return ParseResult::success();
}

template<StdVector T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  const toml::array* arr = node.as_array();
  if (!arr) {
    return ParseResult::error("Value named '" + std::string(fieldName) + "' is not an array.");
  }
  out.clear();
  out.reserve(arr->size());
  using ElemType = typename std::remove_cvref_t<T>::value_type;

  std::vector<ParseResult> errs;
  arr->for_each([&](auto&& val) {
    ElemType elem {};
    auto res = parseNode(elem, fieldName, toml::node_view<const toml::node> {&val}, strict);
    if (!res.ok()) {
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
    return ParseResult::error("Value named '" + std::string(fieldName) + "' is not a table.");
  }
  out.clear();
  using ElemType = typename std::remove_cvref_t<T>::mapped_type;

  std::vector<ParseResult> errs;
  tbl->for_each([&](auto&& key, auto&& val) {
    ElemType elem {};
    auto res = parseNode(elem, key, toml::node_view<const toml::node> {&val}, strict);
    if (!res.ok()) {
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
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  using F = std::remove_cvref_t<T>;
  auto val = node.value<F>();
  if (!val) {
    return ParseResult::error("Value named '" + std::string(fieldName)
                              + "' contains a type mismatch or invalid value.");
  }
  out = std::move(*val);
  return ParseResult::success();
}

template<Reflectable T>
ParseResult parseNode(T& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  const toml::table* tbl = node.as_table();
  if (!tbl) {
    return ParseResult::error("Value named '" + std::string(fieldName) + "' is not a table.");
  }
  return parseTable(out, fieldName, *tbl, strict);
}

template<Reflectable T>
ParseResult parseTable(T& out, std::string_view fieldName, const toml::table& tbl, const bool strict) {
  std::unordered_set<std::string_view> seenFields;

  std::vector<ParseResult> errs;

  auto assignField = [&](auto field) {
    const auto node = tbl[field.name];

    if (!node) {
      if (strict) {
        errs.push_back(ParseResult::error("Field named '" + std::string(field.name) + "' not found."));
      }
      return;
    }
    seenFields.insert(field.name);
    auto res = parseNode(out.*(field.ptr), field.name, node, strict);
    if (!res.ok()) {
      errs.push_back(res);
      return;
    }
  };

  // Do a pass over the fields of T and try to assign its members
  // TODO: replace T::fields with reflection
  std::apply([&](const auto&... fields) { (assignField(fields), ...); }, T::fields());

  if (strict) {
    // In strict mode, we need to do a second pass to spot keys in the TOML but not in T
    for (const auto& [key, value] : tbl) {
      if (!seenFields.contains(key)) {
        errs.push_back(ParseResult::error("Value named '" + std::string(key) + "' not used."));
      }
    }
  }
  if (errs.empty()) {
    return ParseResult::success();
  }
  return ParseResult::error(fieldName, errs);
}

template<Reflectable T>
ParseResult parseTable(T& out, const toml::table& tbl, const bool strict) {
  return parseTable(out, "", tbl, strict);
}

}  // namespace cta::runtime::parsing
