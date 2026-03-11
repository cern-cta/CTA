/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ConfigMeta.hpp"
#include "common/exception/UserError.hpp"

#include <concepts>
#include <map>
#include <sstream>
#include <toml++/toml.hpp>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace cta::runtime {

template<class T>
struct is_std_optional : std::false_type {};

template<class U>
struct is_std_optional<std::optional<U>> : std::true_type {};

template<class T>
concept StdOptional = is_std_optional<std::remove_cvref_t<T>>::value;

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

// Check for reflectable
// TODO: this can be simplified once we have a simple reflection implementation
template<class X>
concept TupleLike = requires { typename std::tuple_size<std::remove_cvref_t<X>>::type; };

template<class T, class Ptr>
struct member_ptr_points_to : std::false_type {};

template<class T, class U>
struct member_ptr_points_to<T, U T::*> : std::true_type {};

template<class T, class Ptr>
inline constexpr bool member_ptr_points_to_v = member_ptr_points_to<T, Ptr>::value;

template<class T, class F>
concept FieldMetaLike = requires(F f) {
  { f.name } -> std::convertible_to<std::string_view>;
  // ptr exists and is a pointer-to-member of T
  requires member_ptr_points_to_v<std::remove_cvref_t<T>, decltype(f.ptr)>;
};

template<class T>
concept Reflectable = requires {
  { std::remove_cvref_t<T>::fields() };
} && TupleLike<decltype(std::remove_cvref_t<T>::fields())> && []<class U>(std::type_identity<U>) consteval {
  using R = decltype(U::fields());
  constexpr std::size_t N = std::tuple_size_v<std::remove_cvref_t<R>>;
  return []<std::size_t... I>(std::index_sequence<I...>) consteval {
    return (FieldMetaLike<U, std::tuple_element_t<I, std::remove_cvref_t<R>>> && ...);
  }(std::make_index_sequence<N> {});
}(std::type_identity<std::remove_cvref_t<T>> {});

template<class T>
concept TomlValueConvertible =
  requires(toml::node_view<const toml::node> nv) { nv.template value<std::remove_cvref_t<T>>(); };

template<class T>
concept ScalarLike = TomlValueConvertible<T> && !StdOptional<T> && !Reflectable<T> && !StdVector<T> && !MapStringKey<T>;

// (forward) declarations

class ParseResult {
public:
  static ParseResult success() { return ParseResult(); }

  static ParseResult error(std::string_view error) { return ParseResult(error); }

  static ParseResult error(std::string_view fieldName, const ParseResult& child) {
    return ParseResult(fieldName, child);
  }

  static ParseResult error(std::string_view fieldName, const std::vector<ParseResult>& children) {
    return ParseResult(fieldName, children);
  }

  // TODO: fix indentation
  // TODO: sort alphabetically to group errors consistently independent of parsing logic
  // TODO: add test to verify error message
  std::string what(int indent = 0) const {
    if (ok()) {
      return "";
    }
    const int indentIncrement = 2;
    std::string indentation;
    for (int c = 0; c < indent; c++) {
      indentation += " ";
    }

    if (m_children.empty()) {
      return m_error + '\n';
    }
    std::string message;
    if (!m_fieldName.empty()) {
      message += "Failed to parse field '" + m_fieldName + "': ";
    }
    if (m_children.size() == 1) {
      message += m_children[0].what(indent + indentIncrement);
    } else {
      if (!message.empty()) {
        message += '\n';
      }
      for (size_t idx = 1; const auto& child : m_children) {
        message += indentation + std::to_string(idx) + ") " + child.what(indent + indentIncrement);
        idx++;
      }
    }
    return message;
  }

  bool ok() const { return m_error.empty() && m_children.empty(); }

  void addError(const ParseResult& child) { m_children.push_back(child); }

private:
  ParseResult() {}

  ParseResult(std::string_view error) : m_error(error) {}

  ParseResult(std::string_view fieldName, const ParseResult& child) : m_fieldName(fieldName), m_children {child} {}

  ParseResult(std::string_view fieldName, const std::vector<ParseResult>& children)
      : m_fieldName(fieldName),
        m_children {children} {}

  std::string m_fieldName;
  std::string m_error;
  std::vector<ParseResult> m_children;
};

template<class Field>
ParseResult parseNode(Field& out, std::string_view fieldName, toml::node_view<const toml::node> nv, const bool strict);

template<class Field>
ParseResult
parseOptional(Field& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict)
  requires StdOptional<Field>
{
  if (!node) {
    // Entry doesn't exist
    if (strict) {
      return ParseResult::error("Field named '" + std::string(fieldName) + "' not found.");
    }
    // In non-strict mode, a missing optional is fine
    return ParseResult::success();
  }
  // Parse the inner result of the optional
  using InnerType = typename std::remove_cvref_t<Field>::value_type;
  InnerType tmp;
  auto res = parseNode(tmp, fieldName, node, strict);
  if (!res.ok()) {
    return ParseResult::error(fieldName, res);
  }
  out = std::move(tmp);
  return ParseResult::success();
}

template<class Field>
ParseResult
parseVector(Field& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict)
  requires StdVector<Field>
{
  const toml::array* arr = node.as_array();
  if (!arr) {
    return ParseResult::error("Value named '" + std::string(fieldName) + "' is not an array.");
  }
  out.clear();
  out.reserve(arr->size());
  using ElemType = typename std::remove_cvref_t<Field>::value_type;

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

template<class Field>
ParseResult
parseMapStringKey(Field& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict)
  requires MapStringKey<Field>
{
  const toml::table* tbl = node.as_table();
  if (!tbl) {
    return ParseResult::error("Value named '" + std::string(fieldName) + "' is not a table.");
  }
  out.clear();
  using ElemType = typename std::remove_cvref_t<Field>::mapped_type;

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

template<class Field>
ParseResult
parseScalar(Field& out, std::string_view& fieldName, toml::node_view<const toml::node> node, const bool strict)
  requires ScalarLike<Field>
{
  using F = std::remove_cvref_t<Field>;
  auto val = node.value<F>();
  if (!val) {
    // Type mismatch or invalid value
    return ParseResult::error("Value named '" + std::string(fieldName)
                              + "' contains a type mismatch or invalid value.");
  }
  out = std::move(*val);
  return ParseResult::success();
}

template<class T>
ParseResult parseTable(T& out, std::string_view fieldName, const toml::table& tbl, const bool strict)
  requires Reflectable<T>
{
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

template<class Field>
ParseResult parseReflectableStruct(Field& out,
                                   std::string_view fieldName,
                                   toml::node_view<const toml::node> node,
                                   const bool strict)
  requires Reflectable<Field>
{
  const toml::table* tbl = node.as_table();
  if (!tbl) {
    return ParseResult::error("Value named '" + std::string(fieldName) + "' is not a table.");
  }
  return parseTable(out, fieldName, *tbl, strict);
}

template<class Field>
ParseResult
parseNode(Field& out, std::string_view fieldName, toml::node_view<const toml::node> node, const bool strict) {
  if constexpr (StdOptional<Field>) {
    return parseOptional(out, fieldName, node, strict);
  } else if constexpr (StdVector<Field>) {
    return parseVector(out, fieldName, node, strict);
  } else if constexpr (MapStringKey<Field>) {
    return parseMapStringKey(out, fieldName, node, strict);
  } else if constexpr (Reflectable<Field>) {
    return parseReflectableStruct(out, fieldName, node, strict);
  } else if constexpr (ScalarLike<Field>) {
    return parseScalar(out, fieldName, node, strict);
  } else {
    static_assert([] { return false; }(), "Field type not supported by parser");
  }
  // Should be unreachable
  return ParseResult::error("Field named '" + std::string(fieldName) + "' is an unsupported type.");
}

/**
 * @brief Loads and verifies the provided .toml and populates the provided struct type.
 * To correctly load the toml file, the names and structure of the struct must match that of the .toml file.
 *
 * @tparam T The struct to populate with the data from the .toml file.
 * @param filePath Path to the .toml file.
 * @param strict If set to true, treat unknown keys, missing keys, and type mismatches in the config file as errors.
 * @return T The populated struct.
 */
template<class T>
T loadFromToml(const std::string& filePath, bool strict = false) {
  toml::table tbl;
  try {
    tbl = toml::parse_file(filePath);
  } catch (const toml::parse_error& e) {
    std::ostringstream oss;
    oss << e;
    throw cta::exception::UserError("Failed to parse toml file '" + filePath + "': " + oss.str());
  }

  T config {};
  auto res = parseTable(config, "", tbl, strict);
  if (!res.ok()) {
    throw cta::exception::UserError("Invalid config in '" + filePath + "':\n" + res.what());
  }
  return config;
}

}  // namespace cta::runtime
