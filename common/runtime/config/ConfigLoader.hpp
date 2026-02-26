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

// TODO: maybe make this more lightweight to ease compiler errors
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

class ParseErrors {
public:
  std::string what() {
    std::string message;
    for (const auto& err : m_errors) {
      message += err + '\n';
    }
    return message;
  }

  void add(const std::string& err) { m_errors.push_back(err); }

  int count() { return m_errors.size(); }

private:
  std::vector<std::string> m_errors;
};

template<class Field>
bool parseNode(Field& out, toml::node_view<const toml::node> nv, const bool strict, ParseErrors& errs);

template<class T>
bool parseTable(T& out, const toml::table& tbl, const bool strict, ParseErrors& errs);

template<class Field>
bool parseOptional(Field& out, toml::node_view<const toml::node> nv, const bool strict, ParseErrors& errs)
  requires StdOptional<Field>
{
  if (!nv) {
    // Entry doesn't exist
    if (strict) {
      errs.add("Field not found in TOML.");
      return false;
    }
    // In non-strict mode, a missing optional is fine
    return true;
  }
  // Parse the inner result of the optional
  using InnerType = typename std::remove_cvref_t<Field>::value_type;
  InnerType tmp;
  if (!parseNode(tmp, nv, strict, errs)) {
    return false;
  }
  out = std::move(tmp);
  return true;
}

template<class Field>
bool parseVector(Field& out, toml::node_view<const toml::node> nv, const bool strict, ParseErrors& errs)
  requires StdVector<Field>
{
  const toml::array* arr = nv.as_array();
  if (!arr) {
    errs.add("Value is not an array.");
    return false;
  }
  out.clear();
  out.reserve(arr->size());
  using ElemType = typename std::remove_cvref_t<Field>::value_type;

  bool ok = true;
  arr->for_each([&](auto&& val) {
    ElemType elem {};
    if (!parseNode(elem, toml::node_view<const toml::node> {&val}, strict, errs)) {
      ok = false;
      return;
    }
    out.push_back(std::move(elem));
  });
  return ok;
}

template<class Field>
bool parseMapStringKey(Field& out, toml::node_view<const toml::node> nv, const bool strict, ParseErrors& errs)
  requires MapStringKey<Field>
{
  const toml::table* tbl = nv.as_table();
  if (!tbl) {
    errs.add("Value is not a table.");
    return false;
  }
  out.clear();
  using ElemType = typename std::remove_cvref_t<Field>::mapped_type;

  bool ok = true;
  tbl->for_each([&](auto&& key, auto&& val) {
    ElemType elem {};
    if (!parseNode(elem, toml::node_view<const toml::node> {&val}, strict, errs)) {
      ok = false;
      return;
    }
    out.emplace(std::string(key.str()), std::move(elem));
  });
  return ok;
}

template<class Field>
bool parseReflectableStruct(Field& out, toml::node_view<const toml::node> nv, const bool strict, ParseErrors& errs)
  requires Reflectable<Field>
{
  const toml::table* tbl = nv.as_table();
  if (!tbl) {
    errs.add("Value is not a table.");
    return false;
  }
  return parseTable(out, *tbl, strict, errs);
}

template<class Field>
bool parseScalar(Field& out, toml::node_view<const toml::node> nv, const bool strict, ParseErrors& errs)
  requires ScalarLike<Field>
{
  using F = std::remove_cvref_t<Field>;
  auto val = nv.value<F>();
  if (!val) {
    // Type mismatch or invalid value
    errs.add("Value contains a type mismatch or invalid value.");
    return false;
  }
  out = std::move(*val);
  return true;
}

template<class Field>
bool parseNode(Field& out, toml::node_view<const toml::node> nv, const bool strict, ParseErrors& errs) {
  if constexpr (StdOptional<Field>) {
    return parseOptional(out, nv, strict, errs);
  } else if constexpr (StdVector<Field>) {
    return parseVector(out, nv, strict, errs);
  } else if constexpr (MapStringKey<Field>) {
    return parseMapStringKey(out, nv, strict, errs);
  } else if constexpr (Reflectable<Field>) {
    return parseReflectableStruct(out, nv, strict, errs);
  } else if constexpr (ScalarLike<Field>) {
    return parseScalar(out, nv, strict, errs);
  } else {
    static_assert([] { return false; }(), "Field type not supported by parser");
  }
  return false;
}

template<class T>
bool parseTable(T& out, const toml::table& tbl, const bool strict, ParseErrors& errs)
  requires Reflectable<T>
{
  std::unordered_set<std::string_view> seenFields;
  bool ok = true;

  auto assignField = [&](auto field) {
    const auto nodeView = tbl[field.name];

    if (!nodeView) {
      if (strict) {
        errs.add("Field named '" + std::string(field.name) + "' not found in TOML.");
        ok = false;
      }
      return;
    }
    seenFields.insert(field.name);
    auto result = parseNode(out.*(field.ptr), nodeView, strict, errs);
    if (!result) {
      // parser error
      ok = false;
      return;
    }
  };

  // Do a pass over the fields of T and try to assign its members
  std::apply([&](const auto&... fields) { (assignField(fields), ...); }, T::fields());

  if (strict) {
    // In strict mode, we need to do a second pass to spot keys in the TOML but not in T
    for (const auto& [key, value] : tbl) {
      if (!seenFields.contains(key)) {
        errs.add("TOML value named '" + std::string(key) + "' not used.");
        ok = false;
      }
    }
  }
  return ok;
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
  ParseErrors errs {};
  bool res = parseTable(config, tbl, strict, errs);
  if (!res) {
    throw cta::exception::UserError("Invalid config in '" + filePath + "':\n" + errs.what());
  }
  return config;
}

}  // namespace cta::runtime
