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

template<class Field>
bool parseNode(Field& out, toml::node_view<const toml::node> nv, bool strict);

template<class T>
bool parseInto(T& out, const toml::table& tbl, bool strict);

// Parse optionals
template<class Field>
bool parseOptional(Field& out, toml::node_view<const toml::node> nv, bool strict)
  requires StdOptional<Field>
{
  if (!nv) {
    // Entry doesn't exist
    if (strict) {
      // error
      return false;
    }
    return true;
  }
  // Parse the inner result of the optional
  using InnerType = typename std::remove_cvref_t<Field>::value_type;
  InnerType tmp;
  if (!parseNode(tmp, nv, strict)) {
    return false;
  }
  out = std::move(tmp);
  return true;
}

// Parse vector
template<class Field>
bool parseVector(Field& out, toml::node_view<const toml::node> nv, bool strict)
  requires StdVector<Field>
{
  const toml::array* arr = nv.as_array();
  if (!arr) {
    // Fail; not an array
    return false;
  }
  out.clear();
  out.reserve(arr->size());
  using ElemType = typename std::remove_cvref_t<Field>::value_type;

  arr->for_each([&](auto&& el) {
    ElemType elem {};
    if (!parseNode(elem, el, strict)) {
      return false;
    }
    out.push_back(std::move(elem));
  });
  return true;
}

// Parse string maps
template<class Field>
bool parseMapStringKey(Field& out, toml::node_view<const toml::node> nv, bool strict)
  requires MapStringKey<Field>
{
  const toml::table* tbl = nv.as_table();
  if (!tbl) {
    // Fail; not a table
    return false;
  }
  out.clear();
  using ElemType = typename std::remove_cvref_t<Field>::mapped_type;

  tbl->for_each([&](auto&& key, auto&& node) {
    ElemType elem {};
    if (!parseNode(elem, toml::node_view<const toml::node> {&node}, strict)) {
      return false;  // see note below
    }
    out.emplace(std::string(key.str()), std::move(elem));
    return true;
  });
  return true;
}

// Parse reflectable struct
template<class Field>
bool parseReflectableStruct(Field& out, toml::node_view<const toml::node> nv, bool strict)
  requires Reflectable<Field>
{
  const toml::table* tbl = nv.as_table();
  if (!tbl) {
    // Fail; not a table
    return false;
  }
  return parseInto(out, *tbl, strict);
}

// Scalar
template<class Field>
bool parseScalar(Field& out, toml::node_view<const toml::node> nv, bool strict)
  requires ScalarLike<Field>
{
  using F = std::remove_cvref_t<Field>;
  auto val = nv.value<F>();
  if (!val) {
    // Type mismatch or invalid value
    return false;
  }
  out = std::move(*val);
  return true;
}

template<class Field>
bool parseNode(Field& out, toml::node_view<const toml::node> nv, bool strict) {
  if constexpr (StdOptional<Field>) {
    return parseOptional(out, nv, strict);
  } else if constexpr (StdVector<Field>) {
    return parseVector(out, nv, strict);
  } else if constexpr (MapStringKey<Field>) {
    return parseMapStringKey(out, nv, strict);
  } else if constexpr (Reflectable<Field>) {
    return parseReflectableStruct(out, nv, strict);
  } else if constexpr (ScalarLike<Field>) {
    return parseScalar(out, nv, strict);
  } else {
    static_assert([] { return false; }(), "Field type not supported by parser");
  }
  return false;
}

// TODO: rename to parse table?
template<class T>
bool parseInto(T& out, const toml::table& tbl, bool strict) {
  std::unordered_set<std::string_view> seenFields;
  bool ok = true;

  auto assignField = [&](auto field) {
    const auto nodeView = tbl[field.name];

    if (!nodeView) {
      // missing key from toml file
      if (strict) {
        ok = false;
        // Add to errors
      }
      return;
    }
    auto result = parseNode(out.*(field.ptr), nodeView, strict);
    if (!result) {
      // parser error
      ok = false;
      return;
    }

    seenFields.insert(field.name);
  };

  // Do a pass over the fields of T and try to assign its members
  std::apply([&](const auto&... fields) { (assignField(fields), ...); }, T::fields());

  if (strict) {
    // In strict mode, we need to do a second pass to spot keys in the TOML but not in T
    for (const auto& [key, value] : tbl) {
      if (!seenFields.contains(key)) {
        // add to list of errors
        return false;
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
T loadFromToml(const std::string& filePath, bool strict = false)
  requires Reflectable<T>
{
  toml::table tbl;
  try {
    tbl = toml::parse_file(filePath);
  } catch (const toml::parse_error& e) {
    std::ostringstream oss;
    oss << e;
    throw cta::exception::UserError("Failed to parse toml file '" + filePath + "': " + oss.str());
  }

  T config {};
  bool res = parseInto(config, tbl, strict);
  if (!res) {
    throw cta::exception::UserError("Invalid config in '" + filePath + "': " + "some error");
  }
  return config;
}

}  // namespace cta::runtime
