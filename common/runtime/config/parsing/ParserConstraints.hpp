/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <concepts>
#include <map>
#include <toml++/toml.hpp>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace cta::runtime::parsing {

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

}  // namespace cta::runtime::parsing
