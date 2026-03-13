/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <array>
#include <concepts>
#include <source_location>
#include <string>
#include <tuple>

/*
 * Note that this is not a full reflection library. Instead, this is a somewhat simple (believe it or not) and constrained implementation that provides
 * the forEachMember function, which can be used to loop over the members of Aggregate types. Note that these types MUST have a member called memberCount() that gives
 * the number of members in the type. This is because figuring this out automatically is so non-trivial that we decided not to take the risk of including it.
 *
 * Most of the implementation relies on template magic, so its usage should be limited to the config parsing.
 * Ideally, this is replaced by proper C++ reflection as soon as this is available.
 *
 * The following resources were used in creating this implementation:
 * - https://stackoverflow.com/questions/72275744/any-c20-alternative-tricks-to-p1061-structured-bindings-can-introduce-a-pack
 * - https://stackoverflow.com/questions/78491639/how-to-get-data-member-names-with-source-locationfunction-name
 * - https://stackoverflow.com/questions/78863041/getting-index-of-current-tuple-item-in-stdapply
 */

namespace cta::runtime::parsing::reflection {

/**
 * @brief Ensures T is an Aggregate type.
 */
template<class T>
concept Aggregate = std::is_aggregate_v<T>;

/**
 * @brief Obtains the name of a type member by inspecting the template arguments using std::source_location and some string manipulation magic.
 * Supports both gcc and clang.
 *
 * @tparam Args Template arguments containing the member.
 * @return consteval The name of the member.
 */
template<auto... Args>
consteval std::string_view getMemberName() {
  std::string_view name = std::source_location::current().function_name();
#ifdef __clang__
  // Example string:
  // std::string_view getMemberName() [Args = <&fake.testField>]
  name = name.substr(name.rfind('.') + 1);
  name = name.substr(0, name.find('>'));
#else
  // Example string:
  // consteval std::string_view getMemberName() [with auto ...Args = {(& StaticWrapper<Foo>::fake.Foo::testField)}; std::string_view = std::basic_string_view<char>]
  name = name.substr(name.rfind('&'));
  name = name.substr(0, name.rfind(")};"));
  name = name.substr(name.rfind("::") + 2);
#endif
  return name;
}

/**
 * @brief Takes the Aggregate type T and unpacks all of its members into a tuple.
 *
 * @tparam T The Aggregate type to unpack.
 * @tparam N The number of members of T. If not provided, this is calculated automatically.
 * @param t Instance of T to unpack.
 * @return constexpr auto A tuple containing references to each member of t.
 */
template<Aggregate T, std::size_t N>
constexpr auto asTuple(T& t) {
  // Yes this is hacky, but it is the only way to unpack T in C++20.
  // All the reflection libraries at the time of writing do something similar.
  // clang-format off
  if constexpr (N == 1) {
    auto& [m1] = t;
    return std::tie(m1);
  } else if constexpr (N == 2) {
    auto& [m1, m2] = t;
    return std::tie(m1, m2);
  } else if constexpr (N == 3) {
    auto& [m1, m2, m3] = t;
    return std::tie(m1, m2, m3);
  } else if constexpr (N == 4) {
    auto& [m1, m2, m3, m4] = t;
    return std::tie(m1, m2, m3, m4);
  } else if constexpr (N == 5) {
    auto& [m1, m2, m3, m4, m5] = t;
    return std::tie(m1, m2, m3, m4, m5);
  } else if constexpr (N == 6) {
    auto& [m1, m2, m3, m4, m5, m6] = t;
    return std::tie(m1, m2, m3, m4, m5, m6);
  } else if constexpr (N == 7) {
    auto& [m1, m2, m3, m4, m5, m6, m7] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7);
  } else if constexpr (N == 8) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8);
  } else if constexpr (N == 9) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9);
  } else if constexpr (N == 10) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10);
  } else if constexpr (N == 11) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11);
  } else if constexpr (N == 12) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12);
  } else if constexpr (N == 13) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13);
  } else if constexpr (N == 14) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14);
  } else if constexpr (N == 15) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15);
  } else if constexpr (N == 16) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16);
  } else if constexpr (N == 17) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17);
  } else if constexpr (N == 18) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18);
  } else if constexpr (N == 19) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19);
  } else if constexpr (N == 20) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20);
  } else if constexpr (N == 21) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21);
  } else if constexpr (N == 22) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22);
  } else if constexpr (N == 23) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23);
  } else if constexpr (N == 24) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24);
  } else if constexpr (N == 25) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25);
  } else if constexpr (N == 26) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26);
  } else if constexpr (N == 27) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27);
  } else if constexpr (N == 28) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28);
  } else if constexpr (N == 29) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29);
  } else if constexpr (N == 30) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30);
  } else if constexpr (N == 31) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31);
  } else if constexpr (N == 32) {
    auto& [m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31, m32] = t;
    return std::tie(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31, m32);
  } else {
    // The 32 limit is somewhat arbitrary. If we want to reflect a struct with more, just add more if statements.
    static_assert(N != N, "T contains too many members. asTuple currently supports a maximum of 32 members.");
  }
  // clang-format on
}

/**
 * @brief Static storage for type T to ensure we can create a compile time instance of T to extract its member names.
 */
template<Aggregate T>
struct StaticWrapper {
  inline static T fake {};
};

/**
 * @brief Returns a list of member names for a given type T.
 *
 * @tparam T The Aggregate type to extract the member names of.
 * @tparam N The number of members of T. If not provided, this is calculated automatically.
 * @return consteval An array of strings containing the member names of T
 */
template<Aggregate T, std::size_t N>
consteval auto getMemberNames() {
  return [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    constexpr auto members = asTuple<T, N>(StaticWrapper<T>::fake);
    return std::array<std::string_view, sizeof...(Is)> {getMemberName<&std::get<Is>(members)>()...};
  }(std::make_index_sequence<N> {});
}

/**
 * @brief The main function of this simply reflection implementations. Allows for looping over the members of Aggregate types.
 * For example:
 *
 *   Foo foo;
 *   forEachMember(foo, [](std::string_view fieldName, auto &field) {
 *      // ...
 *   });
 *
 * Fundamentally, this function solves three things:
 * - It needs to figure out the names of the members of T. This must happen at compile time.
 * - It needs to loop over the members of T. This must happen at runtime.
 * - It needs to somehow combine this information so that when you loop over T at runtime, you have the name of the corresponding member generated at compile time.
 *
 * @tparam T The Aggregate type of t.
 * @tparam F The type of func.
 * @param t Instance of T that should be looped over.
 * @param func The visitor function that should be invoked for each member. Must be a void function taking (std::string_view, auto&).
 */
template<Aggregate T, typename F>
void forEachMember(T& t, F&& func) {
  // Turns out counting the number of members is not trivial at all. There are just too many edge cases...
  // There are a few tricks one can do with aggregate initialisation (T{args}), such as described here: https://stackoverflow.com/a/63172566
  // But this fails with optional fields, so for now, we'll just have to explicitly specify the member count per struct.
  // In theory you could do it with: https://towardsdev.com/counting-the-number-of-fields-in-an-aggregate-in-c-20-c81aecfd725c
  // But good luck with that if anything breaks...
  constexpr std::size_t N = T::memberCount();
  constexpr auto member_names = getMemberNames<T, N>();
  auto members_tuple = asTuple<T, N>(t);
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    ((func(member_names[Is], std::get<Is>(members_tuple))), ...);
  }(std::make_index_sequence<N> {});
}

}  // namespace cta::runtime::parsing::reflection
