/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string_view>

namespace cta::runtime {

template<typename TClass, typename TField>
struct FieldMeta {
  std::string_view name;
  TField TClass::* ptr;
};

template<class TClass, class TField>
consteval auto field(std::string_view name, TField TClass::* ptr) -> FieldMeta<TClass, TField> {
  return {name, ptr};
}

}  // namespace cta::runtime
