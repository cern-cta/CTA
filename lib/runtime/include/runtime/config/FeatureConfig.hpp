/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ValidationResult.hpp"

#include <algorithm>
#include <initializer_list>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace cta::runtime {

inline bool featureEnabled(const std::vector<std::string>& enabled, std::string_view name) {
  return std::ranges::find(enabled, name) != enabled.end();
}

/// Check application-specific names without silently deduplicating the operator's list.
inline ValidationResult validateFeatureNames(const std::vector<std::string>& enabled,
                                             std::initializer_list<std::string_view> supported,
                                             std::string_view field = "enabled_features") {
  ValidationResult result;
  std::set<std::string> seen;
  for (const auto& name : enabled) {
    if (std::ranges::find(supported, name) == supported.end()) {
      result.addError(field, "unsupported feature '" + name + "'");
    }
    if (!seen.insert(name).second) {
      result.addError(field, "duplicate feature '" + name + "'");
    }
  }
  return result;
}

/// Validate maturity-specific lists, including actionable guidance when a feature changes classification.
inline ValidationResult validateFeatureLists(const std::vector<std::string>& enabled,
                                             const std::vector<std::string>& experimental,
                                             std::initializer_list<std::string_view> stableNames,
                                             std::initializer_list<std::string_view> experimentalNames) {
  ValidationResult result;
  const auto check = [&](const auto& selected,
                         auto supported,
                         auto other,
                         std::string_view field,
                         std::string_view destination,
                         std::string_view maturity) {
    std::set<std::string> seen;
    for (const auto& name : selected) {
      if (std::ranges::find(other, name) != other.end()) {
        result.addError(field,
                        "feature '" + name + "' is " + std::string(maturity) + "; move it to "
                          + std::string(destination));
      } else if (std::ranges::find(supported, name) == supported.end()) {
        result.addError(field, "unsupported feature '" + name + "'");
      }
      if (!seen.insert(name).second) {
        result.addError(field, "duplicate feature '" + name + "'");
      }
    }
  };
  check(enabled, stableNames, experimentalNames, "enabled_features", "enabled_experimental_features", "experimental");
  check(experimental, experimentalNames, stableNames, "enabled_experimental_features", "enabled_features", "stable");
  return result;
}

/// Require exactly the sections selected by the parent, then validate supplied settings.
template<class T>
void validateFeature(ValidationResult& result, bool selected, std::string_view name, const std::optional<T>& section) {
  if (selected != section.has_value()) {
    result.addError(name, selected ? "section is required when enabled" : "section must be absent when disabled");
  }
  if (section) {
    result.merge(name, section->validate());
  }
}

template<class T>
void validateFeature(ValidationResult& result,
                     const std::vector<std::string>& enabled,
                     std::string_view name,
                     const std::optional<T>& section) {
  validateFeature(result, featureEnabled(enabled, name), name, section);
}

}  // namespace cta::runtime
