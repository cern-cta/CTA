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
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

namespace cta::runtime {

inline bool featureEnabled(const std::vector<std::string>& enabled, std::string_view name) {
  return std::ranges::find(enabled, name) != enabled.end();
}

/// Check application-specific names without silently deduplicating the operator's list.
inline ValidationResult validateFeatureNames(const std::vector<std::string>& enabled,
                                             std::span<const std::string_view> supported,
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
                                             std::span<const std::string_view> stableNames,
                                             std::span<const std::string_view> experimentalNames) {
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

/// Accept literal name lists for selectors without section descriptors.
inline ValidationResult validateFeatureNames(const std::vector<std::string>& enabled,
                                             std::initializer_list<std::string_view> supported,
                                             std::string_view field = "enabled_features") {
  return validateFeatureNames(enabled, std::span<const std::string_view>(supported.begin(), supported.size()), field);
}

inline ValidationResult validateFeatureLists(const std::vector<std::string>& enabled,
                                             const std::vector<std::string>& experimental,
                                             std::initializer_list<std::string_view> stable,
                                             std::initializer_list<std::string_view> experimentalNames) {
  return validateFeatureLists(enabled,
                              experimental,
                              std::span<const std::string_view>(stable.begin(), stable.size()),
                              std::span<const std::string_view>(experimentalNames.begin(), experimentalNames.size()));
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

/// A named capability and its optional settings member; nullptr denotes a capability without settings.
template<class Member>
struct FeatureDescriptor {
  std::string_view name;
  Member member;
  bool experimental = false;

  template<class Config>
  void validate(ValidationResult& result, const Config& config, const std::vector<std::string>& enabled) const {
    if constexpr (!std::is_same_v<Member, std::nullptr_t>) {
      validateFeature(result, enabled, name, config.*member);
    }
  }
};

/// Declare a stable capability and the section it controls.
template<class Member>
constexpr auto stableFeature(std::string_view name, Member member) {
  return FeatureDescriptor<Member> {name, member, false};
}

/// Declare an experimental capability and the section it controls.
template<class Member>
constexpr auto experimentalFeature(std::string_view name, Member member) {
  return FeatureDescriptor<Member> {name, member, true};
}

/// Declare a routine, optionally with its settings section.
template<class Member = std::nullptr_t>
constexpr auto routine(std::string_view name, Member member = nullptr) {
  return FeatureDescriptor<Member> {name, member, false};
}

/// Select a declared capability without repeating its TOML name.
template<class Member>
bool featureEnabled(const std::vector<std::string>& enabled, const FeatureDescriptor<Member>& feature) {
  return featureEnabled(enabled, feature.name);
}

/// Collect declared names for configuration member initialisers such as all-enabled routines.
template<class Descriptors>
std::vector<std::string> featureNames(const Descriptors& descriptors) {
  std::vector<std::string> names;
  std::apply([&](const auto&... feature) { (names.emplace_back(feature.name), ...); }, descriptors);
  return names;
}

/// Validate both maturity lists and all sections from the config's single feature declaration.
template<class Config>
ValidationResult validateFeatures(const Config& config) {
  std::vector<std::string_view> stable, experimental;
  std::apply(
    [&](const auto&... feature) { ((feature.experimental ? experimental : stable).push_back(feature.name), ...); },
    Config::features);
  auto result = validateFeatureLists(config.enabled_features,
                                     config.enabled_experimental_features,
                                     std::span<const std::string_view>(stable),
                                     std::span<const std::string_view>(experimental));
  std::apply(
    [&](const auto&... feature) {
      (feature.validate(result,
                        config,
                        feature.experimental ? config.enabled_experimental_features : config.enabled_features),
       ...);
    },
    Config::features);
  return result;
}

/// Validate a domain-specific selection list and its declared sections.
template<class Config, class Descriptors>
ValidationResult
validateRoutines(const Config& config, const std::vector<std::string>& enabled, const Descriptors& descriptors) {
  std::vector<std::string_view> names;
  std::apply([&](const auto&... feature) { (names.push_back(feature.name), ...); }, descriptors);
  auto result = validateFeatureNames(enabled, std::span<const std::string_view>(names), "enabled_routines");
  std::apply([&](const auto&... feature) { (feature.validate(result, config, enabled), ...); }, descriptors);
  return result;
}

}  // namespace cta::runtime
