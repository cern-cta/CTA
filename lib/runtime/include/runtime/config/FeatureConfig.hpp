/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ValidationResult.hpp"

#include <algorithm>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

namespace cta::runtime {

/// Check whether a feature name is enabled.
inline bool featureEnabled(const std::vector<std::string>& enabled, std::string_view name) {
  return std::ranges::find(enabled, name) != enabled.end();
}

/// Report unknown or duplicate names under field.
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

/// Require and validate the section when selected; otherwise leave it unchecked.
template<class T>
void validateFeature(ValidationResult& result, bool selected, std::string_view name, const std::optional<T>& section) {
  if (!selected) {
    return;
  }
  if (!section) {
    result.addError(name, "section is required when enabled");
  } else {
    result.merge(name, section->validate());
  }
}

/// Link a feature enum to its config name, settings member and experimental status.
/// Use nullptr for features without settings and a string literal for name.
template<class Member, class Id>
struct FeatureDescriptor {
  std::string_view name;
  Member member;
  bool experimental;
  Id id;

  /// Validate this feature's settings if enabled and it has a settings member.
  template<class Config>
  void validate(ValidationResult& result, const Config& config, const std::vector<std::string>& enabled) const {
    if constexpr (!std::is_same_v<Member, std::nullptr_t>) {
      validateFeature(result, featureEnabled(enabled, name), name, config.*member);
    }
  }
};

/// Declare a stable feature; omit member if it has no settings.
template<class Id, class Member = std::nullptr_t>
  requires std::is_enum_v<Id>
constexpr auto stableFeature(Id id, std::string_view name, Member member = nullptr) {
  return FeatureDescriptor<Member, Id> {name, member, false, id};
}

/// Declare an experimental feature with settings.
template<class Id, class Member>
  requires std::is_enum_v<Id>
constexpr auto experimentalFeature(Id id, std::string_view name, Member member) {
  return FeatureDescriptor<Member, Id> {name, member, true, id};
}

/// Check an enum against a selection list, such as routines or auth methods.
/// Unknown enum values return false.
template<class Id, class Descriptors>
  requires std::is_enum_v<Id>
bool featureEnabled(const std::vector<std::string>& enabled, Id id, const Descriptors& descriptors) {
  return std::apply(
    [&](const auto&... feature) { return ((feature.id == id && featureEnabled(enabled, feature.name)) || ...); },
    descriptors);
}

/// Check a service feature in its stable or experimental list.
/// Unknown enum values return false.
template<class Config>
bool featureEnabled(const Config& config, typename Config::Feature id) {
  return std::apply(
    [&](const auto&... feature) {
      return ((feature.id == id
               && featureEnabled(feature.experimental ? config.enabled_experimental_features : config.enabled_features,
                                 feature.name))
              || ...);
    },
    Config::features);
}

/// List feature names in declaration order, e.g. to enable all routines by default.
template<class Descriptors>
std::vector<std::string> featureNames(const Descriptors& descriptors) {
  std::vector<std::string> names;
  std::apply([&](const auto&... feature) { (names.emplace_back(feature.name), ...); }, descriptors);
  return names;
}

/// Validate both service feature lists and enabled settings using Config::features.
template<class Config>
ValidationResult validateFeatures(const Config& config) {
  std::vector<std::string_view> stable, experimental;
  std::apply(
    [&](const auto&... feature) { ((feature.experimental ? experimental : stable).push_back(feature.name), ...); },
    Config::features);
  auto result = validateFeatureNames(config.enabled_features, stable, "enabled_features");
  result.merge(
    "",
    validateFeatureNames(config.enabled_experimental_features, experimental, "enabled_experimental_features"));
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

/// Validate one selection list and its enabled settings, ignoring experimental status.
/// Report list errors under field and settings errors under the section name.
template<class Config, class Descriptors>
ValidationResult validateFeatureSelection(const Config& config,
                                          const std::vector<std::string>& enabled,
                                          const Descriptors& descriptors,
                                          std::string_view field = "enabled_features") {
  std::vector<std::string_view> names;
  std::apply([&](const auto&... feature) { (names.push_back(feature.name), ...); }, descriptors);
  auto result = validateFeatureNames(enabled, names, field);
  std::apply([&](const auto&... feature) { (feature.validate(result, config, enabled), ...); }, descriptors);
  return result;
}

}  // namespace cta::runtime
