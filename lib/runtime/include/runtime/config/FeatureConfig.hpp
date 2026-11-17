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

/// Return whether enabled contains name; does not validate the name or its settings.
inline bool featureEnabled(const std::vector<std::string>& enabled, std::string_view name) {
  return std::ranges::find(enabled, name) != enabled.end();
}

/// Report names absent from supported and repeated names in enabled, attributing errors to field.
/// Does not modify the list or validate configuration sections.
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

/// Check stable and experimental selections against their respective supported names.
/// Report unknown names, duplicates within each list, and features selected in the wrong list.
/// Wrong-list errors identify which selection list the feature belongs in.
inline ValidationResult validateFeatureLists(const std::vector<std::string>& enabled,
                                             const std::vector<std::string>& experimental,
                                             std::span<const std::string_view> stableNames,
                                             std::span<const std::string_view> experimentalNames) {
  ValidationResult result;
  const auto check =
    [&](const auto& selected, auto supported, auto other, std::string_view field, std::string_view destination) {
      std::set<std::string> seen;
      for (const auto& name : selected) {
        if (std::ranges::find(other, name) != other.end()) {
          result.addError(field, "feature '" + name + "' must be listed in " + std::string(destination));
        } else if (std::ranges::find(supported, name) == supported.end()) {
          result.addError(field, "unsupported feature '" + name + "'");
        }
        if (!seen.insert(name).second) {
          result.addError(field, "duplicate feature '" + name + "'");
        }
      }
    };
  check(enabled, stableNames, experimentalNames, "enabled_features", "enabled_experimental_features");
  check(experimental, experimentalNames, stableNames, "enabled_experimental_features", "enabled_features");
  return result;
}

/// Require and validate settings for a selected feature, attributing errors to name.
/// Unselected settings are retained but are not semantically validated.
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

/// Validate section presence and contents, treating membership of name in enabled as its selection flag.
/// Does not check enabled for unsupported or duplicate names.
template<class T>
void validateFeature(ValidationResult& result,
                     const std::vector<std::string>& enabled,
                     std::string_view name,
                     const std::optional<T>& section) {
  validateFeature(result, featureEnabled(enabled, name), name, section);
}

/// Associate a selectable name with a pointer to its optional configuration member and its experimental status.
/// A nullptr member represents a feature with no settings section.
/// The name is non-owning and must outlive the descriptor; use a string literal for static declarations.
template<class Member, class Id = std::nullptr_t>
struct FeatureDescriptor {
  std::string_view name;
  Member member;
  bool experimental = false;
  Id id = {};

  /// Append section-presence and settings errors using this descriptor's name and member.
  /// Selection comes from enabled; descriptors without settings require no section validation.
  template<class Config>
  void validate(ValidationResult& result, const Config& config, const std::vector<std::string>& enabled) const {
    if constexpr (!std::is_same_v<Member, std::nullptr_t>) {
      validateFeature(result, enabled, name, config.*member);
    }
  }
};

/// Return a stable feature descriptor for a selection list.
/// Pass a pointer to its optional settings member, or omit member for a feature without settings.
template<class Member = std::nullptr_t>
constexpr auto stableFeature(std::string_view name, Member member = nullptr) {
  return FeatureDescriptor<Member> {name, member, false};
}

/// Associate a service-local enum value with a stable feature and its optional settings member.
template<class Id, class Member = std::nullptr_t>
  requires std::is_enum_v<Id>
constexpr auto stableFeature(Id id, std::string_view name, Member member = nullptr) {
  return FeatureDescriptor<Member, Id> {name, member, false, id};
}

/// Associate a service-local enum value with an experimental feature and its settings member.
template<class Id, class Member>
  requires std::is_enum_v<Id>
constexpr auto experimentalFeature(Id id, std::string_view name, Member member) {
  return FeatureDescriptor<Member, Id> {name, member, true, id};
}

/// Return whether enabled contains feature.name; does not change the selection or inspect settings.
template<class Member, class Id>
bool featureEnabled(const std::vector<std::string>& enabled, const FeatureDescriptor<Member, Id>& feature) {
  return featureEnabled(enabled, feature.name);
}

/// Query an enum-tagged descriptor in a domain-specific selection list; undeclared values are disabled.
template<class Id, class Descriptors>
  requires std::is_enum_v<Id>
bool featureEnabled(const std::vector<std::string>& enabled, Id id, const Descriptors& descriptors) {
  return std::apply(
    [&](const auto&... feature) { return ((feature.id == id && featureEnabled(enabled, feature)) || ...); },
    descriptors);
}

/// Return whether a declared service feature is selected in the list matching its experimental status.
template<class Config, class Member, class Id>
  requires requires(const Config& config) {
    config.enabled_features;
    config.enabled_experimental_features;
  }
bool featureEnabled(const Config& config, const FeatureDescriptor<Member, Id>& feature) {
  return featureEnabled(feature.experimental ? config.enabled_experimental_features : config.enabled_features, feature);
}

/// Query a service-local enum through its descriptor; undeclared values are disabled.
template<class Config>
bool featureEnabled(const Config& config, typename Config::Feature id) {
  return std::apply(
    [&](const auto&... feature) { return ((feature.id == id && featureEnabled(config, feature)) || ...); },
    Config::features);
}

/// Return all names from a descriptor tuple in declaration order, without filtering or deduplication.
/// Useful for initializing a selection list to enable every declared feature.
template<class Descriptors>
std::vector<std::string> featureNames(const Descriptors& descriptors) {
  std::vector<std::string> names;
  std::apply([&](const auto&... feature) { (names.emplace_back(feature.name), ...); }, descriptors);
  return names;
}

/// Validate config.enabled_features and config.enabled_experimental_features against Config::features.
/// Return combined errors for unsupported, duplicate or misclassified names and section presence or contents.
/// Config::features must be a tuple of descriptors pointing to optional members of config.
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

/// Validate a single selection list against a descriptor tuple and validate its sections in config.
/// Name errors are attributed to field; section errors are attributed to each descriptor's name.
/// Descriptors without settings participate in name validation only; experimental status is not checked.
template<class Config, class Descriptors>
ValidationResult validateFeatureSelection(const Config& config,
                                          const std::vector<std::string>& enabled,
                                          const Descriptors& descriptors,
                                          std::string_view field = "enabled_features") {
  std::vector<std::string_view> names;
  std::apply([&](const auto&... feature) { (names.push_back(feature.name), ...); }, descriptors);
  auto result = validateFeatureNames(enabled, std::span<const std::string_view>(names), field);
  std::apply([&](const auto&... feature) { (feature.validate(result, config, enabled), ...); }, descriptors);
  return result;
}

}  // namespace cta::runtime
