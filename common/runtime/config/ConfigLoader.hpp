/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ValidationResult.hpp"
#include "common/exception/UserError.hpp"
#include "parsing/TomlParser.hpp"

#include <concepts>
#include <sstream>
#include <toml++/toml.hpp>

namespace cta::runtime {

template<class T>
concept HasValidateMethod = requires(const T& config) {
  { config.validate() } -> std::same_as<ValidationResult>;
};

/**
 * @brief Loads and verifies the provided .toml and populates the provided struct type.
 * To correctly load the toml file, the names and structure of the struct must match that of the .toml file.
 *
 * @tparam T The struct to populate with the data from the .toml file.
 * @param filePath Path to the .toml file.
 * @param strict If set to true, treat unknown keys, missing keys, and type mismatches in the config file as errors.
 * If T defines validate() const, semantic validation is performed after parsing.
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
    throw cta::exception::UserError("Failed to parse toml file '" + filePath + "': " + oss.str(), false);
  }

  T config {};
  if (auto res = parsing::parseTable(config, tbl, strict); !res.ok()) {
    throw cta::exception::UserError("Invalid config in '" + filePath + "':\n" + res.what(), false);
  }
  if constexpr (HasValidateMethod<T>) {
    const auto validationResult = config.validate();
    if (!validationResult.ok()) {
      throw cta::exception::UserError("Invalid config in '" + filePath + "':\n" + validationResult.what(), false);
    }
  }
  return config;
}

}  // namespace cta::runtime
