/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/**
 * @file
 * @brief TOML configuration loading and validation, with optional environment variable expansion.
 */

#pragma once

#include "ValidationResult.hpp"
#include "common/exception/UserError.hpp"
#include "parsing/TomlParser.hpp"

#include <concepts>
#include <sstream>
#include <string>
#include <string_view>
#include <toml++/toml.hpp>

namespace cta::runtime {

/** @brief Original and resolved text from one read, with a source path for diagnostics. */
struct ResolvedToml {
  std::string sourcePath;
  std::string original;
  std::string resolved;
};

/**
 * @brief Resolve environment references in text without parsing or validating TOML.
 *
 * Substitution applies throughout the text, including keys and comments.
 * Values are inserted verbatim and may change TOML structure; the author is responsible for quoting and validity.
 * Disabling expansion preserves the text exactly and ignores errorOnMissingEnvironmentVariable.
 * @param contents Original configuration text, copied into the result.
 * @param sourcePath Source label used in diagnostics; no file is accessed.
 * @param expandEnvironmentVariables Whether to expand environment references.
 * @param errorOnMissingEnvironmentVariable Throw for undefined variables without fallbacks when expansion is enabled.
 * @throws cta::exception::UserError If environment substitution fails.
 */
ResolvedToml resolveToml(std::string_view contents,
                         const std::string& sourcePath,
                         bool expandEnvironmentVariables = true,
                         bool errorOnMissingEnvironmentVariable = false);

/**
 * @brief Read a file once and resolve its contents using resolveToml.
 * @param filePath Path to the TOML file.
 * @param expandEnvironmentVariables Whether to expand environment references.
 * @param errorOnMissingEnvironmentVariable Throw for undefined variables without fallbacks when expansion is enabled.
 * @throws cta::exception::UserError If reading or environment substitution fails.
 */
ResolvedToml readTomlFile(const std::string& filePath,
                          bool expandEnvironmentVariables = true,
                          bool errorOnMissingEnvironmentVariable = false);

template<class T>
concept HasValidateMethod = requires(const T& config) {
  { config.validate() } -> std::same_as<ValidationResult>;
};

/**
 * @brief Parse TOML text and populate and validate the provided struct type without environment substitution.
 * To correctly load the toml file, the names and structure of the struct must match that of the .toml file.
 *
 * @tparam T The struct to populate with the data from the .toml file.
 * @param contents TOML text to parse, either original or already resolved.
 * @param sourcePath Source label used in diagnostics; no file is accessed.
 * @param strict If set to true, treat unknown keys, missing keys, and type mismatches in the config file as errors.
 * If T defines validate() const, semantic validation is performed after parsing.
 * @return T The populated struct.
 */
template<class T>
T loadFromTomlString(std::string_view contents, const std::string& sourcePath, bool strict = false) {
  toml::table tbl;
  try {
    tbl = toml::parse(contents, sourcePath);
  } catch (const toml::parse_error& e) {
    std::ostringstream oss;
    oss << e;
    throw cta::exception::UserError("Failed to parse toml file '" + sourcePath + "': " + oss.str(), false);
  }

  T config {};
  if (auto res = parsing::parseTable(config, tbl, strict); !res.ok()) {
    throw cta::exception::UserError("Invalid config in '" + sourcePath + "':\n" + res.what(), false);
  }
  if constexpr (HasValidateMethod<T>) {
    const auto validationResult = config.validate();
    if (!validationResult.ok()) {
      throw cta::exception::UserError("Invalid config in '" + sourcePath + "':\n" + validationResult.what(), false);
    }
  }
  return config;
}

/**
 * @brief Read, optionally expand environment references, parse, and validate a TOML file.
 * @param filePath Path to the TOML file.
 * @param strict Passed through to loadFromTomlString.
 * @param expandEnvironmentVariables Whether to expand environment references before parsing.
 * @param errorOnMissingEnvironmentVariable Throw for undefined variables without fallbacks when expansion is enabled.
 * Use readTomlFile followed by loadFromTomlString when both original and resolved text are needed.
 */
template<class T>
T loadFromToml(const std::string& filePath,
               bool strict = false,
               bool expandEnvironmentVariables = true,
               bool errorOnMissingEnvironmentVariable = false) {
  const auto document = readTomlFile(filePath, expandEnvironmentVariables, errorOnMissingEnvironmentVariable);
  return loadFromTomlString<T>(document.resolved, document.sourcePath, strict);
}

}  // namespace cta::runtime
