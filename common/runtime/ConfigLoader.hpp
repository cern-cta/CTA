/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/UserError.hpp"

#include <rfl.hpp>
#include <rfl/toml.hpp>
#include <sstream>
#include <toml++/toml.hpp>

namespace cta::runtime {

/**
 * @brief Loads and verifies the provided .toml and populates the provided struct type.
 * To correctly load the toml file, the names and structure of the struct must match that of the .toml file.
 *
 * @tparam T The struct to populate with the data from the .toml file.
 * @param filePath Path to the .toml file.
 * @param strict If set to true, treat unknown keys, missing keys, and type mismatches in the config file as errors.
 * @return T The populated struct.
 */
template<typename T>
T loadFromToml(const std::string& filePath, bool strict = false) {
  toml::table tbl;
  try {
    tbl = toml::parse_file(filePath);
  } catch (const toml::parse_error& e) {
    std::ostringstream oss;
    oss << e;
    throw cta::exception::UserError("Failed to parse toml file '" + filePath + "': " + oss.str());
  }

  auto res = strict ? rfl::toml::read<T, rfl::NoExtraFields, rfl::NoOptionals>(&tbl) :
                      rfl::toml::read<T, rfl::DefaultIfMissing>(&tbl);

  if (!res) {
    throw cta::exception::UserError("Invalid config in '" + filePath + "': " + std::string(res.error().what()));
  }
  return res.value();
}

}  // namespace cta::runtime
