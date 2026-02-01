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

// TODO: request better error messages
// 1) Failed to parse field 'logging': Field named 'format' not found.
// 2) Failed to parse field 'routines': Found 4 errors:
//     1) Failed to parse field 'disk_report_archive': Found 2 errors:
//         1) Value named 'soft_timeout' not used. Remove the rfl::NoExtraFields processor or add rfl::ExtraFields to avoid this error message.
//         2) Field named 'soft_timeout_secs' not found.

// TODO: request insight into default behaviour?

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

  auto res = strict ? rfl::toml::read<T, rfl::NoExtraFields, rfl::NoOptionals>(&tbl) : rfl::toml::read<T>(&tbl);

  if (!res) {
    throw cta::exception::UserError("Invalid config in '" + filePath + "': " + std::string(res.error().what()));
  }
  return res.value();
}

}  // namespace cta::runtime
