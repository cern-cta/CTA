/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <map>
#include <string>

namespace cta {
struct ConfigurationFile {
public:
  explicit ConfigurationFile(const std::string& path);
  struct value_t {
    std::string value;
    uint32_t line;
  };
  std::map<std::string, std::map<std::string, value_t>> entries;
};
} // namespace cta
