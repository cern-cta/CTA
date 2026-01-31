/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace cta::maintd {

struct MaintdCliOptions {
  bool configStrict;
  std::string configPath;
  bool logToFile;
  std::string logFilePath;
};

}  // namespace cta::maintd
