/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::runtime {

// Every options struct passed to ArgParser MUST support these fields. In practice, if you need additional fields, it will be easiest to inherit from this struct
struct CommonCliOptions {
  bool showHelp = false;                   // --help, -h
  bool configStrict = false;               // --config-strict
  std::string configFilePath;              // --config <path>, -c
  std::optional<std::string> logFilePath;  // --log-file <path>
};

}  // namespace cta::runtime
