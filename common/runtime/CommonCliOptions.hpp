/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::runtime {

template<class T>
concept HasRequiredCliOptions = requires(T& opts) {
  { opts.showHelp } -> std::convertible_to<bool>;
  { opts.showVersion } -> std::convertible_to<bool>;
  { opts.configStrict } -> std::convertible_to<bool>;
  { opts.configCheck } -> std::convertible_to<bool>;
  requires std::same_as<std::remove_cvref_t<decltype(opts.configFilePath)>, std::string>;
  requires std::same_as<std::remove_cvref_t<decltype(opts.logFilePath)>, std::string>;
  requires std::same_as<std::remove_cvref_t<decltype(opts.runtimeDir)>, std::string>;
};

// Every options struct passed to ArgParser MUST support these fields. In practice, if you need additional fields, it will be easiest to inherit from this struct
struct CommonCliOptions {
  bool showHelp = false;
  bool showVersion = false;
  std::string runtimeDir;
  std::string logFilePath;
  std::string configFilePath;
  bool configCheck = false;
  bool configStrict = false;
};

}  // namespace cta::runtime
