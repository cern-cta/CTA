/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include <list>
#include "common/log/Param.hpp"

namespace cta::daemon {

struct CommandLineParams {
  /**
   * Translates the command line parameters into a struct
   */
  CommandLineParams(int argc, char **argv);
  bool foreground = false;                                      ///< Prevents daemonisation
  bool logToStdout = false;                                     ///< Log to stdout instead. Foreground is required. Logging to stdout is the default, but this option is kept for compatibility reasons
  bool logToFile = false;                                       ///< Log to file intead of stdout.
  std::string logFilePath;                                      ///< Path to log file
  std::string logFormat;                                        ///< Format of log messages [default|json]
  std::string configFileLocation;                               ///< Location of the configuration file
  bool helpRequested = false;                                   ///< Print help message and exit
  std::list<cta::log::Param> toLogParams() const;               ///< Convert command line into set of parameters for logging
};

} // namespace cta::daemon
