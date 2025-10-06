/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>
#include <list>
#include "common/log/Param.hpp"

namespace cta::common {

struct CmdLineParams {
  /**
   * Translates the command line parameters into a struct
   */
  CmdLineParams(int argc, char **argv, const std::string procName);
  bool foreground = false;                                      ///< Prevents daemonisation
  bool logToStdout = false;                                     ///< Log to stdout instead. Foreground is required. Logging to stdout is the default, but this option is kept for compatibility reasons
  bool logToFile = false;                                       ///< Log to file intead of stdout.
  std::string logFilePath;                                      ///< Path to log file
  std::string logFormat;                                        ///< Format of log messages [default|json]
  std::string configFileLocation;                               ///< Location of the configuration file
  bool helpRequested = false;                                   ///< Print help message and exit
  std::list<cta::log::Param> toLogParams() const;               ///< Convert command line into set of parameters for logging
};

} // namespace cta::common
