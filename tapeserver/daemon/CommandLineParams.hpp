/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

namespace cta { namespace daemon {
/// A class parsing the command line and turning it into a struct.
struct CommandLineParams{
  /**
   * Translates the command line parameters into a struct
   * @param argc
   * @param argv
   */
  CommandLineParams(int argc, char **argv);
  bool foreground;                  ///< Prevents daemonisation
  bool logToStdout;                 ///< Log to stdout instead of syslog. Foreground is required.
  bool logToFile;                   ///< Log to file intead of syslog.
  std::string logFilePath;
  std::string configFileLocation;   ///< Location of the configuration file. Defaults to /etc/cta/cta-taped.conf
  bool helpRequested;               ///< Help requested: will print out help and exit.
  std::list<cta::log::Param> toLogParams() const; ///< Convert the command line into set of parameters for logging.
};
}}
