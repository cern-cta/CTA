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

#include "common/config/Config.hpp"
#include "tapeserver/daemon/CommandLineParams.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/StdoutLogger.hpp"

namespace cta::maintenance {

//------------------------------------------------------------------------------
// exceptionThrowingMain
//
// The main() function delegates the bulk of its implementation to this
// exception throwing version.
//
// @param argc The number of command-line arguments.
// @param argv The command-line arguments.
// @param log The logging system.
//------------------------------------------------------------------------------
static int exceptionThrowingMain(const cta::daemon::CommandLineParams & commandLine,
  cta::log::Logger &log);


//------------------------------------------------------------------------------
// The help string
//------------------------------------------------------------------------------
const std::string gHelpString =
    "Usage: cta-maintenance [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--foreground             or -f         \tRemain in the Foreground\n"
    "\t--stdout                 or -s         \tPrint logs to standard output. Required --foreground\n"
    "\t--log-to-file <log-file> or -l         \tLogs to a given file (instead of default syslog)\n"
    "\t--log-format <format>    or -o         \tOutput format for log messages (default or json)\n"
    "\t--config <config-file>   or -c         \tConfiguration file\n"
    "\t--help                   or -h         \tPrint this help and exit\n";

static int exceptionThrowingMain(const cta::daemon::CommandLineParams& commandLine, cta::log::Logger& log) {
      return 0;
    }

} // namespace cta::maintenance

int main(const int argc, char **const argv) {
    using namespace cta;

    std::unique_ptr<daemon::CommandLineParams> cmd; 
    std::unique_ptr<log::Logger> logPtr;
    logPtr.reset(new log::StdoutLogger("my-machine", "cta-maintenance"));

    log::Logger& log = *logPtr;


    return maintenance::exceptionThrowingMain(*cmd, log);
}

