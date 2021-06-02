/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <stdint.h>
#include <string>

namespace cta {
namespace catalogue {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-database-poll.
 */
struct PollDatabaseCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help;

  /**
   * The total number of seconds the cta-database-pollshould run before exiting.
   */
  uint64_t numberOfSecondsToKeepPolling;

  /**
   * Path to the file containing the connection details of the catalogue
   * database.
   */
  std::string dbConfigPath;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  PollDatabaseCmdLineArgs(const int argc, char *const *const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream &os);
};

} // namespace catalogue
} // namespace cta
