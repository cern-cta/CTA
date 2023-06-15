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
#include <optional>

namespace cta {
namespace tapeserver {
namespace tapelabel {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-tape-label.
 */
struct TapeLabelCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help;

  /**
   * The tape VID to be pre-label.
   */
  std::string m_vid;

  /**
   * The old label on tape to be checked when pre-labeling.
   */
  std::string m_oldLabel;

  /**
   * The unit name of the drive used to mount the tape.
   */
  std::optional<std::string> m_unitName;

  /**
   * The boolean variable to enable verbose output in the command line.
   * By default it prints only ERR and WARNING messages. 
   */
  bool m_debug;

  /**
   * The boolean variable to skip label checks on not-blank tapes. 
   */
  bool m_force;

  /**
   * The timeout to load the tape in the drive slot in seconds
   */
  uint64_t m_tapeLoadTimeout = 2 * 60 * 60;  // 2 hours

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  TapeLabelCmdLineArgs(const int argc, char* const* const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream& os);
};

}  // namespace tapelabel
}  // namespace tapeserver
}  // namespace cta
