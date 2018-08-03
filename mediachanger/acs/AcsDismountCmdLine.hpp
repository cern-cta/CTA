/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "AcsCmdLine.hpp"

extern "C" {
#include "acssys.h"
#include "acsapi.h"
}

#include <string>

namespace cta {
namespace mediachanger {
namespace acs {

/**
 * Data type used to store the results of parsing the command-line.
 */
struct AcsDismountCmdLine: public AcsCmdLine {
  /**
   * True if the debug option has been set.
   */
  bool debug;

  /**
   * True if the dismount should be forced.
   *
   * Forcing a dismount means dismounting the tape in the specified drive
   * without checking the volume identifier of the tape.
   */
  BOOLEAN force;

  /**
   * True if the help option has been set.
   */
  bool help;

  /**
   * Time in seconds to wait between queries to ACS for responses.
   */
  int queryInterval;

  /**
   * Time in seconds to wait for the dismount to conclude.
   */
  int timeout;

  /**
   * The volume identifier of the tape to be mounted.
   */
  VOLID volId;

  /**
   * The slot in the tape library where the drive is located.
   */
  DRIVEID libraryDriveSlot;

  /**
   * Constructor.
   *
   * Initialises all boolean member-variables to false, all integer
   * member-variables to 0 and the volume identifier to an empty string.
   */
  AcsDismountCmdLine();

  /**
   * Constructor.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  AcsDismountCmdLine(const int argc, char *const *const argv);

  /**
   * Gets the usage message that describes the comamnd line.
   *
   * @return The usage message.
   */
  static std::string getUsage();

private:

  /**
   * Processes the specified option that was returned by getopt_long().
   *
   * @param opt The option that was returned by getopt_long().
   */
  void processOption(const int opt);

}; // class AcsDismountCmdLine

} // namespace acs
} // namespace mediachanger
} // namespace cta
