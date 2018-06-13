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
#include "mediachanger/AcsLibrarySlot.hpp"
#include "mediachanger/LibrarySlotParser.hpp"

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
struct AcsQueryDriveCmdLine: public AcsCmdLine {
  /**
   * True if the debug option has been set.
   */
  BOOLEAN debug;

  /**
   * True if the help option has been set.
   */
  BOOLEAN help;

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
   * Constructor.
   *
   * Initialises all BOOLEAN member-variables to FALSE, all integer
   * member-variables to 0 and the volume identifier to an empty string.
   */
  AcsQueryDriveCmdLine() throw();

  /**
   * Constructor.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  AcsQueryDriveCmdLine(const int argc, char *const *const argv);


 /**
  * Gets the usage message that describes the command line.
  *
  * @return The usage message.
  */
  static std::string getUsage() throw();

  /**
   * Return sthe program name.
   *
   * @return sthe program name.
   */
  static std::string getProgramName();

  /**
   * The slot in the tape library where the drive is located.
   */

  DRIVEID libraryDriveSlot;


private:

  /**
   * Processes the specified option that was returned by getopt_long().
   *
   * @param opt The option that was returned by getopt_long().
   */
  void processOption(const int opt);

}; // class AcsQueryDriveCmdLine

} // namespace acs
} // namespace mediachanger
} // namespace cta
