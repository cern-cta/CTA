/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/mediachanger/CmdLine.hpp"
#include "castor/mediachanger/GenericLibrarySlot.hpp"

#include <string>

namespace castor {
namespace mediachanger {

/**
 * Data type used to store the results of parsing the command-line.
 */
struct MountCmdLine: public CmdLine {
  /**
   * True if the debug option has been set.
   */
  bool debug;

  /**
   * True if the help option has been set.
   */
  bool help;

  /**
   * True if the tape is to be mount for read-only access.
   */
  bool readOnly;

  /**
   * The volume identifier of the tape to be mounted.
   */
  std::string vid;

  /**
   * The slot in the tape library where the drive is located.
   */
  GenericLibrarySlot driveLibrarySlot;

  /**
   * Constructor.
   *
   * Initialises all BOOLEAN member-variables to FALSE, all integer
   * member-variables to 0 and the volume identifier to an empty string.
   */
  MountCmdLine() throw();

  /**
   * Constructor.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   * @param defaultQueryInterval The default time in seconds to wait between
   * queries to ACS for responses.
   * @param defaultTimeout The default timeout value in seconds for the mount
   * to conclude either success or failure.
   */
  MountCmdLine(const int argc, char *const *const argv);

private:

  /**
   * Processes the specified option that was returned by getopt_long().
   *
   * @param opt The option that was returned by getopt_long().
   */
  void processOption(const int opt);

}; // class MountCmdLine

} // namespace mediachanger
} // namespace castor
