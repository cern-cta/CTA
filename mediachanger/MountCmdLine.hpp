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

#include "mediachanger/CmdLine.hpp"
#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/LibrarySlotParser.hpp"

#include <string>

namespace cta {
namespace mediachanger {

/**
 * Data type used to store the results of parsing the command-line.
 */
class MountCmdLine: public CmdLine {
public:

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
   */
  MountCmdLine(const int argc, char *const *const argv);

  /**
   * Copy constructor.
   *
   * @param obj The object to be copied.
   */
  MountCmdLine(const MountCmdLine &obj);

  /**
   * Destructor.
   */
  ~MountCmdLine() throw();

  /**
   * Assignment oprator.
   *
   * @param rhs The right-hand side of the operator.
   */
  MountCmdLine &operator=(const MountCmdLine &rhs);

  /**
   * Gets the usage message that describes the command line.
   *
   * @return The usage message.
   */
  static std::string getUsage() throw();

  /**
   * Gets the value of the debug option.
   *
   * @return True if the debug option has been set.
   */
  bool getDebug() const throw();

  /**
   * Gets the value of th ehelp option.
   *
   * True if the help option has been set.
   */
  bool getHelp() const throw();

  /**
   * Gets the value of the read-only option.
   *
   * @return True if the tape is to be mount for read-only access.
   */
  bool getReadOnly() const throw();

  /**
   * Gets the volume identifier of the tape to be mounted.
   *
   * @return The volume identifier of the tape to be mounted.
   */
  std::string getVid() const throw();

  /**
   * Gets the slot in the tape library where the drive is located.
   *
   * @return The slot in the tape library where the drive is located.
   */
  const LibrarySlot &getDriveLibrarySlot() const;

private:

  /**
   * True if the debug option has been set.
   */
  bool m_debug;

  /**
   * True if the help option has been set.
   */
  bool m_help;

  /**
   * True if the tape is to be mount for read-only access.
   */
  bool m_readOnly;

  /**
   * The volume identifier of the tape to be mounted.
   */
  std::string m_vid;

  /**
   * The slot in the tape library where the drive is located.
   */
  LibrarySlot *m_driveLibrarySlot;

  /**
   * Processes the specified option that was returned by getopt_long().
   *
   * @param opt The option that was returned by getopt_long().
   */
  void processOption(const int opt);

}; // class MountCmdLine

} // namespace mediachanger
} // namespace cta
