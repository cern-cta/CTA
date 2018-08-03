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

#include "mediachanger/CmdLine.hpp"
#include "mediachanger/LibrarySlot.hpp"

#include <string>

namespace cta {
namespace mediachanger {

/**
 * Data type used to store the results of parsing the command-line.
 */
class DismountCmdLine: public CmdLine {
public:

  /**
   * Constructor.
   */
  DismountCmdLine();

  /**
   * Constructor.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  DismountCmdLine(const int argc, char *const *const argv);

  /**
   * Copy constructor.
   *
   * @param obj The object to be copied.
   */
  DismountCmdLine(const DismountCmdLine &obj);

  /**
   * Destructor.
   */
  ~DismountCmdLine();

  /**
   * Assignment oprator.
   *
   * @param rhs The right-hand side of the operator.
   */
  DismountCmdLine &operator=(const DismountCmdLine &rhs);

  /**
   * Gets the usage message that describes the command line.
   *
   * @return The usage message.
   */ 
  static std::string getUsage();

  /**
   * Gets the value of the debug option.
   *
   * @return True if the debug option has been set.
   */
  bool getDebug() const;

  /**
   * Gets the value of the help option.
   *
   * @return True if the help option has been set.
   */
  bool getHelp() const;

  /**
   * Gets the volume identifier of the tape to be mounted.
   *
   * @return The volume identifier of the tape to be mounted.
   */
  const std::string &getVid() const;

  /**
   * Gets the slot in the tape library where the drive is located.
   *
   * @return The slot in the tape library where the drive is located.
   */
  const LibrarySlot &getDriveLibrarySlot() const;

  /**
   * Gets the value of the force option.
   *
   * @return True if the dismount should be forced.  Forcing a dismount means
   * rewinding and ejecting the tape where necessary.
   */
  bool getForce() const;

  /**
   * Return sthe program name.
   *
   * @return sthe program name.
   */
  static std::string getProgramName();

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
   * The volume identifier of the tape to be mounted.
   */
  std::string m_vid;

  /**
   * The slot in the tape library where the drive is located.
   */
  LibrarySlot *m_driveLibrarySlot;

  /**
   * True if the dismount should be forced.  Forcing a dismount means
   * rewinding and ejecting the tape where necessary.
   */
  bool m_force;

  /**
   * Processes the specified option that was returned by getopt_long().
   *
   * @param opt The option that was returned by getopt_long().
   */
  void processOption(const int opt);

}; // class DismountCmdLine

} // namespace mediachanger
} // namespace cta
