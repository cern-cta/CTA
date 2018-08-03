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
#include <string.h>

namespace cta{
namespace mediachanger{
namespace acs{
namespace daemon{

struct AcsdCmdLine {

  bool foreground;

  bool help;
 
  std::string configLocation;
 
  /**
   * True if the tape is to be mount for read-only access.
   */
  bool readOnly;

  /**
   * Constructor.
   *
   * Initialises all BOOLEAN member-variables to FALSE, all integer
   * member-variables to 0 and the volume identifier to an empty string.
   */
  AcsdCmdLine();

  /**
   * Constructor.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  static AcsdCmdLine parse(const int argc, char *const *const argv);

  /**
   * Gets the usage message that describes the comamnd line.
   *
   * @return The usage message.
   */
  static std::string getUsage();

};

}
}
}
}

