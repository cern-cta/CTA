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

class AcsdCmdLine: public CmdLine{

public:

  /**
   * Constructor.
   *
   * Initialises all BOOLEAN member-variables to FALSE, all integer
   * member-variables to 0 and the volume identifier to an empty string.
   */
  AcsdCmdLine() throw();

  /**
   * Constructor.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  AcsdCmdLine(const int argc, char *const *const argv);

   bool foreground;

   bool help;
 
   std::string configLocation;
 

};

}
}
}
}






