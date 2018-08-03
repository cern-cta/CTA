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

#include "common/exception/InvalidArgument.hpp"
#include "common/exception/MissingOperand.hpp"
#include "mediachanger/CmdLineTool.hpp"
#include "mediachanger/DismountCmdLine.hpp"

#include <stdint.h>

namespace cta {
namespace mediachanger {

/**
 * The class implementing the command-line tool for dismounting tapes on both
 * ACS and SCSI tape libraries.
 */
class DismountCmd: public CmdLineTool {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param mc Interface to the media changer.
   */
  DismountCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, MediaChangerFacade &mc);

  /**
   * Destructor.
   */
  virtual ~DismountCmd();

  /**
   * The entry function of the command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv);

private:

  /**
   * The parsed command-line.
   *
   * The value of this member variable is set within the main() method of this
   * class.
   */
  DismountCmdLine m_cmdLine;

}; // class DismountCmd

} // namespace mediachanger
} // namespace cta
