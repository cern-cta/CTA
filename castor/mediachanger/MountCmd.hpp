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

#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/MissingOperand.hpp"
#include "castor/exception/MountFailed.hpp"
#include "castor/mediachanger/CmdLineTool.hpp"
#include "castor/mediachanger/MountCmdLine.hpp"

#include <stdint.h>

namespace castor {
namespace mediachanger {

/**
 * The class implementing the command-line tool for mounting tapes on both ACS
 * and SCSI tape libraries.
 */
class MountCmd: public CmdLineTool {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param mc Interface to the media changer.
   */
  MountCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, MediaChangerFacade &mc) throw();

  /**
   * Destructor.
   */
  virtual ~MountCmd() throw();

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
  MountCmdLine m_cmdLine;

  /**
   * Requests the media changer to mount the tape and returns only when the
   * operation has terminated.
   */
  void mountTape();

}; // class MountCmd

} // namespace mediachanger
} // namespace castor
