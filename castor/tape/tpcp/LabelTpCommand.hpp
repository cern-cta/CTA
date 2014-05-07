/******************************************************************************
 *                 castor/tape/tpcp/LabelTpCommand.hpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/tape/tpcp/ParsedTpLabelCommandLine.hpp"

#include <sys/types.h>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * The class implementing the LabelTp command.
 */
class LabelTpCommand {
public:

  /**
   * Constructor.
   */
  LabelTpCommand() throw();

  /**
   * Destructor.
   */
  virtual ~LabelTpCommand() throw();
  
  /**
   * The entry function of the tplabel command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   */
  int main(const int argc, char **argv) throw();


protected:

  /**
   * Writes the command-line usage message of to the specified output stream.
   *
   * @param os Output stream to be written to.
   */
  void usage(std::ostream &os) const throw();

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception);


private:

  /**
   * The umask of the process.
   */
  const mode_t m_umask;
  
  /**
   * The command line structure
   */
  ParsedTpLabelCommandLine m_cmdLine;

}; // class LabelTpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


