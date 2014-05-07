/******************************************************************************
 *                 castor/tape/label/LabelCmd.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/TapeserverProxy.hpp"
#include "castor/tape/label/ParsedTpLabelCommandLine.hpp"
#include "castor/utils/DebugBuf.hpp"

#include <istream>
#include <ostream>
#include <string>

namespace castor {
namespace tape {
namespace label {

/**
 * Class implementing the business logic of the label command-line tool.
 */
class LabelCmd {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param tapeserver Proxy object representing the tapeserverd daemon.
   */
  LabelCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, legacymsg::TapeserverProxy &tapeserver) throw();

  /**
   * The entry function of the command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   */
  int main(const int argc, char **argv) throw();

protected:

  /**
   * The name of the program.
   */
  const std::string m_programName;

  /**
   * Standard input stream.
   */
  std::istream &m_in;

  /**
   * Standard output stream.
   */
  std::ostream &m_out;

  /**
   * Standard error stream.
   */
  std::ostream &m_err;

  /**
   * Proxy object representing the tapeserverd daemon.
   */
  legacymsg::TapeserverProxy &m_tapeserver;

  /**
   * Debug stream buffer that inserts a standard debug preamble before each
   * message-line written to it.
   */
  utils::DebugBuf m_debugBuf;

  /**
   * Stream used to write debug messages.
   *
   * This stream will insert a standard debug preamble before each message-line
   * written to it.
   */
  std::ostream m_dbg;

  /**
   * Returns the string representation of the specfied boolean value.
   *
   * @param value The boolean value.
   */
  std::string bool2Str(const bool value) const throw();

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  ParsedTpLabelCommandLine parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Writes the command-line usage message of to the specified output stream.
   *
   * @param os Output stream to be written to.
   */
  void usage(std::ostream &os) const throw();

}; // class LabelCmd

} // namespace label
} // namespace tape
} // namespace castor
