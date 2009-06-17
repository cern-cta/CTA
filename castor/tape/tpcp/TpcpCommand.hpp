/******************************************************************************
 *                 castor/tape/tpcp/Tpcp.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP
#define CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP 1

#include "castor/BaseObject.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <list>
#include <stdint.h>
#include <stdlib.h>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * The tape copy command.
 */
class TpcpCommand : public castor::BaseObject {
public:

  /**
   * Constructor.
   */
  TpcpCommand() throw();

  /**
   * Destructor.
   */
  ~TpcpCommand() throw();

  /**
   * The entry function of the tpcp command.
   */
  int main(const int argc, char **argv);


private:

  /**
   * TCP/IP callback socket.
   */
  castor::io::ServerSocket m_callbackSocket;

  /**
   * A range of uint32_t's specified by an inclusive upper and lower set of
   * bounds.
   */
  struct Uint32Range {
    uint32_t lower;
    uint32_t upper;
  };

  /**
   * List of ranges.
   */
  typedef std::list<Uint32Range> Uint32RangeList;

  /**
   * Writes the command-line usage message of tpcp onto the specified output
   * stream.
   *
   * @param os Output stream to be written to.
   * @param programName The program name to be used in the message.
   */
  void usage(std::ostream &os, const char *const programName) throw();

  /**
   * Data type used to store the results of parsing the command-line.
   */
  struct ParsedCommandLine {
    bool            debugOptionSet;
    bool            helpOptionSet;
    Action          action;
    const char      *vid;
    Uint32RangeList tapeFseqRanges;

    ParsedCommandLine() :
      debugOptionSet(false),
      helpOptionSet(false),
      action(Action::read),
      vid(NULL) {
    }
  };

  /**
   * The results of parsing the command-line.
   */
  ParsedCommandLine m_parsedCommandLine;

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Writes the specified list of tape file sequence ranges to the specified
   * output stream.
   */
  void writeTapeFseqRangeList(std::ostream &os, Uint32RangeList &list);

  /**
   * Writes the parsed command-line to the specified output stream.
   */
  void writeParsedCommandLine(std::ostream &os);

  /**
   * Returns the port on which the server will listen for connections from the
   * VDQM.
   */
  int getVdqmListenPort() throw(castor::exception::Exception);

  /**
   * This function blocks until one call back has been made
   */
  castor::io::ServerSocket* waitForCallBack()
    throw (castor::exception::Exception);

  /**
   * Parse the specified tape file sequence parameter string and store the
   * resulting ranges into m_parsedCommandLine.tapeFseqRanges.
   *
   * The syntax rules for a tape file sequence specification are:
   * <ul>
   *  <li>  f1            File f1.
   *  <li>  f1-f2         Files f1 to f2 included.
   *  <li>  f1-           Files from f1 to the last file of the tape.
   *  <li>  f1-f2,f4,f6-  Series of ranges "," separated.
   * </ul>
   *
   * @param str The string received as an argument for the TapeFileSequence
   * option.
   */
  void parseTapeFileSequence(char *const str) 
    throw (castor::exception::Exception);


}; // class TpcpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP
