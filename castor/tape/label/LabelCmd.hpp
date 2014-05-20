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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/TapeserverProxy.hpp"
#include "castor/tape/label/ParsedTpLabelCommandLine.hpp"
#include "castor/utils/DebugBuf.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/legacymsg/GenericReplyMsgBody.hpp"

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
  LabelCmd() throw();

  /**
   * The entry function of the command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   */
  int main(const int argc, char **argv) throw();

protected:
  
  /**
   * The ID of the user running the tpcp command.
   */
  const uid_t m_userId;

  /**
   * The ID of default group of the user running the tpcp command.
   */
  const gid_t m_groupId;

  /**
   * The name of the program.
   */
  const std::string m_programName;

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(const int argc, char **argv)
    ;

  /**
   * Writes the command-line usage message of to the specified output stream.
   *
   * @param os Output stream to be written to.
   */
  void usage(std::ostream &os) const throw();
  
  /**
   * Displays the error message passed as string and exits with exit code 1.
   * 
   * @param msg The string containing the error message
   */
  void displayErrorMsgAndExit(const char *msg) throw();
  
  /**
   * Sends the label request and waits for the reply
   * 
   * @return the return code contained in the reply message
   */
  int executeCommand() ;
  
  /**
   * The command line structure
   */
  ParsedTpLabelCommandLine m_cmdLine;
  
  /**
   * Sends the tape label request to the tapeserver
   */
  void writeTapeLabelRequest(const int timeout);
  
  /**
   * Reads the reply coming from the tapeserver containing the return code of the labeling process
   * 
   * @return the reply structure
   */
  legacymsg::GenericReplyMsgBody readTapeLabelReply(const int timeout);
  
  /**
   * File descriptor of the connection with the tapeserver
   */
  castor::utils::SmartFd m_smartClientConnectionSock;

}; // class LabelCmd

} // namespace label
} // namespace tape
} // namespace castor
