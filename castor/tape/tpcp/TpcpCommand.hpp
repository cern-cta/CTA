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
#include "castor/tape/tpcp/Migrator.hpp"
#include "castor/tape/tpcp/Dumper.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/ParsedCommandLine.hpp"
#include "castor/tape/tpcp/Recaller.hpp"
#include "castor/tape/tpcp/Verifier.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/vmgr_api.h"

#include <iostream>
#include <list>
#include <stdint.h>
#include <stdlib.h>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * This class carries out the following steps:
 * <ul>
 * <li>Parses the command-line
 * <li>Gets the DGN of the tape to be used from the VMGR
 * <li>Creates the aggregator callback socket
 * <li>Sends the request for a drive to the VDQM
 * <li>Delegates the requested action (READ, WRITE, DUMP or VERIFY) to the
 * appropriate action handler (DataMover for READ and WRITE, Dumper for DUMP or
 * Verifier for VERIFY).
 * </ul>
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
  int main(const int argc, char **argv) throw();


private:

  /**
   * Vmgr error buffer.
   */
  static char vmgr_error_buffer[VMGRERRORBUFLEN];

  /**
   * The results of parsing the command-line.
   */
  ParsedCommandLine m_cmdLine;

  /**
   * The list of RFIO filenames to be processed by the request handlers.
   */
  FilenameList m_filenames;

  /**
   * Tape information retrieved from the VMGR about the tape to be used.
   */
  vmgr_tape_info m_vmgrTapeInfo;

  /**
   * The DGN of the tape to be used.
   */
  char m_dgn[CA_MAXDGNLEN + 1];

  /**
   * TCP/IP aggregator callback socket.
   */
  castor::io::ServerSocket m_callbackSock;

  /**
   * The volume request ID returned by the VDQM as a result of requesting a
   * drive.
   */
  int m_volReqId;

  /**
   * Writes the command-line usage message of tpcp onto the specified output
   * stream.
   *
   * @param os Output stream to be written to.
   * @param programName The program name to be used in the message.
   */
  void usage(std::ostream &os, const char *const programName) throw();

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Returns the port on which the server will listen for connections from the
   * VDQM.
   */
  int getVdqmListenPort() throw(castor::exception::Exception);

  /**
   * Calculate the minimum number of files specified in the tape file
   * ranges provided as a command-line parameter.
   *
   * @return The minimum number of files.
   */
  unsigned int calculateMinNbOfFiles() throw (castor::exception::Exception);

  /**
   * Count the number of ranges that contain the upper boundary "end of tape"
   * ('m-').
   *
   * @return The number of ranges that contain the upper boundary "end of tape".
   */
  unsigned int countNbRangesWithEnd() throw (castor::exception::Exception);

  /**
   * Retrieves information about the specified tape from the VMGR.
   *
   * This method is basically a C++ wrapper around the C VMGR function
   * vmgr_querytape.  This method converts the return value of -1 and the
   * serrno to an exception in the case of an error.
   &
   * @param vid The tape for which the information should be retrieved.
   * @param side The tape side.
   */
  void vmgrQueryTape(char (&vid)[CA_MAXVIDLEN+1], const int side)
    throw (castor::exception::Exception);

  /**
   * Creates, binds and sets to listening the callback socket to be used for
   * callbacks from the aggregator daemon.
   */
  void setupCallbackSock() throw(castor::exception::Exception);

  /**
   * Request a drive from the VDQM to mount the specified tape for the
   * specified access mode (read or write).
   *
   * @param mode   The access mode, either WRITE_DISABLE or WRITE_ENABLE.
   * @param server If not NULL then this parameter specifies the tape server to
   * be used, therefore overriding drive scheduling of the VDQM.
   */
  void requestDriveFromVdqm(const int mode, char *const server)
    throw(castor::exception::Exception);

  /**
   * Dispaches the action to be performed to the appropriate ActionHandler.
   */
  void dispatchAction() throw(castor::exception::Exception);

}; // class TpcpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP
