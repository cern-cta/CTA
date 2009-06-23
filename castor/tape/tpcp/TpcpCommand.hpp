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
#include "castor/tape/tpcp/DataMover.hpp"
#include "castor/tape/tpcp/Dumper.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/ParsedCommandLine.hpp"
#include "castor/tape/tpcp/Verifier.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/vmgr_struct.h"

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
  ParsedCommandLine m_parsedCommandLine;

  /**
   * The list of RFIO filenames parsed from the "filelist" file if specified
   * on the command-line with the "-f | --filelist" option.
   */
  FilenameList m_fileListFiles;

  /**
   * The DGN of the tape to be used.
   */
  char m_dgn[CA_MAXDGNLEN + 1];

  /**
   * TCP/IP aggregator callback socket.
   */
  castor::io::ServerSocket m_callbackSocket;

  /**
   * The volume request ID returned by the VDQM as a result of requesting a
   * drive.
   */
  int m_volReqId;

  /**
   * ActionHandler responsible for performing the READ and WRITE tape actions.
   */
  DataMover m_dataMover;

  /**
   * ActionHandler responsible for performing the DUMP tape action.
   */
  Dumper m_dumper;

  /**
   * ActionHandler responsible for performing the VERIFY tape action.
   */
  Verifier m_verifier;

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
   * Writes the parsed command-line to the specified output stream.
   *
   * @param os The output stream to be written to.
   */
  void writeParsedCommandLine(std::ostream &os) throw();

  /**
   * Parse the specified "filelist" file  containing a list of filenames, one
   * per line.
   *
   * @param fileList The filename of the "filelist" file.
   */
  void parseFileListFile() throw (castor::exception::Exception);

  /**
   * Writes the list of files parsed from the "filelist" file if specified on
   * the command-line with the "-f | --filelist" option.
   */
  void writeParsedFileListFiles(std::ostream &os) throw();

  /**
   * Returns the port on which the server will listen for connections from the
   * VDQM.
   */
  int getVdqmListenPort() throw(castor::exception::Exception);

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

  /**
   * Count the minimum number of files specified in the tape file ranges
   * provided as a parameter
   */
  int countMinNumberOfFiles() throw (castor::exception::Exception);

  /**
   * Count the number of ranges that contains the upper boundary "end of 
   * tape" ('m-').
   */
  int nbRangesWithEnd() throw (castor::exception::Exception);

  /**
   * Retrieves information about the specified tape from the VMGR.
   *
   * This method is basically a C++ wrapper around the C VMGR function
   * vmgr_querytape.  This method converts the return value of -1 and the
   * serrno to an exception in the case of an error.
   &
   * @param vid The tape for which the information should be retrieved.
   * @param side The tape side.
   * @param tapeInfo Will be filled with the retrieved information.
   * @param dgn Will be filled with the DGN asscoaited with the tape.
   */
  void vmgrQueryTape(char (&vid)[CA_MAXVIDLEN+1], const int side,
    vmgr_tape_info &tapeInfo, char (&dgn)[CA_MAXDGNLEN+1])
    throw (castor::exception::Exception);

  /**
   * Writes the specified vmgr_tape_info structure to the specified output
   * stream.
   *
   * @param os The output stream to be written to.
   * @param tapeInfo The vmgr_tape_info structure to be written.
   */
  void writeVmgrTapeInfo(std::ostream &os, vmgr_tape_info &tapeInfo)
    throw();

  /**
   * Writes the DGN retreived from the VMGR to the specified output stream.
   *
   * @param os The output stream to be written to.
   */
  void writeDgn(std::ostream &os) throw();

  /**
   * Creates, binds and sets to listening the callback socket to be used for
   * callbacks from the aggregator daemon.
   */
  void setupCallbackSocket() throw(castor::exception::Exception);

  /**
   * Writes a textual description of the aggregator callback socket to the
   * specified output stream.
   *
   * @param os The output stream to be written to.
   */
  void writeCallbackSocket(std::ostream &os) throw();

  /**
   * Request a drive from the VDQM to mount the specified tape for the
   * specified access mode (read or write).
   *
   * @param mode The access mode, either WRITE_DISABLE or WRITE_ENABLE.
   */
   void requestDriveFromVdqm(const int mode)
     throw(castor::exception::Exception);

  /**
   * Writes the volume request ID obtained from the VDQM to the specified
   * output stream.
   *
   * @param os The output stream to be written to.
   */
  void writeVolReqId(std::ostream &os) throw();

  /**
   * Writes a texutal description of the specified aggregator callback
   * connection to the specified output stream.
   *
   * @param os The output stream to be written to.
   * @param connectSocketFd The socket descriptor of the aggregator callback
   * connection.
   */
  void writeAggregatorCallbackConnection(std::ostream &os,
    const int connectSocketFd) throw();

}; // class TpcpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP
