/******************************************************************************
 *                 castor/tape/tpcp/WriteTpCommand.hpp
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

#ifndef CASTOR_TAPE_TPCP_READTPCOMMAND_HPP
#define CASTOR_TAPE_TPCP_READTPCOMMAND_HPP 1

#include "castor/tape/tpcp/TpcpCommand.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * The class implementing the writetp command.
 */
class WriteTpCommand : public TpcpCommand {
public:

  /**
   * Constructor.
   */
  WriteTpCommand() throw();

  /**
   * Destructor.
   */
  virtual ~WriteTpCommand() throw();


protected:

  /**
   * Writes the command-line usage message of to the specified output stream.
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
   * Performs the tape copy whether it be DUMP, READ, WRITE or VERIFY.
   */
  void performTransfer() throw(castor::exception::Exception);


private:

  /**
   * Data type for a map of file transaction IDs to the RFIO filenames of
   * files currently being transfered.
   */
  typedef std::map<uint64_t, std::string> FileTransferMap;

  /**
   * Map of file transaction IDs to the RFIO filenames of files currently
   * being transfered.
   */
  FileTransferMap m_pendingFileTransfers;

  /**
   * The next tape file sequence number to be used when migrating using the
   * TPPOSIT_FSEQ positioning method.  This member variable is not used with
   * the TPPOSIT_EOI positioning method.
   */
  int32_t m_nextTapeFseq;

  /**
   * The number of successfully transfered files.
   */
  uint64_t m_nbMigratedFiles;

  /**
   * FileToMigrateRequest message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleFileToMigrateRequest(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * FileMigratedNotification message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleFileMigratedNotification(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotification message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotification(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotificationErrorReport message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotificationErrorReport(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * PingNotification message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handlePingNotification(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

}; // class WriteTpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_READTPCOMMAND_HPP
