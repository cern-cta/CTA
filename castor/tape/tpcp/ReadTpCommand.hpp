/******************************************************************************
 *                 castor/tape/tpcp/ReadTpCommand.hpp
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

#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/tape/tpcp/TapeFseqRangeListSequence.hpp"
#include "castor/tape/tpcp/TpcpCommand.hpp"

#include <sys/types.h>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * The class implementing the readtp command.
 */
class ReadTpCommand : public TpcpCommand {
public:

  /**
   * Constructor.
   */
  ReadTpCommand() throw();

  /**
   * Destructor.
   */
  virtual ~ReadTpCommand() throw();


protected:

  /**
   * Writes the command-line usage message of to the specified output stream.
   *
   * @param os Output stream to be written to.
   */
  void usage(std::ostream &os) throw();

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Performs the tape copy whether it be DUMP, READ or WRITE.
   */
  void performTransfer() throw(castor::exception::Exception);


private:

  /**
   * Count the number of ranges that contain the upper boundary "end of tape"
   * ('m-').
   *
   * @return The number of ranges that contain the upper boundary "end of tape".
   */
  unsigned int countNbRangesWithEnd() throw (castor::exception::Exception);

  /**
   * The umask of the process.
   */
  const mode_t m_umask;

  /**
   * The sequence of tape file sequence numbers to be processed.
   */
  TapeFseqRangeListSequence m_tapeFseqSequence;

  /**
   * The number of successfully transfered files.
   */
  uint64_t m_nbRecalledFiles;

  /**
   * Structure used to remember a file that is currently being transfered.
   */
  struct FileTransfer {

    /**
     * The tape file sequence number.
     */
    uint32_t tapeFseq;

    /**
     * The RFIO filename.
     */
    std::string filename;
  };

  /**
   * Data type for a map of file transaction IDs to files currently being
   * transfered.
   */
  typedef std::map<uint64_t, FileTransfer> FileTransferMap;

  /**
   * Map of file transaction IDs to files currently being transfered.
   */
  FileTransferMap m_pendingFileTransfers;

  /**
   * FilesToRecallListRequest message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleFilesToRecallListRequest(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * FileRecallReportList message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleFileRecallReportList(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * Handles the specified successful recalls of files from tape.
   *
   * @param aggregatorTransactionId The aggregator transaction Id of the
   *                                enclosing FileRecallReportList message.
   * @param files                   The sucessful recalls from tape.
   * @param sock                    The socket on which to reply to the
   *                                tapebridge.
   */
  void handleSuccessfulRecalls(
    const uint64_t aggregatorTransactionId,
    const std::vector<tapegateway::FileRecalledNotificationStruct*> &files,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * Handles the successfull recall of a file from tape.
   *
   * @param aggregatorTransactionId The aggregator transaction Id of the
   *                                enclosing FileRecallReportList message.
   * @param file                    The file that was successfully recalled
   *                                from tape.
   * @param sock                    The socket on which to reply to the
   *                                tapebridge.
   */
  void handleSuccessfulRecall(
    const uint64_t aggregatorTransactionId,
    const tapegateway::FileRecalledNotificationStruct &file,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotification message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotification(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotificationErrorReport message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotificationErrorReport(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * PingNotification message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handlePingNotification(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

}; // class ReadTpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_READTPCOMMAND_HPP
