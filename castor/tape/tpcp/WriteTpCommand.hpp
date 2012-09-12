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

#ifndef CASTOR_TAPE_TPCP_WRITETPCOMMAND_HPP
#define CASTOR_TAPE_TPCP_WRITETPCOMMAND_HPP 1

#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
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

  /**
   * Checks the disk files can be accessed.
   *
   * @throw A castor::exception::Exception exception if the disk files cannot
   *        be accessed.
   */
  void checkAccessToDisk() const throw(castor::exception::Exception);

  /**
   * Checks the tape can be accessed.
   *
   * @throw A castor::exception::Exception exception if the tape cannot be
   *        accessed.
   */
  void checkAccessToTape() const throw(castor::exception::Exception);

  /**
   * Request a drive connected to the specified tape-server from the VDQM.
   *
   * @param tapeServer If not NULL then this parameter specifies the tape
   *                   server to be used, therefore overriding the drive
   *                   scheduling of the VDQM.
   */
  void requestDriveFromVdqm(char *const tapeServer)
    throw(castor::exception::Exception);

  /**
   * Sends the volume message to the tapebridged daemon.
   *
   * @param volumeRequest The volume rerquest message received from the
   *                      tapegatewayd daemon.
   * @param connection    The already open connection to the tapebridged daemon
   *                      over which the volume message should be sent.
   */
  void sendVolumeToTapeBridge(
    const tapegateway::VolumeRequest &volumeRequest,
    castor::io::AbstractTCPSocket    &connection)
    const throw(castor::exception::Exception);

  /**
   * Performs the tape copy whether it be DUMP, READ or WRITE.
   */
  void performTransfer() throw(castor::exception::Exception);

  /**
   * Dispatches the appropriate handler for the specified tape-gateway message.
   *
   * @param obj  The tape-gateway message for which a handler should be
   *             dispatched.
   * @param sock The socket on which to reply to the message.
   * @return     True if there is more work to be done, else false.
   */
  bool dispatchMsgHandler(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);


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
   * Throws a permission denied exception if the user of the tpcp command
   * does not have permission to write to tape.
   *
   * @param poolName   The name of the pool in which the tape to be written
   *                   resides.
   * @param userId     The ID of the user.
   * @param groupId    The group ID of the user.
   * @param sourceHost The CUPV source host.
   */
  void checkUserHasTapeWritePermission(const char *const poolName,
    const uid_t userId, const gid_t groupId, const char *const sourceHost)
    const throw (castor::exception::PermissionDenied);

  /**
   * FilesToMigrateListRequest message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleFilesToMigrateListRequest(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * FileMigrationReportList message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleFileMigrationReportList(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * Handles the specified successful migration of files to tape.
   *
   * @param tapebridgeTransId The tapebridge transaction Id of the enclosing
   *                          FileMigrationReportList message.
   * @param files             The sucessful migrations to tape.
   * @param sock              The socket on which to reply to the tapebridge.
   */
  void handleSuccessfulMigrations(
    const uint64_t tapebridgeTransId,
    const std::vector<tapegateway::FileMigratedNotificationStruct*> &files,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * Handles the successfull migration of a file to tape.
   *
   * @param tapebridgeTransId The tapebridge transaction Id of the enclosing
   *                          FileMigrationReportList message.
   * @param file              The file that was successfully migrated to tape.
   * @param sock              The socket on which to reply to the tapebridge.
   */
  void handleSuccessfulMigration(
    const uint64_t tapebridgeTransId,
    const tapegateway::FileMigratedNotificationStruct &file,
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

}; // class WriteTpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_WRITETPCOMMAND_HPP
