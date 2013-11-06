/******************************************************************************
 *                 castor/tape/tpcp/DumpTpCommand.hpp
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
 * The class implementing the dumptp command.
 */
class DumpTpCommand : public TpcpCommand {
public:

  /**
   * Constructor.
   */
  DumpTpCommand() throw();

  /**
   * Destructor.
   */
  virtual ~DumpTpCommand() throw();


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
   * Checks the disk files can be accessed correctly.
   *
   * @throw A castor::exception::Exception exception if the disk files cannot
   *        be accessed correctly.
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
   * DumpParametersRequest message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleDumpParametersRequest(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * DumpNotification message handler.
   *
   * @param obj  The tapebridge message to be processed.
   * @param sock The socket on which to reply to the tapebridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleDumpNotification(castor::IObject *const obj,
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

}; // class DumpTpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_READTPCOMMAND_HPP
