/******************************************************************************
 *         castor/legacymsg/TapeserverProxyTcpIp.hpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"
#include "castor/legacymsg/TapeserverProxy.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"

namespace castor {
namespace legacymsg {

/**
 * A concrete implementation of the interface to the internal network
 * communications of the tapeserverd daemon.
 */
class TapeserverProxyTcpIp: public TapeserverProxy {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param vdqmHostName The name of the host on which the vdqmd daemon is
   * running.
   * @param vdqmPort The TCP/IP port on which the vdqmd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  TapeserverProxyTcpIp(log::Logger &log, const unsigned short tapeserverPort,
    const int netTimeout) throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~TapeserverProxyTcpIp() throw();

  /**
   * Informs the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param volInfo The volume information of the tape the session is working with
   * @param unitName   The unit name of the tape drive.
   */
  void gotReadMountDetailsFromClient(castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo, const std::string &unitName);

  /**
   * Informs the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param volInfo The volume information of the tape the session is working with
   * @param unitName   The unit name of the tape drive.
   */
  void gotWriteMountDetailsFromClient(castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo, const std::string &unitName);

  /**
   * Informs the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param volInfo The volume information of the tape the session is working with
   * @param unitName   The unit name of the tape drive.
   */
  void gotDumpMountDetailsFromClient(castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo, const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param volInfo The volume information of the tape the session is working with
   * @param unitName   The unit name of the tape drive.
   */
  void tapeMountedForRead(castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo, const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param volInfo The volume information of the tape the session is working with
   * @param unitName   The unit name of the tape drive.
   */
  void tapeMountedForWrite(castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo, const std::string &unitName);
  
  /**
   * Notifies the tapeserverd daemon that the specified tape is unmounting.
   *
   * @param volInfo The volume information of the tape the session is working with
   * @param unitName   The unit name of the tape drive.
   */
 void tapeUnmounting(castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo, const std::string &unitName);
 
  /**
   * Notifies the tapeserverd daemon that the specified tape has been unmounted.
   *
   * @param volInfo The volume information of the tape the session is working with
   * @param unitName   The unit name of the tape drive.
   */
 void tapeUnmounted(castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo, const std::string &unitName);

private:

  /**
   * Fills in the body of the update drive request and calls writeTapeUpdateDriveRqstMsg
   * to send it to the tapeserver
   * 
   * @param event      The status of the tape with respect to the drive mount and unmount operations
   * @param mode       Read (read only), write (read/write), or dump
   * @param clientType The client could be the gateway, readtp, writetp, or dumptp
   * @param unitName   The unit name of the tape drive.
   * @param vid        The Volume ID of the tape to be mounted.
   */
  void updateDriveInfo(
    const castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent event,
    const castor::tape::tapegateway::VolumeMode mode,
    const castor::tape::tapegateway::ClientType clientType,
    const std::string &unitName,
    const std::string &vid);
  
  /**
   * Connects to the vdqmd daemon.
   *
   * @return The socket-descriptor of the connection with the vdqmd daemon.
   */
  int connectToTapeserver() const ;

  /**
   * Writes the specified message to the specified connection.
   *
   * @param body The body of the message.
   */
  void writeTapeUpdateDriveRqstMsg(const int fd, const legacymsg::TapeUpdateDriveRqstMsgBody &body) ;

  /**
   * Reads a reply message from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   */
  void readReplyMsg(const int fd) ;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The name of the host on which the vdqmd daemon is running.
   */
  const std::string m_tapeserverHostName;

  /**
   * The TCP/IP port on which the vdqmd daemon is listening.
   */
  const unsigned short m_tapeserverPort;
  
  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

}; // class TapeserverProxyTcpIp

} // namespace legacymsg
} // namespace castor

