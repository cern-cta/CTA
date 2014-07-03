/******************************************************************************
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
#include "castor/messages/TapeserverProxy.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/utils/ZmqSocket.hpp"

namespace castor {
namespace messages {

/**
 * A concrete implementation of the interface to the internal network
 * communications of the tapeserverd daemon.
 */
class TapeserverProxyZmq: public TapeserverProxy {
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
   * @param zmqContext The ZMQ context.
   */
  TapeserverProxyZmq(log::Logger &log, const unsigned short tapeserverPort,
    const int netTimeout, void *const zmqContext) throw();

  /**
   * Informs the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param volInfo The volume information of the tape the session is working
   * with.
   * @param unitName The unit name of the tape drive.
   */
  void gotReadMountDetailsFromClient(
    castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.  In return the tapeserverd daemon
   * replies with the number of files currently stored on the tape as given by
   * the vmgrd daemon.
   *
   * @param volInfo The volume information of the tape the session is working
   * with.
   * @param unitName The unit name of the tape drive.
   * @return The number of files currently stored on the tape as given by the
   * vmgrd daemon.
   */
  uint64_t gotWriteMountDetailsFromClient(
    castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
    const std::string &unitName);

  /**
   * Informs the tapeserverd daemon that the mount-session child-process got
   * the mount details from the client.
   *
   * @param volInfo The volume information of the tape the session is working
   * with.
   * @param unitName The unit name of the tape drive.
   */
  void gotDumpMountDetailsFromClient(
    castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param volInfo The volume information of the tape the session is working
   * with.
   * @param unitName The unit name of the tape drive.
   */
  void tapeMountedForRead(
    castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param volInfo The volume information of the tape the session is working
   * with.
   * @param unitName The unit name of the tape drive.
   */
  void tapeMountedForWrite(
    castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
    const std::string &unitName);
  
  /**
   * Notifies the tapeserverd daemon that the specified tape is unmounting.
   *
   * @param volInfo The volume information of the tape the session is working
   * with.
   * @param unitName The unit name of the tape drive.
   */
 void tapeUnmounting(
   castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
   const std::string &unitName);
 
  /**
   * Notifies the tapeserverd daemon that the specified tape has been unmounted.
   *
   * @param volInfo The volume information of the tape the session is working
   * with.
   * @param unitName   The unit name of the tape drive.
   */
 void tapeUnmounted(
   castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
   const std::string &unitName);
 
 void notifyHeartbeat(uint64_t nbOfMemblocksMoved);
private:
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
  
  tape::utils::ZmqSocket m_messageSocket;
  tape::utils::ZmqSocket m_heartbeatSocket;

}; // class TapeserverProxyZmq

} // namespace messages
} // namespace castor

