/******************************************************************************
 *                castor/tape/tapeserver/daemon/AdminConnectionHandler.hpp
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
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/TapeConfigRequestMsgBody.hpp"
#include "castor/legacymsg/TapeStatRequestMsgBody.hpp"
#include "castor/legacymsg/TapeStatReplyMsgBody.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/tape/reactor/PollEventHandler.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of the socket listening for connection from the admin
 * commands.
 */
class AdminConnectionHandler: public reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param fd The file descriptor of the socket listening for
   * connections from the vdqmd daemon.
   * @param reactor The reactor to which new Vdqm connection handlers are to
   * be registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param driveCatalogue The catalogue of tape drives controlled by the tape
   * server daemon.
   */
  AdminConnectionHandler(
    const int fd,
    reactor::ZMQReactor &reactor,
    log::Logger &log,
    legacymsg::VdqmProxy &vdqm,
    DriveCatalogue &driveCatalogue,
    const std::string &hostName) throw();

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  void fillPollFd(zmq::Pollitem &fd) throw();

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   * @return true if the event handler should be removed from and deleted by
   * the reactor.
   */
  bool handleEvent(const zmq::Pollitem &fd);

  /**
   * Destructor.
   */
  ~AdminConnectionHandler() throw();

private:
  
  /**
   * Marshals the specified source tape config reply message structure into the
   * specified destination buffer.
   *
   * @param dst    The destination buffer.
   * @param dstLen The length of the destination buffer.
   * @param rc     The return code to reply.
   * @return       The total length of the header.
   */
  size_t marshalTapeRcReplyMsg(char *const dst, const size_t dstLen,
    const int rc);
  
  /**
   * Writes a job reply message to the tape config command connection.
   *
   * @param rc The return code to reply.
   * 
   */
  void writeTapeRcReplyMsg(const int rc);
  
  /**
   * Writes a reply message to the tape stat command connection.
   */
  void writeTapeStatReplyMsg();

  /**
   * Logs the specifed IO event of the admin connection.
   */
  void logAdminConnectionEvent(const zmq::Pollitem &fd);

  /**
   * Throws an exception if the specified file-descriptor does not match the
   * file-descriptor of this event handler.
   *
   * @param fd The file descriptor to be checked.
   */
  void checkHandleEventFd(const int fd);
  
  /**
   * Handles the specified request message.
   *
   * @param header The header of the request message.
   */
  void handleRequest(const legacymsg::MessageHeader &header);

  /**
   * Handles the specified config request-message.
   *
   * @param header The header of the request message.
   */
  void handleConfigRequest(const legacymsg::MessageHeader &header);

  /**
   * Handles the specified stat request-message.
   *
   * @param header The header of the request message.
   */
  void handleStatRequest(const legacymsg::MessageHeader &header);
  
  /**
   * Logs the reception of the specified job message from the tpconfig command.
   */
  void logTapeConfigJobReception(const legacymsg::TapeConfigRequestMsgBody &job)
    const throw();
  
  /**
   * Logs the reception of the specified job message from the tpstat command.
   */
  void logTapeStatJobReception(const legacymsg::TapeStatRequestMsgBody &job)
    const throw();
  
  /**
   * Reads a message header from admin connection.
   *
   * @return The message header.
   */
  legacymsg::MessageHeader readMsgHeader();
  
  /**
   * Reads the body of a job message from the specified connection.
   *
   * @param bodyLen The length of the message body in bytes.
   * @return The message body.
   */
  legacymsg::TapeConfigRequestMsgBody readTapeConfigMsgBody(
    const uint32_t bodyLen);
  
  /**
   * Reads the body of a job message from the specified connection.
   *
   * @param bodyLen The length of the message body in bytes.
   * @return The message body.
   */
  legacymsg::TapeStatRequestMsgBody readTapeStatMsgBody(const uint32_t bodyLen);

  /**
   * The file descriptor of the admin connection.
   */
  const int m_fd;

  /**
   * The reactor to which new Vdqm connection handlers are to be registered.
   */
  reactor::ZMQReactor &m_reactor;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * Proxy object representing the vdqmd daemon.
   */
  legacymsg::VdqmProxy &m_vdqm;

  /**
   * The catalogue of tape drives controlled by the tape server daemon.
   */
  DriveCatalogue &m_driveCatalogue;
  
  /**
   * The name of the host on which tape daemon is running.  This name is
   * needed to fill in messages to be sent to the vdqmd daemon.
   */
  const std::string m_hostName;
  
  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

}; // class VdqmConnectionHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
