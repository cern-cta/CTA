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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/log/Logger.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Heartbeat.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/Frame.hpp"
#include "castor/messages/ZmqMsg.hpp"
#include "castor/messages/ZmqSocketST.hpp"
#include "castor/tape/reactor/ZMQPollEventHandler.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/Catalogue.hpp"
#include "castor/utils/utils.hpp"

#include <string>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of the socket listening for connection from the admin
 * commands.
 */
class TapeMessageHandler: public reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param internalPort The TCP/IP port on which ZMQ sockets will bind for
   * internal communication between forked sessions and the parent tapeserverd
   * process.
   * @param reactor The reactor to which new Vdqm connection handlers are to
   * be registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param driveCatalogue The tape-drive catalogue.
   * @param hostName The name of the host.
   * @param vmgr Proxy object representing the vmgrd daemon.
   * @param zmqContext The ZMQ context.
   */
  TapeMessageHandler(
    const unsigned short internalPort,
    reactor::ZMQReactor &reactor,
    log::Logger &log,
    Catalogue &driveCatalogue,
    const std::string &hostName,
    castor::legacymsg::VmgrProxy & vmgr,
    void *const zmqContext);

  /**
   * Destructor.
   */
  ~TapeMessageHandler() throw();

  /**
   * Returns the human-readable name this event handler.
   */
  std::string getName() const throw();

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  void fillPollFd(zmq_pollitem_t &fd) throw();

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   * @return true if the event handler should be removed from and deleted by
   * the reactor.
   */
  bool handleEvent(const zmq_pollitem_t &fd) throw();
  
private:

  /**
   * Creates a message frame containing a ReturnValue message.
   *
   * @param value The return value of the ReturnValue message.
   * @return The message frame.
   */
  messages::Frame createReturnValueFrame(const int value);

  /**
   * Creates a message frame containing an Exception message.
   *
   * @param code The error code of the exception.
   * @param msg The message string of the exception.
   */
  messages::Frame createExceptionFrame(const int code,
    const std::string& msg);
  
  /**
   * The reactor to which new Vdqm connection handlers are to be registered.
   */
  reactor::ZMQReactor &m_reactor;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;
  
  /**
   * The ZMQ socket listening for messages.
   */
  messages::ZmqSocketST m_socket;

  /**
   * The catalogue of tape drives controlled by the tape server daemon.
   */
  Catalogue &m_driveCatalogue;
  
  /**
   * The name of the host on which tape daemon is running.
   */
  const std::string m_hostName;
  
  /** 
   * Reference to the VmgrProxy, allowing reporting and checking tape status.
   * It is also used by the StatusReporter 
   */
  castor::legacymsg::VmgrProxy & m_vmgr;
  
  /**
   * Make sure the  zmq_pollitem_t's socket is the same as m_socket
   * Throw an exception if it is not the case
   * @param fd the poll item 
   */
  void checkSocket(const zmq_pollitem_t &fd);
  
  /**
   * Dispatches the appropriate handler method for the specified request
   * message.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame dispatchMsgHandler(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleHeartbeat(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleLabelError(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleMigrationJobFromTapeGateway(
    const messages::Frame &rqst);

  /**
   * Creates a message frame containing a NbFilesOnTape message.
   *
   * @param nbFiles The number of files on the tape.
   * @return The message frame.
   */
  messages::Frame createNbFilesOnTapeFrame(const uint32_t nbFiles);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleMigrationJobFromWriteTp(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleRecallJobFromTapeGateway(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */  
  messages::Frame handleRecallJobFromReadTp(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */  
  messages::Frame handleTapeMountedForMigration(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleTapeMountedForRecall(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleNotifyDriveTapeMounted(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleTapeUnmountStarted(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleTapeUnmounted(const messages::Frame &rqst);

  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleAddLogParams(const messages::Frame &rqst);
  
  /**
   * Handles the specified request.
   *
   * @param rqst The request.
   * @return The reply.
   */
  messages::Frame handleDeleteLogParams(const messages::Frame &rqst);
  
}; // class TapeMessageHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
