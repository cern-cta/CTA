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

#include "castor/io/io.hpp"
#include "castor/log/Logger.hpp"
#include "castor/messages/ProcessCrashed.pb.h"
#include "castor/messages/ProcessExited.pb.h"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/tape/reactor/PollEventHandler.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerFrame.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of the incoming connection from the ProcessForker.
 */
class ProcessForkerConnectionHandler: public reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param fd The file descriptor of the incomming connection from the
   * ProcessForker
   * @param reactor The reactor with which this event handler is registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param driveCatalogue The catalogue of tape drives controlled by the tape
   * server daemon.
   */
  ProcessForkerConnectionHandler(
    const int fd,
    reactor::ZMQReactor &reactor,
    log::Logger &log,
    DriveCatalogue &driveCatalogue) throw();

  /**
   * Destructor.
   *
   * Closes the incoming connection with the ProcessForker.
   */
  ~ProcessForkerConnectionHandler() throw();

  /**
   * Returns the human-readable name this event handler.
   */
  std::string getName() const throw();

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * zmq_poll().
   */
  void fillPollFd(zmq_pollitem_t &fd) throw();

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   */
  bool handleEvent(const zmq_pollitem_t &fd) ;

private:

  /**
   * The file descriptor of the incomming connection from the ProcessForker.
   */
  const int m_fd;

  /**
   * The reactor with which this event handler is registered.
   */
  reactor::ZMQReactor &m_reactor;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The catalogue of tape drives controlled by the tape server daemon.
   */
  DriveCatalogue &m_driveCatalogue;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

  /**
   * Logs the specifed IO event of the incoming ProcessForker connection.
   */
  void logConnectionEvent(const zmq_pollitem_t &fd);

  /**
   * Throws an exception if the specified file-descriptor is not that of the
   * connection with the client.
   */
  void checkHandleEventFd(const int fd) ;

  /**
   * Handles an incoming message from the ProcessForker.
   */
  void handleMsg();

  /**
   * Dispatches the appropriate message handler for the message contained
   * within the specified frame.
   *
   * @param frame The frame containing the message.
   */
  void dispatchMsgHandler(const ProcessForkerFrame &frame);

  /**
   * Handles the specified ProcessCrashedMsg.
   *
   * @param frame The frame containing the message.
   */
  void handleProcessCrashedMsg(const ProcessForkerFrame &frame);

  /**
   * Handles the specified ProcessExitedMsg.
   *
   * @param frame The frame containing the message.
   */
  void handleProcessExitedMsg(const ProcessForkerFrame &frame);

}; // class ProcessForkerConnectionHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

