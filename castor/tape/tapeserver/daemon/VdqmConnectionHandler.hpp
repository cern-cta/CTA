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
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/RtcpMarshal.hpp"
#include "castor/legacymsg/VdqmProxyFactory.hpp"
#include "castor/legacymsg/VdqmMarshal.hpp"
#include "castor/tape/reactor/PollEventHandler.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/Catalogue.hpp"
#include "h/vdqm_constants.h"
#include "h/rtcp_constants.h"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of a connection with the vdqmd daemon.
 */
class VdqmConnectionHandler: public reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param fd The file descriptor of the connection with the vdqmd
   * daemon.
   * @param reactor The reactor with which this event handler is registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param driveCatalogue The catalogue of tape drives controlled by the tape
   * server daemon.
   */
  VdqmConnectionHandler(
    const int fd,
    reactor::ZMQReactor &reactor,
    log::Logger &log,
    Catalogue &driveCatalogue) throw();

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

  /**
   * Destructor.
   *
   * Closes the connection with the vdqmd daemon.
   */
  ~VdqmConnectionHandler() throw();

private:

  /**
   * The file descriptor of the vdqm connection.
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
  Catalogue &m_driveCatalogue;
  
  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

  /**
   * Logs the specifed IO event of the vdqm connection.
   *
   * @param fd File descriptor describing the event.
   */
  void logConnectionEvent(const zmq_pollitem_t &fd);

  /**
   * Throws an exception if the specified file-descriptor is not that of the
   * connection with the client.
   */
  void checkHandleEventFd(const int fd) ;

  /**
   * Returns true if the peer host of the connection being handled is
   * authorized.
   */
  bool connectionIsAuthorized() throw();

  /**
   * Logs the reception of the specified job message from the vdqmd daemon.
   */
  void logVdqmJobReception(const legacymsg::RtcpJobRqstMsgBody &job)
    const throw();
  
  /**
   * Reads a job message from the vdqm connection.
   *
   * @return The job message from the vdqm.
   */
  legacymsg::RtcpJobRqstMsgBody readJobMsg();
  
  /**
   * Reads a message header from the vdqm connection.
   *
   * @return The message header.
   */
  legacymsg::MessageHeader readMsgHeader();
  
  /**
   * Reads the body of a job message from the vdqm connection.
   *
   * @param len The length of the message body in bytes.
   * @return The message body.
   */
  legacymsg::RtcpJobRqstMsgBody readJobMsgBody(const uint32_t len);

  /**
   * Writes a job reply message to the vdqm connection.
   */
  void writeJobReplyMsg();

}; // class VdqmConnectionHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

