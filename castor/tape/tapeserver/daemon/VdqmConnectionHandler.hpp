/******************************************************************************
 *                castor/tape/tapeserver/daemon/VdqmConnectionHandler.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPESERVER_DAEMON_VDQMCONNECTIONHANDLER_HPP
#define CASTOR_TAPE_TAPESERVER_DAEMON_VDQMCONNECTIONHANDLER_HPP 1

#include "castor/io/PollEventHandler.hpp"
#include "castor/io/PollReactor.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/legacymsg/VdqmMarshal.hpp"
#include "h/vdqm_constants.h"
#include "h/rtcp_constants.h"

#include <poll.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of a connection with the vdqmd daemon.
 */
class VdqmConnectionHandler: public io::PollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param connection The file descriptor of the connection with the vdqmd
   * daemon.
   * @param reactor The reactor with which this event handler is registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param vdqm The object representing the vdqmd daemon.
   * @param driveCatalogue The catalogue of tape drives controlled by the tape
   * server daemon.
   */
  VdqmConnectionHandler(const int connection, io::PollReactor &reactor,
    log::Logger &log, Vdqm &vdqm, DriveCatalogue &driveCatalogue) throw();

  /**
   * Returns the integer file descriptor of this event handler.
   */
  int getFd() throw();

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  void fillPollFd(struct pollfd &fd) throw();

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   */
  void handleEvent(const struct pollfd &fd) throw(castor::exception::Exception);

  /**
   * Destructor.
   *
   * Closes the connection with the vdqmd daemon.
   */
  ~VdqmConnectionHandler() throw();

private:

  /**
   * The file descriptor of the connection with the vdqm daemon.
   */
  const int m_connection;

  /**
   * The reactor with which this event handler is registered.
   */
  io::PollReactor &m_reactor;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The object representing the vdqmd daemon.
   */
  Vdqm &m_vdqm;

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
   * Throws an exception if the specified file-descriptor is not that of the
   * socket listening for connections from the vdqmd daemon.
   */
  void checkHandleEventFd(const int fd) throw (castor::exception::Exception);

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
   * Reads a job message from the specified connection, sends back a positive
   * acknowledgement and closes the connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @return The job request from the vdqm.
   */
  legacymsg::RtcpJobRqstMsgBody readJobMsg(const int connection)
    throw(castor::exception::Exception);
  
  /**
   * Reads the header of a job message from the specified connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @return The message header.
   */
  legacymsg::MessageHeader readJobMsgHeader(const int connection)
    throw(castor::exception::Exception);
  
  /**
   * Reads the body of a job message from the specified connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @param len The length of the message body in bytes.
   * @return The message body.
   */
  legacymsg::RtcpJobRqstMsgBody readJobMsgBody(const int connection,
    const uint32_t len) throw(castor::exception::Exception);

  /**
   * Writes a job reply message to the specified connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   */
  void writeJobReplyMsg(const int connection)
    throw(castor::exception::Exception);

}; // class VdqmConnectionHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_DAEMON_VDQMCONNECTIONHANDLER_HPP
