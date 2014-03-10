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
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"

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
   * 
   */
  VdqmConnectionHandler(const int connection, io::PollReactor &reactor,
    log::Logger &log, Vdqm &vdqm) throw();

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
   * Logs the reception of the specified job message from the vdqmd daemon.
   */
  void logVdqmJobReception(const legacymsg::RtcpJobRqstMsgBody &job)
    const throw();

}; // class VdqmConnectionHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_DAEMON_VDQMCONNECTIONHANDLER_HPP
