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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"
#include "castor/tape/reactor/PollEventHandler.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of the socket listening for connection from the vdqmd
 * daemon.
 */
class VdqmAcceptHandler: public reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param fd The file descriptor of the socket listening for
   * connections from the vdqmd daemon.
   * @param reactor The reactor to which new Vdqm connection handlers are to be
   * registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param driveCatalogue The catalogue of tape drives controlled by the tape
   * server daemon.
   */
  VdqmAcceptHandler(const int fd, reactor::ZMQReactor &reactor,
    log::Logger &log, DriveCatalogue &driveCatalogue) throw();

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
  bool handleEvent(const zmq_pollitem_t &fd);

  /**
   * Destructor.
   *
   * Closes the listen socket.
   */
  ~VdqmAcceptHandler() throw();

private:

  /**
   * Logs the specifed IO event of the vdqm listen socket.
   */
  void logVdqmAcceptEvent(const zmq_pollitem_t &fd);

  /**
   * Throws an exception if the specified file-descriptor does not match the
   * file-descriptor of this event handler.
   *
   * @param fd The file descriptor to be checked.
   */
  void checkHandleEventFd(const int fd);

  /**
   * The file descriptor of the socket listening for vdqm connections.
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
   * The catalogue of tape drives controlled by the tape server daemon.
   */
  DriveCatalogue &m_driveCatalogue;

}; // class VdqmAcceptHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

