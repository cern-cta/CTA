/******************************************************************************
 *         castor/tape/reactor/PollReactorImpl.hpp
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
#include "castor/tape/reactor/PollReactor.hpp"

#include <map>
#include <poll.h>

namespace castor {
namespace tape {
namespace reactor {

/**
 * Concrete implementation of the a reactor that wraps the poll() system call.
 *
 * This class is part of an implementation of the Reactor architecture pattern
 * described in the following book:
 *
 *    Pattern-Oriented Software Architecture Volume 2
 *    Patterns for Concurrent and Networked Objects
 *    Authors: Schmidt, Stal, Rohnert and Buschmann
 *    Publication date: 2000
 *    ISBN 0-471-60695-2
 */
class PollReactorImpl: public PollReactor {
public:
  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   */
  PollReactorImpl(log::Logger &log) throw();

  /**
   * Destructor.
   */
  ~PollReactorImpl() throw();

  /**
   * Removes and deletes all of the event handlers registered with the reactor.
   */
  void clear() throw();

  /**
   * Registers the specified handler.
   *
   * Please note that the reactor takes ownership of the handler and will
   * delete it as appropriate.
   *
   * @param handler The handler to be registered.  Please note that the handler
   * MUST be allocated on the heap because the reactor will own the handler
   * and therefore delete it as needed.
   */
  void registerHandler(PollEventHandler *const handler);

  /**
   * Handles any pending events.
   *
   * @param timeout Timeout in milliseconds.
   */
  void handleEvents(const int timeout);

private:

  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * Type used to map file descriptor to event handler.
   */
  typedef std::map<int, PollEventHandler*> HandlerMap;

  /**
   * Map of file descriptor to registered event handler.
   */
  HandlerMap m_handlers;

  /**
   * Allocates and builds the array of file descriptors to be passed to poll().
   *
   * @param nfds Output parameter: The number of elements in the array.
   * @return The array of file descriptors.  Please note that is the
   * responsibility of the caller to delete the array.
   */
  struct pollfd *buildPollFds(nfds_t &nfds);

  /**
   * Dispatches the appropriate event handlers based on the specified result
   * from poll().
   */
  void dispatchEventHandlers(const struct pollfd *const fds,
    const nfds_t nfds);

  /**
   * Returns the event handler associated with the specified integer
   * file-descriptor.
   */
  PollEventHandler *findHandler(const int fd);

  /**
   * Removes the specified handler from the reactor.  This method effectively
   * does the opposite of registerHandler().
   *
   * @param handler The handler to be removed.
   */
  void removeHandler(PollEventHandler *const handler);

}; // class PollReactorImpl

} // namespace reactor
} // namespace tape
} // namespace castor
