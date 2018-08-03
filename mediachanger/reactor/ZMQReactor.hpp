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

#include "ZMQPollEventHandler.hpp"
#include "ZMQReactor.hpp"

#include "common/log/log.hpp"
#include <utility>
#include <vector>

namespace cta {
namespace mediachanger {
namespace reactor {

/**
 * This reactor wraps the zmq_poll() function.
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
class ZMQReactor {
public:

  /**
   * Constructor.
   */
  ZMQReactor(cta::log::Logger &);
  /**
   * Destructor.
   */
  virtual ~ZMQReactor();

  /**
   * Removes and deletes all of the event handlers registered with the reactor.
   */
 virtual void clear();

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
  virtual void registerHandler(ZMQPollEventHandler *const handler);

  /**
   * Handles any pending events.
   *
   * @param timeout Timeout in milliseconds.
   */
  virtual void handleEvents(const int timeout);
  
private:

  log::Logger &m_log;
  
  /**
   * Type used to map zmq_pollitem_t to event handler.
   */
typedef std::vector<std::pair<zmq_pollitem_t, ZMQPollEventHandler*> >
    HandlerMap;

  /**
   * Throws a cator::exception::Exception if a handler has already been
   * registered for the specified poll item.
   *
   * @prama item The poll item.
   */
  void checkDoubleRegistration(const zmq_pollitem_t &item) const;

  /**
   * Builds the vector of file descriptors to be passed to poll().
   *
   * Please note that the return type is an std::vector because we can assume
   * that its elements are stored contiguously in memory.  The address of the
   * first element is going to be passed to zmq_poll().
   *
   * @return The vector of file descriptors.
   */
  std::vector<zmq_pollitem_t> buildPollFds() const;
  
  /**
   * Returns the event handler associated with the specified zmq_pollitem_t.
   */
  HandlerMap::iterator findHandler(const zmq_pollitem_t&);
  
  /**
   * Handles the specified ZMQ I/O event.
   *
   * @param pollFd The file-descriptor representing the I/O event.
   */
  void handleEvent(const zmq_pollitem_t &pollFd);
  
  /**
   * Removes the specified handler from the reactor.  This method effectively
   * does the opposite of registerHandler().
   *
   * @param handler The handler to be removed.
   */
  void removeHandler(ZMQPollEventHandler *const handler);
  
  /**
   * Map of file descriptor to registered event handler.
   */
  HandlerMap m_handlers;  
  
}; // class ZMQReactor

} // namespace reactor
} // namespace mediachanger
} // namespace cta
