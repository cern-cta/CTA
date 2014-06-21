/******************************************************************************
 *         castor/tape/reactor/ZMQReactor.hpp
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
 * @author castor-dev
 *****************************************************************************/

#pragma once
#include "castor/log/Logger.hpp"
#include "castor/tape/reactor/ZMQPollEventHandler.hpp"
#include "zmq/zmqcastor.hpp"

#include <vector>
#include <utility>

namespace castor {
namespace tape {
namespace reactor {

/**
 * This reactor wraps the poll() system call.
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
  ZMQReactor(log::Logger& log,zmq::context_t& ctx);

  ~ZMQReactor();
  /**
   * Removes and deletes all of the event handlers registered with the reactor.
   */
  void clear();

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
  void registerHandler(ZMQPollEventHandler *const handler);

  /**
   * Handles any pending events.
   *
   * @param timeout Timeout in milliseconds.
   */
  void handleEvents(const int timeout);
  
  zmq::context_t& getContext()const {return m_context;}

private:

  /**
   * Allocates and builds the array of file descriptors to be passed to poll().
   *
   * @return The array of file descriptors.  Please note that is the
   * responsibility of the caller to delete the array.
   */
  std::vector<zmq::pollitem_t> buildPollFds() const;
  
  /**
   * Returns the event handler associated with the specified integer
   * file-descriptor (null if there no handler associated, if ti happens = bug)
   */
  ZMQPollEventHandler*  findHandler(const zmq::pollitem_t&) const;
  
  /**
   * Dispatches the appropriate event handlers based on the specified result
   * from poll().
   */
  void dispatchEventHandlers(const std::vector<zmq::pollitem_t>& pollFD);
  
  /**
   * Removes the specified handler from the reactor.  This method effectively
   * does the opposite of registerHandler().
   *
   * @param handler The handler to be removed.
   */
  void removeHandler(ZMQPollEventHandler *const handler);
  
  /**
   * Type used to map file descriptor to event handler.
   */
  typedef std::vector<std::pair<zmq::pollitem_t, ZMQPollEventHandler*> > HandlerMap;
  
  /**
   * Map of file descriptor to registered event handler.
   */
  HandlerMap  m_handlers;  
  
  zmq::context_t& m_context;
  
  /**
   * Object representing the API of the CASTOR logging system.
   */
  log::Logger& m_log;

}; // class ZMQReactor

} // namespace reactor
} // namespace tape
} // namespace castor
