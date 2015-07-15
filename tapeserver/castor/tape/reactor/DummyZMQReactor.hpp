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

#include "castor/tape/reactor/ZMQReactor.hpp"

namespace castor {
namespace tape {
namespace reactor {

/**
 * This is a dummy ZmqReactor class that does nothing.  The primary goal of
 * this class is to facilitate unit testing.
 */
class DummyZMQReactor: public ZMQReactor {
public:

  /**
   * Constructor.
   *
   * @param log Interface to the CASTOR logging system.
   */   
  DummyZMQReactor(log::Logger& log) throw();

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
  
}; // class DummyZMQReactor

} // namespace reactor
} // namespace tape
} // namespace castor
