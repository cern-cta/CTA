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

#include "castor/tape/reactor/PollEventHandler.hpp"

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
class PollReactor {
public:

  /**
   * Destructor.
   */
  virtual ~PollReactor() throw() = 0;

  /**
   * Removes and deletes all of the event handlers registered with the reactor.
   */
  virtual void clear() throw() = 0;

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
  virtual void registerHandler(PollEventHandler *const handler) = 0;

  /**
   * Handles any pending events.
   *
   * @param timeout Timeout in milliseconds.
   */
  virtual void handleEvents(const int timeout) = 0;

}; // class PollReactor

} // namespace reactor
} // namespace tape
} // namespace castor

