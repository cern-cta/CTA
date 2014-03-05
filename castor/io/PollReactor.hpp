/******************************************************************************
 *                castor/tape/tapeserver/daemon/PollReactor.hpp
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

#ifndef CASTOR_IO_POLLREACTOR_HPP
#define CASTOR_IO_POLLREACTOR_HPP 1

#include "castor/io/PollEventHandler.hpp"

namespace castor {
namespace io {

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
   * Registers the specified handler.
   *
   * Please note that the reactor takes ownership of the handler and will
   * delete it as appropriate.
   *
   * @param handler The handler to be registered.
   */
  virtual void registerHandler(PollEventHandler *const handler)
    throw(castor::exception::Exception) = 0;

  /**
   * Removes the specified handler from the reactor.  This method effectively
   * does the opposite of registerHandler().
   *
   * @param handler The handler to be removed.
   */
  virtual void removeHandler(PollEventHandler *const handler)
    throw(castor::exception::Exception) = 0;

  /**
   * Handles any pending events.
   *
   * @param timeout Timeout in milliseconds.
   */
  virtual void handleEvents(const int timeout)
    throw(castor::exception::Exception) = 0;

  /**
   * Destructor.
   */
  virtual ~PollReactor() throw() = 0;

}; // class PollReactor

} // namespace io
} // namespace castor

#endif // CASTOR_IO_POLLREACTOR_HPP
