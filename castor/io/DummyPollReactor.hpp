/******************************************************************************
 *                castor/io/DummyPollReactor.hpp
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

#ifndef CASTOR_IO_DUMMYPOLLREACTOR_HPP
#define CASTOR_IO_DUMMYPOLLREACTOR_HPP 1

#include "castor/io/PollReactor.hpp"

namespace castor {
namespace io {

/**
 * A dummy poll() reactor.
 *
 * The main goal of this class is to facilitate the development of unit tests.
 */
class DummyPollReactor: public PollReactor {
public:
  /**
   * Destructor.
   */
  ~DummyPollReactor() throw();

  /**
   * Registers the specified handler.
   *
   * Please note that the reactor takes ownership of the handler and will
   * delete it as appropriate.
   *
   * @param handler The handler to be registered.
   */
  void registerHandler(PollEventHandler *const handler)
    throw(castor::exception::Exception);

  /**
   * Removes the specified handler from the reactor.  This method effectively
   * does the opposite of registerHandler().
   *
   * @param handler The handler to be removed.
   */
  void removeHandler(PollEventHandler *const handler)
    throw(castor::exception::Exception);

  /**
   * Handles any pending events.
   *
   * @param timeout Timeout in milliseconds.
   */
  void handleEvents(const int timeout) throw(castor::exception::Exception);

}; // class DummyPollReactor

} // namespace io
} // namespace castor

#endif // CASTOR_IO_DUMMYPOLLREACTOR_HPP
