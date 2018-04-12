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

#include "common/exception/Exception.hpp"

#include <zmq.h>

namespace cta {
namespace mediachanger {
namespace reactor {

/**
 * Handles the events that occur on a poll() file descriptor.
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
class ZMQPollEventHandler {
public:

  /**
   * Destructor.
   */
  virtual ~ZMQPollEventHandler() throw() = 0;

  /**
   * Returns the human-readable name this event handler.
   */
  virtual std::string getName() const throw() = 0;

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  virtual void fillPollFd(zmq_pollitem_t &pollitem) =0;

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   * @return true if the event handler should be removed from and deleted by
   * the reactor.
   */
  virtual bool handleEvent(const zmq_pollitem_t &fd)=0;

}; // class ZMQPollEventHandler

} // namespace reactor
} // namespace tape
} // namespace cta

