/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/threading/SocketPair.hpp"
#include "common/threading/Threading.hpp"

namespace cta { namespace tape { namespace  session {

/** A class wrapping the socket pair provided to the session process 
 * This interface provides direct access functions for simple messages
 * and accumulates the more frequent information (block transfers) and
 * accumulates error counts. This class contains the core of the reporting
 * functions, and is the (virtual) base of the 2 variants for retrieve and archive. 
 * A fourth, simplified (and base) version is used when scheduling the session,
 * before knowing whether it will be an archive or a retrieve. */

class HandlerInterface: private threading::Thread {
public:
  HandlerInterface(server::SocketPair & socketPair);
  
private:

};

/** The base class for the HandlerInterface. This is used during tape cleaning and
 * scheduling. */

class BaseHandlerInterface {
public:
  BaseHandlerInterface(server::SocketPair & socketPair);
  /** Report a beginning of a scheduling round. That implicitly reports the and
   * of the drive cleanup, when applicable. */
  void announceSchedulingRound();
protected:
  /** The socket pair allowing communication with the handler in the parent process */
  server::SocketPair & m_socketPair;  
};

}}}  // namespace cta::tape::session
