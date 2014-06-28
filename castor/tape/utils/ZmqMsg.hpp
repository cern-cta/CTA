/******************************************************************************
 *         castor/utils/ZmqMsg.hpp
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
 *
 * 
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/NotAnOwner.hpp"

#include <stdio.h>
#include <zmq.h>


namespace castor {
namespace tape {
namespace utils {

/**
 * C++ wrapper around a ZMQ message.
 */
class ZmqMsg {

public:

  /**
   * Constructor.
   */
  ZmqMsg() throw();

  /**
   * Constructor.
   *
   * @param msgSize The size of the ZMQ message.
   */
  ZmqMsg(const size_t msgSize) throw();

  /**
   * Destructor.
   *
   * Calls zmq_msg_close().
   */
  ~ZmqMsg() throw();

  /**
   * Returns the underlying ZMQ message.
   *
   * @return The underlying ZMQ message.
   */
  zmq_msg_t &getZmqMsg() throw();

private:

  /**
   * The enclosed ZMQ message.
   */ 
  zmq_msg_t m_zmqMsg;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   *
   * Not implemented so that it cannot be called
   */
  ZmqMsg(const ZmqMsg &obj) throw();

}; // class ZmqMsg

} // namespace utils
} // namespace tape
} // namespace castor

