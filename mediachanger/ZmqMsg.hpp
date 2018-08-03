/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

#include "common/exception/NotAnOwner.hpp"

#include <stdio.h>
#include <zmq.h>


namespace cta {
namespace mediachanger {

/**
 * C++ wrapper around a ZMQ message.
 */
class ZmqMsg {

public:

  /**
   * Constructor.
   */
  ZmqMsg();

  /**
   * Constructor.
   *
   * @param msgSize The size of the ZMQ message.
   */
  ZmqMsg(const size_t msgSize);

  /**
   * Destructor.
   *
   * Calls zmq_msg_close().
   */
  ~ZmqMsg();

  /**
   * Returns the enclosed ZMQ message.
   *
   * @return The underlying ZMQ message.
   */
  zmq_msg_t &getZmqMsg();
  
  /**
   * Gives read access to the data of the enclosed ZMQ message.
   *
   * @return A pointer to the beginning of the data
   */
  const void* getData() const;

  /**
   * Gives read/write access to the data of the enclosed ZMQ message.
   *
   * @return A pointer to the beginning of the data
   */
  void* getData();
  
  /**
   * Gets the size of the enclosed ZMQ message.
   *
   * @return The size of the ZMQ message.
   */
  size_t size() const;

  /**
   * Returns true if the enclosed ZMQ message is part of a multi-part message
   * and there are more parts to receive.
   */
  bool more() const;

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
  ZmqMsg(const ZmqMsg &obj);

}; // class ZmqMsg

} // namespace mediachanger
} // namespace cta

