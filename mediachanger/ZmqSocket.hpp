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

#include "ZmqMsg.hpp"

#include <string>
#include <zmq.h>

namespace cta {
namespace mediachanger {

/**
 * Abstract C++ class that defines the interace of a wrapper around a ZMQ
 * socket.
 */
class ZmqSocket {
public:
  /**
   * Constructor.
   */
  ZmqSocket();

  /**
   * Destructor.
   */
  virtual ~ZmqSocket() = 0;
    
  /**
   * Closes the ZMQ socket.
   */
  virtual void close() = 0;
    
  /**
   * Binds the ZMQ socket to the specified endpoint.
   *
   * @param endpoint The endpoint to bind to.
   */
  virtual void bind(const std::string &endpoint) = 0;
    
  /**
   * Connects the socket to the spedicied endpoint.
   *
   * @param endpoint The endpoint to connect to.
   */ 
  virtual void connect(const std::string &endpoint) = 0;

  /**
   * Sends the specified ZMQ message over the socket.
   *
   * @param msg The ZMQ messge to be sent.
   * @param flags See manual page of  zmq_msg_send().
   */
  virtual void send(ZmqMsg &msg, const int flags = 0) = 0;
    
  /**
   * Sends the specified ZMQ message over the socket.
   *
   * @param msg The ZMQ messge to be sent.
   * @param flags See manual page of  zmq_msg_send().
   */
  virtual void send(zmq_msg_t *const msg, const int flags = 0) = 0;
    
  /**
   * Receives a ZMQ mesage from the socket.
   *
   * @param msg Output parameter: The received ZMQ messge.
   * @param flags See manual page of  zmq_msg_send().
   */
  virtual void recv(ZmqMsg &msg, const int flags = 0) = 0;
    
  /**
   * Receives a ZMQ mesage from the socket.
   *
   * @param msg Output parameter: The received ZMQ messge.
   * @param flags See manual page of  zmq_msg_send().
   */
  virtual void recv(zmq_msg_t *const msg, const int flags = 0) = 0;

  /**
   * Returns the ZMQ socket wrappeed by this class.
   *
   * @return The ZMQ socket wrappeed by this class.
   */
  virtual void *getZmqSocket() const = 0;

private:

  /**
   * Copy constructor made private to prevent copies.
   */
  ZmqSocket(const ZmqSocket&);

  /**
   * Assignment operator made private to prevent assignments.
   */
  void operator=(const ZmqSocket &);

}; // class ZmqSocket

} // namespace mediachanger
} // namespace cta
