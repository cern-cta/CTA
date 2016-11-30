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

#include "mediachanger/ZmqSocketMT.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::ZmqSocketMT::ZmqSocketMT(void *const zmqContext,
  const int socketType): m_socket(zmqContext, socketType) {
}
  
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::ZmqSocketMT::~ZmqSocketMT() throw() {
  try {
    close();
  } catch(...) {
    // Ignore any exceptions because this is a destructor.
  }  
}
  
//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::close() {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_socket.close();
}
    
//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::bind (const std::string &endpoint) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_socket.bind(endpoint);
}
  
//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::connect(const std::string &endpoint) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_socket.connect(endpoint);
}

//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::send(ZmqMsg &msg, const int flags) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_socket.send(msg, flags);
}
  
//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::send(zmq_msg_t *const msg,
  const int flags) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_socket.send(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::recv(ZmqMsg &msg, const int flags) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_socket.recv(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::recv(zmq_msg_t *const msg, int flags) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_socket.recv(msg, flags);
}

//------------------------------------------------------------------------------
// getZmqSocket
//------------------------------------------------------------------------------
void *cta::mediachanger::ZmqSocketMT::getZmqSocket() const throw() {
  return m_socket.getZmqSocket();
}
