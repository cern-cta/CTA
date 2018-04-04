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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/threading/Mutex.hpp"
#include "ZmqSocketMT.hpp"

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
  cta::threading::MutexLocker lock(cta::threading::Mutex &m_mutex);
  m_socket.close();
}
    
//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::bind (const std::string &endpoint) {
  cta::threading::MutexLocker lock(cta::threading::Mutex &m_mutex);
  m_socket.bind(endpoint);
}
  
//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::connect(const std::string &endpoint) {
  cta::threading::MutexLocker lock(cta::threading::Mutex &m_mutex);
  m_socket.connect(endpoint);
}

//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::send(ZmqMsg &msg, const int flags) {
  cta::threading::MutexLocker lock(cta::threading::Mutex &m_mutex);
  m_socket.send(msg, flags);
}
  
//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::send(zmq_msg_t *const msg,
  const int flags) {
  cta::threading::MutexLocker lock(cta::threading::Mutex &m_mutex);
  m_socket.send(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::recv(ZmqMsg &msg, const int flags) {
  cta::threading::MutexLocker lock(cta::threading::Mutex &m_mutex);
  m_socket.recv(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void cta::mediachanger::ZmqSocketMT::recv(zmq_msg_t *const msg, int flags) {
  cta::threading::MutexLocker lock(cta::threading::Mutex &m_mutex);
  m_socket.recv(msg, flags);
}

//------------------------------------------------------------------------------
// getZmqSocket
//------------------------------------------------------------------------------
void *cta::mediachanger::ZmqSocketMT::getZmqSocket() const throw() {
  return m_socket.getZmqSocket();
}
