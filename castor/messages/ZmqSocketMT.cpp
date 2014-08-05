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

#include "castor/exception/Exception.hpp"
#include "castor/messages/ZmqSocketMT.hpp"
#include "h/serrno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::ZmqSocketMT::ZmqSocketMT(void *const zmqContext,
  const int socketType): m_socket(zmqContext, socketType) {
}
  
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::messages::ZmqSocketMT::~ZmqSocketMT() throw() {
  close();
}
  
//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::close() {
 castor::server::MutexLocker lock(&m_mutex);
  m_socket.close();
}
    
//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::bind (const std::string &endpoint) {
 castor::server::MutexLocker lock(&m_mutex);
  m_socket.bind(endpoint);
}
  
//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::connect(const std::string &endpoint) {
 castor::server::MutexLocker lock(&m_mutex);
  m_socket.connect(endpoint);
}

//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::send(ZmqMsg &msg, const int flags) {
 castor::server::MutexLocker lock(&m_mutex);
  m_socket.send(msg, flags);
}
  
//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::send(zmq_msg_t *const msg,
  const int flags) {
 castor::server::MutexLocker lock(&m_mutex);
  m_socket.send(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::recv(ZmqMsg &msg, const int flags) {
 castor::server::MutexLocker lock(&m_mutex);
  m_socket.recv(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::recv(zmq_msg_t *const msg, int flags) {
 castor::server::MutexLocker lock(&m_mutex);
  m_socket.recv(msg, flags);
}

//------------------------------------------------------------------------------
// getZmqSocket
//------------------------------------------------------------------------------
void *castor::messages::ZmqSocketMT::getZmqSocket() const throw() {
  return m_socket.getZmqSocket();
}
