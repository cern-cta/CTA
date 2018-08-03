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

#include "mediachanger/messages.hpp"
#include "mediachanger/ZmqMsg.hpp"

#include <errno.h>
#include <unistd.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::ZmqMsg::ZmqMsg() {
  if(zmq_msg_init(&m_zmqMsg)) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_msg_init() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::ZmqMsg::ZmqMsg(const size_t msgSize) {
  if(zmq_msg_init_size(&m_zmqMsg, msgSize)) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_msg_init_size() failed: " <<
      zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
cta::mediachanger::ZmqMsg::~ZmqMsg() {
  zmq_msg_close(&m_zmqMsg);
}

//-----------------------------------------------------------------------------
// getZmqMsg
//-----------------------------------------------------------------------------
zmq_msg_t &cta::mediachanger::ZmqMsg::getZmqMsg() {
  return m_zmqMsg;
}

//-----------------------------------------------------------------------------
// getData
//-----------------------------------------------------------------------------
const void* cta::mediachanger::ZmqMsg::getData() const {
  return zmq_msg_data(const_cast<zmq_msg_t*>(&m_zmqMsg));
}

//-----------------------------------------------------------------------------
// getData
//-----------------------------------------------------------------------------
void* cta::mediachanger::ZmqMsg::getData() {
  return zmq_msg_data(&m_zmqMsg);
}

//-----------------------------------------------------------------------------
// size
//-----------------------------------------------------------------------------
size_t cta::mediachanger::ZmqMsg::size() const {
  return zmq_msg_size(const_cast<zmq_msg_t*>(&m_zmqMsg));
}

//-----------------------------------------------------------------------------
// more
//-----------------------------------------------------------------------------
bool cta::mediachanger::ZmqMsg::more() const {
  return zmq_msg_more(const_cast<zmq_msg_t*>(&m_zmqMsg));
}
