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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/utils/ZmqMsg.hpp"

#include <errno.h>
#include <unistd.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::ZmqMsg::ZmqMsg() throw() {
  if(zmq_msg_init(&m_zmqMsg)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to construct a ZmqMsg: zmq_msg_init() failed: "
      << message;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::ZmqMsg::ZmqMsg(const size_t msgSize) throw() {
  if(zmq_msg_init_size(&m_zmqMsg, msgSize)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to construct a ZmqMsg"
     ": zmq_msg_init_size() failed: " << message;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::utils::ZmqMsg::~ZmqMsg() throw() {
  zmq_msg_close(&m_zmqMsg);
}

//-----------------------------------------------------------------------------
// getZmqMsg
//-----------------------------------------------------------------------------
zmq_msg_t &castor::tape::utils::ZmqMsg::getZmqMsg() throw() {
  return m_zmqMsg;
}

const void*  castor::tape::utils::ZmqMsg::data () const
{
  return zmq_msg_data (const_cast<zmq_msg_t*>(&m_zmqMsg));
}

size_t  castor::tape::utils::ZmqMsg::size () const
{
  return zmq_msg_size (const_cast<zmq_msg_t*>(&m_zmqMsg));
}

