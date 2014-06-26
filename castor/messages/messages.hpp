/******************************************************************************
 *         castor/messages/messages.hpp
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

#include "zmq/ZmqWrapper.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "h/Ctape.h"
#include "castor/exception/Exception.hpp"
#pragma once 

namespace castor {
namespace messages {
    
template <class T> void sendMessage(zmq::Socket& socket,const T& msg,int flag=0) {

  if(!msg.IsInitialized()){
    castor::exception::Exception ex("the protocol buffer message was not correctly set");
    throw ex;
  }

  const int size=msg.ByteSize();
  zmq::Message blob(size);
  msg.SerializeToArray(blob.data(),size);
  socket.send(blob,flag);
}

void connectToLocalhost(zmq::Socket& m_socket);
castor::messages::Header preFillHeader();

} // namespace messages
} // namespace castor
