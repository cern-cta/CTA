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

#include "castor/messages/messages.hpp"
#include "castor/utils/utils.hpp"

//------------------------------------------------------------------------------
// connectToLocalhost
//------------------------------------------------------------------------------
void castor::messages::connectToLocalhost(tape::utils::ZmqSocket& m_socket){
  std::string bindingAdress("tcp://127.0.0.1:");
  bindingAdress+=castor::utils::toString(tape::tapeserver::daemon::TAPE_SERVER_INTERNAL_LISTENING_PORT);
  m_socket.connect(bindingAdress.c_str());
}

//------------------------------------------------------------------------------
// preFillHeader
//------------------------------------------------------------------------------
castor::messages::Header castor::messages::preFillHeader() {
  castor::messages::Header header;
  header.set_magic(TPMAGIC);
  header.set_protocoltype(castor::messages::protocolType::Tape);
  header.set_protocolversion(castor::messages::protocolVersion::prototype);
  header.set_bodyhashtype("SHA1");
  header.set_bodysignaturetype("SHA1");
  return header;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::ReplyContainer::ReplyContainer(tape::utils::ZmqSocket& socket)  {
  tape::utils::ZmqMsg blobHeader;
  socket.recv(&blobHeader.getZmqMsg()); 
  
  if(!zmq_msg_more(&blobHeader.getZmqMsg())) {
    throw castor::exception::Exception("Expecting a multi part message. Got a header without a body");
  }
  socket.recv(&blobBody.getZmqMsg());
  
  if(!header.ParseFromArray(blobHeader.data(),blobHeader.size())){
    throw castor::exception::Exception("Message header cant be parsed from binary data read");
  }
  
  if(header.magic()!=TPMAGIC){
    throw castor::exception::Exception("Wrong magic number in the header");
  }
  if(header.protocoltype()!=messages::protocolType::Tape){
    throw castor::exception::Exception("Wrong protocol type in the header");
  }
  if(header.protocolversion()!=messages::protocolVersion::prototype){
    throw castor::exception::Exception("Wrong protocol version in the header");
  }
  
  if(header.reqtype()==castor::messages::reqType::ReturnValue){
    castor::messages::ReturnValue body;
    if(!body.ParseFromArray(blobBody.data(),blobBody.size())){
        throw castor::exception::Exception("Expecting a ReturnValue body but"
                " cant parse it from the binary");
    }
    
    if(body.returnvalue()!=0){
     throw castor::exception::Exception(body.message());
    }
  }
}
