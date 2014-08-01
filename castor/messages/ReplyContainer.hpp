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

#include "castor/messages/Constants.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/messages.hpp"
#include "castor/messages/ReturnValue.pb.h"
#include "castor/messages/ZmqMsg.hpp"
#include "castor/messages/ZmqSocket.hpp"
#include "castor/exception/Exception.hpp"
#include "h/Ctape.h"

namespace castor {
namespace messages {

/**
 * This struct is used to read an answer (=header+body) 
 * over the socket given in the constructor. It will also performs several checks 
 * and deal with the body if it is a ReturnValue
 *It is templated in order to be able to use it with different 
 * protocolType and  protocolVersion
 */
template <int protocolType, int protocolVersion> 
struct ReplyContainer{
  castor::messages::Header header;
  ZmqMsg blobBody;
  ReplyContainer(ZmqSocket& socket){
  
  ZmqMsg blobHeader;
  socket.recv(&blobHeader.getZmqMsg()); 
  
  if(!zmq_msg_more(&blobHeader.getZmqMsg())) {
    throw castor::exception::Exception("Expecting a multi part message. Got a header without a body");
  }
  socket.recv(&blobBody.getZmqMsg());
  
  if(!header.ParseFromArray(blobHeader.getData(),blobHeader.size())){
    throw castor::exception::Exception("Message header cant be parsed from binary data read");
  }
  //check magic, protocolTypa and protocolVerison
  if(header.magic()!=TPMAGIC){
    throw castor::exception::Exception("Wrong magic number in the header");
  }
  if(header.protocoltype()!=protocolType){
    throw castor::exception::Exception("Wrong protocol type in the header");
  }
  if(header.protocolversion()!=protocolVersion){
    throw castor::exception::Exception("Wrong protocol version in the header");
  }
  
  //check if everything is ok in a answer where we are not expecting any real va;ue
  if(header.msgtype() == MSG_TYPE_RETURNVALUE){
    castor::messages::ReturnValue body;
    // TO BE DONE - SHOUDL DO WITH REPLACMENT FRAME CLASS
    //checkSHA1(header,blobBody);
    
    if(!body.ParseFromArray(blobBody.getData(),blobBody.size())){
        throw castor::exception::Exception("Expecting a ReturnValue body but"
                " cant parse it from the binary");
    }
    
    if(body.returnvalue()!=0){
     throw castor::exception::Exception(body.message());
    }
  }
  }
private :
  ReplyContainer(const ReplyContainer&);
  ReplyContainer& operator=(const ReplyContainer&);
};

typedef ReplyContainer<PROTOCOL_TYPE_TAPE, PROTOCOL_VERSION_1>
  ProtoTapeReplyContainer;

} // namespace messages
} // namespace castor
