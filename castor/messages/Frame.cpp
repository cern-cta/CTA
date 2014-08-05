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
#include "castor/messages/Constants.hpp"
#include "castor/messages/Frame.hpp"
#include "castor/messages/messages.hpp"

//------------------------------------------------------------------------------
// checkHashValueOfBody
//------------------------------------------------------------------------------
void castor::messages::Frame::checkHashValueOfBody() const {
  const std::string bodyHash = castor::messages::computeSHA1Base64(body);
  if(bodyHash != header.bodyhashvalue()){
    castor::exception::Exception ex;
    ex.getMessage() << "Hash value of frame body does match the value stored"
      " in the header: header.bodyhashvalue=" << header.bodyhashvalue() <<
      " bodyHash=" << bodyHash;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serialiseHeaderToZmqMsg
//------------------------------------------------------------------------------
void castor::messages::Frame::serializeHeaderToZmqMsg(ZmqMsg &msg) const {
  try {
    if(!header.IsInitialized()) {
      castor::exception::Exception ex;
      ex.getMessage() << "Frame header is not initialized";
      throw ex;
    }

    if(header.ByteSize() != (int)msg.size()) {
      castor::exception::Exception ex;
      ex.getMessage() << "Size of frame header does not match that of ZMQ"
        " message: header.ByteSize()=" << header.ByteSize() << " msg.size()="
        << msg.size();
      throw ex;
    }

    if(!header.SerializeToArray(msg.getData(), header.ByteSize())) {
      castor::exception::Exception ex;
      ex.getMessage() << "header.SerializeToArray() returned false";
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to serialize frame header to ZMQ message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parseZmqMsgIntoHeader
//------------------------------------------------------------------------------
void castor::messages::Frame::parseZmqMsgIntoHeader(const ZmqMsg &msg) {
  if(!header.ParseFromArray(msg.getData(), msg.size())) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ZMQ message into frame header: "
      "header.ParseFromArray() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// calcAndSetHashValueOfBody
//------------------------------------------------------------------------------
void castor::messages::Frame::calcAndSetHashValueOfBody() {
  try {
    header.set_bodyhashvalue(messages::computeSHA1Base64(body));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Frame failed to calculate the hash value of the frame"
      "body and store it in the header: " << ne.getMessage().str();
    throw ex;
  }
}
