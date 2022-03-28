/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "castor/messages/Constants.hpp"
#include "castor/messages/Frame.hpp"
#include "castor/messages/messages.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// checkHashValueOfBody
//------------------------------------------------------------------------------
void castor::messages::Frame::checkHashValueOfBody() const {
  const std::string bodyHash = castor::messages::computeSHA1Base64(body);
  if(bodyHash != header.bodyhashvalue()){
    cta::exception::Exception ex;
    ex.getMessage() << "Hash value of frame body does match the value stored"
      " in the header: header.bodyhashvalue=" << header.bodyhashvalue() <<
      " bodyHash=" << bodyHash;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializeProtocolBufferIntoBody
//------------------------------------------------------------------------------
void castor::messages::Frame::serializeProtocolBufferIntoBody(
  const google::protobuf::Message &protocolBuffer) {
  try {
    if(!protocolBuffer.SerializeToString(&body)) {
      cta::exception::Exception ex;
      ex.getMessage() << "SerializeToString() returned false";
      throw ex;
    }

    calcAndSetHashValueOfBody();
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Frame failed to serialize protocol buffer " <<
      demangledNameOf(protocolBuffer) << " into frame body: "
      << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parseBodyIntoProtocolBuffer
//------------------------------------------------------------------------------
void castor::messages::Frame::parseBodyIntoProtocolBuffer(
  google::protobuf::Message &protocolBuffer) const {
  if(!protocolBuffer.ParseFromString(body)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Frame failed to parse contents of enclosed ZMQ"
      " message into protocol buffer " <<
      demangledNameOf(protocolBuffer)
      << ": ParseFromString() returned false";
    throw ex;
  } 
}   

//------------------------------------------------------------------------------
// calcAndSetHashValueOfBody
//------------------------------------------------------------------------------
void castor::messages::Frame::calcAndSetHashValueOfBody() {
  try {
    header.set_bodyhashvalue(messages::computeSHA1Base64(body));
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Frame failed to calculate the hash value of the frame"
      "body and store it in the header: " << ne.getMessage().str();
    throw ex;
  }
}
