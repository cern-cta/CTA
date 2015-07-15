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

#include "castor/acs/AcsRequest.hpp"
#include "castor/messages/messages.hpp"
#include "castor/messages/ReturnValue.pb.h"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::acs::AcsRequest::AcsRequest(messages::ZmqSocketST &socket,
  messages::ZmqMsg &address,  messages::ZmqMsg &empty,
  const SEQ_NO seqNo, const std::string vid, const uint32_t acs,
  const uint32_t lsm, const uint32_t panel, const uint32_t drive):
  m_seqNo(seqNo), 
  m_vid(vid), 
  m_acs(acs), 
  m_lsm(lsm),
  m_panel(panel),
  m_drive(drive), 
  m_socket(socket),
  m_identity(utils::hexDump(address.getData(),address.size())),
  m_isReplaySent(false) {
    zmq_msg_init_size (&m_addressMsg, address.size());
    memcpy (zmq_msg_data (&m_addressMsg), (const void*)&address.getZmqMsg(), 
      address.size());

    zmq_msg_init_size (&m_emptyMsg, empty.size());
    memcpy (zmq_msg_data (&m_emptyMsg), (const void*)&empty.getZmqMsg(),
      empty.size());
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::acs::AcsRequest::~AcsRequest() {
    zmq_msg_close(&m_addressMsg);
    zmq_msg_close(&m_emptyMsg);  
}

//-----------------------------------------------------------------------------
// isToExecute
//-----------------------------------------------------------------------------
bool castor::acs::AcsRequest::isToExecute() const throw () {
  if (ACS_REQUEST_TO_EXECUTE == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isRunning
//-----------------------------------------------------------------------------
bool castor::acs::AcsRequest::isRunning() const  throw () {
  if (ACS_REQUEST_IS_RUNNING == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isCompleted
//-----------------------------------------------------------------------------
bool castor::acs::AcsRequest::isCompleted() const throw () {
  if (ACS_REQUEST_COMPLETED == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isFailed
//-----------------------------------------------------------------------------
bool castor::acs::AcsRequest::isFailed() const  throw () {
  if (ACS_REQUEST_FAILED == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isToDelete
//-----------------------------------------------------------------------------
bool castor::acs::AcsRequest::isToDelete() const  throw () {
  if (ACS_REQUEST_TO_DELETE == m_state) {
    return true;
  } else {
    return false;
  };
}

//------------------------------------------------------------------------------
// createReturnValueFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsRequest::createReturnValueFrame(
  const int value) {
  castor::messages::Frame frame;

  frame.header = castor::messages::protoTapePreFillHeader();
  frame.header.set_msgtype(castor::messages::MSG_TYPE_RETURNVALUE);
  frame.header.set_bodyhashvalue(
    castor::messages::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  castor::messages::ReturnValue body;
  body.set_value(value);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}

//------------------------------------------------------------------------------
// createExceptionFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::acs::AcsRequest::
  createExceptionFrame(const int code, const std::string& msg) {
  messages::Frame frame;

  frame.header = castor::messages::protoTapePreFillHeader();
  frame.header.set_msgtype(messages::MSG_TYPE_EXCEPTION);
  frame.header.set_bodyhashvalue(messages::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  messages::Exception body;
  body.set_code(code);
  body.set_message(msg);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}


//-----------------------------------------------------------------------------
// sendReplayToClientOnce
//-----------------------------------------------------------------------------
void castor::acs::AcsRequest::sendReplayToClientOnce() {
  if(m_isReplaySent) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send second replay to the client";
    throw ex;
  }
  m_socket.send(&m_addressMsg,ZMQ_SNDMORE);
  m_socket.send(&m_emptyMsg,ZMQ_SNDMORE);

  messages::sendFrame(m_socket, m_reply);
}

//-----------------------------------------------------------------------------
// getIdentity
//-----------------------------------------------------------------------------
std::string castor::acs::AcsRequest::getIdentity() const throw() {
  return m_identity;
}

//-----------------------------------------------------------------------------
// str
//-----------------------------------------------------------------------------
std::string castor::acs::AcsRequest::str() const {
  std::ostringstream oss;
  oss << "vid=" << m_vid << " acs=" << m_acs << " lsm=" << m_lsm << " panel=" <<
    m_panel << " drive=" << m_drive << " identity=" << getIdentity();
  return oss.str();
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &castor::acs::AcsRequest::getVid() const throw () {
  return m_vid;
}

//------------------------------------------------------------------------------
// getAcs
//------------------------------------------------------------------------------
uint32_t castor::acs::AcsRequest::getAcs() const throw () {
  return m_acs;
}

//------------------------------------------------------------------------------
// getLsm
//------------------------------------------------------------------------------
uint32_t castor::acs::AcsRequest::getLsm() const throw () {
  return m_lsm;
}

//------------------------------------------------------------------------------
// getPanel
//------------------------------------------------------------------------------
uint32_t castor::acs::AcsRequest::getPanel() const throw () {
  return m_panel;
}

//------------------------------------------------------------------------------
// getDrive
//------------------------------------------------------------------------------
uint32_t castor::acs::AcsRequest::getDrive() const throw () {
  return m_drive;
}


//------------------------------------------------------------------------------
// getSeqNo
//------------------------------------------------------------------------------
SEQ_NO castor::acs::AcsRequest::getSeqNo() const throw () {
  return m_seqNo; 
}

//------------------------------------------------------------------------------
// setResponse
//------------------------------------------------------------------------------
void castor::acs::AcsRequest::setResponse(
  const ACS_RESPONSE_TYPE responseType, 
  const ALIGNED_BYTES *const responseMsg) throw () {
  m_responseType = responseType;
  memcpy(m_responseMsg,responseMsg,MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES));
}

//------------------------------------------------------------------------------
// setStateFailed
//------------------------------------------------------------------------------
void castor::acs::AcsRequest::setStateFailed(const int code,
  const std::string& msg) {
  m_reply = createExceptionFrame(code, msg);
  m_state = ACS_REQUEST_FAILED;
}

//------------------------------------------------------------------------------
// setStateCompleted
//------------------------------------------------------------------------------
void castor::acs::AcsRequest::setStateCompleted() {
  m_reply = createReturnValueFrame(0);
  m_state = ACS_REQUEST_COMPLETED;       
}

//------------------------------------------------------------------------------
// setStateIsRunning
//------------------------------------------------------------------------------
void castor::acs::AcsRequest::setStateIsRunning() {
  if (isToExecute()) {
    m_state = ACS_REQUEST_IS_RUNNING;
  } else {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set request state to running from state="<<
      m_state;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setStateToExecute
//------------------------------------------------------------------------------
void castor::acs::AcsRequest::setStateToExecute() throw() {
  m_state = ACS_REQUEST_TO_EXECUTE;       
}

//------------------------------------------------------------------------------
// setStateToDelete
//------------------------------------------------------------------------------
void castor::acs::AcsRequest::setStateToDelete() throw() {
  m_state = ACS_REQUEST_TO_DELETE;       
}
