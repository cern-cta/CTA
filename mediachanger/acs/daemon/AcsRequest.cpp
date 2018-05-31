/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsRequest.hpp"
#include "mediachanger/messages.hpp"
#include "mediachanger/ReturnValue.pb.h"
#include "mediachanger/Exception.pb.h"
#include "common/utils/utils.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsRequest::AcsRequest(cta::mediachanger::ZmqSocketST &socket,
  cta::mediachanger::ZmqMsg &address,  cta::mediachanger::ZmqMsg &empty,
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
cta::mediachanger::acs::daemon::AcsRequest::~AcsRequest() {
    zmq_msg_close(&m_addressMsg);
    zmq_msg_close(&m_emptyMsg);  
}

//-----------------------------------------------------------------------------
// isToExecute
//-----------------------------------------------------------------------------
bool cta::mediachanger::acs::daemon::AcsRequest::isToExecute() const throw () {
  if (ACS_REQUEST_TO_EXECUTE == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isRunning
//-----------------------------------------------------------------------------
bool cta::mediachanger::acs::daemon::AcsRequest::isRunning() const  throw () {
  if (ACS_REQUEST_IS_RUNNING == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isCompleted
//-----------------------------------------------------------------------------
bool cta::mediachanger::acs::daemon::AcsRequest::isCompleted() const throw () {
  if (ACS_REQUEST_COMPLETED == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isFailed
//-----------------------------------------------------------------------------
bool cta::mediachanger::acs::daemon::AcsRequest::isFailed() const  throw () {
  if (ACS_REQUEST_FAILED == m_state) {
    return true;
  } else {
    return false;
  };
}

//-----------------------------------------------------------------------------
// isToDelete
//-----------------------------------------------------------------------------
bool cta::mediachanger::acs::daemon::AcsRequest::isToDelete() const  throw () {
  if (ACS_REQUEST_TO_DELETE == m_state) {
    return true;
  } else {
    return false;
  };
}

//------------------------------------------------------------------------------
// createReturnValueFrame
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsRequest::createReturnValueFrame(
  const int value) {
  cta::mediachanger::Frame frame;

  frame.header = cta::mediachanger::protoTapePreFillHeader();
  frame.header.set_msgtype(cta::mediachanger::MSG_TYPE_RETURNVALUE);
  frame.header.set_bodyhashvalue(
    cta::mediachanger::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  cta::mediachanger::ReturnValue body;
  body.set_value(value);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}

//------------------------------------------------------------------------------
// createExceptionFrame
//------------------------------------------------------------------------------
cta::mediachanger::Frame cta::mediachanger::acs::daemon::AcsRequest::
  createExceptionFrame(const int code, const std::string& msg) {
  cta::mediachanger::Frame frame;

  frame.header = cta::mediachanger::protoTapePreFillHeader();
  frame.header.set_msgtype(mediachanger::MSG_TYPE_EXCEPTION);
  frame.header.set_bodyhashvalue(mediachanger::computeSHA1Base64(frame.body));
  frame.header.set_bodysignature("PIPO");

  cta::mediachanger::Exception body;
  body.set_code(code);
  body.set_message(msg);
  frame.serializeProtocolBufferIntoBody(body);

  return frame;
}


//-----------------------------------------------------------------------------
// sendReplayToClientOnce
//-----------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequest::sendReplayToClientOnce() {
  if(m_isReplaySent) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to send second replay to the client";
    throw ex;
  }
  m_socket.send(&m_addressMsg,ZMQ_SNDMORE);
  m_socket.send(&m_emptyMsg,ZMQ_SNDMORE);

  //messages::sendFrame(m_socket, m_reply);
  sendFrame(m_socket, m_reply);
}

//-----------------------------------------------------------------------------
// getIdentity
//-----------------------------------------------------------------------------
std::string cta::mediachanger::acs::daemon::AcsRequest::getIdentity() const throw() {
  return m_identity;
}

//-----------------------------------------------------------------------------
// str
//-----------------------------------------------------------------------------
std::string cta::mediachanger::acs::daemon::AcsRequest::str() const {
  std::ostringstream oss;
  oss << "vid=" << m_vid << " acs=" << m_acs << " lsm=" << m_lsm << " panel=" <<
    m_panel << " drive=" << m_drive << " identity=" << getIdentity();
  return oss.str();
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::mediachanger::acs::daemon::AcsRequest::getVid() const throw () {
  return m_vid;
}

//------------------------------------------------------------------------------
// getAcs
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::acs::daemon::AcsRequest::getAcs() const throw () {
  return m_acs;
}

//------------------------------------------------------------------------------
// getLsm
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::acs::daemon::AcsRequest::getLsm() const throw () {
  return m_lsm;
}

//------------------------------------------------------------------------------
// getPanel
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::acs::daemon::AcsRequest::getPanel() const throw () {
  return m_panel;
}

//------------------------------------------------------------------------------
// getDrive
//------------------------------------------------------------------------------
uint32_t cta::mediachanger::acs::daemon::AcsRequest::getDrive() const throw () {
  return m_drive;
}


//------------------------------------------------------------------------------
// getSeqNo
//------------------------------------------------------------------------------
SEQ_NO cta::mediachanger::acs::daemon::AcsRequest::getSeqNo() const throw () {
  return m_seqNo; 
}

//------------------------------------------------------------------------------
// setResponse
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequest::setResponse(
  const ACS_RESPONSE_TYPE responseType, 
  const ALIGNED_BYTES *const responseMsg) throw () {
  m_responseType = responseType;
  memcpy(m_responseMsg,responseMsg,MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES));
}

//------------------------------------------------------------------------------
// setStateFailed
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequest::setStateFailed(const int code,
  const std::string& msg) {
  m_reply = createExceptionFrame(code, msg);
  m_state = ACS_REQUEST_FAILED;
}

//------------------------------------------------------------------------------
// setStateCompleted
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequest::setStateCompleted() {
  m_reply = createReturnValueFrame(0);
  m_state = ACS_REQUEST_COMPLETED;       
}

//------------------------------------------------------------------------------
// setStateIsRunning
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequest::setStateIsRunning() {
  if (isToExecute()) {
    m_state = ACS_REQUEST_IS_RUNNING;
  } else {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to set request state to running from state="<<
      m_state;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setStateToExecute
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequest::setStateToExecute() throw() {
  m_state = ACS_REQUEST_TO_EXECUTE;       
}

//------------------------------------------------------------------------------
// setStateToDelete
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsRequest::setStateToDelete() throw() {
  m_state = ACS_REQUEST_TO_DELETE;       
}
