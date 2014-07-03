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
#include "castor/messages/ForkCleaner.pb.h"
#include "castor/messages/ForkDataTransfer.pb.h"
#include "castor/messages/ForkLabel.pb.h"
#include "castor/messages/StopProcessForker.pb.h"
#include "castor/tape/tapeserver/daemon/ProcessForker.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerMsgType.hpp"
#include "castor/utils/SmartArrayPtr.hpp"
#include "h/serrno.h"

#include <errno.h>
#include <memory>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::ProcessForker(log::Logger &log,
  const int socketFd) throw(): m_log(log), m_socketFd(socketFd) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForker::~ProcessForker() throw() {
  if(-1 == close(m_socketFd)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    log::Param params[] = {log::Param("socketFd", m_socketFd),
      log::Param("message", message)};
    m_log(LOG_ERR, "Failed to close file-descriptor of ProcessForkerSocket",
      params);
  }
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForker::execute() {
  while(handleEvents()) {
  }
}

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::handleEvents() {
  return handleMsg();
}

//------------------------------------------------------------------------------
// handleMsg
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::handleMsg() {
  const ProcessForkerMsgType::Enum msgType =
    (ProcessForkerMsgType::Enum)readUint32FromSocket();
  const uint32_t payloadLen = readUint32FromSocket();
  const std::string payload = readPayloadFromSocket(payloadLen);

  log::Param params[] = {
    log::Param("msgType", ProcessForkerMsgType::toString(msgType)),
    log::Param("payloadLen", payloadLen)};
  m_log(LOG_INFO, "ProcessForker handling a ProcessForker message", params);

  return dispatchMsgHandler(msgType, payload);
}

//------------------------------------------------------------------------------
// readUint32
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::ProcessForker::
  readUint32FromSocket() {
  uint32_t value = 0;
  const ssize_t readRc = read(m_socketFd, &value, sizeof(value));

  if(-1 == readRc) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read 32-bit unsigned integer: " << message;
    throw ex;
  }

  if(sizeof(value) != readRc) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read 32-bit unsigned integer"
      ": incomplete read: expectedNbBytes=" << sizeof(value) <<
      " actualNbBytes=" << readRc;
    throw ex;
  }

  return value;
}

//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::dispatchMsgHandler(
  const uint32_t msgType, const std::string &payload) {
  switch(msgType) {
  case ProcessForkerMsgType::MSG_STOPPROCESSFORKER:
    return handleStopProcessForkerMsg(payload);
  case ProcessForkerMsgType::MSG_FORKLABEL:
    return handleForkLabelMsg(payload);
  case ProcessForkerMsgType::MSG_FORKDATATRANSFER:
    return handleForkDataTransferMsg(payload);
  case ProcessForkerMsgType::MSG_FORKCLEANER:
    return handleForkCleanerMsg(payload);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch message handler"
        ": Unknown message type: msgType=" << msgType;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleStopProcessForkerMsg
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::
  handleStopProcessForkerMsg(const std::string &payload) {
  messages::StopProcessForker msg;
  if(!msg.ParseFromString(payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse StopProcessForker message"
      ": ParseFromString() returned false";
    throw ex;
  }
  log::Param params[] = {log::Param("reason", msg.reason())};
  m_log(LOG_INFO, "Gracefully stopping ProcessForker", params);
  return false; // The main event loop should stop
}

//------------------------------------------------------------------------------
// handleForkLabelMsg
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::handleForkLabelMsg(
  const std::string &payload) {
  messages::ForkLabel msg;
  if(!msg.ParseFromString(payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkLabel message"
      ": ParseFromString() returned false";
    throw ex;
  }
  log::Param params[] = {log::Param("unitName", msg.unitname()),
    log::Param("vid", msg.vid())};
  m_log(LOG_INFO, "ProcessForker handling ForkLabel message", params);
  return true; // The event loop should continue
}

//------------------------------------------------------------------------------
// handleDataTransferMsg
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::handleForkDataTransferMsg(
  const std::string &payload) {
  messages::ForkDataTransfer msg;
  if(!msg.ParseFromString(payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkDataTransfer message"
      ": ParseFromString() returned false";
    throw ex;
  }
  log::Param params[] = {log::Param("unitName", msg.unitname())};
  m_log(LOG_INFO, "ProcessForker handling ForkDataTransfer message", params);
  return true; // The event loop should continue
}

//------------------------------------------------------------------------------
// handleCleanerMsg
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForker::handleForkCleanerMsg(
  const std::string &payload) {
  messages::ForkCleaner msg;
  if(!msg.ParseFromString(payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkCleaner message"
      ": ParseFromString() returned false";
    throw ex;
  }
  log::Param params[] = {log::Param("unitName", msg.unitname()),
    log::Param("vid", msg.vid())};
  m_log(LOG_INFO, "ProcessForker handling ForkCleaner message", params);
  return true; // The event loop should continue
}

//------------------------------------------------------------------------------
// readPayloadFromSocket
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::ProcessForker::
  readPayloadFromSocket(const ssize_t payloadLen) {
  if(payloadLen > s_maxPayloadLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read frame payload"
      ": Maximum payload length exceeded: max=" << s_maxPayloadLen <<
      " actual=" << payloadLen;
    throw ex;
  }

  utils::SmartArrayPtr<char> payloadBuf(new char[payloadLen]);
  const ssize_t readRc = read(m_socketFd, payloadBuf.get(), payloadLen);
  if(-1 == readRc) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read frame payload: " << message;
    throw ex;
  }

  if(payloadLen != readRc) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read frame payload"
      ": incomplete read: expectedNbBytes=" << payloadLen <<
      " actualNbBytes=" << readRc;
    throw ex;
  }

  return std::string(payloadBuf.get(), payloadLen);
}
