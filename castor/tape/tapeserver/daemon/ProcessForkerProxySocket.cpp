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

#include "castor/messages/ForkCleaner.pb.h"
#include "castor/messages/ForkDataTransfer.pb.h"
#include "castor/messages/ForkLabel.pb.h"
#include "castor/messages/StopProcessForker.pb.h"
#include "castor/tape/tapeserver/daemon/ProcessForkerMsgType.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxySocket.hpp"
#include "h/serrno.h"

#include <errno.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  ProcessForkerProxySocket(log::Logger &log, const int socketFd) throw():
  m_log(log), m_socketFd(socketFd) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  ~ProcessForkerProxySocket() throw() {
  if(-1 == close(m_socketFd)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    log::Param params[] = {log::Param("socketFd", m_socketFd), 
      log::Param("message", message)};
    m_log(LOG_ERR,
      "Failed to close file-descriptor of ProcessForkerProxySocket", params);
  }
}

//------------------------------------------------------------------------------
// stopProcessForker
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  stopProcessForker(const std::string &reason) {
  messages::StopProcessForker msg;
  msg.set_reason(reason);
  writeFrameToSocket(ProcessForkerMsgType::MSG_STOPPROCESSFORKER, msg);
}

//------------------------------------------------------------------------------
// forkDataTransfer
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkDataTransfer(const std::string &unitName) {
  messages::ForkDataTransfer msg;
  msg.set_unitname(unitName);
  writeFrameToSocket(ProcessForkerMsgType::MSG_FORKDATATRANSFER, msg);
}

//------------------------------------------------------------------------------
// forkLabel
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkLabel(const std::string &unitName, const std::string &vid) {
  messages::ForkLabel msg;
  msg.set_unitname(unitName);
  msg.set_vid(vid);
  writeFrameToSocket(ProcessForkerMsgType::MSG_FORKLABEL, msg);
}

//------------------------------------------------------------------------------
// forkCleaner
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkCleaner(const std::string &unitName, const std::string &vid) {
  messages::ForkCleaner msg;
  msg.set_unitname(unitName);
  msg.set_vid(vid);
  writeFrameToSocket(ProcessForkerMsgType::MSG_FORKCLEANER, msg);
}

//------------------------------------------------------------------------------
// writeFrameToSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  writeFrameToSocket(const ProcessForkerMsgType::Enum msgType,
  const google::protobuf::Message &msg) {
  writeFrameHeaderToSocket(msgType, msg.ByteSize());
  writeFramePayloadToSocket(msg);

  log::Param params[] = {
    log::Param("msgType", ProcessForkerMsgType::toString(msgType)),
    log::Param("payloadLen", msg.ByteSize())};
  m_log(LOG_INFO, "Wrote frame to ProcessForker", params);
}

//------------------------------------------------------------------------------
// writeFrameHeaderTOSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  writeFrameHeaderToSocket(const ProcessForkerMsgType::Enum msgType,
  const uint32_t payloadLen) {
  try {
    writeUint32ToSocket(msgType);
    writeUint32ToSocket(payloadLen);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write frame header: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeUint32ToSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  writeUint32ToSocket(const uint32_t value) {
  const ssize_t writeRc = write(m_socketFd, &value, sizeof(value));

  if(-1 == writeRc) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write uint32_t: " << message;
    throw ex;
  }

  if(sizeof(value) != writeRc) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write uint32_t: Incomplete write"
     ": expectedNbBytes=" << sizeof(value) << " actualNbBytes=" << writeRc;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeFramePayloadToSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  writeFramePayloadToSocket(const google::protobuf::Message &msg) {
  try {
    std::string msgStr;
    if(!msg.SerializeToString(&msgStr)) {
      castor::exception::Exception ex;
      ex.getMessage() << "msg.SerializeToString() returned false";
      throw ex;
    }
    writeStringToSocket(msgStr);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload: " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload: " << ne.what();
    throw ex;
  } catch(...) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload"
      ": Caught an unknown exception";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeStringToSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  writeStringToSocket(const std::string &str) {
  const ssize_t writeRc = write(m_socketFd, str.c_str(), str.length());

  if(-1 == writeRc) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write string: " << message;
    throw ex;
  }

  if((ssize_t)str.length() != writeRc) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write string: Incomplete write"
     ": expectedNbBytes=" << str.length() << " actualNbBytes=" << writeRc;
    throw ex;
  }
}
