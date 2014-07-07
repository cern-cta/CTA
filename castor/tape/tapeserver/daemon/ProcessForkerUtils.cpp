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
#include "castor/messages/Status.pb.h"
#include "castor/tape/tapeserver/daemon/ProcessForkerUtils.hpp"
#include "castor/utils/SmartArrayPtr.hpp"
#include "h/serrno.h"

#include <errno.h>

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ForkCleaner &msg) {
  frame.type = ProcessForkerMsgType::MSG_FORKCLEANER;
  if(!msg.SerializeToString(&frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ForkCleaner payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ForkDataTransfer &msg) {
  frame.type = ProcessForkerMsgType::MSG_FORKDATATRANSFER;
  if(!msg.SerializeToString(&frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ForkDataTransfer payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ForkLabel &msg) {
  frame.type = ProcessForkerMsgType::MSG_FORKLABEL;
  if(!msg.SerializeToString(&frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ForkLabel payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ForkSucceeded &msg) {
  frame.type = ProcessForkerMsgType::MSG_FORKSUCCEEDED;
  if(!msg.SerializeToString(&frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ForkSucceeded payload"
      ": SerializeToString() returned false";
    throw ex;
  }   
}  

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::StopProcessForker &msg) {
  frame.type = ProcessForkerMsgType::MSG_STOPPROCESSFORKER;
  if(!msg.SerializeToString(&frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to serialize StopProcessForker payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::Status &msg) {
  frame.type = ProcessForkerMsgType::MSG_STATUS;
  if(!msg.SerializeToString(&frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to serialize Status payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::writeFrame(
  const int fd, const messages::ForkCleaner &msg) {
  writeFrame(fd, ProcessForkerMsgType::MSG_FORKCLEANER, msg);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::writeFrame(
  const int fd, const messages::ForkDataTransfer &msg) {
  writeFrame(fd, ProcessForkerMsgType::MSG_FORKDATATRANSFER, msg);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::writeFrame(
  const int fd, const messages::ForkLabel &msg) {
  writeFrame(fd, ProcessForkerMsgType::MSG_FORKLABEL, msg);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const messages::Status &msg) {
  writeFrame(fd, ProcessForkerMsgType::MSG_STATUS, msg);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const messages::StopProcessForker &msg) {
  writeFrame(fd, ProcessForkerMsgType::MSG_STOPPROCESSFORKER, msg);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const ProcessForkerMsgType::Enum type,
  const google::protobuf::Message &msg) {
  writeFrameHeader(fd, type, msg.ByteSize());
  writeFramePayload(fd, msg);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const ProcessForkerFrame &frame) {
  writeFrameHeader(fd, frame.type, frame.payload.length());
  writeFramePayload(fd, frame.payload);
}

//------------------------------------------------------------------------------
// writeFrameHeader
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrameHeader(const int fd, const ProcessForkerMsgType::Enum type,
  const uint32_t payloadLen) {
  try {
    writeUint32(fd, type);
    writeUint32(fd, payloadLen);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write frame header: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeUint32
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeUint32(const int fd, const uint32_t value) {
  const ssize_t writeRc = write(fd, &value, sizeof(value));

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
// writeFramePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFramePayload(const int fd, const google::protobuf::Message &msg) {
  try {
    std::string msgStr;
    if(!msg.SerializeToString(&msgStr)) {
      castor::exception::Exception ex;
      ex.getMessage() << "msg.SerializeToString() returned false";
      throw ex;
    }
    writeString(fd, msgStr);
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
// writeFramePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFramePayload(const int fd, const std::string &msg) {
  try {
    writeString(fd, msg);
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
// writeString
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeString(const int fd, const std::string &str) {
  const ssize_t writeRc = write(fd, str.c_str(), str.length());

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

//------------------------------------------------------------------------------
// readFrame
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerFrame
  castor::tape::tapeserver::daemon::ProcessForkerUtils::readFrame(
  const int fd) {
  try {
    ProcessForkerFrame frame;
    frame.type = readPayloadType(fd);
    const uint32_t payloadLen = readPayloadLen(fd);
    frame.payload = readPayload(fd, payloadLen);
    return frame;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read frame: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readPayloadType
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerMsgType::Enum
  castor::tape::tapeserver::daemon::ProcessForkerUtils::readPayloadType(
  const int fd) {
  try {
    return (ProcessForkerMsgType::Enum)readUint32(fd);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read payload type: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readPayloadLen
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::ProcessForkerUtils::readPayloadLen(
  const int fd) {
  try {
    return readUint32(fd);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read payload length: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readUint32
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::ProcessForkerUtils::readUint32(
  const int fd) {
  uint32_t value = 0;
  const ssize_t readRc = read(fd, &value, sizeof(value));

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
// readPayload
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::ProcessForkerUtils::
  readPayload(const int fd, const ssize_t payloadLen) {
  if(payloadLen > s_maxPayloadLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read frame payload"
      ": Maximum payload length exceeded: max=" << s_maxPayloadLen <<
      " actual=" << payloadLen;
    throw ex;
  }

  utils::SmartArrayPtr<char> payloadBuf(new char[payloadLen]);
  const ssize_t readRc = read(fd, payloadBuf.get(), payloadLen);
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

//------------------------------------------------------------------------------
// readAck
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::readAck(
  const int fd) {
  const ProcessForkerFrame frame = readFrame(fd);
  messages::Status msg;

  if(!msg.ParseFromString(frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse Status message"
      ": ParseFromString() returned false";
    throw ex;
  }

  // Throw an exception if the acknowledge is negative
  if(0 != msg.status()) {
    castor::exception::Exception ex(msg.status());
    ex.getMessage() << msg.message();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ForkCleaner &msg) {
  if(ProcessForkerMsgType::MSG_FORKCLEANER != frame.type) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkCleaner payload"
      ": Unexpected message type: type=" <<
      ProcessForkerMsgType::toString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkCleaner payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ForkDataTransfer &msg) {
  if(ProcessForkerMsgType::MSG_FORKDATATRANSFER != frame.type) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkDataTransfer payload"
      ": Unexpected message type: type=" <<
      ProcessForkerMsgType::toString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkDataTransfer payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ForkLabel &msg) {
  if(ProcessForkerMsgType::MSG_FORKLABEL != frame.type) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkLabel payload"
      ": Unexpected message type: type=" <<
      ProcessForkerMsgType::toString(frame.type);
    throw ex;
  }   
    
  if(!msg.ParseFromString(frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkLabel payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ForkSucceeded &msg) {
  if(ProcessForkerMsgType::MSG_FORKSUCCEEDED != frame.type) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkSucceeded payload"
      ": Unexpected message type: type=" <<
      ProcessForkerMsgType::toString(frame.type);
    throw ex;
  }   
    
  if(!msg.ParseFromString(frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkSucceeded payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::Status &msg) {
  if(ProcessForkerMsgType::MSG_STATUS != frame.type) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse Status payload"
      ": Unexpected message type: type=" <<
      ProcessForkerMsgType::toString(frame.type);
    throw ex;
  }   
    
  if(!msg.ParseFromString(frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse Status payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::StopProcessForker &msg) {
  if(ProcessForkerMsgType::MSG_STOPPROCESSFORKER != frame.type) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse StopProcessForker payload"
      ": Unexpected message type: type=" <<
      ProcessForkerMsgType::toString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to parse StopProcessForker payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}
