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

#include "castor/io/io.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerUtils.hpp"
#include "common/SmartArrayPtr.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

#include <errno.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ForkCleaner &msg) {
  frame.type = messages::MSG_TYPE_FORKCLEANER;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
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
  frame.type = messages::MSG_TYPE_FORKDATATRANSFER;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
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
  frame.type = messages::MSG_TYPE_FORKLABEL;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
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
  frame.type = messages::MSG_TYPE_FORKSUCCEEDED;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ForkSucceeded payload"
      ": SerializeToString() returned false";
    throw ex;
  }   
}  

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ProcessCrashed &msg) {
  frame.type = messages::MSG_TYPE_PROCESSCRASHED;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ProcessCrashed payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ProcessExited &msg) {
  frame.type = messages::MSG_TYPE_PROCESSEXITED;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ProcessExited payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::ReturnValue &msg) {
  frame.type = messages::MSG_TYPE_RETURNVALUE;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to serialize ReturnValue payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::StopProcessForker &msg) {
  frame.type = messages::MSG_TYPE_STOPPROCESSFORKER;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to serialize StopProcessForker payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serializePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::serializePayload(
  ProcessForkerFrame &frame, const messages::Exception &msg) {
  frame.type = messages::MSG_TYPE_EXCEPTION;
  if(!msg.SerializeToString(&frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to serialize Exception payload"
      ": SerializeToString() returned false";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::writeFrame(
  const int fd, const messages::ForkCleaner &msg, cta::log::Logger *const log) {
  writeFrame(fd, messages::MSG_TYPE_FORKCLEANER, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::writeFrame(
  const int fd, const messages::ForkDataTransfer &msg,
  cta::log::Logger *const log) {
  writeFrame(fd, messages::MSG_TYPE_FORKDATATRANSFER, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::writeFrame(
  const int fd, const messages::ForkLabel &msg, cta::log::Logger *const log) {
  writeFrame(fd, messages::MSG_TYPE_FORKLABEL, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const messages::Exception &msg,
  cta::log::Logger *const log) {
  writeFrame(fd, messages::MSG_TYPE_EXCEPTION, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const messages::ProcessCrashed &msg,
  cta::log::Logger *const log) {
  writeFrame(fd, messages::MSG_TYPE_PROCESSCRASHED, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const messages::ProcessExited &msg,
  cta::log::Logger *const log) {
  writeFrame(fd, messages::MSG_TYPE_PROCESSEXITED, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const messages::StopProcessForker &msg,
  cta::log::Logger *const log) {
  writeFrame(fd, messages::MSG_TYPE_STOPPROCESSFORKER, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const messages::MsgType type,
  const google::protobuf::Message &msg, cta::log::Logger *const log) {
  writeFrameHeader(fd, type, msg.ByteSize(), log);
  writeFramePayload(fd, msg, log);
}

//------------------------------------------------------------------------------
// writeFrame
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrame(const int fd, const ProcessForkerFrame &frame,
  cta::log::Logger *const log) {
  try {
    if(0 > fd) {
      cta::exception::Exception ex;
      ex.getMessage() << "Invalid file-descriptor";
      throw ex;
    }

    writeFrameHeader(fd, frame.type, frame.payload.length(), log);
    writeFramePayload(fd, frame.payload, log);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write ProcessForkerFrame: fd=" << fd <<
      " type=" << messages::msgTypeToString(frame.type) << " payloadLen="
      << frame.payload.length() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeFrameHeader
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFrameHeader(const int fd, const messages::MsgType type,
  const uint32_t payloadLen, cta::log::Logger *const log) {
  try {
    if(0 == payloadLen) {
      cta::exception::Exception ex;
      ex.getMessage() << "Payload length must be greater than 0";
      throw ex;
    }

    writeUint32(fd, type);
    writeUint32(fd, payloadLen);

    if(NULL != log) {
      std::list<cta::log::Param> params = {cta::log::Param("type", messages::msgTypeToString(type)),
        cta::log::Param("payloadLen", payloadLen)};
      (*log)(cta::log::DEBUG, "ProcessForkerUtils wrote frame header", params);
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
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
    const std::string message = cta::utils::errnoToString(errno);
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write uint32_t: " << message;
    throw ex;
  }

  if(sizeof(value) != writeRc) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write uint32_t: Incomplete write"
     ": expectedNbBytes=" << sizeof(value) << " actualNbBytes=" << writeRc;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeFramePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeFramePayload(const int fd, const google::protobuf::Message &msg,
  cta::log::Logger *const log) {
  try {
    std::string msgStr;
    if(!msg.SerializeToString(&msgStr)) {
      cta::exception::Exception ex;
      ex.getMessage() << "msg.SerializeToString() returned false";
      throw ex;
    }
    writeString(fd, msgStr, log);

    if(NULL != log) {
      std::list<cta::log::Param> params = {cta::log::Param("payloadLen", msgStr.length())};
      (*log)(cta::log::DEBUG, "ProcessForkerUtils wrote frame payload", params);
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload: " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload: " << ne.what();
    throw ex;
  } catch(...) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload"
      ": Caught an unknown exception";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeFramePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::writeFramePayload(
 const int fd, const std::string &msg, cta::log::Logger *const log) {
  try {
    writeString(fd, msg, log);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload: " <<
      ne.getMessage().str();
    throw ex;
  } catch(std::exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload: " << ne.what();
    throw ex;
  } catch(...) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write frame payload"
      ": Caught an unknown exception";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeString
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::
  writeString(const int fd, const std::string &str, cta::log::Logger *const log) {
  const ssize_t writeRc = write(fd, str.c_str(), str.length());

  if(-1 == writeRc) {
    const std::string message = cta::utils::errnoToString(errno);
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write string: " << message;
    throw ex;
  }

  if((ssize_t)str.length() != writeRc) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to write string: Incomplete write"
     ": expectedNbBytes=" << str.length() << " actualNbBytes=" << writeRc;
    throw ex;
  }

  if(NULL != log) {
    std::list<cta::log::Param> params = {cta::log::Param("length",  str.length())};
    (*log)(cta::log::DEBUG, "ProcessForkerUtils wrote string", params);
  }
}

//------------------------------------------------------------------------------
// readFrame
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerFrame
  castor::tape::tapeserver::daemon::ProcessForkerUtils::readFrame(
  const int fd, const int timeout) {
  try {
    ProcessForkerFrame frame;
    frame.type = readPayloadType(fd, timeout);
    const uint32_t payloadLen = readPayloadLen(fd, timeout);
    frame.payload = readPayload(fd, timeout, payloadLen);
    return frame;
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to read frame: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readPayloadType
//------------------------------------------------------------------------------
castor::messages::MsgType
  castor::tape::tapeserver::daemon::ProcessForkerUtils::readPayloadType(
  const int fd, const int timeout) {
  try {
    return (messages::MsgType)readUint32(fd, timeout);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to read payload type: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readPayloadLen
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::ProcessForkerUtils::readPayloadLen(
  const int fd, const int timeout) {
  try {
    return readUint32(fd, timeout);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to read payload length: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readUint32
//------------------------------------------------------------------------------
uint32_t castor::tape::tapeserver::daemon::ProcessForkerUtils::readUint32(
  const int fd, const int timeout) {
  try {
    uint32_t value = 0;
    io::readBytes(fd, timeout, sizeof(value), (char*)&value);
    return value;
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to read 32-bit unsigned integer: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readPayload
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::ProcessForkerUtils::
  readPayload(const int fd, const int timeout, const ssize_t payloadLen) {
  try {
    if(payloadLen > s_maxPayloadLen) {
      cta::exception::Exception ex;
      ex.getMessage() << "Maximum payload length exceeded: max=" <<
        s_maxPayloadLen << " actual=" << payloadLen;
      throw ex;
    }

    cta::SmartArrayPtr<char> payloadBuf(new char[payloadLen]);
    io::readBytes(fd, timeout, payloadLen, payloadBuf.get());

    return std::string(payloadBuf.get(), payloadLen);

  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to read frame payload: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::Exception &msg) {
  if(messages::MSG_TYPE_EXCEPTION != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse Exception payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }   
    
  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse Exception payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ForkCleaner &msg) {
  if(messages::MSG_TYPE_FORKCLEANER != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkCleaner payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
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
  if(messages::MSG_TYPE_FORKDATATRANSFER != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkDataTransfer payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
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
  if(messages::MSG_TYPE_FORKLABEL != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkLabel payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }   
    
  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
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
  if(messages::MSG_TYPE_FORKSUCCEEDED != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkSucceeded payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }   
    
  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ForkSucceeded payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ProcessCrashed &msg) {
  if(messages::MSG_TYPE_PROCESSCRASHED != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ProcessCrashed payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ProcessCrashed payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ProcessExited &msg) {
  if(messages::MSG_TYPE_PROCESSEXITED != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ProcessExited payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }   
    
  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ProcessExited payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }   
}   

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::ReturnValue &msg) {
  if(messages::MSG_TYPE_RETURNVALUE != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ReturnValue payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse ReturnValue payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// parsePayload
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerUtils::parsePayload(
  const ProcessForkerFrame &frame, messages::StopProcessForker &msg) {
  if(messages::MSG_TYPE_STOPPROCESSFORKER != frame.type) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse StopProcessForker payload"
      ": Unexpected message type: type=" <<
      messages::msgTypeToString(frame.type);
    throw ex;
  }

  if(!msg.ParseFromString(frame.payload)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse StopProcessForker payload"
      ": ParseString() returned false: payloadLen="  << frame.payload.length();
    throw ex;
  }
}
