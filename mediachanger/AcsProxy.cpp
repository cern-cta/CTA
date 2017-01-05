/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "mediachanger/AcsDismountTape.pb.h"
#include "mediachanger/AcsForceDismountTape.pb.h"
#include "mediachanger/AcsLibrarySlot.hpp"
#include "mediachanger/AcsMountTapeReadOnly.pb.h"
#include "mediachanger/AcsMountTapeReadWrite.pb.h"
#include "mediachanger/AcsProxy.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/MediaChangerReturnValue.pb.h"
#include "mediachanger/messages.hpp"

namespace cta {
namespace mediachanger {

namespace {

/**
 * Creates a frame containing an AcsMountTapeReadOnly message.
 *
 * @param vid The tape to be mounted.
 * @param librarySlot The slot in the library that contains the tape drive.
 * @return The frame.
 */
Frame createAcsMountTapeReadOnlyFrame(const std::string &vid, const AcsLibrarySlot &librarySlot) {
  try {
    Frame frame;

    frame.header = protoTapePreFillHeader();
    frame.header.set_msgtype(MSG_TYPE_ACSMOUNTTAPEREADONLY);
    frame.header.set_bodysignature("PIPO");

    AcsMountTapeReadOnly body;
    body.set_vid(vid);
    body.set_acs(librarySlot.getAcs());
    body.set_lsm(librarySlot.getLsm());
    body.set_panel(librarySlot.getPanel());
    body.set_drive(librarySlot.getDrive());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsMountTapeReadOnly frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

/**
 * Creates a frame containing a AcsMountTapeReadWrite message.
 *
 * @param vid The tape to be mounted.
 * @param librarySlot The slot in the library that contains the tape drive.
 * @return The frame.
 */
Frame createAcsMountTapeReadWriteFrame(const std::string &vid, const AcsLibrarySlot &librarySlot) {
  try {
    Frame frame;

    frame.header = protoTapePreFillHeader();
    frame.header.set_msgtype(MSG_TYPE_ACSMOUNTTAPEREADWRITE);
    frame.header.set_bodysignature("PIPO");

    AcsMountTapeReadWrite body;
    body.set_vid(vid);
    body.set_acs(librarySlot.getAcs());
    body.set_lsm(librarySlot.getLsm());
    body.set_panel(librarySlot.getPanel());
    body.set_drive(librarySlot.getDrive());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsMountTapeReadWrite frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

/**
 * Creates a frame containing an AcsDismountTape message.
 *
 * @param vid The tape to be dismounted.
 * @param librarySlot The slot in the library that contains the tape drive.
 * @return The frame.
 */
Frame createAcsDismountTapeFrame(const std::string &vid, const AcsLibrarySlot &librarySlot) {
  try {
    Frame frame;

    frame.header = protoTapePreFillHeader();
    frame.header.set_msgtype(MSG_TYPE_ACSDISMOUNTTAPE);
    frame.header.set_bodysignature("PIPO");

    AcsDismountTape body;
    body.set_vid(vid);
    body.set_acs(librarySlot.getAcs());
    body.set_lsm(librarySlot.getLsm());
    body.set_panel(librarySlot.getPanel());
    body.set_drive(librarySlot.getDrive());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsDismountTape frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

/**
 * Creates a frame containing an AcsForceDismountTape message.
 *
 * @param vid The tape to be dismounted.
 * @param librarySlot The slot in the library that contains the tape drive.
 * @return The frame.
 */
Frame createAcsForceDismountTapeFrame(const std::string &vid, const AcsLibrarySlot &librarySlot) {
  try {
    Frame frame;
  
    frame.header = protoTapePreFillHeader();
    frame.header.set_msgtype(MSG_TYPE_ACSFORCEDISMOUNTTAPE);
    frame.header.set_bodysignature("PIPO");

    AcsForceDismountTape body;
    body.set_vid(vid);
    body.set_acs(librarySlot.getAcs());
    body.set_lsm(librarySlot.getLsm());
    body.set_panel(librarySlot.getPanel());
    body.set_drive(librarySlot.getDrive());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsForceDismountTape frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

} // anonyous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
AcsProxy::AcsProxy(void *const zmqContext, const unsigned short serverPort) throw():
  m_zmqContext(zmqContext),
  m_serverPort(serverPort) {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void AcsProxy::mountTapeReadOnly(const std::string &vid, const LibrarySlot &librarySlot) {
  std::lock_guard<std::mutex> lock(m_mutex);
  
  try {
    const Frame rqst = createAcsMountTapeReadOnlyFrame(vid, dynamic_cast<const AcsLibrarySlot&>(librarySlot));
    sendFrame(serverSocketInstance(), rqst);

    MediaChangerReturnValue reply;
    recvTapeReplyOrEx(serverSocketInstance(), reply);
    if(0 != reply.value()) {
      // Should never get here
      cta::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to request CASTOR ACS daemon to mount tape for read only access: "
      << librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void AcsProxy::mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) {
  std::lock_guard<std::mutex> lock(m_mutex);
  
  try {
    const Frame rqst = createAcsMountTapeReadWriteFrame(vid, dynamic_cast<const AcsLibrarySlot&>(librarySlot));
    sendFrame(serverSocketInstance(), rqst);

    MediaChangerReturnValue reply;
    recvTapeReplyOrEx(serverSocketInstance(), reply);
    if(0 != reply.value()) {
      // Should never get here
      cta::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to request CASTOR ACS daemon to mount tape for read/write " 
      "access: " << librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void AcsProxy::dismountTape(const std::string &vid, const LibrarySlot &librarySlot) {
  std::lock_guard<std::mutex> lock(m_mutex);
  
  try {
    const Frame rqst = createAcsDismountTapeFrame(vid, dynamic_cast<const AcsLibrarySlot&>(librarySlot));
    sendFrame(serverSocketInstance(), rqst);

    MediaChangerReturnValue reply;
    recvTapeReplyOrEx(serverSocketInstance(), reply);
    if(0 != reply.value()) {
      // Should never get here
      cta::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to request CASTOR ACS daemon to dismount tape: " <<
      librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forceDismountTape
//------------------------------------------------------------------------------
void AcsProxy::forceDismountTape(const std::string &vid, const LibrarySlot &librarySlot) {
  std::lock_guard<std::mutex> lock(m_mutex);
  
  try {
    const Frame rqst = createAcsForceDismountTapeFrame(vid, dynamic_cast<const AcsLibrarySlot&>(librarySlot));
    sendFrame(serverSocketInstance(), rqst);

    MediaChangerReturnValue reply;
    recvTapeReplyOrEx(serverSocketInstance(), reply);
    if(0 != reply.value()) {
      // Should never get here
      cta::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to request CASTOR ACS daemon to force dismount tape: " <<
      librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// serverSocketInstance
//------------------------------------------------------------------------------
ZmqSocket &AcsProxy::serverSocketInstance() {
  if(nullptr == m_serverSocket) {
    m_serverSocket.reset(new ZmqSocket(m_zmqContext, ZMQ_REQ));
    connectZmqSocketToLocalhost(*m_serverSocket, m_serverPort);
  }
  return *m_serverSocket;
}

} // namespace mediachanger
} // namespace cta
