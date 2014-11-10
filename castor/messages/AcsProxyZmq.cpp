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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/messages/AcsMountTapeReadOnly.pb.h"
#include "castor/messages/AcsMountTapeReadWrite.pb.h"
#include "castor/messages/AcsDismountTape.pb.h"
#include "castor/messages/ReturnValue.pb.h"
#include "castor/messages/AcsProxyZmq.hpp"
#include "castor/messages/Constants.hpp"
#include "castor/messages/messages.hpp"
#include "castor/messages/MutexLocker.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::AcsProxyZmq::AcsProxyZmq(log::Logger &log, 
  const unsigned short serverPort, void *const zmqContext) throw():
  m_log(log),
  m_serverPort(serverPort),
  m_serverSocket(zmqContext, ZMQ_REQ) {
  connectZmqSocketToLocalhost(m_serverSocket, serverPort);
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void castor::messages::AcsProxyZmq::mountTapeReadOnly(const std::string &vid,
  const mediachanger::AcsLibrarySlot &librarySlot) {
  MutexLocker lock(&m_mutex);
  
  try {
    const Frame rqst = createAcsMountTapeReadOnlyFrame(vid, librarySlot);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to request CASTOR ACS daemon to mount tape for read only access: "
      << librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAcsMountTapeReadnOnlyFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::AcsProxyZmq::
  createAcsMountTapeReadOnlyFrame(const std::string &vid,
  const mediachanger::AcsLibrarySlot &librarySlot) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ACSMOUNTTAPEREADONLY);
    frame.header.set_bodysignature("PIPO");

    AcsMountTapeReadOnly body;
    body.set_vid(vid);
    body.set_acs(librarySlot.getAcs());
    body.set_lsm(librarySlot.getLsm());
    body.set_panel(librarySlot.getPanel());
    body.set_drive(librarySlot.getDrive());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsMountTapeReadOnly frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void castor::messages::AcsProxyZmq::mountTapeReadWrite(const std::string &vid,
  const mediachanger::AcsLibrarySlot &librarySlot) {
  MutexLocker lock(&m_mutex);
  
  try {
    const Frame rqst = createAcsMountTapeReadWriteFrame(vid, librarySlot);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to request CASTOR ACS daemon to mount tape for read/write " 
      "access: " << librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAcsMountTapeReadWriteFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::AcsProxyZmq::
  createAcsMountTapeReadWriteFrame(const std::string &vid, 
  const mediachanger::AcsLibrarySlot &librarySlot) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ACSMOUNTTAPEREADWRITE);
    frame.header.set_bodysignature("PIPO");

    AcsMountTapeReadWrite body;
    body.set_vid(vid);
    body.set_acs(librarySlot.getAcs());
    body.set_lsm(librarySlot.getLsm());
    body.set_panel(librarySlot.getPanel());
    body.set_drive(librarySlot.getDrive());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsMountTapeReadWrite frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::messages::AcsProxyZmq::dismountTape(const std::string &vid,
  const mediachanger::AcsLibrarySlot &librarySlot) {
  MutexLocker lock(&m_mutex);
  
  try {
    const Frame rqst = createAcsDismountTapeFrame(vid, librarySlot);
    sendFrame(m_serverSocket, rqst);

    ReturnValue reply;
    recvTapeReplyOrEx(m_serverSocket, reply);
    if(0 != reply.value()) {
      // Should never get here
      castor::exception::Exception ex;
      ex.getMessage() << "Received an unexpected return value"
        ": expected=0 actual=" << reply.value();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to request CASTOR ACS daemon to dismount tape: " <<
      librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAcsDismountTapeFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::AcsProxyZmq::
  createAcsDismountTapeFrame(const std::string &vid, 
  const mediachanger::AcsLibrarySlot &librarySlot) {
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ACSDISMOUNTTAPE);
    frame.header.set_bodysignature("PIPO");

    AcsDismountTape body;
    body.set_vid(vid);
    body.set_acs(librarySlot.getAcs());
    body.set_lsm(librarySlot.getLsm());
    body.set_panel(librarySlot.getPanel());
    body.set_drive(librarySlot.getDrive());
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsDismountTape frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}
