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

#include "castor/messages/AcsMountTapeForRecall.pb.h"
#include "castor/messages/AcsProxyZmq.hpp"
#include "castor/messages/Constants.hpp"
#include "castor/messages/messages.hpp"
#include "castor/messages/ReturnValue.pb.h"

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
  try {
    const Frame rqst = createAcsMountTapeForRecallFrame(vid, librarySlot);
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
      "Failed to request CASTOR ACS daemon to mount tape for recall: " <<
      librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAcsMountTapeForRecallFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::AcsProxyZmq::
  createAcsMountTapeForRecallFrame(const std::string &vid,
  const mediachanger::AcsLibrarySlot &librarySlot) {
/*
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ACSMOUNTTAPEFORRECALL);
    frame.header.set_bodysignature("PIPO");

    AcsMountTapeForRecall body;
    body.set_vid(vid);
    body.set_acs(acs);
    body.set_lsm(lsm);
    body.set_panel(panel);
    body.set_drive(drive);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsMountTapeForRecall frame: " <<
      ne.getMessage().str();
    throw ex;
  }
*/

  return Frame();
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void castor::messages::AcsProxyZmq::mountTapeReadWrite(const std::string &vid,
  const mediachanger::AcsLibrarySlot &librarySlot) {
  try {
    const Frame rqst = createAcsMountTapeForMigrationFrame(vid, librarySlot);
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
      "Failed to request CASTOR ACS daemon to mount tape for migration: " <<
      librarySlot.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAcsMountTapeForMigrationFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::AcsProxyZmq::
  createAcsMountTapeForMigrationFrame(const std::string &vid, 
  const mediachanger::AcsLibrarySlot &librarySlot) {
/*
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ACSMOUNTTAPEFORMIGRATION);
    frame.header.set_bodysignature("PIPO");

    AcsMountTapeForRecall body;
    body.set_vid(vid);
    body.set_acs(acs);
    body.set_lsm(lsm);
    body.set_panel(panel);
    body.set_drive(drive);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsMountTapeForMigration frame: " <<
      ne.getMessage().str();
    throw ex;
  }
*/

  return Frame();
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::messages::AcsProxyZmq::dismountTape(const std::string &vid,
  const mediachanger::AcsLibrarySlot &librarySlot) {
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
/*
  try {
    Frame frame;

    frame.header = messages::protoTapePreFillHeader();
    frame.header.set_msgtype(messages::MSG_TYPE_ACSDISMOUNTTAPE);
    frame.header.set_bodysignature("PIPO");

    AcsMountTapeForRecall body;
    body.set_vid(vid);
    body.set_acs(acs);
    body.set_lsm(lsm);
    body.set_panel(panel);
    body.set_drive(drive);
    frame.serializeProtocolBufferIntoBody(body);

    return frame;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create AcsDismountTape frame: " <<
      ne.getMessage().str();
    throw ex;
  }
*/

  return Frame();
}
