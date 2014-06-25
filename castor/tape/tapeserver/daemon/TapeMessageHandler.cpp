/******************************************************************************
 *         castor/tape/tapeserver/daemon/AdminConnectionHandler.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/


#include "castor/messages/Header.pb.h"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/TapeMessageHandler.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/NotifyDrive.pb.h"
#include "castor/messages/messages.hpp"
#include "zmq/castorZmqWrapper.hpp"
#include "h/vmgr_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeMessageHandler::TapeMessageHandler(
   reactor::ZMQReactor &reactor,
  log::Logger &log,DriveCatalogue &driveCatalogue,
  const std::string &hostName,
  castor::legacymsg::VdqmProxy & vdqm,
  castor::legacymsg::VmgrProxy & vmgr):
  m_reactor(reactor),
  m_log(log),m_socket(reactor.getContext(),ZMQ_REP),
  m_driveCatalogue(driveCatalogue),
  m_hostName(hostName),
  m_vdqm(vdqm),
  m_vmgr(vmgr){ 
  
  std::string bindingAdress("tcp://127.0.0.1:");
  bindingAdress+=castor::utils::toString(tape::tapeserver::daemon::TAPE_SERVER_INTERNAL_LISTENING_PORT);
  std::vector<log::Param> v;
  v.push_back(log::Param("endpoint",bindingAdress));
  
  try{
    m_socket.bind(bindingAdress.c_str());
    m_log(LOG_INFO,"bind succesful",v);
  }
  catch(const std::exception& e){
    std::vector<log::Param> v;
    v.push_back(log::Param("ex code",e.what()));
    m_log(LOG_INFO,"Failed to bind",v);
  }
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::fillPollFd(
  zmq::pollitem_t &fd) throw() {
  fd.revents = 0;
  fd.socket = m_socket;
  fd.fd= -1;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeMessageHandler::handleEvent(
const zmq::pollitem_t &fd) {
  checkSocket(fd);
  m_log(LOG_INFO,"handling event in TapeMessageHandler");
  messages::Header header; 
  
  try{
    zmq::message_t headerBlob;
    m_socket.recv(&headerBlob);
    
    if(!headerBlob.more()){
      castor::exception::Exception ex;
      ex.getMessage() <<"Read header blob, expecting a multi parts message but nothing to come";
      throw ex;
    }
    
    header = buildHeader(headerBlob);
  }
  catch(const castor::exception::Exception& ex){
    m_log(LOG_ERR,"Error while dealing the message's header");
    return false;
  }
  
  dispatchEvent(header);
  return false; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// checkSocket
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeMessageHandler::checkSocket(
  const zmq::pollitem_t &fd) {
  void* underlyingSocket = m_socket;
  if(fd.socket != underlyingSocket){
    castor::exception::Exception ex;
    ex.getMessage() <<"TapeMessageHandler passed wrong poll item";
    throw ex;
  }
}


castor::messages::Header castor::tape::tapeserver::daemon::TapeMessageHandler::buildHeader(
const zmq::message_t& headerBlob){  
  messages::Header header; 
  const bool headerCorrectlyParsed = header.ParseFromArray(headerBlob.data(),headerBlob.size());
  
  if(!headerCorrectlyParsed){
    castor::exception::Exception ex;
    ex.getMessage() <<"TapeMessageHandler received a bad header while handling event";
    throw ex;
  }
    if(header.magic() != TPMAGIC){
    castor::exception::Exception ex;
    ex.getMessage() <<"Wrong magic number in the header";
    throw ex;
  }
  if(header.protocoltype() != messages::protocolType::Tape){
    castor::exception::Exception ex;
    ex.getMessage() <<"Wrong protocol specified in the header";
    throw ex;
  }
  return header;
}

void castor::tape::tapeserver::daemon::TapeMessageHandler::dispatchEvent(
const messages::Header& header){
  m_log(LOG_INFO,"dispathing  event in TapeMessageHandler");
  zmq::message_t bodyBlob;
  m_socket.recv(&bodyBlob);
  
  switch(header.reqtype()){
    case messages::reqType::Heartbeat:
    {
      castor::messages::Heartbeat body;
      unserialize(body,bodyBlob);
      dealWith(header,body);
    }
      break;
    case messages::reqType::NotifyDriveBeforeMountStarted:
    {
      castor::messages::NotifyDriveBeforeMountStarted body;
      unserialize(body,bodyBlob);
      dealWith(header,body);
    }
      break;
    case messages::reqType::NotifyDriveTapeMounted:
    {
      castor::messages::NotifyDriveTapeMounted body;
      unserialize(body,bodyBlob);
      dealWith(header,body);
    }
      break;
    case messages::reqType::NotifyDriveTapeUnmounted:
      sendEmptyReplyToClient();
      break;
    case messages::reqType::NotifyDriveUnmountStarted:
      sendEmptyReplyToClient();
      break;
    default:
      m_log(LOG_ERR,"default  dispatch in TapeMessageHandler");
      break;
  }
}


void castor::tape::tapeserver::daemon::TapeMessageHandler::dealWith(
const castor::messages::Header& header, const castor::messages::Heartbeat& body){
  
  std::vector<log::Param> param;
  param.push_back(log::Param("bytesMoved",body.bytesmoved()));
  m_log(LOG_INFO,"IT IS ALIVE",param);
  
  zmq::message_t bodyBlob(body.ByteSize());
   body.SerializeToArray(bodyBlob.data(),body.ByteSize());
  try{
    m_socket.send(bodyBlob);
  }
  catch(const std::exception& e){
    m_log(LOG_ERR,"failed to reply to heartbeat",param);
  }
}

void castor::tape::tapeserver::daemon::TapeMessageHandler::dealWith(
const castor::messages::Header& header, 
const castor::messages::NotifyDriveBeforeMountStarted& body){
  m_log(LOG_INFO,"NotifyDriveBeforeMountStarted-dealWith");
  //check castor consistensy
    if(body.mode()==castor::messages::TAPE_MODE_READWRITE) {
    legacymsg::VmgrTapeInfoMsgBody tapeInfo;
    m_vmgr.queryTape(body.vid(), tapeInfo);
    // If the client is the tape gateway and the volume is not marked as BUSY
    if(body.clienttype() == castor::messages::NotifyDriveBeforeMountStarted::CLIENT_TYPE_GATEWAY && !(tapeInfo.status & TAPE_BUSY)) {
      castor::exception::Exception ex;
      ex.getMessage() << "The tape gateway is the client and the tape to be mounted is not BUSY: vid=" << body.vid();
      throw ex;
    }
  }
    sendEmptyReplyToClient();
}

void castor::tape::tapeserver::daemon::TapeMessageHandler::dealWith(
const castor::messages::Header& header, 
const castor::messages::NotifyDriveTapeMounted& body){
  m_log(LOG_INFO,"NotifyDriveTapeMounted-dealWith");
  DriveCatalogueEntry *const drive = m_driveCatalogue.findDrive(body.unitname());
  drive->updateVolumeInfo(body);
  const utils::DriveConfig &driveConfig = drive->getConfig();
    
  const std::string vid = body.vid();
  switch(body.mode()) {
    case castor::messages::TAPE_MODE_READ:
      m_vmgr.tapeMountedForRead(vid);
      break;
    case castor::messages::TAPE_MODE_READWRITE:
      m_vmgr.tapeMountedForWrite(vid);
      break;
    case castor::messages::TAPE_MODE_DUMP:
      m_vmgr.tapeMountedForRead(vid);
      break;
    case castor::messages::TAPE_MODE_NONE:
      break;
    default:
      castor::exception::Exception ex;
      ex.getMessage() << "Unknown tape mode: " << body.mode();
      throw ex;
  }
  m_vdqm.tapeMounted(m_hostName, body.unitname(), driveConfig.dgn, body.vid(),
    drive->getSessionPid());
  
  sendEmptyReplyToClient();
}

void castor::tape::tapeserver::daemon::TapeMessageHandler::sendEmptyReplyToClient(){
    castor::messages::Header header = castor::messages::preFillHeader();
    header.set_reqtype(messages::reqType::NoReturnValue);
    header.set_bodyhashvalue("PIPO");
    header.set_bodysignature("PIPO");
    castor::messages::NoReturnValue body;
    castor::messages::sendMessage(m_socket,header,ZMQ_SNDMORE);
    castor::messages::sendMessage(m_socket,body);
}
