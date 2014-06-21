/******************************************************************************
 *         castor/tape/tapeserver/daemon/MountSessionConnectionHandler.cpp
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

#include "castor/exception/BadAlloc.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/tape/tapeserver/daemon/MountSessionConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/common.h"
#include "h/serrno.h"
#include "h/Ctape.h"
#include "h/vdqm_api.h"
#include "h/vmgr_constants.h"

#include <errno.h>
#include <fcntl.h>
#include <memory>
#include <list>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::MountSessionConnectionHandler::MountSessionConnectionHandler(
  const int fd, reactor::ZMQReactor &reactor, log::Logger &log,
  DriveCatalogue &driveCatalogue, const std::string &hostName,
  castor::legacymsg::VdqmProxy & vdqm,
  castor::legacymsg::VmgrProxy & vmgr) throw():
  m_fd(fd),
  m_thisEventHandlerOwnsFd(true),
  m_reactor(reactor),
  m_log(log),
  m_driveCatalogue(driveCatalogue),
  m_hostName(hostName),
  m_vdqm(vdqm),
  m_vmgr(vmgr),
  m_netTimeout(10) { // Timeout in seconds
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  ~MountSessionConnectionHandler() throw() {
  if(m_thisEventHandlerOwnsFd) {
    log::Param params[] = {log::Param("fd", m_fd)};
    m_log(LOG_DEBUG, "Closing mount-session connection", params);
    close(m_fd);
  }
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::MountSessionConnectionHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::fillPollFd(zmq::pollitem_t &fd) throw() {
  fd.fd = m_fd;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::MountSessionConnectionHandler::handleEvent(
  const zmq::pollitem_t &fd)  {
  logMountSessionConnectionEvent(fd);

  checkHandleEventFd(fd.fd);

  std::list<log::Param> params;
  params.push_back(log::Param("fd", m_fd));

  try {
    const legacymsg::MessageHeader header = readMsgHeader();
    handleRequest(header);
  } catch(castor::exception::Exception &ex) {
    params.push_back(log::Param("message", ex.getMessage().str()));
    m_log(LOG_ERR, "Failed to handle IO event on mount-session connection",
      params);
  }

  m_log(LOG_DEBUG, "Asking reactor to remove and delete"
    " MountSessionConnectionHandler", params);
  return true; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// logMountSessionConnectionEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  logMountSessionConnectionEvent(const zmq::pollitem_t &fd) {
  std::list<log::Param> params;
  params.push_back(log::Param("fd", fd.fd));
  params.push_back(log::Param("POLLIN",
    fd.revents & POLLIN ? "true" : "false"));
  params.push_back(log::Param("POLLRDNORM",
    fd.revents & POLLRDNORM ? "true" : "false"));
  params.push_back(log::Param("POLLRDBAND",
    fd.revents & POLLRDBAND ? "true" : "false"));
  params.push_back(log::Param("POLLPRI",
    fd.revents & POLLPRI ? "true" : "false"));
  params.push_back(log::Param("POLLOUT",
    fd.revents & POLLOUT ? "true" : "false"));
  params.push_back(log::Param("POLLWRNORM",
    fd.revents & POLLWRNORM ? "true" : "false"));
  params.push_back(log::Param("POLLWRBAND",
    fd.revents & POLLWRBAND ? "true" : "false"));
  params.push_back(log::Param("POLLERR",
    fd.revents & POLLERR ? "true" : "false"));
  params.push_back(log::Param("POLLHUP",
    fd.revents & POLLHUP ? "true" : "false"));
  params.push_back(log::Param("POLLNVAL",
    fd.revents & POLLNVAL ? "true" : "false"));
  m_log(LOG_DEBUG, "I/O event on mount-session connection", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "MountSessionConnectionHandler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readMsgHeader
//------------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::tape::tapeserver::daemon::
  MountSessionConnectionHandler::readMsgHeader()  {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(m_fd, m_netTimeout, sizeof(buf), buf);

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(TPMAGIC != header.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid admin job message: Invalid magic"
      ": expected=0x" << std::hex << TPMAGIC << " actual=0x" <<
      header.magic;
    throw ex;
  }

  // The length of the message body is checked later, just before it is read in
  // to memory

  return header;
}

//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void
castor::tape::tapeserver::daemon::MountSessionConnectionHandler::handleRequest(
  const legacymsg::MessageHeader &header) {

  switch(header.reqType) {
  case UPDDRIVE:
    handleUpdateRequest(header);
    break;
  case TPLABEL:
    handleLabelRequest(header);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to handle mount-session request"
        ": Unknown request type: reqType=" << header.reqType;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleUpdateRequest
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  handleUpdateRequest(const legacymsg::MessageHeader &header) {
  const char *const task = "handle incoming update drive job";

  try {
    // Read message body
    const uint32_t totalLen = header.lenOrStatus;
    const uint32_t headerLen = 3 * sizeof(uint32_t); // magic, type and length
    const uint32_t bodyLen = totalLen - headerLen;
    const legacymsg::TapeUpdateDriveRqstMsgBody body =
      readTapeUpdateDriveRqstMsgBody(bodyLen);
    logUpdateDriveJobReception(body);
    const std::string unitName(body.drive);
    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(unitName);
    drive.updateVolumeInfo(body);

    const utils::DriveConfig &driveConfig = drive.getConfig();

    switch(body.event) {
    case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_BEFORE_MOUNT_STARTED:
      checkTapeConsistencyWithVMGR(body.vid, body.clientType, body.mode);
      break;
    case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_MOUNTED:
      tellVMGRTapeWasMounted(body.vid, body.mode);
      m_vdqm.tapeMounted(m_hostName, body.drive, driveConfig.dgn, body.vid,
        drive.getSessionPid());
      break;
    case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_UNMOUNT_STARTED:
      break;
    case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_UNMOUNTED:
      break;
    case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_NONE:
      break;
    default:
      castor::exception::Exception ex;
      ex.getMessage() << "Unknown tape event: " << body.event;
      throw ex;
      break;
    }    
    
    // 0 as return code for the tape config command, as in: "all went fine"
    legacymsg::writeTapeReplyMsg(m_netTimeout, m_fd, 0, "");
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Informing mount session of error", params);
    
    // Inform the client there was an error
    try {
      legacymsg::writeTapeReplyMsg(m_netTimeout, m_fd, 1,
        ex.getMessage().str());
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task <<
        ": Failed to inform the client there was an error: " <<
        ne.getMessage().str();
      throw ex;
    }
  }    
}


//------------------------------------------------------------------------------
// logUpdateDriveJobReception
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::logUpdateDriveJobReception(
  const legacymsg::TapeUpdateDriveRqstMsgBody &job) const throw() {
  log::Param params[] = {
    log::Param("drive", job.drive),
    log::Param("vid", job.vid)};
  m_log(LOG_INFO, "Received message from mount session to update the status of a drive", params);
}

//------------------------------------------------------------------------------
// logLabelJobReception
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  logLabelJobReception(const legacymsg::TapeLabelRqstMsgBody &job)
  const throw() {
  log::Param params[] = {
    log::Param("drive", job.drive),
    log::Param("vid", job.vid),
    log::Param("dgn", job.dgn),
    log::Param("uid", job.uid),
    log::Param("gid", job.gid)};
  m_log(LOG_INFO, "Received message to label a tape", params);
}

//------------------------------------------------------------------------------
// handleTapeStatusBeforeMountStarted
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  checkTapeConsistencyWithVMGR(const std::string& vid, const uint32_t type,
  const uint32_t mode) {
  if(mode==castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_READWRITE) {
    legacymsg::VmgrTapeInfoMsgBody tapeInfo;
    m_vmgr.queryTape(vid, tapeInfo);
    // If the client is the tape gateway and the volume is not marked as BUSY
    if(type == castor::legacymsg::TapeUpdateDriveRqstMsgBody::CLIENT_TYPE_GATEWAY && !(tapeInfo.status & TAPE_BUSY)) {
      castor::exception::Exception ex;
      ex.getMessage() << "The tape gateway is the client and the tape to be mounted is not BUSY: vid=" << vid;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleTapeStatusMounted
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  tellVMGRTapeWasMounted(const std::string& vid, const uint32_t mode) {
  switch(mode) {
  case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_READ:
    m_vmgr.tapeMountedForRead(vid);
    break;
  case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_READWRITE:
    m_vmgr.tapeMountedForWrite(vid);
    break;
  case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_DUMP:
    m_vmgr.tapeMountedForRead(vid);
    break;
  case legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_NONE:
    break;
  default:
    castor::exception::Exception ex;
    ex.getMessage() << "Unknown tape mode: " << mode;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleLabelRequest
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::MountSessionConnectionHandler::
  handleLabelRequest(const legacymsg::MessageHeader &header) {
  const char *const task = "handle incoming label job";

  try {
    // Read message body
    const uint32_t totalLen = header.lenOrStatus;
    const uint32_t headerLen = 3 * sizeof(uint32_t); // magic, type and length
    const uint32_t bodyLen = totalLen - headerLen;
    const legacymsg::TapeLabelRqstMsgBody body = readLabelRqstMsgBody(bodyLen);
    logLabelJobReception(body);

    // Try to inform the drive catalogue of the reception of the label job
    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(body.drive);
    drive.receivedLabelJob(body, m_fd);

    // The drive catalogue will now remember and own the client connection,
    // therefore it is now safe and necessary for this connection handler to
    // relinquish ownership of the connection
    m_thisEventHandlerOwnsFd = false;
    {
      log::Param params[] = {log::Param("fd", m_fd)};
      m_log(LOG_DEBUG, "Mount-session handler released label connection",
        params);
    }
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Informing client label-command of error", params);

    // Inform the client there was an error
    try {
      legacymsg::writeTapeReplyMsg(m_netTimeout, m_fd, 1,
        ex.getMessage().str());
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task <<
        ": Failed to inform the client there was an error: " <<
        ne.getMessage().str();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// readTapeUpdateDriveRqstMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::TapeUpdateDriveRqstMsgBody castor::tape::tapeserver::daemon::
  MountSessionConnectionHandler::readTapeUpdateDriveRqstMsgBody(
  const uint32_t bodyLen) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of update message"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << bodyLen;
    throw ex;
  }

  try {
    io::readBytes(m_fd, m_netTimeout, bodyLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of update message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeUpdateDriveRqstMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}

//------------------------------------------------------------------------------
// readLabelRqstMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::TapeLabelRqstMsgBody castor::tape::tapeserver::daemon::
  MountSessionConnectionHandler::readLabelRqstMsgBody(const uint32_t bodyLen) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of label request"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << bodyLen;
    throw ex;
  }

  try {
    io::readBytes(m_fd, m_netTimeout, bodyLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of label request"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeLabelRqstMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}
