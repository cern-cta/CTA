/******************************************************************************
 *         castor/tape/tapeserver/daemon/AdminAcceptHandler.cpp
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
#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/tapeserver/daemon/AdminAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/VdqmConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/common.h"
#include "h/serrno.h"
#include "h/Ctape.h"
#include "h/vdqm_api.h"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"

#include <errno.h>
#include <memory>
#include <string.h>
#include <list>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminAcceptHandler::AdminAcceptHandler(
  const int fd,
  io::PollReactor &reactor,
  log::Logger &log,
  legacymsg::VdqmProxyFactory &vdqmFactory,
  DriveCatalogue &driveCatalogue,
  const std::string &hostName)
  throw():
    m_fd(fd),
    m_reactor(reactor),
    m_log(log),
    m_vdqmFactory(vdqmFactory),
    m_driveCatalogue(driveCatalogue),
    m_hostName(hostName),
    m_netTimeout(10) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminAcceptHandler::~AdminAcceptHandler()
  throw() {
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::AdminAcceptHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::fillPollFd(
  struct pollfd &fd) throw() {
  fd.fd = m_fd;
  fd.events = POLLRDNORM;
  fd.revents = 0;
}

//-----------------------------------------------------------------------------
// marshalTapeConfigReplyMsg
//-----------------------------------------------------------------------------
size_t castor::tape::tapeserver::daemon::AdminAcceptHandler::marshalTapeRcReplyMsg(char *const dst,
  const size_t dstLen, const int rc)
  throw(castor::exception::Exception) {
  legacymsg::MessageHeader src;
  src.magic = TPMAGIC;
  src.reqType = TAPERC;
  src.lenOrStatus = rc;  
  return legacymsg::marshal(dst, dstLen, src);
}

//------------------------------------------------------------------------------
// writeTapeConfigReplyMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::writeTapeRcReplyMsg(const int fd, const int rc) throw(castor::exception::Exception) {
  char buf[REPBUFSZ];
  const size_t len = marshalTapeRcReplyMsg(buf, sizeof(buf), rc);
  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write job reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeTapeStatReplyMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::writeTapeStatReplyMsg(const int fd) throw(castor::exception::Exception) {
  legacymsg::TapeStatReplyMsgBody body;
  
  const std::list<std::string> unitNames = m_driveCatalogue.getUnitNames();
  if(unitNames.size()>CA_MAXNBDRIVES) {
    castor::exception::Internal ex;
    ex.getMessage() << "Too many drives in drive catalogue: " << unitNames.size() << ". Max allowed: " << CA_MAXNBDRIVES << ".";
    throw ex;
  }
  body.number_of_drives = unitNames.size();
  int i=0;
  for(std::list<std::string>::const_iterator itor = unitNames.begin(); itor!=unitNames.end() and i<CA_MAXNBDRIVES; itor++) {
    fillTapeStatDriveEntry(body.drives[i], *itor);
    i++;
  }
  
  char buf[REPBUFSZ];
  const size_t len = legacymsg::marshal(buf, sizeof(buf), body);
  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write job reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// fillTapeStatDriveEntry
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::fillTapeStatDriveEntry(
  legacymsg::TapeStatDriveEntry &entry, const std::string &unitName)
  throw (castor::exception::Exception) {
  try {
    entry.uid = getuid();
    entry.jid = m_driveCatalogue.getSessionPid(unitName);
    castor::utils::copyString(entry.dgn, m_driveCatalogue.getDgn(unitName).c_str());
    const DriveCatalogue::DriveState driveState = m_driveCatalogue.getState(unitName);
    entry.up = driveStateToStatEntryUp(driveState);
    entry.asn = driveStateToStatEntryAsn(driveState);
    entry.asn_time = m_driveCatalogue.getAssignmentTime(unitName);
    castor::utils::copyString(entry.drive, unitName.c_str());
    entry.mode = WRITE_DISABLE; // TODO: SetVidRequestMsgBody needs to be augmented
    castor::utils::copyString(entry.lblcode, "aul"); // only aul format is used
    entry.tobemounted = 0; // TODO: put 1 if the tape is mounting 0 otherwise
    castor::utils::copyString(entry.vid, m_driveCatalogue.getVid(unitName).c_str());
    castor::utils::copyString(entry.vsn, entry.vid);
    entry.cfseq = 0; // the fseq is ignored by tpstat, so we leave it empty
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to fill TapeStatDriveEntry: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// driveStateToStatEntryUp
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::AdminAcceptHandler::driveStateToStatEntryUp(
  const DriveCatalogue::DriveState state) throw(castor::exception::Exception) {
  switch(state) {
    case DriveCatalogue::DRIVE_STATE_INIT:
    case DriveCatalogue::DRIVE_STATE_DOWN:
    case DriveCatalogue::DRIVE_STATE_WAITDOWN:
      return 0;
      break;
    case DriveCatalogue::DRIVE_STATE_UP:
    case DriveCatalogue::DRIVE_STATE_WAITFORK:
    case DriveCatalogue::DRIVE_STATE_WAITLABEL:
    case DriveCatalogue::DRIVE_STATE_RUNNING:
      return 1;
      break;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Failed to translate drive state to TapeStatDriveEntry.up"
          ": Unknown drive-state: state=" << state;
        throw ex;
      }
  }
}

//------------------------------------------------------------------------------
// driveStateToStatEntryAsn
//------------------------------------------------------------------------------
uint16_t castor::tape::tapeserver::daemon::AdminAcceptHandler::driveStateToStatEntryAsn(
  const DriveCatalogue::DriveState state) throw(castor::exception::Exception) {
  switch(state) {
    case DriveCatalogue::DRIVE_STATE_INIT:
    case DriveCatalogue::DRIVE_STATE_DOWN:
      return 0;
    case DriveCatalogue::DRIVE_STATE_UP:
    case DriveCatalogue::DRIVE_STATE_WAITFORK:
    case DriveCatalogue::DRIVE_STATE_WAITLABEL:
    case DriveCatalogue::DRIVE_STATE_RUNNING:
    case DriveCatalogue::DRIVE_STATE_WAITDOWN:
      return 1;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Failed to translate drive state to TapeStatDriveEntry.asn"
          ": Unknown drive-state: state=" << state;
        throw ex;
      }
  }
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::AdminAcceptHandler::handleEvent(
  const struct pollfd &fd) throw(castor::exception::Exception) {
  checkHandleEventFd(fd.fd);

  // Do nothing if there is no data to read
  //
  // POLLIN is unfortunately not the logical or of POLLRDNORM and POLLRDBAND
  // on SLC 5.  I therefore replaced POLLIN with the logical or.  I also
  // added POLLPRI into the mix to cover all possible types of read event.
  if(0 == (fd.revents & POLLRDNORM) && 0 == (fd.revents & POLLRDBAND) &&
    0 == (fd.revents & POLLPRI)) {
    return false; // Stay registered with the reactor
  }

  // Accept the connection from the admin command
  castor::utils::SmartFd connection;
  try {
    connection.reset(io::acceptConnection(fd.fd, 1));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to accept a connection from the admin command"
      ": " << ne.getMessage().str();
    throw ex;
  }
  
  dispatchJob(connection.get());  
  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::checkHandleEventFd(
  const int fd) throw (castor::exception::Exception) {
  if(m_fd != fd) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to accept connection from the admin command"
      ": Event handler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// logTapeConfigJobReception
//------------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::AdminAcceptHandler::logTapeConfigJobReception(
  const legacymsg::TapeConfigRequestMsgBody &job) const throw() {
  log::Param params[] = {
    log::Param("drive", job.drive),
    log::Param("gid", job.gid),
    log::Param("uid", job.uid),
    log::Param("status", job.status)};
  m_log(LOG_INFO, "Received message from tpconfig command", params);
}

//------------------------------------------------------------------------------
// logTapeStatJobReception
//------------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::AdminAcceptHandler::logTapeStatJobReception(
  const legacymsg::TapeStatRequestMsgBody &job) const throw() {
  log::Param params[] = {
    log::Param("gid", job.gid),
    log::Param("uid", job.uid)};
  m_log(LOG_INFO, "Received message from tpstat command", params);
}

//------------------------------------------------------------------------------
// handleTapeConfigJob
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::handleTapeConfigJob(const legacymsg::TapeConfigRequestMsgBody &body) throw(castor::exception::Exception) {
  const std::string unitName(body.drive);
  const std::string dgn = m_driveCatalogue.getDgn(unitName);

  std::auto_ptr<legacymsg::VdqmProxy> vdqm(m_vdqmFactory.create());

  if(CONF_UP==body.status) {
    vdqm->setDriveUp(m_hostName, unitName, dgn);
    m_driveCatalogue.configureUp(unitName);
    m_log(LOG_INFO, "Drive is up now");
  }
  else if(CONF_DOWN==body.status) {
    vdqm->setDriveDown(m_hostName, unitName, dgn);
    m_driveCatalogue.configureDown(unitName);
    m_log(LOG_INFO, "Drive is down now");
  }
  else {
    castor::exception::Internal ex;
    ex.getMessage() << "Wrong drive status requested:" << body.status;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleTapeStatJob
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::handleTapeStatJob(const legacymsg::TapeStatRequestMsgBody &body) throw(castor::exception::Exception) {
  
}

//------------------------------------------------------------------------------
// dispatchJob
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::dispatchJob(
    const int connection) throw(castor::exception::Exception) {
  
  const legacymsg::MessageHeader header = readJobMsgHeader(connection);
  if(TPCONF == header.reqType) {
    const legacymsg::TapeConfigRequestMsgBody body = readTapeConfigMsgBody(connection, header.lenOrStatus-sizeof(header));
    logTapeConfigJobReception(body);
    handleTapeConfigJob(body);
    writeTapeRcReplyMsg(connection, 0); // 0 as return code for the tape config command, as in: "all went fine"
  }
  else { //TPSTAT
    const legacymsg::TapeStatRequestMsgBody body = readTapeStatMsgBody(connection, header.lenOrStatus-sizeof(header));
    logTapeStatJobReception(body);
    handleTapeStatJob(body);
    writeTapeStatReplyMsg(connection);
    writeTapeRcReplyMsg(connection, 0); // 0 as return code for the tape config command, as in: "all went fine"
  }
}

//------------------------------------------------------------------------------
// readJobMsgHeader
//------------------------------------------------------------------------------
castor::legacymsg::MessageHeader
  castor::tape::tapeserver::daemon::AdminAcceptHandler::readJobMsgHeader(
    const int connection) throw(castor::exception::Exception) {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(connection, m_netTimeout, sizeof(buf), buf);

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(TPMAGIC != header.magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid admin job message: Invalid magic"
      ": expected=0x" << std::hex << TPMAGIC << " actual=0x" <<
      header.magic;
    throw ex;
  }

  if(TPCONF != header.reqType and TPSTAT != header.reqType) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid admin job message: Invalid request type"
       ": expected= 0x" << std::hex << TPCONF << " or 0x" << TPSTAT << " actual=0x" <<
       header.reqType;
    throw ex;
  }

  // The length of the message body is checked later, just before it is read in
  // to memory

  return header;
}

//------------------------------------------------------------------------------
// readTapeConfigMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::TapeConfigRequestMsgBody
  castor::tape::tapeserver::daemon::AdminAcceptHandler::readTapeConfigMsgBody(
    const int connection, const uint32_t len)
    throw(castor::exception::Exception) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < len) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of job message"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << len;
    throw ex;
  }

  try {
    io::readBytes(connection, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of job message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeConfigRequestMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}

//------------------------------------------------------------------------------
// readTapeStatMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::TapeStatRequestMsgBody
  castor::tape::tapeserver::daemon::AdminAcceptHandler::readTapeStatMsgBody(
    const int connection, const uint32_t len)
    throw(castor::exception::Exception) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < len) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of job message"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << len;
    throw ex;
  }

  try {
    io::readBytes(connection, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of job message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeStatRequestMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}
