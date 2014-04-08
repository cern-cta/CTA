/******************************************************************************
 *         castor/tape/tapeserver/daemon/DummyVdqm.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/DummyVdqm.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DummyVdqm::DummyVdqm(const legacymsg::RtcpJobRqstMsgBody &job) throw(): m_job(job) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DummyVdqm::~DummyVdqm() throw() {
}

//------------------------------------------------------------------------------
// receiveJob
//------------------------------------------------------------------------------
castor::tape::legacymsg::RtcpJobRqstMsgBody castor::tape::tapeserver::daemon::DummyVdqm::receiveJob(const int connection) throw(castor::exception::Exception) {
  return m_job;
}

//------------------------------------------------------------------------------
// setDriveStatusDown
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DummyVdqm::setDriveStatusDown(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// setDriveStatusUp
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DummyVdqm::setDriveStatusUp(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// assignDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DummyVdqm::assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t sessionPid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// tapeMounted
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DummyVdqm::tapeMounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid, const pid_t sessionPid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// releaseDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DummyVdqm::releaseDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const bool forceUnmount, const pid_t sessionPid) throw(castor::exception::Exception) {
}
