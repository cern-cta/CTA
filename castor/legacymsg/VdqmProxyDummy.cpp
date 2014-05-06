/******************************************************************************
 *         castor/legacymsg/VdqmProxyDummy.cpp
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

#include "castor/legacymsg/VdqmProxyDummy.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::VdqmProxyDummy::VdqmProxyDummy(const RtcpJobRqstMsgBody &job) throw(): m_job(job) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::VdqmProxyDummy::~VdqmProxyDummy() throw() {
}

//------------------------------------------------------------------------------
// receiveJob
//------------------------------------------------------------------------------
castor::legacymsg::RtcpJobRqstMsgBody castor::legacymsg::VdqmProxyDummy::receiveJob(const int connection) throw(castor::exception::Exception) {
  return m_job;
}

//------------------------------------------------------------------------------
// setDriveDown
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyDummy::setDriveDown(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// setDriveUp
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyDummy::setDriveUp(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// assignDrive
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyDummy::assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t sessionPid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// tapeMounted
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyDummy::tapeMounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid, const pid_t sessionPid) throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// releaseDrive
//------------------------------------------------------------------------------
void castor::legacymsg::VdqmProxyDummy::releaseDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const bool forceUnmount, const pid_t sessionPid) throw(castor::exception::Exception) {
}

//-----------------------------------------------------------------------------
// tapeUnmounted
//-----------------------------------------------------------------------------
void  castor::legacymsg::VdqmProxyDummy::tapeUnmounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid) throw(castor::exception::Exception) {
}
