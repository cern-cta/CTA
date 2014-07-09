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

#include "castor/tape/tapeserver/daemon/DriveCatalogueTransferSession.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueTransferSession::DriveCatalogueTransferSession(
        const castor::tape::tapeserver::daemon::DriveCatalogueSession::SessionState state,
        const castor::legacymsg::RtcpJobRqstMsgBody vdqmJob):
DriveCatalogueSession(state), m_vdqmJob(vdqmJob) {

}

//------------------------------------------------------------------------------
// setVdqmJob
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueTransferSession::setVdqmJob(const castor::legacymsg::RtcpJobRqstMsgBody vdqmJob){
  m_vdqmJob = vdqmJob;
}

//------------------------------------------------------------------------------
// getVdqmJob
//------------------------------------------------------------------------------
castor::legacymsg::RtcpJobRqstMsgBody castor::tape::tapeserver::daemon::DriveCatalogueTransferSession::getVdqmJob() const{
  return m_vdqmJob;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueTransferSession::setVid(const std::string &vid) {
  m_vid = vid;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueTransferSession::getVid() const {
  return m_vid;
}
