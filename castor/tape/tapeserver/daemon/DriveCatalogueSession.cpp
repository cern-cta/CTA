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

#include "castor/tape/tapeserver/daemon/DriveCatalogueSession.hpp"

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueSession::~DriveCatalogueSession(){
}
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueSession::
DriveCatalogueSession(const castor::tape::tapeserver::daemon::DriveCatalogueSession::SessionState state):
m_state(state) {

}

//------------------------------------------------------------------------------
// setPid
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueSession::setPid(const pid_t pid) {
  m_pid = pid;
}

//------------------------------------------------------------------------------
// getPid
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::DriveCatalogueSession::getPid() const {
  return m_pid;
}

//------------------------------------------------------------------------------
// setState
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueSession::setState(const castor::tape::tapeserver::daemon::DriveCatalogueSession::SessionState state) {
  m_state = state;
}

//------------------------------------------------------------------------------
// getState
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueSession::SessionState castor::tape::tapeserver::daemon::DriveCatalogueSession::getState() const {
  return m_state;
}

//------------------------------------------------------------------------------
// setEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueSession::setEvent(const castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent event) {
  m_event = event;
}

//------------------------------------------------------------------------------
// getEvent
//------------------------------------------------------------------------------
castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent castor::tape::tapeserver::daemon::DriveCatalogueSession::getEvent() const {
  return m_event;
}

//------------------------------------------------------------------------------
// setAssignmentTime
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueSession::setAssignmentTime(const time_t assignmentTime) {
 m_assignmentTime = assignmentTime;
}

//------------------------------------------------------------------------------
// getAssignmentTime
//------------------------------------------------------------------------------
time_t castor::tape::tapeserver::daemon::DriveCatalogueSession::getAssignmentTime() const {
  return m_assignmentTime;
}
