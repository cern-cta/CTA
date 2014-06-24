/******************************************************************************
 *             castor/tape/tapeserver/daemon/DriveCatalogueLabelSession.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/DriveCatalogueLabelSession.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::DriveCatalogueLabelSession(
        const castor::tape::tapeserver::daemon::DriveCatalogueSession::SessionState state,
        const castor::legacymsg::TapeLabelRqstMsgBody labelJob,
        const int labelCmdConnection):
DriveCatalogueSession(state), m_labelJob(labelJob), m_labelCmdConnection(labelCmdConnection) {

}

//------------------------------------------------------------------------------
// setLabelJob
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::setLabelJob(const castor::legacymsg::TapeLabelRqstMsgBody labelJob){
  m_labelJob = labelJob;
}

//------------------------------------------------------------------------------
// getLabelJob
//------------------------------------------------------------------------------
castor::legacymsg::TapeLabelRqstMsgBody castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::getLabelJob() const{
  return m_labelJob;
}
    
//------------------------------------------------------------------------------
// setLabelCmdConnection
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::setLabelCmdConnection(const int labelCmdConnection){
  m_labelCmdConnection = labelCmdConnection;
}

//------------------------------------------------------------------------------
// getLabelCmdConnection
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::getLabelCmdConnection() const{
  return m_labelCmdConnection;
}
