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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueSession.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  /**
   * Derived class containing info about the label session attached to a drive catalogue entry
   */
  class DriveCatalogueLabelSession : public DriveCatalogueSession {
  public:
    
    /**
     * Constructor
     * 
     * @param state state of the drive catalogue session
     * @param labelJob label job received from the castor-tape-label command-line tool
     * @param labelCmdConnection file descriptor of the TCP/IP connection with the tape labeling command-line tool castor-tape-label
     */
    DriveCatalogueLabelSession(
            const SessionState state,
            const castor::legacymsg::TapeLabelRqstMsgBody labelJob, 
            const int labelCmdConnection);
    
    /**
     * labelJob setter method
     * 
     * @param labelJob label job received from the castor-tape-label command-line tool
     */
    void setLabelJob(const castor::legacymsg::TapeLabelRqstMsgBody labelJob);

    /**
     * labelJob getter method
     * 
     * @return label job received from the castor-tape-label command-line tool
     */
    castor::legacymsg::TapeLabelRqstMsgBody getLabelJob() const;
    
    /**
     * labelCmdConnection setter method
     * 
     * @param labelCmdConnection user ID of the process of the session
     */
    void setLabelCmdConnection(const int labelCmdConnection);

    /**
     * labelCmdConnection getter method
     * 
     * @return user ID of the process of the session
     */
    int getLabelCmdConnection() const;
    
  private:
    
    /**
     * The label job received from the castor-tape-label command-line tool.
     */
    castor::legacymsg::TapeLabelRqstMsgBody m_labelJob;
    
    /**
     * If the drive state is either DRIVE_WAITLABEL, DRIVE_STATE_RUNNING or
     * DRIVE_STATE_WAITDOWN and the type of the session is SESSION_TYPE_LABEL
     * then this is the file descriptor of the TCP/IP connection with the tape
     * labeling command-line tool castor-tape-label.  In any other state, the
     * value of this field is undefined.
     */
    int m_labelCmdConnection;

  }; // class DriveCatalogueLabelSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
