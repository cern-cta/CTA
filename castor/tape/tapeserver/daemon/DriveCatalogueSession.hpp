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

#pragma once

#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

#include <unistd.h>
#include <sys/types.h>
#include <string>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  /**
   * Base class containing info about the session attached to a drive catalogue entry
   */
  class DriveCatalogueSession {
  public:
    
    virtual ~DriveCatalogueSession();
    /**
     * The status of the tape with respect to the drive mount and unmount operations
     */
    enum SessionState {
      SESSION_STATE_NONE,
      SESSION_STATE_WAITFORK,
      SESSION_STATE_RUNNING 
    };
        
    /**
     * pid setter method
     * 
     * @param pid process ID of the session
     */
    virtual void setPid(const pid_t pid);
    
    /**
     * pid getter method
     * 
     * @return process ID of the session
     */
    virtual pid_t getPid() const;
    
    /**
     * event setter method
     * 
     * @param vid The status of the tape with respect to the drive mount and unmount
     * operations.
     */
    virtual void setEvent(const castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent event);
    
    /**
     * event getter method
     * 
     * @return The status of the tape with respect to the drive mount and unmount
     * operations.
     */
    virtual castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent getEvent() const;
    
    /**
     * Sets the point in time when the drive was assigned a tape.
     */
    virtual void setAssignmentTime(const time_t assignmentTime);

    /**
     * Gets the point in time when the drive was assigned a tape.
     *
     * @return Te point in time when the drive was assigned a tape.
     */
    virtual time_t getAssignmentTime() const;
    
    /**
     * state setter method
     * 
     * @param state of the drive catalogue session
     */
    virtual void setState(const SessionState state);
    
    /**
     * state getter method
     * 
     * @return state of the drive catalogue session
     */
    virtual SessionState getState() const;
    
  protected:
    
    /**
     * Constructor
     * 
     * @param pid process ID of the session
     * @param state state of the drive catalogue session
     */
    DriveCatalogueSession(const SessionState state);
        
    /**
     * The process ID of the session
     */
    pid_t m_pid;
    
    /**
     * The point in time when the drive was assigned a tape.
     */
    time_t m_assignmentTime;
    
    /**
     * The status of the tape with respect to the drive mount and unmount
     * operations.
     */
    castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent m_event;
    
    /**
     * The state of the drive catalogue session
     */
    SessionState m_state;

 }; // class DriveCatalogueSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
