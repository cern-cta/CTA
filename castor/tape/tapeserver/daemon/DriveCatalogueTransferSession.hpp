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

#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueSession.hpp"
#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  /**
   * Derived class containing info about the label session attached to a drive catalogue entry
   */
  class DriveCatalogueTransferSession : public DriveCatalogueSession {
  public:
    
    /**
     * Constructor
     * 
     * @param state state of the drive catalogue session
     * @param vdqmJob job received from the vdqmd daemon
     */
    DriveCatalogueTransferSession(
            const SessionState state,
            const castor::legacymsg::RtcpJobRqstMsgBody vdqmJob);
    
    /**
     * vdqmJob setter method
     * 
     * @param vdqmJob job received from the vdqmd daemon
     */
    void setVdqmJob(const castor::legacymsg::RtcpJobRqstMsgBody vdqmJob);

    /**
     * vdqmJob getter method
     * 
     * @return vdqm job received from the vdqmd daemon
     */
    castor::legacymsg::RtcpJobRqstMsgBody getVdqmJob() const;
    
    /**
     * vid setter method
     * 
     * @param vid volume ID of the tape involved in the session
     */
    void setVid(const std::string &vid);
    
    /**
     * vid getter method
     * 
     * @return volume ID of the tape involved in the session
     */
    std::string getVid() const;
    
  private:
    
    /**
     * The volume ID of the tape involved in the session
     */
    std::string m_vid;
    
    /**
     * The job received from the vdqmd daemon.
     */
    castor::legacymsg::RtcpJobRqstMsgBody m_vdqmJob;
    
    //TODO add assignment time

  }; // class DriveCatalogueTransferSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
