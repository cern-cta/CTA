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

#include "castor/tape/tapeserver/daemon/CleanerSession.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CleanerSession::CleanerSession(
  legacymsg::RmcProxy &rmc,
  castor::log::Logger &log,
  const utils::DriveConfig &driveConfig,
  System::virtualWrapper &sysWrapper):
  m_rmc(rmc),
  m_log(log),
  m_driveConfig(driveConfig),
  m_sysWrapper(sysWrapper) {
  
}

//------------------------------------------------------------------------------
// clean
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::CleanerSession::clean(const std::string &vid) {
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);    
  castor::tape::SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.devFilename);
  
  // Instantiate the drive object
  std::auto_ptr<castor::tape::drives::DriveInterface> drive(
    castor::tape::drives::DriveFactory(driveInfo, m_sysWrapper));

  if(NULL == drive.get()) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Cleaner session ended with error. Failed to instantiate drive object";
    throw ex;
  }
  
  //temporization to allow for actions to complete
  try {
    drive->waitUntilReady(60); //wait 1 minute for a possible ongoing mount to complete or for an IO operation to complete
  } catch (castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_INFO, "Cleaner session caught a non-fatal exception while waiting for the drive to become ready. One of the reasons we get here is if the drive has no tape inside.", params);
  }
  
  //here we check if the drive contains a tape: if not, there's nothing to clean
  bool driveHasTapeInPlace = drive->hasTapeInPlace();  
  if(driveHasTapeInPlace) { //a tape is still mounted in the drive      
    castor::tape::tapeFile::VOL1 vol1;
    try {   
      drive->rewind();
      drive->readExactBlock((void * )&vol1, sizeof(vol1), "[CleanerSession::clean()] - Reading header VOL1");
      vol1.verify();
      if(vid.empty()) { // vid given is empty
        log::Param params[] = {log::Param("vid", vid)};
        m_log(LOG_INFO, "Cleaner session received an empty vid.", params);
      }
      else if(!(vid.compare(vol1.getVSN()))) { // vid provided and vid read on VOL1 correspond
        log::Param params[] = {log::Param("vid", vid)};
        m_log(LOG_INFO, "Cleaner session received the same vid read on tape.", params);        
      }
      else { // vid provided and vid read on VOL1 don NOT correspond!
        log::Param params[] = {log::Param("vid provided", vid), log::Param("vid read on label", vol1.getVSN())};
        m_log(LOG_WARNING, "Cleaner session received a different vid from the one read on tape.", params);
      }
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Cleaner session could not rewind the tape or read its label. Giving up now. Reason: " << ne.getMessage().str();
      m_log(LOG_ERR, ex.getMessage().str());
      return 1; //Tells caller to put the drive down
    }
    try {
      drive->unloadTape();
    } catch (castor::exception::Exception &ex) {
      log::Param params[] = {log::Param("message", ex.getMessage().str())};
      m_log(LOG_INFO, "Cleaner session could not unload the tape. Will still try to unmount it in case it is already unloaded.", params);
    }
    try {
      m_rmc.unmountTape(vol1.getVSN(), m_driveConfig.librarySlot);
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Cleaner session could not unmount the tape. Giving up now. Reason: " << ne.getMessage().str();
      m_log(LOG_ERR, ex.getMessage().str());
      return 1; //Tells caller to put the drive down
    }
    return 0; //Tells caller the drive can stay up
  }  
  else { //the drive is empty here we don't care about the drive being ready or not
    return 0; //Tells caller the drive can stay up
  }
}
