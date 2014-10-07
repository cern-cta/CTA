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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/mediachanger/MediaChangerFacade.hpp"
#include "h/Castor_limits.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::MediaChangerFacade::MediaChangerFacade(
  messages::AcsProxy &acs,
  MmcProxy &mmc,
  legacymsg::RmcProxy &rmc) throw():
  m_acs(acs),
  m_mmc(mmc),
  m_rmc(rmc) {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void castor::mediachanger::MediaChangerFacade::mountTapeReadOnly(
  const std::string &vid, const ConfigLibrarySlot &librarySlot) {
  try {
    const TapeLibraryType libraryType = librarySlot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      return m_acs.mountTapeReadOnly(vid, librarySlot.str());
    case TAPE_LIBRARY_TYPE_MANUAL:
      return m_mmc.mountTapeReadOnly(vid, librarySlot.str());
    case TAPE_LIBRARY_TYPE_SCSI:
      // SCSI media-changers to not support read-only mounts
      return m_rmc.mountTapeReadWrite(vid, librarySlot.str());
    default:
      {
        // Should never get here
        castor::exception::Exception ex;
        ex.getMessage() << "Library slot has an unexpected library type";
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read-only access"
      ": vid=" << vid << " librarySlot=" << librarySlot.str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void castor::mediachanger::MediaChangerFacade::mountTapeReadWrite(
  const std::string &vid, const ConfigLibrarySlot &librarySlot) {
  try {
    const TapeLibraryType libraryType = librarySlot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS: 
      return m_acs.mountTapeReadWrite(vid, librarySlot.str());
    case TAPE_LIBRARY_TYPE_MANUAL: 
      return m_mmc.mountTapeReadWrite(vid, librarySlot.str());
    case TAPE_LIBRARY_TYPE_SCSI:
      return m_rmc.mountTapeReadWrite(vid, librarySlot.str());
    default:
      {
        // Should never get here
        castor::exception::Exception ex;
        ex.getMessage() << "Library slot has an unexpected library type";
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read/write access"
      ": vid=" << vid << " librarySlot=" << librarySlot.str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::mediachanger::MediaChangerFacade::dismountTape(
  const std::string &vid, const ConfigLibrarySlot &librarySlot) {
  try {
    const TapeLibraryType libraryType = librarySlot.getLibraryType();
  
    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      return m_acs.dismountTape(vid, librarySlot.str());
    case TAPE_LIBRARY_TYPE_MANUAL:
      return m_mmc.dismountTape(vid, librarySlot.str());
    case TAPE_LIBRARY_TYPE_SCSI:
      return m_rmc.dismountTape(vid, librarySlot.str());
    default:
      {
        // Should never get here
        castor::exception::Exception ex;
        ex.getMessage() << "Library slot has an unexpected library type";
        throw ex;
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape"
      ": vid=" << vid << " librarySlot=" << librarySlot.str();
    throw ex;
  }
}
