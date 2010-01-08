/******************************************************************************
 *                      castor/tape/aggregator/TapeSessionCatalogue.cpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/TapeSessionCatalogue.hpp"
#include "castor/tape/utils/ScopedLock.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/serrno.h"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::TapeSessionCatalogue::TapeSessionCatalogue()
  throw(castor::exception::Exception) {

  // Initialise the catalogue's mutex dynamically
  {
    const int rc = pthread_mutex_init(&m_mutex, NULL);

    if(rc) {
      TAPE_THROW_CODE(rc,
        "Failed to initialise the tape session catalogue's mutex"
        ": " << sstrerror(rc));
    }
  }
}


//-----------------------------------------------------------------------------
// addTapeSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::TapeSessionCatalogue::addTapeSession(
  const uint64_t mountTransactionId, const char *const driveName)
  throw(castor::exception::InvalidArgument) {

  bool sessionAlreadyOnGoing = false;
  bool sameDriveUnitName     = false;

  utils::ScopedLock scopedLock(m_mutex);

  // For each maplet
  for(TransIdToDriveNameMap::const_iterator itor =
    m_transIdToDriveNameMap.begin(); itor != m_transIdToDriveNameMap.end();
    itor++) {

    if(itor->first == mountTransactionId) {
      sessionAlreadyOnGoing = true;
    }

    if(itor->second == driveName) {
      sameDriveUnitName = true;
    }
  }

  // Throw an exception if the session is already on-going or the drive is
  // already in use
  if(sessionAlreadyOnGoing || sameDriveUnitName) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Failed to add tape session"
      ": mountTransactionId=" << mountTransactionId <<
      ": driveName=" << driveName;

    if(sessionAlreadyOnGoing) {
      ex.getMessage() << ": session is already on-going";
    }

    if(sameDriveUnitName) {
      ex.getMessage() << ": The drive is already in use";
    }

    throw(ex);
  }
}


//-----------------------------------------------------------------------------
// removeTapeSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::TapeSessionCatalogue::removeTapeSession(
  const uint64_t mountTransactionId)
  throw(castor::exception::InvalidArgument) {

  utils::ScopedLock scopedLock(m_mutex);

  // Try to remove the tape session
  TransIdToDriveNameMap::size_type nbRemoved =
    m_transIdToDriveNameMap.erase(mountTransactionId);

  // Throw an exception if the tape session was not on-going, i.e. it was not
  // in the catalogue
  if(nbRemoved == 0) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Failed to remove tape session"
      ": mountTransactionId=" << mountTransactionId <<
      ": tape session is not on-going";

    throw(ex);
  }
}
