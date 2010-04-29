/******************************************************************************
 *                castor/tape/tapegateway/daemon/MountIdToFseqMap.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/tape/tapegateway/daemon/MountIdToFseqMap.hpp"
#include "castor/tape/utils/ScopedLock.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sstream>
#include <string.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapegateway::daemon::MountIdToFseqMap::MountIdToFseqMap()
  throw(castor::exception::Exception) {
  const int rc = pthread_mutex_init(&m_mutex, NULL);

  if(rc) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to initialize mutex"
      ": " << sstrerror(rc));
  }
}


//-----------------------------------------------------------------------------
// insert
//-----------------------------------------------------------------------------
void castor::tape::tapegateway::daemon::MountIdToFseqMap::insert(
  const uint64_t mountTransactionId, const int32_t fseq)
  throw(castor::exception::Exception) {
  utils::ScopedLock scopedLock(m_mutex);

  Map::const_iterator itor = m_map.find(mountTransactionId);

  // Throw an exception if the mount transaction ID is already inserted
  if(itor != m_map.end()) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      __FUNCTION__ << " function failed"
      ": Mount transaction id already inserted"
      ": id=" <<  mountTransactionId;

    throw(ex);
  }

  m_map[mountTransactionId] = fseq;
}


//-----------------------------------------------------------------------------
// nextFseq
//-----------------------------------------------------------------------------
int32_t castor::tape::tapegateway::daemon::MountIdToFseqMap::nextFseq(
  const uint64_t mountTransactionId) throw(castor::exception::Exception) {
  utils::ScopedLock scopedLock(m_mutex);

  Map::iterator itor = m_map.find(mountTransactionId);

  // Throw an exception if the mount transaction ID is not known
  if(itor == m_map.end()) {
    castor::exception::Exception ex(ENOENT);

    ex.getMessage() <<
      __FUNCTION__ << " function failed"
      ": No such mount transaction id"
      ": id=" <<  mountTransactionId;

    throw(ex);
  }

  return itor->second++;
}


//-----------------------------------------------------------------------------
// erase
//-----------------------------------------------------------------------------
void castor::tape::tapegateway::daemon::MountIdToFseqMap::erase(
  const uint64_t mountTransactionId) throw(castor::exception::Exception) {
  utils::ScopedLock scopedLock(m_mutex);

  Map::const_iterator itor = m_map.find(mountTransactionId);

  // Throw an exception if the mount transaction ID is not known
  if(itor == m_map.end()) {
    castor::exception::Exception ex(ENOENT);

    ex.getMessage() <<
      __FUNCTION__ << " function failed"
      ": No such mount transaction id"
      ": id=" <<  mountTransactionId;

    throw(ex);
  }

  m_map.erase(mountTransactionId);
}
