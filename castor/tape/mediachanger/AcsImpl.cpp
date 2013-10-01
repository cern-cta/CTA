/******************************************************************************
 *                 castor/tape/mediachanger/AcsImpl.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/mediachanger/AcsImpl.hpp"

#include <errno.h>
#include <sstream>
#include <string.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::AcsImpl::~AcsImpl() throw() {
}

//------------------------------------------------------------------------------
// mount
//------------------------------------------------------------------------------
STATUS castor::tape::mediachanger::AcsImpl::mount(
  const SEQ_NO seqNumber,
  const LOCKID lockId,
  const VOLID &volId,
  const DRIVEID &driveId,
  const BOOLEAN readOnly,
  const BOOLEAN bypass)
  throw() {
  return acs_mount(seqNumber, lockId, volId, driveId, readOnly, bypass);
}

//------------------------------------------------------------------------------
// dismount
//------------------------------------------------------------------------------
STATUS castor::tape::mediachanger::AcsImpl::dismount(
  const SEQ_NO seqNumber,
  const LOCKID lockId,
  const VOLID &volId,
  const DRIVEID &driveId,
  const BOOLEAN force)
  throw() {
  return acs_dismount(seqNumber, lockId, volId, driveId, force);
}

//------------------------------------------------------------------------------
// response
//------------------------------------------------------------------------------
STATUS castor::tape::mediachanger::AcsImpl::response(
  const int timeout,
  SEQ_NO &seqNumber,
  REQ_ID &reqId,
  ACS_RESPONSE_TYPE &rType,
  ALIGNED_BYTES rBuf) throw() {
  return acs_response(timeout, &seqNumber, &reqId, &rType, rBuf);
}

//------------------------------------------------------------------------------
// queryVolume
//------------------------------------------------------------------------------
STATUS castor::tape::mediachanger::AcsImpl::queryVolume(
  const SEQ_NO seqNumber,
  VOLID (&volIds)[MAX_ID],
  const unsigned short count) throw() {
  return acs_query_volume(seqNumber, volIds, count);
}
