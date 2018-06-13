/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsImpl.hpp"

#include <errno.h>
#include <sstream>
#include <string.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsImpl::~AcsImpl() throw() {
}

//------------------------------------------------------------------------------
// mount
//------------------------------------------------------------------------------
STATUS cta::mediachanger::acs::AcsImpl::mount(
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
STATUS cta::mediachanger::acs::AcsImpl::dismount(
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
STATUS cta::mediachanger::acs::AcsImpl::response(
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
STATUS cta::mediachanger::acs::AcsImpl::queryVolume(
  const SEQ_NO seqNumber,
  VOLID (&volIds)[MAX_ID],
  const unsigned short count) throw() {
  return acs_query_volume(seqNumber, volIds, count);
}
