/*
 * The CERN Tape Retrieve (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "scheduler/RetrieveJob.hpp"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveJob::~RetrieveJob() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveJob::RetrieveJob(
// TO BE DECIDED
//RetrieveMount &mount,
  const TapeCopyInfo &tapeCopyLocation,
  const std::string &id, 
  const std::string &userRequestId,
  const uint32_t copyNb,
  const std::string &remoteFile,
  const uint64_t castorNsFileId):
  TapeJob(id, userRequestId, copyNb, remoteFile, castorNsFileId),
// TO BE DECIDED
//m_mount(mount),
  tapeCopyLocation(tapeCopyLocation) {
}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::RetrieveJob::complete(const uint32_t checksumOfTransfer, 
  const uint64_t fileSizeOfTransfer) {
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::RetrieveJob::failed(const std::exception &ex) {
}
  
//------------------------------------------------------------------------------
// retry
//------------------------------------------------------------------------------
void cta::RetrieveJob::retry() { }

//------------------------------------------------------------------------------
// toString(PositioningMethod)
//------------------------------------------------------------------------------
std::string cta::RetrieveJob::toString(PositioningMethod pm) {
  switch(pm) {
    case PositioningMethod::ByBlock:
      return "ByBlock";
    case PositioningMethod::ByFSeq:
      return "ByFSeq";
    default:
      return "Unknown Positioning Method";
  }
}

