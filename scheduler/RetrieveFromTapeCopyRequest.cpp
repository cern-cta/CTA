/*
 * The CERN Tape Archive (CTA) project
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

#include "scheduler/RetrieveFromTapeCopyRequest.hpp"
#include "common/archiveNS/TapeCopyLocation.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveFromTapeCopyRequest::RetrieveFromTapeCopyRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveFromTapeCopyRequest::~RetrieveFromTapeCopyRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveFromTapeCopyRequest::RetrieveFromTapeCopyRequest(
  const std::string &archiveFile,
  const uint64_t copyNb,
  const TapeCopyLocation &tapeCopy,
  const std::string &remoteFile,
  const uint64_t priority,
  const CreationLog & creationLog):
  RetrieveRequest(priority, creationLog),
  archiveFile(archiveFile),
  copyNb(copyNb),
  tapeCopy(tapeCopy),
  remoteFile(remoteFile) {
}
