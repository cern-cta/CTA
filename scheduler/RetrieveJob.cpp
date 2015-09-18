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
cta::RetrieveJob::RetrieveJob(/*RetrieveMount &mount,*/
  const ArchiveFile &archiveFile,
  const RemotePathAndStatus &remotePathAndStatus,
  const TapeFileLocation &tapeFileLocation,
  const PositioningMethod positioningMethod):
  /*mount(mount),*/
  archiveFile(archiveFile),
  remotePathAndStatus(remotePathAndStatus),
  tapeFileLocation(tapeFileLocation),
  positioningMethod(positioningMethod),
  transferredSize(std::numeric_limits<decltype(transferredSize)>::max()) {}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::RetrieveJob::complete() {
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::RetrieveJob::failed() {
}
  
//------------------------------------------------------------------------------
// retry
//------------------------------------------------------------------------------
void cta::RetrieveJob::retry() { }
