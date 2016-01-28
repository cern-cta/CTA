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
cta::RetrieveJob::RetrieveJob(RetrieveMount &mount,
  const common::archiveNS::ArchiveFile &archiveFile,
  const std::string &remotePath,
  const NameServerTapeFile &nameServerTapeFile,
  const PositioningMethod positioningMethod):
  m_mount(mount),
  archiveFile(archiveFile),
  remotePath(remotePath),
  nameServerTapeFile(nameServerTapeFile),
  positioningMethod(positioningMethod),
  transferredSize(std::numeric_limits<decltype(transferredSize)>::max()) {}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::RetrieveJob::complete() {
  m_dbJob->succeed();
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::RetrieveJob::failed() {
  throw std::runtime_error("cta::RetrieveJob::failed(): not implemented");
}
  
//------------------------------------------------------------------------------
// retry
//------------------------------------------------------------------------------
void cta::RetrieveJob::retry() {
  throw std::runtime_error("cta::RetrieveJob::retry(): not implemented");
}
