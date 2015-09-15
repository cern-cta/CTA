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

#include "scheduler/ArchiveMount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveMount::ArchiveMount(NameServer & ns): m_ns(ns) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveMount::ArchiveMount(NameServer & ns,
  std::unique_ptr<SchedulerDatabase::ArchiveMount> dbMount): m_ns(ns) {
  m_dbMount.reset(
    dynamic_cast<SchedulerDatabase::ArchiveMount*>(dbMount.release()));
  if(!m_dbMount.get()) {
    throw WrongMountType(std::string(__FUNCTION__) +
      ": could not cast mount to SchedulerDatabase::ArchiveMount");
  }
}

//------------------------------------------------------------------------------
// getMountType
//------------------------------------------------------------------------------
cta::MountType::Enum cta::ArchiveMount::getMountType() const throw() {
  return MountType::ARCHIVE;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getVid() const throw() {
  return "UNKNOWN_VID_FOR_ARCHIVE_MOUNT";
}

//------------------------------------------------------------------------------
// getDensity
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getDensity() const throw() {
  return "UNKNOWN_DENSITY_FOR_ARCHIVE_MOUNT";
}

//------------------------------------------------------------------------------
// getPoolName
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getPoolName() const throw() {
  return "UNKNOWN_POOL_FOR_ARCHIVE_MOUNT";
}

//------------------------------------------------------------------------------
// getCopyNumber
//------------------------------------------------------------------------------
int cta::ArchiveMount::getCopyNumber() const throw() {
  return 1;
}

//------------------------------------------------------------------------------
// getMountTransactionId
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getMountTransactionId() const throw(){
  return "UNKNOWN_MOUNTTRANSACTIONID_FOR_ARCHIVE_MOUNT";
}

//------------------------------------------------------------------------------
// getNextJob
//------------------------------------------------------------------------------
std::unique_ptr<cta::ArchiveJob> cta::ArchiveMount::getNextJob() {
  // try and get a new job from the DB side
  std::unique_ptr<cta::SchedulerDatabase::ArchiveJob> dbJob(m_dbMount->getNextJob().release());
  if (!dbJob.get())
    return std::unique_ptr<cta::ArchiveJob>(NULL);
  // We have something to migrate: prepare the response
  std::unique_ptr<cta::ArchiveJob> ret(new ArchiveJob(*this, m_ns,
      dbJob->archiveFile, dbJob->remoteFile, dbJob->nameServerTapeFile));
  ret->m_dbJob.reset(dbJob.release());
  return ret;
}
    
//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::ArchiveMount::complete() {
  throw NotImplemented(std::string(__FUNCTION__) + ": Not implemented");
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveMount::~ArchiveMount() throw() {
}
