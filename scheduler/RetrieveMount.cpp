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

#include "scheduler/RetrieveMount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount():
  m_sessionRunning(false) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount(
  std::unique_ptr<SchedulerDatabase::RetrieveMount> dbMount): 
  m_sessionRunning(false) {
  m_dbMount.reset(dbMount.release());
}

//------------------------------------------------------------------------------
// getMountType
//------------------------------------------------------------------------------
cta::common::dataStructures::MountType cta::RetrieveMount::getMountType() const{
  return cta::common::dataStructures::MountType::Retrieve;
}

//------------------------------------------------------------------------------
// getNbFiles
//------------------------------------------------------------------------------
uint32_t cta::RetrieveMount::getNbFiles() const {
  return m_dbMount->nbFilesCurrentlyOnTape;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getVid() const{
  return m_dbMount->mountInfo.vid;
}

//------------------------------------------------------------------------------
// getMountTransactionId
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getMountTransactionId() const{
  std::stringstream id;
  if (!m_dbMount.get())
    throw exception::Exception("In cta::RetrieveMount::getMountTransactionId(): got NULL dbMount");
  id << m_dbMount->mountInfo.mountId;
  return id.str();
}

//------------------------------------------------------------------------------
// getNextJob
//------------------------------------------------------------------------------
std::unique_ptr<cta::RetrieveJob> cta::RetrieveMount::getNextJob(log::LogContext & logContext) {
  if (!m_sessionRunning)
    throw SessionNotRunning("In RetrieveMount::getNextJob(): trying to get job from complete/not started session");
  // Try and get a new job from the DB
  std::unique_ptr<cta::SchedulerDatabase::RetrieveJob> dbJob(m_dbMount->getNextJob(logContext).release());
  if (!dbJob.get())
    return std::unique_ptr<cta::RetrieveJob>();
  // We have something to retrieve: prepare the response
  std::unique_ptr<cta::RetrieveJob> ret (new RetrieveJob(*this, 
    dbJob->retrieveRequest, dbJob->archiveFile, dbJob->selectedCopyNb, 
    PositioningMethod::ByBlock));
  ret->m_dbJob.reset(dbJob.release());
  return ret;
}

//------------------------------------------------------------------------------
// tapeComplete())
//------------------------------------------------------------------------------
void cta::RetrieveMount::tapeComplete() {
  m_tapeRunning = false;
  if (!m_diskRunning) {
    // Just set the session as complete in the DB.
    m_dbMount->complete(time(NULL));
    // and record we are done with the mount
    m_sessionRunning = false;
  } else {
    // This is a special case: we have to report the tape server is draining
    // its memory to disk
    setDriveStatus(cta::common::dataStructures::DriveStatus::DrainingToDisk);
  }
}

//------------------------------------------------------------------------------
// diskComplete())
//------------------------------------------------------------------------------
void cta::RetrieveMount::diskComplete() {
  m_diskRunning = false;
  if (!m_tapeRunning) {
    // Just set the session as complete in the DB.
    cta::SchedulerDatabase::RetrieveMount  * ptr = m_dbMount.get();
    ptr=ptr;
    m_dbMount->complete(time(NULL));
    // and record we are done with the mount
    m_sessionRunning = false;
  }
}

//------------------------------------------------------------------------------
// abort())
//------------------------------------------------------------------------------
void cta::RetrieveMount::abort() {
  diskComplete();
  tapeComplete();
}

//------------------------------------------------------------------------------
// setDriveStatus()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status) {
  m_dbMount->setDriveStatus(status, time(NULL));
}

//------------------------------------------------------------------------------
// setTapeSessionStats()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  m_dbMount->setTapeSessionStats(stats);
}

//------------------------------------------------------------------------------
// bothSidesComplete())
//------------------------------------------------------------------------------
bool cta::RetrieveMount::bothSidesComplete() {
  return !(m_diskRunning || m_tapeRunning);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveMount::~RetrieveMount() throw() {
}
