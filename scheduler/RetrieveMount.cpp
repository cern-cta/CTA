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
cta::MountType::Enum cta::RetrieveMount::getMountType() const{
  return MountType::RETRIEVE;
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
std::unique_ptr<cta::RetrieveJob> cta::RetrieveMount::getNextJob() {
  if (!m_sessionRunning)
    throw SessionNotRunning("In RetrieveMount::getNextJob(): trying to get job from complete/not started session");
  // Try and get a new job from the DB
  std::unique_ptr<cta::SchedulerDatabase::RetrieveJob> dbJob(m_dbMount->getNextJob().release());
  if (!dbJob.get())
    return std::unique_ptr<cta::RetrieveJob>(NULL);
  // We have something to retrieve: prepare the response
  std::unique_ptr<cta::RetrieveJob> ret (new RetrieveJob(*this, 
    dbJob->archiveFile, dbJob->remoteFile, dbJob->nameServerTapeFile, 
    PositioningMethod::ByBlock));
  ret->m_dbJob.reset(dbJob.release());
  return ret;
}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::RetrieveMount::complete() {
  // Just set the session as complete in the DB.
  m_dbMount->complete(time(NULL));
  // and record we are done with the mount
  m_sessionRunning = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveMount::~RetrieveMount() throw() {
}
