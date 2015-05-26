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
 * but WITHOUT ANY WARRANTY {
} without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "scheduler/Scheduler.hpp"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Scheduler::~Scheduler() throw() {
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void Scheduler::cta::Scheduler::queue(const ArchiveToDirRequest &rqst) {
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void Scheduler::cta::Scheduler::queue(const ArchiveToFileRequest &rqst) {
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::map<TapePool, std::list<ArchivalJob> > Scheduler::getArchivalJobs(
  const SecurityIdentity &requester) const {
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::list<ArchivalJob> Scheduler::getArchivalJobs(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
}

//------------------------------------------------------------------------------
// deleteArchivalJob
//------------------------------------------------------------------------------
void Scheduler::deleteArchivalJob(
  const SecurityIdentity &requester,
  const std::string &dstPath) {
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void Scheduler::queue(RetrieveToDirRequest &rqst) {
}

//------------------------------------------------------------------------------
// queue
//------------------------------------------------------------------------------
void Scheduler::queue(RetrieveToFileRequest &rqst) {
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
std::map<Tape, std::list<RetrievalJob> > Scheduler::getRetrievalJobs(
  const SecurityIdentity &requester) const {
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
std::list<RetrievalJob> Scheduler::getRetrievalJobs(
  const SecurityIdentity &requester,
  const std::string &vid) const {
}
  
//------------------------------------------------------------------------------
// deleteRetrievalJob
//------------------------------------------------------------------------------
void Scheduler::deleteRetrievalJob(
  const SecurityIdentity &requester,
  const std::string &dstUrl) {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void Scheduler::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void Scheduler::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<AdminUser> Scheduler::getAdminUsers(const SecurityIdentity &requester)
 const {
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void Scheduler::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void Scheduler::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
}

//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<AdminHost> Scheduler::getAdminHosts(const SecurityIdentity &requester)
 const {
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void Scheduler::createStorageClass(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbCopies,
  const std::string &comment) {
  // IMPORTANT
  // KEEP NS AND DB IN SYNC
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void Scheduler::deleteStorageClass(
  const SecurityIdentity &requester,
  const std::string &name) {
  //IMPORTANT
  //m_ns.assertStorageClassIsNotInUse(requester, name, "/");
  //m_db.deleteStorageClass(requester, name);
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<StorageClass> Scheduler::getStorageClasses(
  const SecurityIdentity &requester) const {
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void Scheduler::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void Scheduler::deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<TapePool> Scheduler::getTapePools(
  const SecurityIdentity &requester) const {
}

//------------------------------------------------------------------------------
// createArchivalRoute
//------------------------------------------------------------------------------
void Scheduler::createArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteArchivalRoute
//------------------------------------------------------------------------------
void Scheduler::deleteArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
}

//------------------------------------------------------------------------------
// getArchivalRoutes
//------------------------------------------------------------------------------
std::list<ArchivalRoute> Scheduler::getArchivalRoutes(
  const SecurityIdentity &requester) const {
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void Scheduler::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void Scheduler::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<LogicalLibrary> Scheduler::getLogicalLibraries(
  const SecurityIdentity &requester) const {
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void Scheduler::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const std::string &comment) {
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void Scheduler::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<Tape> Scheduler::getTapes(
  const SecurityIdentity &requester) const {
}
