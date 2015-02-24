#include "Exception.hpp"
#include "MockMiddleTierAdmin.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockMiddleTierAdmin::MockMiddleTierAdmin(MockDatabase &database):
  m_db(database) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockMiddleTierAdmin::~MockMiddleTierAdmin() throw() {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  m_db.adminUsers.createAdminUser(requester, user, comment);
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
  m_db.adminUsers.deleteAdminUser(requester, user);
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::MockMiddleTierAdmin::getAdminUsers(
  const SecurityIdentity &requester) const {
  return m_db.adminUsers.getAdminUsers(requester);
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  m_db.adminHosts.createAdminHost(requester, hostName, comment);
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
  m_db.adminHosts.deleteAdminHost(requester, hostName);
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::MockMiddleTierAdmin::getAdminHosts(
  const SecurityIdentity &requester) const {
  return m_db.adminHosts.getAdminHosts(requester);
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createStorageClass(
  const SecurityIdentity &requester, const std::string &name,
  const uint8_t nbCopies, const std::string &comment) {
  m_db.storageClasses.createStorageClass(name, nbCopies, requester.user,
    comment);
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteStorageClass(const SecurityIdentity &requester,
  const std::string &name) {
  checkStorageClassIsNotInAMigrationRoute(name);
  m_db.storageClasses.deleteStorageClass(name);
}

//------------------------------------------------------------------------------
// checkStorageClassIsNotInAMigrationRoute
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkStorageClassIsNotInAMigrationRoute(
  const std::string &name) const {
  if(m_db.migrationRoutes.storageClassIsInAMigrationRoute(name)) {
    std::ostringstream message;
    message << "The " << name << " storage class is in use";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::MockMiddleTierAdmin::getStorageClasses(
  const SecurityIdentity &requester) const {
  return m_db.storageClasses.getStorageClasses();
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbDrives,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
  m_db.tapePools.createTapePool(requester, name, nbDrives, nbPartialTapes,
    comment);
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteTapePool(const SecurityIdentity &requester,
  const std::string &name) {
  checkTapePoolIsNotInUse(name);
  m_db.tapePools.deleteTapePool(requester, name);
}

//------------------------------------------------------------------------------
// checkTapePoolIsNotInUse
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkTapePoolIsNotInUse(const std::string &name)
  const {
  if(m_db.migrationRoutes.tapePoolIsInAMigrationRoute(name)) {
    std::ostringstream message;
    message << "The " << name << " tape pool is in use";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::MockMiddleTierAdmin::getTapePools(
  const SecurityIdentity &requester) const {
  return m_db.tapePools.getTapePools(requester);
}

//------------------------------------------------------------------------------
// createMigrationRoute
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createMigrationRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint8_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  return m_db.migrationRoutes.createMigrationRoute(
    storageClassName,
    copyNb,
    tapePoolName,
    requester.user,
    comment);
}

//------------------------------------------------------------------------------
// deleteMigrationRoute
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteMigrationRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint8_t copyNb) {
  return m_db.migrationRoutes.deleteMigrationRoute(storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getMigrationRoutes
//------------------------------------------------------------------------------
std::list<cta::MigrationRoute> cta::MockMiddleTierAdmin::getMigrationRoutes(
  const SecurityIdentity &requester) const {
  return m_db.migrationRoutes.getMigrationRoutes();
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
  m_db.libraries.createLogicalLibrary(requester, name, comment);
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_db.libraries.deleteLogicalLibrary(requester, name);
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::MockMiddleTierAdmin::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  return m_db.libraries.getLogicalLibraries(requester);
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const std::string &comment) {
  m_db.libraries.checkLogicalLibraryExists(logicalLibraryName);
  m_db.tapePools.checkTapePoolExists(tapePoolName);
  m_db.tapes.createTape(requester, vid, logicalLibraryName, tapePoolName,
    capacityInBytes, comment);
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  m_db.tapes.deleteTape(requester, vid);
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::MockMiddleTierAdmin::getTapes(
  const SecurityIdentity &requester) const {
  return m_db.tapes.getTapes(requester);
}
