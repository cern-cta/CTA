#include "cta/Exception.hpp"
#include "cta/SqliteMiddleTierAdmin.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SqliteMiddleTierAdmin::SqliteMiddleTierAdmin(Vfs &vfs, SqliteDatabase &sqlite_db):
  m_sqlite_db(sqlite_db), m_vfs(vfs) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::SqliteMiddleTierAdmin::~SqliteMiddleTierAdmin() throw() {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &comment) {
  m_sqlite_db.insertAdminUser(requester, user, comment);
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
  m_sqlite_db.deleteAdminUser(requester, user);
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::SqliteMiddleTierAdmin::getAdminUsers(
  const SecurityIdentity &requester) const {
  return m_sqlite_db.selectAllAdminUsers(requester);
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  m_sqlite_db.insertAdminHost(requester, hostName, comment);
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
  m_sqlite_db.deleteAdminHost(requester, hostName);
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::SqliteMiddleTierAdmin::getAdminHosts(
  const SecurityIdentity &requester) const {
  return m_sqlite_db.selectAllAdminHosts(requester);
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createStorageClass(
  const SecurityIdentity &requester, const std::string &name,
  const uint16_t nbCopies, const std::string &comment) {
  m_sqlite_db.insertStorageClass(requester, name, nbCopies, comment);
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteStorageClass(const SecurityIdentity &requester,
  const std::string &name) {
  m_vfs.checkStorageClassIsNotInUse(requester, name, "/");
  m_sqlite_db.deleteStorageClass(requester, name);
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::SqliteMiddleTierAdmin::getStorageClasses(
  const SecurityIdentity &requester) const {
  return m_sqlite_db.selectAllStorageClasses(requester);
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint16_t nbDrives,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
  m_sqlite_db.insertTapePool(requester, name, nbDrives, nbPartialTapes, comment);
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteTapePool(const SecurityIdentity &requester,
  const std::string &name) {
  m_sqlite_db.deleteTapePool(requester, name);
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::SqliteMiddleTierAdmin::getTapePools(
  const SecurityIdentity &requester) const {
  return m_sqlite_db.selectAllTapePools(requester);
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  return m_sqlite_db.insertArchiveRoute(requester, storageClassName, copyNb, tapePoolName, comment);
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteArchiveRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
  return m_sqlite_db.deleteArchiveRoute(requester, storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchiveRoute> cta::SqliteMiddleTierAdmin::getArchiveRoutes(
  const SecurityIdentity &requester) const {
  return m_sqlite_db.selectAllArchiveRoutes(requester);
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
  m_sqlite_db.insertLogicalLibrary(requester, name, comment);
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_sqlite_db.deleteLogicalLibrary(requester, name);
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::SqliteMiddleTierAdmin::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  return m_sqlite_db.selectAllLogicalLibraries(requester);
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createTape(
  const SecurityIdentity &requester,
  const std::string &vid,
  const std::string &logicalLibraryName,
  const std::string &tapePoolName,
  const uint64_t capacityInBytes,
  const std::string &comment) {
  m_sqlite_db.insertTape(requester, vid, logicalLibraryName, tapePoolName,
    capacityInBytes, comment);
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  m_sqlite_db.deleteTape(requester, vid);
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::SqliteMiddleTierAdmin::getTapes(
  const SecurityIdentity &requester) const {
  return m_sqlite_db.selectAllTapes(requester);
}
