#include "Exception.hpp"
#include "MockMiddleTierAdmin.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockMiddleTierAdmin::MockMiddleTierAdmin(MockMiddleTierDatabase &database):
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
  checkAdminUserDoesNotAlreadyExist(user);
  AdminUser adminUser(user, requester.user, comment);
  m_db.adminUsers[user.getUid()] = adminUser;
}

//------------------------------------------------------------------------------
// checkAdminUserDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkAdminUserDoesNotAlreadyExist(
  const UserIdentity &user) const {
  std::map<uint32_t, AdminUser>::const_iterator itor =
    m_db.adminUsers.find(user.getUid());
  if(itor != m_db.adminUsers.end()) {
    std::ostringstream message;
    message << "Administrator with uid " << user.getUid() <<
      " already exists";
    throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
  for(std::map<uint32_t, AdminUser>::iterator itor = m_db.adminUsers.begin();
    itor != m_db.adminUsers.end(); itor++) {
    if(user.getUid() == itor->first) {
      m_db.adminUsers.erase(itor);
      return;
    }
  }

  // Reaching this point means the administrator to be deleted does not exist
  std::ostringstream message;
  message << "Administration with uid " << user.getUid() << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::MockMiddleTierAdmin::getAdminUsers(
  const SecurityIdentity &requester) const {
  std::list<cta::AdminUser> users;
  for(std::map<uint32_t, AdminUser>::const_iterator itor =
    m_db.adminUsers.begin(); itor != m_db.adminUsers.end(); itor++) {
    users.push_back(itor->second);
  }
  return users;
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  checkAdminHostDoesNotAlreadyExist(hostName);
  AdminHost adminHost(hostName, requester.user, comment);
  m_db.adminHosts[hostName] = adminHost;
}

//------------------------------------------------------------------------------
// checkAdminHostDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkAdminHostDoesNotAlreadyExist(
  const std::string &hostName) const {
  std::map<std::string, AdminHost>::const_iterator itor =
    m_db.adminHosts.find(hostName);
  if(itor != m_db.adminHosts.end()) {
    std::ostringstream message;
    message << "Administration host " << hostName << " already exists";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
  for(std::map<std::string, AdminHost>::iterator itor = m_db.adminHosts.begin();
    itor != m_db.adminHosts.end(); itor++) {
    if(hostName == itor->first) {
      m_db.adminHosts.erase(itor);
      return;
    }
  }

  // Reaching this point means the administration host to be deleted does not
  // exist
  std::ostringstream message;
  message << "Administration host " << hostName << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::MockMiddleTierAdmin::getAdminHosts(
  const SecurityIdentity &requester) const {
  std::list<cta::AdminHost> hosts;
  for(std::map<std::string, AdminHost>::const_iterator itor =
    m_db.adminHosts.begin(); itor != m_db.adminHosts.end(); itor++) {
    hosts.push_back(itor->second);
  }
  return hosts;
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
  checkTapePoolDoesNotAlreadyExist(name);
  TapePool tapePool(name, nbDrives, nbPartialTapes,requester.user, comment);
  m_db.tapePools[name] = tapePool;
}

//------------------------------------------------------------------------------
// checkTapePoolDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkTapePoolDoesNotAlreadyExist(
  const std::string &name) const {
  std::map<std::string, TapePool>::const_iterator itor =
    m_db.tapePools.find(name);
  if(itor != m_db.tapePools.end()) {
    std::ostringstream message;
    message << "The " << name << " tape pool already exists";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteTapePool(const SecurityIdentity &requester,
  const std::string &name) {
  std::map<std::string, TapePool>::iterator itor = m_db.tapePools.find(name);
  if(itor == m_db.tapePools.end()) {
    std::ostringstream message;
    message << "The " << name << " tape pool does not exist";
    throw Exception(message.str());
  }
  checkTapePoolIsNotInUse(name);
  m_db.tapePools.erase(itor);
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
  std::list<cta::TapePool> tapePools;

  for(std::map<std::string, TapePool>::const_iterator itor =
    m_db.tapePools.begin(); itor != m_db.tapePools.end(); itor++) {
    tapePools.push_back(itor->second);
  }
  return tapePools;
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
  checkLogicalLibraryDoesNotAlreadyExist(name);
  LogicalLibrary logicalLibrary(name, requester.user, comment);
  m_db.libraries[name] = logicalLibrary;
}

//------------------------------------------------------------------------------
// checkLogicalLibraryDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkLogicalLibraryDoesNotAlreadyExist(
  const std::string &name) const {
  std::map<std::string, LogicalLibrary>::const_iterator itor =
    m_db.libraries.find(name);
  if(itor != m_db.libraries.end()) {
    std::ostringstream message;
    message << "Logical library " << name << " already exists";
    throw(Exception(message.str()));
  }
}
  
//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  for(std::map<std::string, LogicalLibrary>::iterator itor =
    m_db.libraries.begin(); itor != m_db.libraries.end();
    itor++) {
    if(name == itor->first) {
      m_db.libraries.erase(itor);
      return;
    }
  }

  // Reaching this point means the logical library to be deleted does not
  // exist
  std::ostringstream message;
  message << "Logical library " << name << " does not exist";
  throw Exception(message.str());
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::MockMiddleTierAdmin::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  std::list<LogicalLibrary> libraries;

  for(std::map<std::string, LogicalLibrary>::const_iterator itor =
    m_db.libraries.begin(); itor != m_db.libraries.end();
    itor++) {
    libraries.push_back(itor->second);
  }
  return libraries;
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
  checkTapeDoesNotAlreadyExist(vid);
  Tape tape(
    vid,
    logicalLibraryName,
    tapePoolName,
    capacityInBytes,
    requester.user,
    comment);
  m_db.tapes[vid] = tape;
}

//------------------------------------------------------------------------------
// checkTapeDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkTapeDoesNotAlreadyExist(
  const std::string &vid) const {
  std::map<std::string, Tape>::const_iterator itor =
    m_db.tapes.find(vid);
  if(itor != m_db.tapes.end()) {
    std::ostringstream message;
    message << "Tape with vid " << vid << " already exists";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  for(std::map<std::string, Tape>::iterator itor = m_db.tapes.begin();
    itor != m_db.tapes.end(); itor++) {
    if(vid == itor->first) {
      m_db.tapes.erase(itor);
      return;
    }
  }

  // Reaching this point means the tape to be deleted does not
  // exist
  std::ostringstream message;
  message << "Tape iwith volume identifier " << vid << " does not exist";
  throw Exception(message.str());
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::MockMiddleTierAdmin::getTapes(
  const SecurityIdentity &requester) const {
  std::list<cta::Tape> tapes;

  for(std::map<std::string, Tape>::const_iterator itor = m_db.tapes.begin();
    itor != m_db.tapes.end(); itor++) {
    tapes.push_back(itor->second);
  }

  return tapes;
}
