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
  const UserIdentity &adminUser) {
  checkAdminUserDoesNotAlreadyExist(adminUser);
  m_db.adminUsers.push_back(adminUser);
}

//------------------------------------------------------------------------------
// checkAdminUserDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkAdminUserDoesNotAlreadyExist(
  const UserIdentity &adminUser) {
  for(std::list<UserIdentity>::const_iterator itor = m_db.adminUsers.begin();
    itor != m_db.adminUsers.end(); itor++) {
    if(adminUser.uid == itor->uid) {
      std::ostringstream message;
      message << "Administrator with uid " << adminUser.uid <<
        " already exists";
      throw(Exception(message.str()));
    }
  }
}
  
//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &adminUser) {
  for(std::list<UserIdentity>::iterator itor = m_db.adminUsers.begin();
    itor != m_db.adminUsers.end(); itor++) {
    if(adminUser.uid == itor->uid) {
      m_db.adminUsers.erase(itor);
      return;
    }
  }

  // Reaching this point means the administrator to be deleted does not exist
  std::ostringstream message;
  message << "Administration with uid " << adminUser.uid << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::UserIdentity> cta::MockMiddleTierAdmin::getAdminUsers(
  const SecurityIdentity &requester) const {
  return m_db.adminUsers;
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &adminHost) {
  checkAdminHostDoesNotAlreadyExist(adminHost);
  m_db.adminHosts.push_back(adminHost);
}

//------------------------------------------------------------------------------
// checkAdminHostDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::checkAdminHostDoesNotAlreadyExist(
  const std::string &adminHost) {
  for(std::list<std::string>::const_iterator itor = m_db.adminHosts.begin();
    itor != m_db.adminHosts.end(); itor++) {
    if(adminHost == *itor) {
      std::ostringstream message;
      message << "Administration host " << adminHost << " already exists";
      throw(Exception(message.str()));
    }
  }
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &adminHost) {
  for(std::list<std::string>::iterator itor = m_db.adminHosts.begin();
    itor != m_db.adminHosts.end(); itor++) {
    if(adminHost == *itor) {
      m_db.adminHosts.erase(itor);
      return;
    }
  }

  // Reaching this point means the administration host to be deleted does not
  // exist
  std::ostringstream message;
  message << "Administration host " << adminHost << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<std::string> cta::MockMiddleTierAdmin::getAdminHosts(
  const SecurityIdentity &requester) const {
  return m_db.adminHosts;
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
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::MockMiddleTierAdmin::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::MockMiddleTierAdmin::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  std::list<LogicalLibrary> libraries;
  return libraries;
}
