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

#include "utils/exception/Exception.hpp"
#include "middletier/SQLite/SqliteMiddleTierAdmin.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SqliteMiddleTierAdmin::SqliteMiddleTierAdmin(
   NameServer &ns,
   SqliteDatabase &db):
   m_ns(ns),
   m_db(db) {
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
  m_db.insertAdminUser(requester, user, comment);
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &user) {
  m_db.deleteAdminUser(requester, user);
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::AdminUser> cta::SqliteMiddleTierAdmin::getAdminUsers(
  const SecurityIdentity &requester) const {
  return m_db.selectAllAdminUsers(requester);
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName,
  const std::string &comment) {
  m_db.insertAdminHost(requester, hostName, comment);
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &hostName) {
  m_db.deleteAdminHost(requester, hostName);
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<cta::AdminHost> cta::SqliteMiddleTierAdmin::getAdminHosts(
  const SecurityIdentity &requester) const {
  return m_db.selectAllAdminHosts(requester);
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createStorageClass(
  const SecurityIdentity &requester, const std::string &name,
  const uint16_t nbCopies, const std::string &comment) {
  m_db.insertStorageClass(requester, name, nbCopies, comment);
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteStorageClass(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_ns.assertStorageClassIsNotInUse(requester, name, "/");
  m_db.deleteStorageClass(requester, name);
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::SqliteMiddleTierAdmin::getStorageClasses(
  const SecurityIdentity &requester) const {
  return m_db.selectAllStorageClasses(requester);
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createTapePool(
  const SecurityIdentity &requester,
  const std::string &name,
  const uint32_t nbPartialTapes,
  const std::string &comment) {
  m_db.insertTapePool(requester, name, nbPartialTapes, comment);
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteTapePool(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_db.deleteTapePool(requester, name);
}

//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<cta::TapePool> cta::SqliteMiddleTierAdmin::getTapePools(
  const SecurityIdentity &requester) const {
  return m_db.selectAllTapePools(requester);
}

//------------------------------------------------------------------------------
// createArchivalRoute
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  return m_db.insertArchivalRoute(requester, storageClassName, copyNb,
    tapePoolName, comment);
}

//------------------------------------------------------------------------------
// deleteArchivalRoute
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteArchivalRoute(
  const SecurityIdentity &requester,
  const std::string &storageClassName,
  const uint16_t copyNb) {
  return m_db.deleteArchivalRoute(requester, storageClassName, copyNb);
}

//------------------------------------------------------------------------------
// getArchivalRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchivalRoute> cta::SqliteMiddleTierAdmin::getArchivalRoutes(
  const SecurityIdentity &requester) const {
  return m_db.selectAllArchivalRoutes(requester);
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::createLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name,
  const std::string &comment) {
  m_db.insertLogicalLibrary(requester, name, comment);
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteLogicalLibrary(
  const SecurityIdentity &requester,
  const std::string &name) {
  m_db.deleteLogicalLibrary(requester, name);
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<cta::LogicalLibrary> cta::SqliteMiddleTierAdmin::getLogicalLibraries(
  const SecurityIdentity &requester) const {
  return m_db.selectAllLogicalLibraries(requester);
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
  m_db.insertTape(requester, vid, logicalLibraryName, tapePoolName,
    capacityInBytes, comment);
}

//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierAdmin::deleteTape(
  const SecurityIdentity &requester,
  const std::string &vid) {
  m_db.deleteTape(requester, vid);
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<cta::Tape> cta::SqliteMiddleTierAdmin::getTapes(
  const SecurityIdentity &requester) const {
  return m_db.selectAllTapes(requester);
}
