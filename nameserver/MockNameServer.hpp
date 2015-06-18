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

#pragma once

#include "scheduler/DirIterator.hpp"
#include "nameserver/NameServer.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/UserIdentity.hpp"

#include <list>
#include <string>

namespace cta {

/**
 * Local file system implementation of a name server to contain the archive
 * namespace.
 */
class MockNameServer: public NameServer {

public:

  /**
   * Constructor.
   */
  MockNameServer();

  /**
   * Destructor.
   */
  ~MockNameServer() throw();  
  
  void setDirStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName);
  
  void clearDirStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  std::string getDirStorageClass(const SecurityIdentity &requester, const std::string &path) const;
  
  void createFile(const SecurityIdentity &requester, const std::string &path, const mode_t mode);

  void setOwner(const SecurityIdentity &requester, const std::string &path, const UserIdentity &owner);

  UserIdentity getOwner(const SecurityIdentity &requester, const std::string &path) const;
  
  void createDir(const SecurityIdentity &requester, const std::string &path, const mode_t mode);
  
  void deleteFile(const SecurityIdentity &requester, const std::string &path);
  
  void deleteDir(const SecurityIdentity &requester, const std::string &path);
  
  cta::DirEntry statFile(const SecurityIdentity &requester, const std::string &path) const;
  
  cta::DirIterator getDirContents(const SecurityIdentity &requester, const std::string &path) const;
  
  bool regularFileExists(const SecurityIdentity &requester, const std::string &path) const;

  bool dirExists(const SecurityIdentity &requester, const std::string &path) const;
  
  std::string getVidOfFile(const SecurityIdentity &requester, const std::string &path, const uint16_t copyNb) const;
  
  void assertStorageClassIsNotInUse(const SecurityIdentity &requester, const std::string &storageClass, const std::string &path) const;

private:
  
  std::string m_fsDir;
  
  void assertFsDirExists(const std::string &path) const;
  
  void assertFsPathDoesNotExist(const std::string &path) const;
  
  std::list<cta::DirEntry> getDirEntries(const SecurityIdentity &requester, const std::string &path) const;

  /**
   * Throws an exception if the specified user is not the owner of the
   * specified namepsace entry.
   *
   * @param user The user.
   * @param path The absolute path of the namespace entry.
   */
  void assertIsOwner(const SecurityIdentity &requester, const UserIdentity &user, const std::string &path) const;
  
}; // class MockNameServer

} // namespace cta
