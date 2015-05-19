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

#include "middletier/interface/DirIterator.hpp"
#include "middletier/interface/SecurityIdentity.hpp"

namespace cta {

/**
 * Mock database.
 */
class Vfs {

public:

  /**
   * Constructor.
   */
  Vfs();

  /**
   * Destructor.
   */
  ~Vfs() throw();  
  
  void setDirStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName);
  
  void clearDirStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  std::string getDirStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  void createFile(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void createDir(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void deleteFile(const SecurityIdentity &requester, const std::string &pathname);
  
  void deleteDir(const SecurityIdentity &requester, const std::string &pathname);
  
  cta::DirEntry statDirEntry(const SecurityIdentity &requester, const std::string &pathname);
  
  cta::DirIterator getDirContents(const SecurityIdentity &requester, const std::string &dirPath);
  
  bool isExistingDir(const SecurityIdentity &requester, const std::string &dirPath);
  
  std::string getVidOfFile(const SecurityIdentity &requester, const std::string &pathname, uint16_t copyNb);
  
  void checkStorageClassIsNotInUse(const SecurityIdentity &requester, const std::string &storageClass, const std::string &dirPath);

private:
  
  std::string m_fsDir;
  
  void checkDirectoryExists(const std::string &dirPath);
  
  void checkPathnameDoesNotExist(const std::string &dirPath);
  
  std::list<cta::DirEntry> getDirEntries(const SecurityIdentity &requester, const std::string &dirPath);
  
}; // struct Vfs

} // namespace cta
