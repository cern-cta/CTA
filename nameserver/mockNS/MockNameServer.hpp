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

#include "common/archiveNS/ArchiveDirIterator.hpp"
#include "common/UserIdentity.hpp"
#include "nameserver/NameServer.hpp"
#include "common/SecurityIdentity.hpp"

#include <cstdatomic>
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

  void createStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies); 

  void createStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const uint32_t id);

  void deleteStorageClass(const SecurityIdentity &requester, const std::string &name);

  void updateStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies);
  
  void setDirStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName);
  
  void clearDirStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  std::string getDirStorageClass(const SecurityIdentity &requester, const std::string &path) const;
  
  void createFile(const SecurityIdentity &requester, const std::string &path, const mode_t mode, const uint64_t size);

  void setOwner(const SecurityIdentity &requester, const std::string &path, const UserIdentity &owner);

  UserIdentity getOwner(const SecurityIdentity &requester, const std::string &path) const;
  
  void createDir(const SecurityIdentity &requester, const std::string &path, const mode_t mode);
  
  void deleteFile(const SecurityIdentity &requester, const std::string &path);
  
  void deleteDir(const SecurityIdentity &requester, const std::string &path);
  
  std::unique_ptr<ArchiveFileStatus> statFile(const SecurityIdentity &requester, const std::string &path) const;
  
  ArchiveDirIterator getDirContents(const SecurityIdentity &requester, const std::string &path) const;
  
  std::string getVidOfFile(const SecurityIdentity &requester, const std::string &path, const uint16_t copyNb) const;
  
  void assertStorageClassIsNotInUse(const SecurityIdentity &requester, const std::string &storageClass, const std::string &path) const;

  /**
   * Add the specified tape file entry to the archive namespace.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the archive file.
   * @param nameServerTapeFile The tape file entry.
   */
  void addTapeFile(
    const SecurityIdentity &requester,
    const std::string &path,
    const NameServerTapeFile &tapeFile);

  /**
   * Gets the tape entries from the archive namespace corresponding the archive
   * with the specified path.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the archive file.
   * @return The tape file entries.
   */
  std::list<NameServerTapeFile> getTapeFiles(
    const SecurityIdentity &requester,
    const std::string &path) const;
  
  /**
   * Delete the specified tape file entry from the archive namespace.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the archive file.
   * @param copyNb The tape copy to delete.
   */
  virtual void deleteTapeFile(
    const SecurityIdentity &requester,
    const std::string &path,
    const uint16_t copyNb);

private:
  
  /**
   * Converts a NameServerTapeFile to a string to be used as an extended attribute
   * 
   * @param tapeFile The NameServerTapeFile object
   * @return the converted string
   */
  std::string fromNameServerTapeFileToString(const cta::NameServerTapeFile &tapeFile) const;
  
  /**
   * Converts a string (the value of an extended attribute) to a NameServerTapeFile
   * 
   * @param xAttributeString The value of an extended attribute
   * @return the NameServerTapeFile object resulting from the conversion
   */
  cta::NameServerTapeFile fromStringToNameServerTapeFile(const std::string &xAttributeString) const;

  std::string m_fsDir;
  
  void assertFsDirExists(const std::string &path) const;
  
  void assertFsFileExists(const std::string &path) const;
  
  void assertFsPathDoesNotExist(const std::string &path) const;
  
  std::list<cta::ArchiveDirEntry> getDirEntries(const SecurityIdentity &requester, const std::string &path) const;

  /**
   * Throws an exception if the specified user is not the owner of the
   * specified namespace entry.
   *
   * @param requester The identity of the requester.
   * @param user The user.
   * @param path The absolute path of the namespace entry.
   */
  void assertIsOwner(const SecurityIdentity &requester, const UserIdentity &user, const std::string &path) const;

  /**
   * Returns the directory entry corresponding to the specified path.
   *
   * @param requester The identity of the requester.
   * @param The absolute path of the namespace entry.
   */
  ArchiveDirEntry getArchiveDirEntry(
    const SecurityIdentity &requester,
    const std::string &path) const;

  /**
   * Returns the directory entry corresponding to the specified path and stat()
   * result.
   *
   * @param requester The identity of the requester.
   * @param The absolute path of the namespace entry.
   * @param statResult The result of running stat().
   */
  ArchiveDirEntry getArchiveDirEntry(
    const SecurityIdentity &requester,
    const std::string &path,
    const struct stat statResult) const;

  /**
   * The string name and numeric identifier of a storage class.
   */
  struct StorageClassNameAndId {
    std::string name;
    uint32_t id;

    StorageClassNameAndId(): id(0) {
    }

    StorageClassNameAndId(const std::string &name, const uint32_t id):
      name(name), id(id) {
    }
  };

  /**
   * The list of storage class.
   */
  std::list<StorageClassNameAndId> m_storageClasses;

  /**
   * Throws an exception if the specified storage class name already exists.
   *
   * @paran name The name of teh storage class.
   */
  void assertStorageClassNameDoesNotExist(const std::string &name) const;

  /**
   * Throws an exception if the specified storage class numeric identifier
   * already exists.
   *
   * @param id The numeric identifier of the storage class.
   */
  void assertStorageClassIdDoesNotExist(const uint32_t id) const;

  /**
   * Returns the next unique numeric identifier for a new storage class.
   *
   * Please note that the numeric identifiers of deleted storage classes can be
   * reused.
   *
   * @return The next unique numeric identifier for a new storage class.
   */
  uint32_t getNextStorageClassId() const;
  
  /**
   * Counter for file ID of new files
   */
  std::atomic<uint64_t> m_fileIdCounter;

}; // class MockNameServer

} // namespace cta
