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
#include "common/archiveNS/ArchiveFileStatus.hpp"
#include "common/SecurityIdentity.hpp"
#include "nameserver/NameServerTapeFile.hpp"

#include <memory>
#include <string>

namespace cta {

/**
 * Abstract class specifying the interface to the name server containing the
 * archive namespace.
 */
class NameServer {
public:

  /**
   * Destructor.
   */
  virtual ~NameServer() throw() = 0;

  /**
   * Creates the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @param mode The mode bits of the directory entry.
   */
  virtual void createDir(
    const SecurityIdentity &requester,
    const std::string &path,
    const mode_t mode) = 0;

  /**
   * Deletes the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   */
  virtual void deleteDir(
   const SecurityIdentity &requester,
   const std::string &path) = 0;
  
  /**
   * Returns the volume identifier of the tape on which the specified tape copy
   * has been archived.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   * @param copyNb The copy number of the file.
   */
  virtual std::string getVidOfFile(
    const SecurityIdentity &requester,
    const std::string &path,
    const uint16_t copyNb) const = 0;

  /**
   * Gets the contents of the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @return An iterator over the contents of the directory.
   */
  virtual ArchiveDirIterator getDirContents(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

  /**
   * Returns the status of the specified file or directory within the archive
   * namespace or NULL if the file or directory does not exist.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file or directory within the archive
   * namespace.
   * @return The status of the file or directory or NULL if the file or
   * directory does not exist.
   */
  virtual std::unique_ptr<ArchiveFileStatus> statFile(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies) = 0;

  /**
   * Creates the specified storage class with the specified numeric indentifier.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param id The numeric indentifier of the storage class.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies,
    const uint32_t id) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  virtual void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Updates the specified storage class with the specified number of tape
   * copies.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  virtual void updateStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies) = 0;

  /**
   * Sets the storage class of the specified directory to the specified value.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @param storageClassName The name of the storage class.
   */
  virtual void setDirStorageClass(
    const SecurityIdentity &requester,
    const std::string &path,
    const std::string &storageClassName) = 0;

  /**
   * Clears the storage class of the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   */
  virtual void clearDirStorageClass(
    const SecurityIdentity &requester,
    const std::string &path) = 0;

  /**
   * Returns the name of the storage class of the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @return The name of the storage class of the specified directory.
   */
  virtual std::string getDirStorageClass(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;
 
  /**
   * Throws an exception if the specified storage class is in use within the
   * specified file or directory. This is a nice to have but probably impractical,
   * due to the fact that to manage large name spaces one would need a running counter
   * of storage class usages, which could be a major area of contention.
   *
   * @param requester The identity of the requester.
   * @param storageClassName The name of the storage class.
   * @param path The absolute path of the file or directory.
   */
  virtual void assertStorageClassIsNotInUse(
    const SecurityIdentity &requester,
    const std::string &storageClass,
    const std::string &path) const = 0;

  /**
   * Creates the specified file entry.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   * @param mode The mode bits of the file entry.
   */
  virtual void createFile(
    const SecurityIdentity &requester,
    const std::string &path,
    const mode_t mode,
    const uint64_t size) = 0;

  /**
   * Sets the owner of the specified file or directory entry.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file or directory.
   * @param owner The owner.
   */
  virtual void setOwner(
    const SecurityIdentity &requester,
    const std::string &path,
    const UserIdentity &owner) = 0;

  /**
   * Returns the owner of the specified file or directory entry.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file or directory.
   * @return The owner of the specified file or directory entry.
   */
  virtual UserIdentity getOwner(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

  /**
   * Deletes the specified file entry.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   */
  virtual void deleteFile(
    const SecurityIdentity &requester,
    const std::string &path) = 0;

  /**
   * Add the specified tape file entry to the archive namespace.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the archive file.
   * @param nameServerTapeFile The tape file entry.
   */
  virtual void addTapeFile(
    const SecurityIdentity &requester,
    const std::string &path,
    const NameServerTapeFile &tapeFile) = 0;
  
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
    const uint16_t copyNb) = 0;

  /**
   * Gets the tape entries from the archive namespace corresponding the archive
   * with the specified path.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the archive file.
   * @return The tape file entries.
   */
  virtual std::list<NameServerTapeFile> getTapeFiles(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

}; // class NameServer

} // namespace 
