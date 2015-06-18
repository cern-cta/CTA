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

#include "common/FileStatus.hpp"
#include "common/DirIterator.hpp"
#include "scheduler/SecurityIdentity.hpp"

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
  virtual DirIterator getDirContents(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

  /**
   * Returns the status of the specified file or directory within the archive
   * namespace.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file or directory within the archive
   * namespace.
   * @return The status of the file or directory.
   */
  virtual FileStatus statFile(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

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
   * specified file or directory.
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
   * @param path The absolute path of the directory.
   * @param mode The mode bits of the file entry.
   */
  virtual void createFile(
    const SecurityIdentity &requester,
    const std::string &path,
    const mode_t mode) = 0;

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
   * Returns true if the specified regular file exists.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  virtual bool regularFileExists(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

  /**
   * Returns true if the specified directory exists.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  virtual bool dirExists(
    const SecurityIdentity &requester,
    const std::string &path) const = 0;

}; // class NameServer

} // namespace 
