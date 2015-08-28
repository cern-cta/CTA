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

#include "common/exception/Exception.hpp"
#include "scheduler/TapeJobFailure.hpp"

#include <list>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

namespace cta {

// Forward declarations for opaque references.
class AdminHost;
class AdminUser;
class ArchiveFileStatus;
class ArchiveRoute;
class ArchiveToDirRequest;
class ArchiveToFileRequest;
class ArchiveToTapeCopyRequest;
class ArchiveDirIterator;
class CreationLog;
class LogicalLibrary;
class NameServer;
class RemoteNS;
class RemotePathAndStatus;
class RetrieveFromTapeCopyRequest;
class RetrieveToDirRequest;
class RetrieveToFileRequest;
class SchedulerDatabase;
class SecurityIdentity;
class StorageClass;
class Tape;
class TapeMount;
class TapeSession;
class TapePool;
class UserIdentity;

/**
 * Abstract class defining the interface to a tape resource scheduler.
 */
class IScheduler {
public:

  /**
   * Destructor.
   */
  virtual ~IScheduler() throw() = 0;

  /**
   * Returns all of the queued archive requests.  The returned requests are
   * grouped by tape pool and then sorted by creation time, oldest first.
   *
   * @param requester The identity of the user requesting the list.
   * @return The queued requests.
   */
  virtual std::map<TapePool, std::list<ArchiveToTapeCopyRequest> >
    getArchiveRequests(const SecurityIdentity &requester) const = 0;

  /**
   * Returns the list of queued archive requests for the specified tape pool.
   * The returned requests are sorted by creation time, oldest first.
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The queued requests.
   */
  virtual std::list<ArchiveToTapeCopyRequest> getArchiveRequests(
    const SecurityIdentity &requester,
    const std::string &tapePoolName) const = 0;

  /**
   * Deletes the specified archive request.
   *
   * @param requester The identity of the requester.
   * @param archiveFile The absolute path of the destination file within the
   * archive namespace.
   */
  virtual void deleteArchiveRequest(
    const SecurityIdentity &requester,
    const std::string &remoteFile) = 0;

  /**
   * Returns all of the queued retrieve requests.  The returned requests are
   * grouped by tape and then sorted by creation time, oldest first.
   *
   * @param requester The identity of requester.
   * @return all of the queued retrieve requests.  The returned requsts are
   * grouped by tape and then sorted by creation time, oldest first.
   */
  virtual std::map<Tape, std::list<RetrieveFromTapeCopyRequest> >
    getRetrieveRequests(const SecurityIdentity &requester) const = 0;

  /**
   * Returns the queued retrieve requests for the specified tape.  The
   * returned requests are sorted by creation time, oldest first.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   * @return The queued retrieve requests for the specified tape.  The
   * returned requests are sorted by creation time, oldest first.
   */
  virtual std::list<RetrieveFromTapeCopyRequest> getRetrieveRequests(
    const SecurityIdentity &requester,
    const std::string &vid) const = 0;
  
  /**
   * Deletes the specified retrieve request.
   *
   * @param requester The identity of the requester.
   * @param remoteFile The URL of the remote file.
   */
  virtual void deleteRetrieveRequest(
    const SecurityIdentity &requester,
    const std::string &remoteFile) = 0;

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the requester.
   * @param user The identity of the administrator.
   * @param comment The comment describing the sministrator.
   */
  virtual void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment) = 0;

  /**
   * Creates the specified administrator without performing any authorisation
   * checks.
   *
   * This method provides a way to bootstrap the list of administrators.
   * This method does not perform any authorizations checks therefore please
   * take any necessary precautions before calling this method.
   *
   * @param requester The identity of the user requesting the creation of the
   * administrator.
   * @param user The identity of the administrator.
   * @param comment The comment describing the sministrator.
   */
  virtual void createAdminUserWithoutAuthorizingRequester(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment) = 0;

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param user The identity of the administrator.
   */
  virtual void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user) = 0;

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administrators in lexicographical order.
   */
  virtual std::list<AdminUser> getAdminUsers(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the requester.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  virtual void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment) = 0;

  /**
   * Creates the specified administration host with performing any authorisation
   * checks.
   *
   * This method provides a way to bootstrap the list of administration hosts.
   * This method does not perform any authorizations checks therefore please
   * take any necessary precautions before calling this method.
   *
   * @param requester The identity of the requester.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  virtual void createAdminHostWithoutAuthorizingRequester(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param hostName The network name of the administration host.
   */
  virtual void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName) = 0;

  /**
   * Returns the current list of administration hosts in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administration hosts in lexicographical order.
   */
  virtual std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester)
    const = 0;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param comment The comment describing the storage class.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies,
    const std::string &comment) = 0;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param id The numeric identifer of the storage class.
   * @param comment The comment describing the storage class.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies,
    const uint32_t id,
    const std::string &comment) = 0;

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
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<StorageClass> getStorageClasses(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the creation of the
   * tape pool.
   * @param name The name of the tape pool.
   * @param nbPartialTapes The maximum number of tapes that can be partially
   * full at any moment in time.
   * @param comment The comment describing the tape pool.
   */
  virtual void createTapePool(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint32_t nbPartialTapes,
    const std::string &comment) = 0;

  /**
   * Delete the tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape pool.
   * @param name The name of the tape pool.
   */
  virtual void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tape pools in lexicographical order.
   */
  virtual std::list<TapePool> getTapePools(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates the specified archive route.
   *
   * @param requester The identity of the user requesting the creation of the
   * archive route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param comment The comment describing the archive route.
   */
  virtual void createArchiveRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified archive route.
   *
   * @param requester The identity of the user requesting the deletion of the
   * archive route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  virtual void deleteArchiveRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb) = 0;

  /**
   * Gets the current list of archive routes.
   *
   * @param requester The identity of the user requesting the list.
   */
  virtual std::list<ArchiveRoute> getArchiveRoutes(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a logical library with the specified name.
   *
   * @param requester The identity of the user requesting the creation of the
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  virtual void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment) = 0;

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * logical library.
   * @param name The name of the logical library.
   */
  virtual void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of libraries in lexicographical order.
   */
  virtual std::list<LogicalLibrary> getLogicalLibraries(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a tape.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   * @param logicalLibraryName The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param creationLog The who, where, when an why of this modification.
   */
  virtual void createTape(
    const SecurityIdentity &requester,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const CreationLog &creationLog) = 0;

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   */
  virtual void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid) = 0;

  /**
   * Returns the tape with the specified volume identifier.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   * @return The tape with the specified volume identifier.
   */
  virtual Tape getTape(
    const SecurityIdentity &requester,
    const std::string &vid) const = 0;

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @param requester The identity of the requester
   * @return The current list of tapes in the lexicographical order of their
   * volume identifiers.
   */
  virtual std::list<Tape> getTapes(
    const SecurityIdentity &requester) const = 0;

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
   * @return The status of the file or directory or NULL the the file or
   * directory does not exist.
   */
  virtual std::unique_ptr<ArchiveFileStatus> statArchiveFile(
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
   * Queues the specified request to archive one or more remote files.
   *
   * If there is more than one source file then the destination must be a
   * directory.
   *
   * If there is only one source file then the destination can be either a file
   * or a directory.
   *
   * The storage class of the archived file(s) will be inherited from the
   * destination directory.
   *
   * @param requester The identity of the user requesting the archive.
   * @param remoteFiles The URLs of one or more remote files.
   * @param archiveFileOrDir The absolute path of the destination file or
   * directory within the archive namespace.
   */
  virtual void queueArchiveRequest(
    const SecurityIdentity &requester,
    const std::list<std::string> &remoteFiles,
    const std::string &archiveFileOrDir) = 0;

  /**
   * Queues the specified request to retrieve one or more archived files.
   *
   * If there is more than one archived file then the destination must be a
   * directory.
   *
   * If there is only one archived file then the destination can be either a
   * file or a directory.
   *
   * @param requester The identity of the user requesting the retrieve.
   * @param archiveFiles The full path of each source file in the archive
   * namespace.
   * @param remoteFileOrDir The URL of the destination remote file or directory.
   */
  virtual void queueRetrieveRequest(
    const SecurityIdentity &requester,
    const std::list<std::string> &archiveFiles,
    const std::string &remoteFileOrDir) = 0;
  
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);

  /**
   * Returns the next tape mount for the specified logical library.
   * All the functions of the mount will handled via the mount object
   * itself. This is the entry point to the tape server's interface
   *
   * @param logicalLibraryName The name of the logical library.
   * @param driveName The drive's name.
   * @return The next tape mount or NULL if there is currently no work to do.
   */
  virtual std::unique_ptr<TapeMount> getNextMount(const std::string &logicalLibraryName,
    const std::string & driveName) = 0;

}; // class IScheduler

} // namespace cta
