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

#include "scheduler/Scheduler.hpp"

#include <memory>

namespace cta {

/**
 * Class implementimg a dummy tape resource scheduler.
 */
class DummyScheduler: public Scheduler {
public:

  /**
   * Constructor.
   *
   * @param ns The name server containing the namespace of the archive.
   * @param db The scheduler database.
   * @param remoteNS The name space of the remote storage system.
   */
  DummyScheduler(
    NameServer &ns,
    SchedulerDatabase &db,
    RemoteNS &remoteNS);

  /**
   * Destructor.
   */
  ~DummyScheduler() throw();

  /**
   * Returns all of the queued archive requests.  The returned requests are
   * grouped by tape pool and then sorted by creation time, oldest first.
   *
   * @param requester The identity of the user requesting the list.
   * @return The queued requests.
   */
  std::map<TapePool, std::list<ArchiveToTapeCopyRequest> > getArchiveRequests(
    const SecurityIdentity &requester) const;

  /**
   * Returns the list of queued archive requests for the specified tape pool.
   * The returned requests are sorted by creation time, oldest first.
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The queued requests.
   */
  std::list<ArchiveToTapeCopyRequest> getArchiveRequests(
    const SecurityIdentity &requester,
    const std::string &tapePoolName) const;

  /**
   * Deletes the specified archive request.
   *
   * @param requester The identity of the requester.
   * @param archiveFile The absolute path of the destination file within the
   * archive namespace.
   */
  void deleteArchiveRequest(
    const SecurityIdentity &requester,
    const std::string &remoteFile);

  /**
   * Returns all of the queued retrieve requests.  The returned requests are
   * grouped by tape and then sorted by creation time, oldest first.
   *
   * @param requester The identity of requester.
   * @return all of the queued retrieve requests.  The returned requsts are
   * grouped by tape and then sorted by creation time, oldest first.
   */
  std::map<Tape, std::list<RetrieveRequestDump> > getRetrieveRequests(
    const SecurityIdentity &requester) const;

  /**
   * Returns the queued retrieve requests for the specified tape.  The
   * returned requests are sorted by creation time, oldest first.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   * @return The queued retrieve requests for the specified tape.  The
   * returned requests are sorted by creation time, oldest first.
   */
  std::list<RetrieveRequestDump> getRetrieveRequests(
    const SecurityIdentity &requester,
    const std::string &vid) const;
  
  /**
   * Deletes the specified retrieve request.
   *
   * @param requester The identity of the requester.
   * @param remoteFile The URL of the remote file.
   */
  void deleteRetrieveRequest(
    const SecurityIdentity &requester,
    const std::string &remoteFile);

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the requester.
   * @param user The identity of the administrator.
   * @param comment The comment describing the sministrator.
   */
  void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment);

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
  void createAdminUserWithoutAuthorizingRequester(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment);

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param user The identity of the administrator.
   */
  void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user);

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administrators in lexicographical order.
   */
  std::list<common::admin::AdminUser> getAdminUsers(const SecurityIdentity &requester) const;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the requester.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment);

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
  void createAdminHostWithoutAuthorizingRequester(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment);

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param hostName The network name of the administration host.
   */
  void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName);

  /**
   * Returns the current list of administration hosts in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administration hosts in lexicographical order.
   */
  std::list<common::admin::AdminHost> getAdminHosts(const SecurityIdentity &requester) const;

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
  void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies,
    const std::string &comment);

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
  void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbCopies,
    const uint32_t id,
    const std::string &comment);

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  std::list<StorageClass> getStorageClasses(
    const SecurityIdentity &requester) const;

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
  void createTapePool(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint32_t nbPartialTapes,
    const std::string &comment);

  /**
   * Delete the tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape pool.
   * @param name The name of the tape pool.
   */
  void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tape pools in lexicographical order.
   */
  std::list<TapePool> getTapePools(
    const SecurityIdentity &requester) const;

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
  void createArchiveRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment);

  /**
   * Deletes the specified archive route.
   *
   * @param requester The identity of the user requesting the deletion of the
   * archive route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void deleteArchiveRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb);

  /**
   * Gets the current list of archive routes.
   *
   * @param requester The identity of the user requesting the list.
   */
  std::list<ArchiveRoute> getArchiveRoutes(
    const SecurityIdentity &requester) const;

  /**
   * Creates a logical library with the specified name.
   *
   * @param requester The identity of the user requesting the creation of the
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment);

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * logical library.
   * @param name The name of the logical library.
   */
  void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of libraries in lexicographical order.
   */
  std::list<LogicalLibrary> getLogicalLibraries(
    const SecurityIdentity &requester) const;

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
  void createTape(
    const SecurityIdentity &requester,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const CreationLog &creationLog);

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   */
  void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid);

  /**
   * Returns the tape with the specified volume identifier.
   *
   * @param requester The identity of the requester.
   * @param vid The volume identifier of the tape.
   * @return The tape with the specified volume identifier.
   */
  Tape getTape(
    const SecurityIdentity &requester,
    const std::string &vid) const;

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @param requester The identity of the requester
   * @return The current list of tapes in the lexicographical order of their
   * volume identifiers.
   */
  std::list<Tape> getTapes(
    const SecurityIdentity &requester) const;

  /**
   * Creates the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @param mode The mode bits of the directory entry.
   */
  void createDir(
    const SecurityIdentity &requester,
    const std::string &path,
    const mode_t mode);
  
  /**
   * Sets the owner of the specified file or directory entry.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file or directory.
   * @param owner The owner.
   */
  void setOwner(
    const SecurityIdentity &requester,
    const std::string &path,
    const UserIdentity &owner);
    
  /**
   * Returns the owner of the specified file or directory entry.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file or directory.
   * @return The owner of the specified file or directory entry.
   */
  virtual UserIdentity getOwner(
    const SecurityIdentity &requester,
    const std::string &path) const;

  /**
   * Deletes the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   */
  void deleteDir(
   const SecurityIdentity &requester,
   const std::string &path);

  /**
   * Returns the volume identifier of the tape on which the specified tape copy
   * has been archived.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   * @param copyNb The copy number of the file.
   */
  std::string getVidOfFile(
    const SecurityIdentity &requester,
    const std::string &path,
    const uint16_t copyNb) const;

  /**
   * Gets the contents of the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @return An iterator over the contents of the directory.
   */
  common::archiveNS::ArchiveDirIterator getDirContents(
    const SecurityIdentity &requester,
    const std::string &path) const;

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
  std::unique_ptr<ArchiveFileStatus> statArchiveFile(
    const SecurityIdentity &requester,
    const std::string &path) const;

  /**
   * Sets the storage class of the specified directory to the specified value.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @param storageClassName The name of the storage class.
   */
  void setDirStorageClass(
    const SecurityIdentity &requester,
    const std::string &path,
    const std::string &storageClassName);

  /**
   * Clears the storage class of the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   */ 
  void clearDirStorageClass(
    const SecurityIdentity &requester,
    const std::string &path);

  /**
   * Returns the name of the storage class of the specified directory.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory.
   * @return The name of the storage class of the specified directory.
   */
  std::string getDirStorageClass(
    const SecurityIdentity &requester,
    const std::string &path) const;

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
  void queueArchiveRequest(
    const SecurityIdentity &requester,
    const std::list<std::string> &remoteFiles,
    const std::string &archiveFileOrDir);

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
  void queueRetrieveRequest(
    const SecurityIdentity &requester,
    const std::list<std::string> &archiveFiles,
    const std::string &remoteFileOrDir);
  
  /**
   * Returns the next tape mount for the specified logical library.
   * All the functions of the mount will handled via the mount object
   * itself. This is the entry point to the tape server's interface
   *
   * @param logicalLibraryName The name of the logical library.
   * @param driveName The drive's name.
   * @return The next tape mount or NULL if there is currently no work to do.
   */
  std::unique_ptr<TapeMount> getNextMount(const std::string &logicalLibraryName,
    const std::string & driveName);

}; // class DummyScheduler

} // namespace cta
