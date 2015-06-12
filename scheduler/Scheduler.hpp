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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta {

// Forward declarations for opaque references.
class AdminHost;
class AdminUser;
class ArchivalRoute;
class ArchiveToDirRequest;
class ArchiveToFileRequest;
class ArchiveToTapeCopyRequest;
class DirEntry;
class DirIterator;
class LogicalLibrary;
class NameServer;
class RemoteNS;
class RetrieveFromTapeCopyRequest;
class RetrieveToDirRequest;
class RetrieveToFileRequest;
class SchedulerDatabase;
class SecurityIdentity;
class StorageClass;
class Tape;
class TapePool;
class UserIdentity;

/**
 * Class implementimg a tape resource scheduler.
 */
class Scheduler {
public:

  /**
   * Constructor.
   *
   * @param ns The name server containing the namespace of the archive.
   * @param db The scheduler database.
   * @param remoteNS The name space of the remote storage system.
   */
  Scheduler(
    NameServer &ns,
    SchedulerDatabase &db,
    RemoteNS &remoteNS);

  /**
   * Destructor.
   */
  ~Scheduler() throw();

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
  std::map<Tape, std::list<RetrieveFromTapeCopyRequest> > getRetrieveRequests(
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
  std::list<RetrieveFromTapeCopyRequest> getRetrieveRequests(
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
  std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester)
   const;

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
  std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester) const;

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
   * Creates the specified archival route.
   *
   * @param requester The identity of the user requesting the creation of the
   * archival route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param comment The comment describing the archival route.
   */
  void createArchivalRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment);

  /**
   * Deletes the specified archival route.
   *
   * @param requester The identity of the user requesting the deletion of the
   * archival route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void deleteArchivalRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint16_t copyNb);

  /**
   * Gets the current list of archival routes.
   *
   * @param requester The identity of the user requesting the list.
   */
  std::list<ArchivalRoute> getArchivalRoutes(
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
   * @param requester The identity of the user requesting the creation of the
   * tape.
   * @param vid The volume identifier of the tape.
   * @param logicalLibraryName The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param comment The comment describing the logical library.
   */
  void createTape(
    const SecurityIdentity &requester,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const std::string &comment);

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param vid The volume identifier of the tape.
   */
  void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid);

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @param requester The identity of the user requesting the list.
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
    const uint16_t mode);

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
   * Returns true if the specified regular file exists.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  bool regularFileExists(
    const SecurityIdentity &requester,
    const std::string &path) const;

  /**
   * Returns true if the specified directory exists.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the file.
   * @return True if the specified directory exists.
   */
  bool dirExists(
    const SecurityIdentity &requester,
    const std::string &path) const;
  
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
  DirIterator getDirContents(
    const SecurityIdentity &requester,
    const std::string &path) const;

  /**
   * Returns the directory entry information for the specified directory or file
   * within the archive namespace.
   *
   * @param requester The identity of the requester.
   * @param path The absolute path of the directory or file within the archive
   * namespace.
   * @return The directory entry information for the specified directory or file
   * within the archive namespace.
   */
  DirEntry statDirEntry(
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
   * @param requester The identity of the user requesting the archival.
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
   * @param requester The identity of the user requesting the retrieval.
   * @param archiveFiles The full path of each source file in the archive
   * namespace.
   * @param remoteFileOrDir The URL of the destination remote file or directory.
   */
  void queueRetrieveRequest(
    const SecurityIdentity &requester,
    const std::list<std::string> &archiveFiles,
    const std::string &remoteFileOrDir);

private:

  /**
   * The name server containing the namespace of the archive.
   */
  NameServer &m_ns;

  /**
   * The scheduler database.
   */
  SchedulerDatabase &m_db;

  /**
   * The name space of the remote storage system.
   */
  RemoteNS &m_remoteNS;

  /**
   * Queues the specified request to archive one or more remote files to an
   * archive directory.
   *
   * The list of source files should contain at least one file.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the user requesting the archival.
   * @param remoteFiles The URLs of one or more remote files.
   * @param archiveDir The full path of the destination directory within the
   * archive namespace.
   */
  void queueArchiveToDirRequest(
    const SecurityIdentity &requester,
    const std::list<std::string> &remoteFiles,
    const std::string &archiveDir);

  /**
   * Throws an exception if the specified storage class has a tape copy count
   * of 0.
   *
   * @param storageClass The storage class.
   */
  void assertStorageClassHasAtLeastOneCopy(
    const StorageClass &storageClass) const;

  /**
   * Returns the list of ArchiveToFileRequest objects representing the specified
   * request to archive multiple files to an archive directory.
   *
   * @param requester The identity of the requester.
   * @param remoteFiles The URLs of the remote files.
   * @param archiveFile The full path of the destination directory within the
   * archive namespace.
   * @param priority The priority of the request.
   */
  std::list<ArchiveToFileRequest> createArchiveToFileRequests(
    const SecurityIdentity &requester,
    const std::list<std::string> &remoteFiles,
    const std::string &archiveDir,
    const uint64_t priority);

  /**
   * Queues the specifed request to archive one remote file to on archive file.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the requester.
   * @param remoteFile The URL of the remote file.
   * @param archiveFile The full path of the destination file within the
   * archive namespace.
   */
  void queueArchiveToFileRequest(
    const SecurityIdentity &requester,
    const std::string &remoteFile,
    const std::string &archiveFile);

  /**
   * Returns the ArchiveToFileRequest object representing the specified request.
   *
   * @param requester The identity of the requester.
   * @param remoteFile The URL of the remote file.
   * @param archiveFile The full path of the destination file within the
   * archive namespace.
   * @param priority The priority of the request.
   */
  ArchiveToFileRequest createArchiveToFileRequest(
    const SecurityIdentity &requester,
    const std::string &remoteFile,
    const std::string &archiveFile,
    const uint64_t priority) const;

  /**
   * Returns the map from tape copy number to tape pool name for the specified
   * set of archival routes.
   *
   * @param routes The archive routes.
   * @return The map from tape copy number to tape pool name for the specified
   * set of archival routes.
   */
  std::map<uint16_t, std::string> createCopyNbToPoolMap(
    const std::list<ArchivalRoute> &routes) const;

}; // class Scheduler

} // namespace cta
