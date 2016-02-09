/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

#include "common/ArchiveRequest.hpp"

#include <list>
#include <stdint.h>
#include <string>
#include <map>

namespace cta {

class AdminHost {};
class AdminUser {};
class ArchiveFile {};
class ArchiveRoute {};
class Dedication {};
class LogicalLibrary {};
class SecurityIdentity {};
class StorageClass {};
class Tape {};
class TapePool {};
class User {};
class UserGroup {};
class UserIdentity {};

namespace catalogue {

/**
 * Abstract class defining the interface to the CTA catalogue responsible for
 * storing crticial information about archive files, tapes and tape files.
 */
class Catalogue {
public:

  /**
 * Destructor.
 */
  virtual ~Catalogue() = 0;
  
  virtual void createBootstrapAdminAndHostNoAuth(const SecurityIdentity &requester, const UserIdentity &user, const std::string &hostName, const std::string &comment) = 0;

  virtual void createAdminUser(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment) = 0;
  virtual void deleteAdminUser(const SecurityIdentity &requester, const UserIdentity &user) = 0;
  virtual std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester) const = 0;
  virtual void modifyAdminUserComment(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment) = 0;

  virtual void createAdminHost(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment) = 0;
  virtual void deleteAdminHost(const SecurityIdentity &requester, const std::string &hostName) = 0;
  virtual std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester) const = 0;
  virtual void modifyAdminHostComment(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment) = 0;

  virtual void createStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment) = 0;
  virtual void deleteStorageClass(const SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<StorageClass> getStorageClasses(const SecurityIdentity &requester) const = 0;
  virtual void modifyStorageClassNbCopies(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies) = 0;
  virtual void modifyStorageClassComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createTapePool(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const std::string &comment) = 0;
  virtual void deleteTapePool(const SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<TapePool> getTapePools(const SecurityIdentity &requester) const = 0;
  virtual void modifyTapePoolNbPartialTapes(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes) = 0;
  virtual void modifyTapePoolComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName, const std::string &comment) = 0;
  virtual void deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb) = 0;
  virtual std::list<ArchiveRoute> getArchiveRoutes(const SecurityIdentity &requester) const = 0;
  virtual void modifyArchiveRouteTapePoolName(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName) = 0;
  virtual void modifyArchiveRouteComment(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<LogicalLibrary> getLogicalLibraries(const SecurityIdentity &requester) const = 0;
  virtual void modifyLogicalLibraryComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createTape(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, 
                          const bool disabledValue, const bool fullValue, const std::string &comment) = 0;
  virtual void deleteTape(const SecurityIdentity &requester, const std::string &vid) = 0;
  virtual std::list<Tape> getTapes(const SecurityIdentity &requester, const std::map<std::string, std::string> &where) = 0; // "where" is a map resembling an "and-ed" where clause in a SQL query
  virtual void labelTape(const SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag) = 0;
  virtual void reclaimTape(const SecurityIdentity &requester, const std::string &vid) = 0;
  virtual void modifyTapeLogicalLibraryName(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeCapacityInBytes(const SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) = 0;
  virtual void setTapeBusy(const SecurityIdentity &requester, const std::string &vid, const bool busyValue) = 0; // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const SecurityIdentity &requester, const std::string &vid, const bool fullValue) = 0;
  virtual void setTapeDisabled(const SecurityIdentity &requester, const std::string &vid, const bool disabledValue) = 0;
  virtual void modifyTapeComment(const SecurityIdentity &requester, const std::string &vid, const std::string &comment) = 0;

  virtual void createUser(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup, const std::string &comment) = 0;
  virtual void deleteUser(const SecurityIdentity &requester, const std::string &name, const std::string &group) = 0;
  virtual std::list<User> getUsers(const SecurityIdentity &requester) const = 0;
  virtual void modifyUserUserGroup(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup) = 0;
  virtual void modifyUserComment(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment) = 0;

  virtual void createUserGroup(const SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint32_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment) = 0;
  virtual void deleteUserGroup(const SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<UserGroup> getUserGroups(const SecurityIdentity &requester) const = 0;
  virtual void modifyUserGroupArchivePriority(const SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority) = 0;
  virtual void modifyUserGroupArchiveMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued) = 0;
  virtual void modifyUserGroupArchiveMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued) = 0;
  virtual void modifyUserGroupArchiveMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge) = 0;
  virtual void modifyUserGroupRetrievePriority(const SecurityIdentity &requester, const std::string &name, const uint32_t retrievePriority) = 0;
  virtual void modifyUserGroupRetrieveMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued) = 0;
  virtual void modifyUserGroupRetrieveMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued) = 0;
  virtual void modifyUserGroupRetrieveMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge) = 0;
  virtual void modifyUserGroupMaxDrivesAllowed(const SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed) = 0;
  virtual void modifyUserGroupComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createDedication(const SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) = 0;
  virtual void deleteDedication(const SecurityIdentity &requester, const std::string &drivename) = 0;
  virtual std::list<Dedication> getDedications(const SecurityIdentity &requester) const = 0;
  virtual void modifyDedicationReadonly(const SecurityIdentity &requester, const std::string &drivename, const bool readonly) = 0;
  virtual void modifyDedicationWriteonly(const SecurityIdentity &requester, const std::string &drivename, const bool writeonly) = 0;
  virtual void modifyDedicationUserGroup(const SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup) = 0;
  virtual void modifyDedicationTag(const SecurityIdentity &requester, const std::string &drivename, const std::string &tag) = 0;
  virtual void modifyDedicationVid(const SecurityIdentity &requester, const std::string &drivename, const std::string &vid) = 0;
  virtual void modifyDedicationFrom(const SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) = 0;
  virtual void modifyDedicationUntil(const SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) = 0;
  virtual void modifyDedicationComment(const SecurityIdentity &requester, const std::string &drivename, const std::string &comment) = 0;

  virtual std::list<ArchiveFile> getArchiveFiles(const SecurityIdentity &requester, const std::map<std::string, std::string> &where) = 0; // "where" is a map resembling an "and-ed" where clause in a SQL query

  virtual void setDriveStatus(const SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force) = 0;

  /**
   * Returns the next identifier to be used for a new archive file.
   *
   * @return The next identifier to be used for a new archive file.
   */
  virtual uint64_t getNextArchiveFileId() = 0;

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param archiveRequest The identifier of the archive file.
   *
   */
  virtual void fileWrittenToTape(
 const ArchiveRequest &archiveRequest,
 const std::string vid,
 const uint64_t fSeq,
 const uint64_t blockId) = 0;

}; // class Catalogue

} // namespace catalogue
} // namespace cta
