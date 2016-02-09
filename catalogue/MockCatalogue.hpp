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

#include "catalogue/Catalogue.hpp"
#include "catalogue/SQLiteDatabase.hpp"

// The header file for atomic was is actually called cstdatomic in gcc 4.4
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else
  #include <atomic>
#endif

#include <string>

namespace cta {
namespace catalogue {

/**
 * Mock CTA catalogue to facilitate unit testing.
 */
class MockCatalogue: public Catalogue {
public:

  /**
   * Constructor.
   */
  MockCatalogue();

  /**
   * Destructor.
   */
  virtual ~MockCatalogue();

  virtual void createBootstrapAdminAndHostNoAuth(const SecurityIdentity &requester, const UserIdentity &user, const std::string &hostName, const std::string &comment);

  virtual void createAdminUser(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment);
  virtual void deleteAdminUser(const SecurityIdentity &requester, const UserIdentity &user);
  virtual std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester) const;
  virtual void modifyAdminUserComment(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment);

  virtual void createAdminHost(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment);
  virtual void deleteAdminHost(const SecurityIdentity &requester, const std::string &hostName);
  virtual std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester) const;
  virtual void modifyAdminHostComment(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment);

  virtual void createStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment);
  virtual void deleteStorageClass(const SecurityIdentity &requester, const std::string &name);
  virtual std::list<StorageClass> getStorageClasses(const SecurityIdentity &requester) const;
  virtual void modifyStorageClassNbCopies(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies);
  virtual void modifyStorageClassComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createTapePool(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const std::string &comment);
  virtual void deleteTapePool(const SecurityIdentity &requester, const std::string &name);
  virtual std::list<TapePool> getTapePools(const SecurityIdentity &requester) const;
  virtual void modifyTapePoolNbPartialTapes(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes);
  virtual void modifyTapePoolComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName, const std::string &comment);
  virtual void deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb);
  virtual std::list<ArchiveRoute> getArchiveRoutes(const SecurityIdentity &requester) const;
  virtual void modifyArchiveRouteTapePoolName(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName);
  virtual void modifyArchiveRouteComment(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment);

  virtual void createLogicalLibrary(const SecurityIdentity &requester, const std::string &name, const std::string &comment);
  virtual void deleteLogicalLibrary(const SecurityIdentity &requester, const std::string &name);
  virtual std::list<LogicalLibrary> getLogicalLibraries(const SecurityIdentity &requester) const;
  virtual void modifyLogicalLibraryComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createTape(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, 
                          const bool disabledValue, const bool fullValue, const std::string &comment);
  virtual void deleteTape(const SecurityIdentity &requester, const std::string &vid);
  virtual std::list<Tape> getTapes(const SecurityIdentity &requester, const std::map<std::string, std::string> &where); // "where" is a map resembling an "and-ed" where clause in a SQL query
  virtual void labelTape(const SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag);
  virtual void reclaimTape(const SecurityIdentity &requester, const std::string &vid);
  virtual void modifyTapeLogicalLibraryName(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName);
  virtual void modifyTapeTapePoolName(const SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName);
  virtual void modifyTapeCapacityInBytes(const SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes);
  virtual void setTapeBusy(const SecurityIdentity &requester, const std::string &vid, const bool busyValue); // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const SecurityIdentity &requester, const std::string &vid, const bool fullValue);
  virtual void setTapeDisabled(const SecurityIdentity &requester, const std::string &vid, const bool disabledValue);
  virtual void modifyTapeComment(const SecurityIdentity &requester, const std::string &vid, const std::string &comment);

  virtual void createUser(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup, const std::string &comment);
  virtual void deleteUser(const SecurityIdentity &requester, const std::string &name, const std::string &group);
  virtual std::list<User> getUsers(const SecurityIdentity &requester) const;
  virtual void modifyUserUserGroup(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup);
  virtual void modifyUserComment(const SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment);

  virtual void createUserGroup(const SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint32_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment);
  virtual void deleteUserGroup(const SecurityIdentity &requester, const std::string &name);
  virtual std::list<UserGroup> getUserGroups(const SecurityIdentity &requester) const;
  virtual void modifyUserGroupArchivePriority(const SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority);
  virtual void modifyUserGroupArchiveMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued);
  virtual void modifyUserGroupArchiveMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued);
  virtual void modifyUserGroupArchiveMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge);
  virtual void modifyUserGroupRetrievePriority(const SecurityIdentity &requester, const std::string &name, const uint32_t retrievePriority);
  virtual void modifyUserGroupRetrieveMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued);
  virtual void modifyUserGroupRetrieveMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued);
  virtual void modifyUserGroupRetrieveMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge);
  virtual void modifyUserGroupMaxDrivesAllowed(const SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed);
  virtual void modifyUserGroupComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createDedication(const SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment);
  virtual void deleteDedication(const SecurityIdentity &requester, const std::string &drivename);
  virtual std::list<Dedication> getDedications(const SecurityIdentity &requester) const;
  virtual void modifyDedicationReadonly(const SecurityIdentity &requester, const std::string &drivename, const bool readonly);
  virtual void modifyDedicationWriteonly(const SecurityIdentity &requester, const std::string &drivename, const bool writeonly);
  virtual void modifyDedicationUserGroup(const SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup);
  virtual void modifyDedicationTag(const SecurityIdentity &requester, const std::string &drivename, const std::string &tag);
  virtual void modifyDedicationVid(const SecurityIdentity &requester, const std::string &drivename, const std::string &vid);
  virtual void modifyDedicationFrom(const SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp);
  virtual void modifyDedicationUntil(const SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp);
  virtual void modifyDedicationComment(const SecurityIdentity &requester, const std::string &drivename, const std::string &comment);

  virtual std::list<ArchiveFile> getArchiveFiles(const SecurityIdentity &requester, const std::map<std::string, std::string> &where); // "where" is a map resembling an "and-ed" where clause in a SQL query

  virtual void setDriveStatus(const SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force);
  /**
   * Returns the next identifier to be used for a new archive file.
   *
   * @return The next identifier to be used for a new archive file.
   */
  virtual uint64_t getNextArchiveFileId();

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
    const uint64_t blockId);

private:

  /**
   * SQLite database handle.
   */
  SQLiteDatabase m_db;

  /**
   * The next identifier to be used for a new archive file.
   */
  std::atomic<uint64_t> m_nextArchiveFileId;

}; // class MockCatalogue

} // namespace catalogue
} // namespace cta
