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

  virtual void createBootstrapAdminAndHostNoAuth(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user, const std::string &hostName, const std::string &comment);

  virtual void createAdminUser(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user, const std::string &comment);
  virtual void deleteAdminUser(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user);
  virtual std::list<cta::dataStructures::AdminUser> getAdminUsers(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyAdminUserComment(const cta::dataStructures::SecurityIdentity &requester, const cta::dataStructures::UserIdentity &user, const std::string &comment);

  virtual void createAdminHost(const cta::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment);
  virtual void deleteAdminHost(const cta::dataStructures::SecurityIdentity &requester, const std::string &hostName);
  virtual std::list<cta::dataStructures::AdminHost> getAdminHosts(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyAdminHostComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &hostName, const std::string &comment);

  virtual void createStorageClass(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment);
  virtual void deleteStorageClass(const cta::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::dataStructures::StorageClass> getStorageClasses(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyStorageClassNbCopies(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies);
  virtual void modifyStorageClassComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createTapePool(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const std::string &comment);
  virtual void deleteTapePool(const cta::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::dataStructures::TapePool> getTapePools(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyTapePoolNbPartialTapes(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes);
  virtual void modifyTapePoolComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createArchiveRoute(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName, const std::string &comment);
  virtual void deleteArchiveRoute(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb);
  virtual std::list<cta::dataStructures::ArchiveRoute> getArchiveRoutes(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyArchiveRouteTapePoolName(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName);
  virtual void modifyArchiveRouteComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment);

  virtual void createLogicalLibrary(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);
  virtual void deleteLogicalLibrary(const cta::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::dataStructures::LogicalLibrary> getLogicalLibraries(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyLogicalLibraryComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, 
                          const bool disabledValue, const bool fullValue, const std::string &comment);
  virtual void deleteTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid);
  virtual std::list<cta::dataStructures::Tape> getTapes(const cta::dataStructures::SecurityIdentity &requester, const std::map<std::string, std::string> &where); // "where" is a map resembling an "and-ed" where clause in a SQL query
  virtual void labelTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool force, const bool lbp, const std::string &tag);
  virtual void reclaimTape(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid);
  virtual void modifyTapeLogicalLibraryName(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName);
  virtual void modifyTapeTapePoolName(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName);
  virtual void modifyTapeCapacityInBytes(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes);
  virtual void setTapeBusy(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool busyValue); // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool fullValue);
  virtual void setTapeDisabled(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const bool disabledValue);
  virtual void modifyTapeComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &vid, const std::string &comment);

  virtual void createUser(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup, const std::string &comment);
  virtual void deleteUser(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group);
  virtual std::list<cta::dataStructures::User> getUsers(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyUserUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &userGroup);
  virtual void modifyUserComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &group, const std::string &comment);

  virtual void createUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority, const uint64_t minArchiveFilesQueued, 
                               const uint64_t minArchiveBytesQueued, const uint64_t minArchiveRequestAge, const uint32_t retrievePriority, const uint64_t minRetrieveFilesQueued,
                               const uint64_t minRetrieveBytesQueued, const uint64_t minRetrieveRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment);
  virtual void deleteUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &name);
  virtual std::list<cta::dataStructures::UserGroup> getUserGroups(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyUserGroupArchivePriority(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t archivePriority);
  virtual void modifyUserGroupArchiveMinFilesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveFilesQueued);
  virtual void modifyUserGroupArchiveMinBytesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveBytesQueued);
  virtual void modifyUserGroupArchiveMinRequestAge(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minArchiveRequestAge);
  virtual void modifyUserGroupRetrievePriority(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint32_t retrievePriority);
  virtual void modifyUserGroupRetrieveMinFilesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveFilesQueued);
  virtual void modifyUserGroupRetrieveMinBytesQueued(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveBytesQueued);
  virtual void modifyUserGroupRetrieveMinRequestAge(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint64_t minRetrieveRequestAge);
  virtual void modifyUserGroupMaxDrivesAllowed(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed);
  virtual void modifyUserGroupComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createDedication(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly, const bool writeonly, const std::string &userGroup,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment);
  virtual void deleteDedication(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename);
  virtual std::list<cta::dataStructures::Dedication> getDedications(const cta::dataStructures::SecurityIdentity &requester) const;
  virtual void modifyDedicationReadonly(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool readonly);
  virtual void modifyDedicationWriteonly(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const bool writeonly);
  virtual void modifyDedicationUserGroup(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &userGroup);
  virtual void modifyDedicationTag(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &tag);
  virtual void modifyDedicationVid(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &vid);
  virtual void modifyDedicationFrom(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp);
  virtual void modifyDedicationUntil(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp);
  virtual void modifyDedicationComment(const cta::dataStructures::SecurityIdentity &requester, const std::string &drivename, const std::string &comment);

  virtual std::list<cta::dataStructures::ArchiveFile> getArchiveFiles(const cta::dataStructures::SecurityIdentity &requester, const std::map<std::string, std::string> &where); // "where" is a map resembling an "and-ed" where clause in a SQL query

  virtual void setDriveStatus(const cta::dataStructures::SecurityIdentity &requester, const std::string &driveName, const bool up, const bool force);
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
    const cta::dataStructures::ArchiveRequest &archiveRequest,
    const cta::dataStructures::TapeFileLocation tapeFileLocation);

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
