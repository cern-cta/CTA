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

  virtual void createTapePool(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const uint64_t minFilesQueued, const uint64_t minBytesQueued, const uint64_t minRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment);
  virtual void deleteTapePool(const SecurityIdentity &requester, const std::string &name);
  virtual std::list<TapePool> getTapePools(const SecurityIdentity &requester) const;
  virtual void modifyTapePoolNbPartialTapes(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes);
  virtual void modifyTapePoolMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minFilesQueued);
  virtual void modifyTapePoolMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minBytesQueued);
  virtual void modifyTapePoolMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minRequestAge);
  virtual void modifyTapePoolMaxDrivesAllowed(const SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed);
  virtual void modifyTapePoolComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName,
                                                                                                             const std::string &comment);
  virtual void deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb);
  virtual std::list<ArchiveRoute> getArchiveRoutes(const SecurityIdentity &requester) const;
  virtual void modifyArchiveRouteTapePoolName(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName);
  virtual void modifyArchiveRouteComment(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment);

  virtual void createLogicalLibrary(const SecurityIdentity &requester, const std::string &name, const std::string &comment);
  virtual void deleteLogicalLibrary(const SecurityIdentity &requester, const std::string &name);
  virtual std::list<LogicalLibrary> getLogicalLibraries(const SecurityIdentity &requester) const;
  virtual void modifyLogicalLibraryComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createTape(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                                                                                                             const uint64_t capacityInBytes, const std::string &comment);
  virtual void deleteTape(const SecurityIdentity &requester, const std::string &vid);
  virtual Tape getTape(const SecurityIdentity &requester, const std::string &vid) const;
  virtual std::list<Tape> getTapesByLogicalLibrary(const SecurityIdentity &requester, const std::string &logicalLibraryName) const;
  virtual std::list<Tape> getTapesByPool(const SecurityIdentity &requester, const std::string &tapePoolName) const;
  virtual std::list<Tape> getTapesByCapacity(const SecurityIdentity &requester, const uint64_t capacityInBytes) const;
  virtual std::list<Tape> getAllTapes(const SecurityIdentity &requester) const;
  virtual void reclaimTape(const SecurityIdentity &requester, const std::string &vid);
  virtual void modifyTapeLogicalLibraryName(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName);
  virtual void modifyTapeTapePoolName(const SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName);
  virtual void modifyTapeCapacityInBytes(const SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes);
  virtual void modifyTapeComment(const SecurityIdentity &requester, const std::string &vid, const std::string &comment);

  virtual void createRetrieveUser(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup, const std::string &retrieveGroup,
                                                                                                             const std::string &comment);
  virtual void deleteRetrieveUser(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup);
  virtual std::list<RetrieveUser> getRetrieveUsers(const SecurityIdentity &requester) const;
  virtual void modifyRetrieveUserRetrieveGroup(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup, const std::string &retrieveGroup);
  virtual void modifyRetrieveUserComment(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup, const std::string &comment);

  virtual void createRetrieveGroup(const SecurityIdentity &requester, const std::string &name, const uint64_t minFilesQueued, const uint64_t minBytesQueued, const uint64_t minRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment);
  virtual void deleteRetrieveGroup(const SecurityIdentity &requester, const std::string &name);
  virtual std::list<RetrieveGroup> getRetrieveGroups(const SecurityIdentity &requester) const;
  virtual void modifyRetrieveGroupMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minFilesQueued);
  virtual void modifyRetrieveGroupMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minBytesQueued);
  virtual void modifyRetrieveGroupMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minRequestAge);
  virtual void modifyRetrieveGroupMaxDrivesAllowed(const SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed);
  virtual void modifyRetrieveGroupComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment);

  virtual void createDedication(const SecurityIdentity &requester, const std::string &drivename, const bool self, const bool readonly, const bool writeonly, const std::string &VO, const std::string &instanceName, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment);
  virtual void deleteDedication(const SecurityIdentity &requester, const std::string &drivename);
  virtual std::list<Dedication> getDedications(const SecurityIdentity &requester) const;
  virtual void modifyDedicationSelf(const SecurityIdentity &requester, const std::string &drivename, const bool self);
  virtual void modifyDedicationReadonly(const SecurityIdentity &requester, const std::string &drivename, const bool readonly);
  virtual void modifyDedicationWriteonly(const SecurityIdentity &requester, const std::string &drivename, const bool writeonly);
  virtual void modifyDedicationVO(const SecurityIdentity &requester, const std::string &drivename, const std::string &VO);
  virtual void modifyDedicationInstance(const SecurityIdentity &requester, const std::string &drivename, const std::string &instanceName);
  virtual void modifyDedicationVid(const SecurityIdentity &requester, const std::string &drivename, const std::string &vid);
  virtual void modifyDedicationFrom(const SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp);
  virtual void modifyDedicationUntil(const SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp);
  virtual void modifyDedicationComment(const SecurityIdentity &requester, const std::string &drivename, const std::string &comment);

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
