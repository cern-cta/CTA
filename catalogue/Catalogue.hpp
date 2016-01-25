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

#include "common/ArchiveRequest.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

class AdminHost {};
class AdminUser {};
class ArchiveRoute {};
class Dedication {};
class LogicalLibrary {};
class RetrieveGroup {};
class RetrieveUser {};
class SecurityIdentity {};
class StorageClass {};
class Tape {};
class TapePool {};
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

  virtual void createTapePool(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes, const uint64_t minFilesQueued, const uint64_t minBytesQueued, const uint64_t minRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment) = 0;
  virtual void deleteTapePool(const SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<TapePool> getTapePools(const SecurityIdentity &requester) const = 0;
  virtual void modifyTapePoolNbPartialTapes(const SecurityIdentity &requester, const std::string &name, const uint32_t nbPartialTapes) = 0;
  virtual void modifyTapePoolMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minFilesQueued) = 0;
  virtual void modifyTapePoolMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minBytesQueued) = 0;
  virtual void modifyTapePoolMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minRequestAge) = 0;
  virtual void modifyTapePoolMaxDrivesAllowed(const SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed) = 0;
  virtual void modifyTapePoolComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName,
                                                                                                             const std::string &comment) = 0;
  virtual void deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb) = 0;
  virtual std::list<ArchiveRoute> getArchiveRoutes(const SecurityIdentity &requester) const = 0;
  virtual void modifyArchiveRouteTapePoolName(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName) = 0;
  virtual void modifyArchiveRouteComment(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<LogicalLibrary> getLogicalLibraries(const SecurityIdentity &requester) const = 0;
  virtual void modifyLogicalLibraryComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createTape(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                                                                                                             const uint64_t capacityInBytes, const std::string &comment) = 0;
  virtual void deleteTape(const SecurityIdentity &requester, const std::string &vid) = 0;
  virtual Tape getTape(const SecurityIdentity &requester, const std::string &vid) const = 0;
  virtual std::list<Tape> getTapesByLogicalLibrary(const SecurityIdentity &requester, const std::string &logicalLibraryName) const = 0;
  virtual std::list<Tape> getTapesByPool(const SecurityIdentity &requester, const std::string &tapePoolName) const = 0;
  virtual std::list<Tape> getTapesByCapacity(const SecurityIdentity &requester, const uint64_t capacityInBytes) const = 0;
  virtual std::list<Tape> getAllTapes(const SecurityIdentity &requester) const = 0;
  virtual void reclaimTape(const SecurityIdentity &requester, const std::string &vid) = 0;
  virtual void modifyTapeLogicalLibraryName(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const SecurityIdentity &requester, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeCapacityInBytes(const SecurityIdentity &requester, const std::string &vid, const uint64_t capacityInBytes) = 0;
  virtual void modifyTapeComment(const SecurityIdentity &requester, const std::string &vid, const std::string &comment) = 0;

  virtual void createRetrieveUser(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup, const std::string &retrieveGroup,
                                                                                                             const std::string &comment) = 0;
  virtual void deleteRetrieveUser(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup) = 0;
  virtual std::list<RetrieveUser> getRetrieveUsers(const SecurityIdentity &requester) const = 0;
  virtual void modifyRetrieveUserRetrieveGroup(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup, const std::string &retrieveGroup) = 0;
  virtual void modifyRetrieveUserComment(const SecurityIdentity &requester, const std::string &username, const std::string &usergroup, const std::string &comment) = 0;

  virtual void createRetrieveGroup(const SecurityIdentity &requester, const std::string &name, const uint64_t minFilesQueued, const uint64_t minBytesQueued, const uint64_t minRequestAge, const uint16_t maxDrivesAllowed, const std::string &comment) = 0;
  virtual void deleteRetrieveGroup(const SecurityIdentity &requester, const std::string &name) = 0;
  virtual std::list<RetrieveGroup> getRetrieveGroups(const SecurityIdentity &requester) const = 0;
  virtual void modifyRetrieveGroupMinFilesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minFilesQueued) = 0;
  virtual void modifyRetrieveGroupMinBytesQueued(const SecurityIdentity &requester, const std::string &name, const uint64_t minBytesQueued) = 0;
  virtual void modifyRetrieveGroupMinRequestAge(const SecurityIdentity &requester, const std::string &name, const uint64_t minRequestAge) = 0;
  virtual void modifyRetrieveGroupMaxDrivesAllowed(const SecurityIdentity &requester, const std::string &name, const uint16_t maxDrivesAllowed) = 0;
  virtual void modifyRetrieveGroupComment(const SecurityIdentity &requester, const std::string &name, const std::string &comment) = 0;

  virtual void createDedication(const SecurityIdentity &requester, const std::string &drivename, const bool self, const bool readonly, const bool writeonly, const std::string &VO, const std::string &instanceName, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) = 0;
  virtual void deleteDedication(const SecurityIdentity &requester, const std::string &drivename) = 0;
  virtual std::list<Dedication> getDedications(const SecurityIdentity &requester) const = 0;
  virtual void modifyDedicationSelf(const SecurityIdentity &requester, const std::string &drivename, const bool self) = 0;
  virtual void modifyDedicationReadonly(const SecurityIdentity &requester, const std::string &drivename, const bool readonly) = 0;
  virtual void modifyDedicationWriteonly(const SecurityIdentity &requester, const std::string &drivename, const bool writeonly) = 0;
  virtual void modifyDedicationVO(const SecurityIdentity &requester, const std::string &drivename, const std::string &VO) = 0;
  virtual void modifyDedicationInstance(const SecurityIdentity &requester, const std::string &drivename, const std::string &instanceName) = 0;
  virtual void modifyDedicationVid(const SecurityIdentity &requester, const std::string &drivename, const std::string &vid) = 0;
  virtual void modifyDedicationFrom(const SecurityIdentity &requester, const std::string &drivename, const uint64_t fromTimestamp) = 0;
  virtual void modifyDedicationUntil(const SecurityIdentity &requester, const std::string &drivename, const uint64_t untilTimestamp) = 0;
  virtual void modifyDedicationComment(const SecurityIdentity &requester, const std::string &drivename, const std::string &comment) = 0;

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
