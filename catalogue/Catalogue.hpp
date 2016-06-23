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

#include "catalogue/ArchiveFileItor.hpp"
#include "catalogue/ArchiveFileSearchCriteria.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "common/dataStructures/AdminHost.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/Dedication.hpp"
#include "common/dataStructures/DedicationType.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RepackType.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/TapeCopyToPoolMap.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/dataStructures/TapePool.hpp"
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/VerifyInfo.hpp"
#include "common/dataStructures/WriteTestResult.hpp"

#include <list>
#include <stdint.h>
#include <string>
#include <map>
#include <memory>

namespace cta {

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
  
  virtual void createBootstrapAdminAndHostNoAuth(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &username, const std::string &hostName, const std::string &comment) = 0;

  virtual void createAdminUser(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &username, const std::string &comment) = 0;
  virtual void deleteAdminUser(const std::string &username) = 0;
  virtual std::list<common::dataStructures::AdminUser> getAdminUsers() const = 0;
  virtual void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &username, const std::string &comment) = 0;

  virtual void createAdminHost(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) = 0;
  virtual void deleteAdminHost(const std::string &hostName) = 0;
  virtual std::list<common::dataStructures::AdminHost> getAdminHosts() const = 0;
  virtual void modifyAdminHostComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &hostName, const std::string &comment) = 0;

  virtual void createStorageClass(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies, const std::string &comment) = 0;
  virtual void deleteStorageClass(const std::string &name) = 0;
  virtual std::list<common::dataStructures::StorageClass> getStorageClasses() const = 0;
  virtual void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbCopies) = 0;
  virtual void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;

  virtual void createTapePool(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment) = 0;
  virtual void deleteTapePool(const std::string &name) = 0;
  virtual std::list<common::dataStructures::TapePool> getTapePools() const = 0;
  virtual void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t nbPartialTapes) = 0;
  virtual void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;
  virtual void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const bool encryptionValue) = 0;

  virtual void createArchiveRoute(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName, const std::string &comment) = 0;
  virtual void deleteArchiveRoute(const std::string &storageClassName, const uint64_t copyNb) = 0;
  virtual std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const = 0;
  virtual void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) = 0;
  virtual void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const std::string &name) = 0;
  virtual std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const = 0;
  virtual void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;

  /**
   * The createTape function does not take the lbp bool value as it defaults to false. The lbp can only be changed through the labelTape command.
   */
  virtual void createTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
                          const std::string &encryptionKey, const uint64_t capacityInBytes, const bool disabledValue, const bool fullValue, const std::string &comment) = 0;
  virtual void deleteTape(const std::string &vid) = 0;
  virtual std::list<common::dataStructures::Tape> getTapes(const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName,
        const std::string &capacityInBytes, const std::string &disabledValue, const std::string &fullValue, const std::string &busyValue, const std::string &lbpValue) = 0;
  virtual void reclaimTape(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) = 0;
  virtual void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const uint64_t capacityInBytes) = 0;
  virtual void modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &encryptionKey) = 0;
  virtual void modifyTapeLabelLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) = 0; // internal function (noCLI)
  virtual void modifyTapeLastWrittenLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) = 0; // internal function (noCLI)
  virtual void modifyTapeLastReadLog(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &drive, const uint64_t timestamp) = 0; // internal function (noCLI)
  virtual void setTapeBusy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue) = 0; // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue) = 0;
  virtual void setTapeDisabled(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue) = 0;
  virtual void setTapeLbp(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue) = 0; // internal function (noCLI)
  virtual void modifyTapeComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &comment) = 0;

  virtual void modifyRequesterMountPolicy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &requesterName, const std::string &mountPolicy) = 0;
  virtual void modifyRequesterComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &requesterName, const std::string &comment) = 0;
  virtual void modifyRequesterGroupMountPolicy(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &requesterGroupName, const std::string &mountPolicy) = 0;
  virtual void modifyRequesterGroupComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &requesterGroupName, const std::string &comment) = 0;

   virtual void createMountPolicy(
    const common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &name, 
    const uint64_t archivePriority, 
    const uint64_t minArchiveRequestAge, 
    const uint64_t retrievePriority, 
    const uint64_t minRetrieveRequestAge, 
    const uint64_t maxDrivesAllowed, 
    const std::string &comment) = 0;

  /**
   * Returns the list of all existing mount policies.
   *
   * @return the list of all existing mount policies.
   */
  virtual std::list<common::dataStructures::MountPolicy> getMountPolicies() const = 0;

  /**
   * Deletes the specified mount policy.
   *
   * @param name The name of the mount policy.
   */
  virtual void deleteMountPolicy(const std::string &name) = 0;

  /**
   * Creates the rule that the specified mount policy will be used for the
   * specified requester.
   *
   * Please note that requester mount-rules overrule requester-group
   * mount-rules.
   *
   * @param cliIdentity The user of the command-line tool.
   * @param mountPolicyName The name of the mount policy.
   * @param requesterName The name of the requester.
   * @param comment Comment.
   */
  virtual void createRequesterMountRule(
    const common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &mountPolicyName,
    const std::string &requesterName,
    const std::string &comment) = 0;

  /**
   * Returns the rules that specify which mount policy is be used for which
   * requester.
   *
   * @return the rules that specify which mount policy is be used for which
   * requester.
   */
  virtual std::list<common::dataStructures::RequesterMountRule> getRequesterMountRules() const = 0;

  /**
   * Deletes the specified mount rule.
   *
   * @param requesterName The name of the requester.
   */
  virtual void deleteRequesterMountRule(const std::string &requesterName) = 0;

  /**
   * Creates the rule that the specified mount policy will be used for the
   * specified requester group.
   *
   * Please note that requester mount-rules overrule requester-group
   * mount-rules.
   *
   * @param cliIdentity The user of the command-line tool.
   * @param mountPolicyName The name of the mount policy.
   * @param requesterGroupName The name of the requester group.
   * @param comment Comment.
   */
  virtual void createRequesterGroupMountRule(
    const common::dataStructures::SecurityIdentity &cliIdentity,
    const std::string &mountPolicyName,
    const std::string &requesterGroupName,
    const std::string &comment) = 0;

  /**
   * Returns the rules that specify which mount policy is be used for which
   * requester group.
   *
   * @return the rules that specify which mount policy is be used for which
   * requester group.
   */
  virtual std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const = 0;

  /**
   * Deletes the specified mount rule.
   *
   * @param requesterGroupName The name of the requester group.
   */
  virtual void deleteRequesterGroupMountRule(const std::string &requesterGroupName) = 0;

  virtual void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archivePriority) = 0;
  virtual void modifyMountPolicyArchiveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveFilesQueued) = 0;
  virtual void modifyMountPolicyArchiveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t archiveMinBytesQueued) = 0;
  virtual void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minArchiveRequestAge) = 0;
  virtual void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrievePriority) = 0;
  virtual void modifyMountPolicyRetrieveMinFilesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveFilesQueued) = 0;
  virtual void modifyMountPolicyRetrieveMinBytesQueued(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t retrieveMinBytesQueued) = 0;
  virtual void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t minRetrieveRequestAge) = 0;
  virtual void modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const uint64_t maxDrivesAllowed) = 0;
  virtual void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &name, const std::string &comment) = 0;

  virtual void createDedication(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const common::dataStructures::DedicationType dedicationType,
 const std::string &tag, const std::string &vid, const uint64_t fromTimestamp, const uint64_t untilTimestamp,const std::string &comment) = 0;
  virtual void deleteDedication(const std::string &drivename) = 0;
  virtual std::list<common::dataStructures::Dedication> getDedications() const = 0;
  virtual void modifyDedicationType(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const common::dataStructures::DedicationType dedicationType) = 0;
  virtual void modifyDedicationTag(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &tag) = 0;
  virtual void modifyDedicationVid(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &vid) = 0;
  virtual void modifyDedicationFrom(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t fromTimestamp) = 0;
  virtual void modifyDedicationUntil(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const uint64_t untilTimestamp) = 0;
  virtual void modifyDedicationComment(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &drivename, const std::string &comment) = 0;

  /**
   * Returns an iterator over the list of archive files that meet the specified
   * search criteria.
   *
   * Please note that the list is ordered by archive file ID.
   *
   * Please note that this method will throw an exception if the
   * nbArchiveFilesToPrefetch parameter is set to 0.  The parameter must be set
   * to a value greater than or equal to 1.
   *
   * @param searchCriteria The search criteria.
   * @param nbArchiveFilesToPrefetch The number of archive files to prefetch.
   * This parameter must be set to a value equal to or greater than 1.
   * @return An iterator over the list of archive files.
   */
  virtual std::unique_ptr<ArchiveFileItor> getArchiveFileItor(
    const ArchiveFileSearchCriteria &searchCriteria = ArchiveFileSearchCriteria(),
    const uint64_t nbArchiveFilesToPrefetch = 1000) const = 0;

  /**
   * Returns a summary of the archive files that meet the specified search
   * criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The summary.
   */
  virtual common::dataStructures::ArchiveFileSummary getArchiveFileSummary(
    const ArchiveFileSearchCriteria &searchCriteria = ArchiveFileSearchCriteria()) const = 0;

  /**
   * Returns the archive file with the specified unique identifier.
   *
   * This method assumes that the archive file being requested exists and will
   * therefore throw an exception if it does not.
   *
   * @param id The unique identifier of the archive file.
   * @return The archive file.
   */
  virtual common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) = 0;
  
  virtual void setDriveStatus(const common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force) = 0;

  /**
   * Prepares the catalogue for a new archive file and returns the information
   * required to queue the associated archive request.
   *
   * @param storageClass The storage class of the file to be archived.  This
   * will be used by the Catalogue to determine the destinate tape pool for
   * each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The information required to queue the associated archive request.
   */
  virtual common::dataStructures::ArchiveFileQueueCriteria prepareForNewFile(
    const std::string &storageClass,
    const common::dataStructures::UserIdentity &user) = 0;

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param event The tape file written event.
   */
  virtual void fileWrittenToTape(const TapeFileWritten &event) = 0;

  /**
   * Deletes the specified archive file and its associated tape copies from the
   * catalogue.
   *
   * @param archiveFileId The unique identifier of the archive file.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  virtual common::dataStructures::ArchiveFile deleteArchiveFile(const uint64_t archiveFileId) = 0;

  /**
   * Prepares for a file retrieval by returning the information required to
   * queue the associated retrieve request(s).
   *
   * @param archiveFileId The unique identifier of the archived file that is
   * to be retrieved.
   * @param user The user for whom the file is to be retrieved.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * retrieving the file.
   *
   * @return The information required to queue the associated retrieve request(s).
   */
  virtual common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(
    const uint64_t archiveFileId,
    const common::dataStructures::UserIdentity &user) = 0;

  virtual common::dataStructures::TapeCopyToPoolMap getTapeCopyToPoolMap(const std::string &storageClass) const = 0;

  /**
   * Returns true if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   *
   * @param cliIdentity The name of the user and the host on which they are
   * running the CTA command-line tool.
   * @return True if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   */
  virtual bool isAdmin(const common::dataStructures::SecurityIdentity &cliIdentity) const = 0;

  /**
   * Returns the list of tapes that can be written to by a tape drive in the
   * specified logical library, in other words tapes that are not disabled, not
   * full and are in the specified logical library.
   *
   * @param logicalLibraryName The name of the logical library.
   */
  virtual std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const = 0;

}; // class Catalogue

} // namespace catalogue
} // namespace cta
