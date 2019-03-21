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
#include "catalogue/TapeFileSearchCriteria.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteriaAndFileId.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
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
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/VerifyInfo.hpp"
#include "common/dataStructures/VidToTapeMap.hpp"
#include "common/dataStructures/WriteTestResult.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <map>
#include <set>
#include <stdint.h>
#include <string>
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

  //////////////////////////////////////////////////////////////////
  // START OF METHODS DIRECTLY INVOLVED DATA TRANSFER AND SCHEDULING
  //////////////////////////////////////////////////////////////////

  /**
   * Notifies the catalogue that the specified tape was labelled.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of tape drive that was used to label the tape.
   */
  virtual void tapeLabelled(const std::string &vid, const std::string &drive) = 0;

  /**
   * Checks the specified archival could take place and returns a new and
   * unique archive file identifier that can be used by a new archive file
   * within the catalogue.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param storageClassName The name of the storage class of the file to be
   * archived.  The storage class name is only guaranteed to be unique within
   * its disk instance.  The storage class name will be used by the Catalogue
   * to determine the destination tape pool for each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The new archive file identifier.
   */
  virtual uint64_t checkAndGetNextArchiveFileId(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const common::dataStructures::UserIdentity &user) = 0;

  /**
   * Returns the information required to queue an archive request.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param storageClassName The name of the storage class of the file to be
   * archived.  The storage class name is only guaranteed to be unique within
   * its disk instance.  The storage class name will be used by the Catalogue
   * to determine the destination tape pool for each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The information required to queue an archive request.
   */
  virtual common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const common::dataStructures::UserIdentity &user) = 0;

  /**
   * Returns the list of tapes that can be written to by a tape drive in the
   * specified logical library, in other words tapes that are labelled, not
   * disabled, not full and are in the specified logical library.
   *
   * @param logicalLibraryName The name of the logical library.
   * @return The list of tapes for writing.
   */
  virtual std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const = 0;

  /**
   * Notifies the catalogue that the specified files have been written to tape.
   *
   * @param events The tape file written events.
   */
  virtual void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) = 0;

  /**
   * Notifies the CTA catalogue that the specified tape has been mounted in
   * order to archive files.
   *
   * The purpose of this method is to keep track of which drive mounted a given
   * tape for archiving files last.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of the drive where the tape was mounted.
   */
  virtual void tapeMountedForArchive(const std::string &vid, const std::string &drive) = 0; // internal function (noCLI)

  /**
   * Prepares for a file retrieval by returning the information required to
   * queue the associated retrieve request(s).
   *
   * @param diskInstanceName The name of the instance from where the retrieval
   * request originated
   * @param archiveFileId The unique identifier of the archived file that is
   * to be retrieved.
   * @param user The user for whom the file is to be retrieved.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * retrieving the file.
   * @param lc The log context.
   *
   * @return The information required to queue the associated retrieve request(s).
   */
  virtual common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(
    const std::string &diskInstanceName,
    const uint64_t archiveFileId,
    const common::dataStructures::UserIdentity &user,
    log::LogContext &lc) = 0;

  /**
   * Notifies the CTA catalogue that the specified tape has been mounted in
   * order to retrieve files.
   *
   * The purpose of this method is to keep track of which drive mounted a given
   * tape for retrieving files last.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of the drive where the tape was mounted.
   */
  virtual void tapeMountedForRetrieve(const std::string &vid, const std::string &drive) = 0; // internal function (noCLI)

  /**
   * This method notifies the CTA catalogue that there is no more free space on
   * the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  virtual void noSpaceLeftOnTape(const std::string &vid) = 0;

  ////////////////////////////////////////////////////////////////
  // END OF METHODS DIRECTLY INVOLVED DATA TRANSFER AND SCHEDULING
  ////////////////////////////////////////////////////////////////

  virtual void createAdminUser(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) = 0;
  virtual void deleteAdminUser(const std::string &username) = 0;
  virtual std::list<common::dataStructures::AdminUser> getAdminUsers() const = 0;
  virtual void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) = 0;

  /**
   * Creates the specified storage class.
   *
   * @param admin The administrator.
   * @param storageClass The storage class.
   */
  virtual void createStorageClass(
    const common::dataStructures::SecurityIdentity &admin,
    const common::dataStructures::StorageClass &storageClass) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param stoargeClassName The name of the storage class which is only
   * guaranteed to be unique within its disk isntance.
   */
  virtual void deleteStorageClass(const std::string &diskInstanceName, const std::string &storageClassName) = 0;

  virtual std::list<common::dataStructures::StorageClass> getStorageClasses() const = 0;
  virtual void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &name, const uint64_t nbCopies) = 0;
  virtual void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &name, const std::string &comment) = 0;

  virtual void createTapePool(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment) = 0;
  virtual void deleteTapePool(const std::string &name) = 0;
  virtual std::list<TapePool> getTapePools() const = 0;
  virtual void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) = 0;
  virtual void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbPartialTapes) = 0;
  virtual void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;
  virtual void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool encryptionValue) = 0;

  virtual void createArchiveRoute(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const uint32_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified archive route.
   *
   * @param diskInstanceName The name of the disk instance to which the storage
   * class belongs.
   * @param storageClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   * @param copyNb The copy number of the tape file.
   */
  virtual void deleteArchiveRoute(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const uint32_t copyNb) = 0;

  virtual std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const = 0;
  virtual void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName) = 0;
  virtual void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &storageClassName, const uint32_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const std::string &name) = 0;
  virtual std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const = 0;
  virtual void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;

  /**
   * Creates a tape which is assumed to have logical block protection (LBP)
   * enabled.
   */
  virtual void createTape(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &vid,
    const std::string &mediaType,
    const std::string &vendor,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const bool disabled,
    const bool full,
    const std::string &comment) = 0;

  virtual void deleteTape(const std::string &vid) = 0;

  /**
   * Returns the list of tapes that meet the specified search criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The list of tapes.
   */
  virtual std::list<common::dataStructures::Tape> getTapes(
    const TapeSearchCriteria &searchCriteria = TapeSearchCriteria()) const = 0;

  /**
   * Returns the tapes with the specified volume identifiers.
   *
   * This method will throw an exception if it cannot find ALL of the specified
   * tapes.
   *
   * @param vids The tape volume identifiers (VIDs).
   * @return Map from tape volume identifier to tape.
   */
  virtual common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string> &vids) const = 0;

  /**
   * Returns all the tapes within the CTA catalogue.
   *
   * @return Map from tape volume identifier to tape.
   */
  virtual common::dataStructures::VidToTapeMap getAllTapes() const = 0;

  /**
   * Reclaims the specified tape.
   *
   * This method will throw an exception if the specified tape does not exist.
   *
   * This method will throw an exception if the specified tape is not FULL.
   *
   * This method will throw an exception if there is still at least one tape
   * file recorded in the cataligue as being on the specified tape.
   *
   * @param admin The administrator.
   * @param vid The volume identifier of the tape to be reclaimed.
   */
  virtual void reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid) = 0;

  virtual void modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &mediaType) = 0;
  virtual void modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &vendor) = 0;
  virtual void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const uint64_t capacityInBytes) = 0;
  virtual void modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &encryptionKey) = 0;

  /**
   * Sets the full status of the specified tape.
   *
   * Please note that this method is to be called by the CTA front-end in
   * response to a command from the CTA command-line interface (CLI).
   *
   * @param admin The administrator.
   * @param vid The volume identifier of the tape to be marked as full.
   * @param fullValue Set to true if the tape is full.
   */
  virtual void setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool fullValue) = 0;

  virtual void setTapeDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool disabledValue) = 0;
  virtual void modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &comment) = 0;

  virtual void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) = 0;
  virtual void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &comment) = 0;
  virtual void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) = 0;
  virtual void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) = 0;

   virtual void createMountPolicy(
    const common::dataStructures::SecurityIdentity &admin,
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
   * @param admin The administrator.
   * @param mountPolicyName The name of the mount policy.
   * @param diskInstance The name of the disk instance to which the requester
   * belongs.
   * @param requesterName The name of the requester which is only guarantted to
   * be unique within its disk instance.
   * @param comment Comment.
   */
  virtual void createRequesterMountRule(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName,
    const std::string &diskInstance,
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
   * @param diskInstanceName The name of the disk instance to which the
   * requester belongs.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   */
  virtual void deleteRequesterMountRule(const std::string &diskInstanceName, const std::string &requesterName) = 0;

  /**   
   * Creates the rule that the specified mount policy will be used for the
   * specified requester group.
   *
   * Please note that requester mount-rules overrule requester-group
   * mount-rules.
   *
   * @param admin The administrator.
   * @param mountPolicyName The name of the mount policy.
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guarantted to be unique within its disk instance.
   * @param comment Comment.
   */
  virtual void createRequesterGroupMountRule(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName,
    const std::string &diskInstanceName,
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
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   */
  virtual void deleteRequesterGroupMountRule(
    const std::string &diskInstanceName,
    const std::string &requesterGroupName) = 0;

  virtual void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t archivePriority) = 0;
  virtual void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minArchiveRequestAge) = 0;
  virtual void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t retrievePriority) = 0;
  virtual void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minRetrieveRequestAge) = 0;
  virtual void modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t maxDrivesAllowed) = 0;
  virtual void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;

  /**
   * Returns the specified archive files.  Please note that the list of files
   * is ordered by archive file ID.
   *
   * @param searchCriteria The search criteria.
   * @return The archive files.
   */
  virtual ArchiveFileItor getArchiveFilesItor(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const = 0;

  /**
   * Returns the specified files in tape file sequence order.
   *
   * @param vid The volume identifier of the tape.
   * @param startFSeq The file sequence number of the first file.  Please note
   * that there might not be a file with this exact file sequence number.
   * @param maxNbFiles The maximum number of files to be returned.
   * @return The specified files in tape file sequence order.
   */
  virtual std::list<common::dataStructures::ArchiveFile> getFilesForRepack(
    const std::string &vid,
    const uint64_t startFSeq,
    const uint64_t maxNbFiles) const = 0;

  /**
   * Returns all the tape copies (no matter their VIDs) of the archive files
   * associated with the tape files on the specified tape in FSEQ order
   * starting at the specified startFSeq.
   *
   * @param vid The volume identifier of the tape.
   * @param startFSeq The file sequence number of the first file.  Please note
   * that there might not be a file with this exact file sequence number.
   * @return The specified files in FSEQ order.
   */
  virtual ArchiveFileItor getArchiveFilesForRepackItor(
    const std::string &vid,
    const uint64_t startFSeq) const = 0;

  /**
   * Returns a summary of the tape files that meet the specified search
   * criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The summary.
   */
  virtual common::dataStructures::ArchiveFileSummary getTapeFileSummary(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const = 0;

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

  /**
   * Deletes the specified archive file and its associated tape copies from the
   * catalogue.
   *
   * Please note that the name of the disk instance is specified in order to
   * prevent a disk instance deleting an archive file that belongs to another
   * disk instance.
   *
   * Please note that this method is idempotent.  If the file to be deleted does
   * not exist in the CTA catalogue then this method returns without error.
   *
   * @param instanceName The name of the instance from where the deletion request
   * originated
   * @param archiveFileId The unique identifier of the archive file.
   * @param lc The log context.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  virtual void deleteArchiveFile(const std::string &instanceName, const uint64_t archiveFileId,
    log::LogContext &lc) = 0;

  /**
   * Returns true if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   *
   * @param admin The administrator.
   * @return True if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   */
  virtual bool isAdmin(const common::dataStructures::SecurityIdentity &admin) const = 0;

  /**
   * Checks that the most trivial query goes through. Throws an exception if not.
   */
  virtual void ping() = 0;

  /**
   * Returns true if the specified tape pool exists.
   *
   * @param tapePoolName The name of the tape pool.
   * @return True if the tape pool exists.
   */
  virtual bool tapePoolExists(const std::string &tapePoolName) const = 0;

  /**
   * Returns true if the specified tape exists.
   *
   * @param vid The volume identifier of the tape.
   * @return True if the tape exists.
   */
  virtual bool tapeExists(const std::string &vid) const = 0;

}; // class Catalogue

} // namespace catalogue
} // namespace cta
