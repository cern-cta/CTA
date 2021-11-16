/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/SchemaVersion.hpp"
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
#include "common/dataStructures/DiskSpaceReservationRequest.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
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
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/dataStructures/VidToTapeMap.hpp"
#include "common/dataStructures/WriteTestResult.hpp"
#include "common/exception/FileSizeMismatch.hpp"
#include "common/exception/TapeFseqMismatch.hpp"
#include "common/exception/UserError.hpp"
#include "common/exception/UserErrorWithCacheInfo.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/optional.hpp"
#include "disk/DiskSystem.hpp"
#include "RecyleTapeFileSearchCriteria.hpp"
#include "CreateMountPolicyAttributes.hpp"
#include "TapePoolSearchCriteria.hpp"

#include <list>
#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>

namespace cta {

namespace catalogue {

CTA_GENERATE_EXCEPTION_CLASS(WrongSchemaVersionException);
CTA_GENERATE_EXCEPTION_CLASS(NegativeDiskSpaceReservationReached);

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentDiskSystem);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyDiskSystemAfterDelete);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyLogicalLibrary);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyTape);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentArchiveRoute);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentLogicalLibrary);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentTape);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentTapePool);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentVirtualOrganization);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringComment);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringDiskSystemName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringFileRegexp);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringFreeSpaceQueryURL);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroRefreshInterval);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroSleepTime);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroTargetedFreeSpace);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringActivity);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringCartridge);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringDiskInstanceName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringLogicalLibraryName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringMediaType);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringMediaTypeName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringStorageClassName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringTapePoolName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringUsername);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringVendor);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringVid);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringVo);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyTapePool);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnOutOfRangeActivityWeight);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroCapacity);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroCopyNb);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedMediaTypeUsedByTapes);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedStorageClassUsedByArchiveFiles);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedStorageClassUsedByArchiveRoutes);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedStorageClassUsedByFileRecycleLogs);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedTapePoolUsedInAnArchiveRoute);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedExistingDeletedFileCopy);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentTapeState);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

/**
 * Abstract class defining the interface to the CTA catalogue responsible for
 * storing critical information about archive files, tapes and tape files.
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
   * @throw UserErrorWithCacheInfo if there was a user error.
   */
  virtual uint64_t checkAndGetNextArchiveFileId(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const common::dataStructures::RequesterIdentity &user) = 0;

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
    const common::dataStructures::RequesterIdentity &user) = 0;

  /**
   * Returns the list of tapes that can be written to by a tape drive in the
   * specified logical library, in other words tapes that are labelled, not
   * disabled, not full, not read-only and are in the specified logical library.
   *
   * @param logicalLibraryName The name of the logical library.
   * @return The list of tapes for writing.
   */
  virtual std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const = 0;

  /**
   * Notifies the catalogue that the specified files have been written to tape.
   *
   * @param events The tape file written events.
   * @throw TapeFseqMismatch If an unexpected tape file sequence number is encountered.
   * @throw FileSizeMismatch If an unexpected tape file size is encountered.
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
   * @param activity The activity under which the user wants to start the retrieve
   * The call will fail if the activity is set and unknown.
   * @param lc The log context.
   *
   * @return The information required to queue the associated retrieve request(s).
   */
  virtual common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(
    const std::string &diskInstanceName,
    const uint64_t archiveFileId,
    const common::dataStructures::RequesterIdentity &user,
    const optional<std::string> & activity,
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
   * Creates the specified Virtual Organization
   * @param admin The administrator.
   * @param vo the Virtual Organization
   */
  virtual void createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) = 0;

  /**
   * Deletes the specified Virtual Organization
   * @param voName the name of the VirtualOrganization to delete
   */
  virtual void deleteVirtualOrganization(const std::string &voName) = 0;

  /**
   * Get all the Virtual Organizations from the Catalogue
   * @return the list of all the Virtual Organizations
   */
  virtual std::list<common::dataStructures::VirtualOrganization> getVirtualOrganizations() const = 0;

  /**
   * Get the virtual organization corresponding to the tapepool passed in parameter
   * @param tapepoolName the name of the tapepool which we want the virtual organization
   * @return the VirtualOrganization associated to the tapepool passed in parameter
   */
  virtual common::dataStructures::VirtualOrganization getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const = 0;

  /**
   * Get, from the cache, the virtual organization corresponding to the tapepool passed in parameter
   * @param tapepoolName the name of the tapepool which we want the virtual organization
   * @return the VirtualOrganization associated to the tapepool passed in parameter
   */
  virtual common::dataStructures::VirtualOrganization getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const = 0;

  /**
   * Modifies the name of the specified Virtual Organization.
   *
   * @param currentVoName The current name of the Virtual Organization.
   * @param newVoName The new name of the Virtual Organization.
   */
  virtual void modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) = 0;

  /**
   * Modifies the max number of allocated drives for read for the specified Virtual Organization
   *
   * @param voName the VO name
   * @param readMaxDrives the new max number of allocated drives for read for the specified Virtual Organization
   */
  virtual void modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) = 0;

  /**
   * Modifies the max number of allocated drives for write for the specified Virtual Organization
   *
   * @param voName the VO name
   * @param writeMaxDrives the new max number of allocated drives for write for the specified Virtual Organization
   */
  virtual void modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) = 0;

  /**
   * Modifies the max size of files  for the specified Virtual Organization
   *
   * @param voName the VO name
   * @param maxFileSize the new max file size for the specified Virtual Organization
   */
  virtual void modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) = 0;

  /**
   * Modifies the comment of the specified Virtual Organization
   *
   * @param voName The name of the Virtual Organization.
   * @param comment The new comment of the Virtual Organization.
   */
  virtual void modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) = 0;
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
   * @param storageClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   */
  virtual void deleteStorageClass(const std::string &storageClassName) = 0;

  virtual std::list<common::dataStructures::StorageClass> getStorageClasses() const = 0;
  virtual common::dataStructures::StorageClass getStorageClass(const std::string &name) const = 0;

  virtual void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbCopies) = 0;
  virtual void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;
  virtual void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) = 0;

  /**
   * Modifies the name of the specified storage class.
   *
   * @param currentName The current name of the storage class.
   * @param newName The new name of the storage class.
   */
  virtual void modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) = 0;

  /**
   * Creates a tape media type.
   *
   * @param admin The administrator.
   * @param mediaType The tape media type.
   */
  virtual void createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) = 0;

  /**
   * Deletes the specified tape media type.
   *
   * @param name The name of the tape media type.
   */
  virtual void deleteMediaType(const std::string &name) = 0;

  /**
   * Returns all tape media types.
   *
   * @return All tape media types.
   */
  virtual std::list<MediaTypeWithLogs> getMediaTypes() const = 0;

  /**
   * Return the media type associated to the tape corresponding to the
   * vid passed in parameter
   * @param vid the vid of the tape to return its media type
   * @return the media type associated to the tape corresponding to the vid passed in parameter
   */
  virtual MediaType getMediaTypeByVid(const std::string & vid) const = 0;

  /**
   * Modifies the name of the specified tape media type.
   *
   * @param admin The administrator.
   * @param currentName The current name of the tape media type.
   * @param newName The new name of the tape media type.
   */
  virtual void modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) = 0;

  /**
   * Modifies the cartidge of the specified tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param cartridge The new cartidge.
   */
  virtual void modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &cartridge) = 0;

  /**
   * Modify the capacity in bytes of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param capacityInBytes The new capacity in bytes.
   */
  virtual void modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) = 0;

  /**
   * Modify the SCSI primary density code of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param primaryDensityCode The new SCSI primary density code.
   */
  virtual void modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) = 0;

  /**
   * Modify the SCSI secondary density code of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param secondaryDensityCode The new SCSI secondary density code.
   */
  virtual void modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) = 0;

  /**
   * Modify the number of tape wraps of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param nbWraps The new number of tape wraps.
   */
  virtual void modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const cta::optional<std::uint32_t> &nbWraps) = 0;

  /**
   * Modify the minimum longitudinal tape position of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param minLPos The new minimum longitudinal tape position.
   */
  virtual void modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const cta::optional<std::uint64_t> &minLPos) = 0;

  /**
   * Modify the maximum longitudinal tape position of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param maxLPos The new maximum longitudinal tape position.
   */
  virtual void modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const cta::optional<std::uint64_t> &maxLPos) = 0;

  /**
   * Modify the comment of a tape media type.
   *
   * @param admin The administrator.
   * @param name The name of the tape media type.
   * @param comment The new comment.
   */
  virtual void modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;

  virtual void createTapePool(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue, const cta::optional<std::string> &supply, const std::string &comment) = 0;

 /**
  * Deletes the specified tape pool.

  * @name The name of th epatpe pool.
  * @throw UserSpecifiedTapePoolUsedInAnArchiveRoute If the specified tape pool
  *  is used in an archive route.
  */
  virtual void deleteTapePool(const std::string &name) = 0;

  virtual std::list<TapePool> getTapePools(const TapePoolSearchCriteria &searchCriteria = TapePoolSearchCriteria()) const = 0;

  /**
   * @return The tape pool with the specified name.
   * @param tapePoolName The name of the tape pool.
   */
  virtual cta::optional<TapePool> getTapePool(const std::string &tapePoolName) const = 0;

  virtual void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) = 0;
  virtual void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbPartialTapes) = 0;
  virtual void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;
  virtual void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool encryptionValue) = 0;
  virtual void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &supply) = 0;

  /**
   * Modifies the name of the specified tape pool.
   *
   * @param admin The administrator.
   * @param currentName The current name of the tape pool.
   * @param newName The new name of the tape pool.
   */
  virtual void modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) = 0;

  virtual void createArchiveRoute(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName,
    const uint32_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified archive route.
   *
   * @param storageClassName The name of the storage class.
   * @param copyNb The copy number of the tape file.
   */
  virtual void deleteArchiveRoute(
    const std::string &storageClassName,
    const uint32_t copyNb) = 0;

  virtual std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const = 0;

  /**
   * @return the archive routes of the given storage class and destination tape
   * pool.
   *
   * Under normal circumstances this method should return either 0 or 1 route.
   * For a given storage class there should be no more than one route to any
   * given tape pool.
   *
   * @param storageClassName The name of the storage class.
   * @param tapePoolName The name of the tape pool.
   */
  virtual std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(
    const std::string &storageClassName,
    const std::string &tapePoolName) const = 0;

  /**
   * Modifies the tape pool of the specified archive route.
   *
   * @param admin The administrator.
   * @param storageClassName The name of the storage class.
   * @param copyNb The copy number.
   * @param tapePoolName The name of the tape pool.
   * @throw UserSpecifiedANonExistentTapePool if the user specified a
   * non-existent tape pool.
   */
  virtual void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName) = 0;

  virtual void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &comment) = 0;

  virtual void createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool isDisabled, const std::string &comment) = 0;
  virtual void deleteLogicalLibrary(const std::string &name) = 0;
  virtual std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const = 0;

  /**
   * Modifies the name of the specified logical library.
   *
   * @param admin The administrator.
   * @param currentName The current name of the logical library.
   * @param newName The new name of the logical library.
   */
  virtual void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) = 0;

  virtual void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;
  virtual void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) = 0;

  /**
   * Creates a tape which is assumed to have isFromCastor disabled.
   *
   * @param admin The administrator.
   * @param tape The attributes of the tape to be created.
   */
  virtual void createTape(
    const common::dataStructures::SecurityIdentity &admin,
    const CreateTapeAttributes &tape) = 0;

  virtual void deleteTape(const std::string &vid) = 0;

  /**
   * Returns the list of tapes that meet the specified search criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The list of tapes.
   * @throw UserSpecifiedANonExistentTapePool if the user specified a
   * non-existent tape pool.
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
   * Returns map from VID to logical library name for specified set of VIDs.
   *
   * @param vids The tape volume identifiers (VIDs).
   * @return map from VID to logical library name.
   */
  virtual std::map<std::string, std::string> getVidToLogicalLibrary(const std::set<std::string> &vids) const = 0;

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
   * @param lc the logContext
   */
  virtual void reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, cta::log::LogContext & lc) = 0;

  /**
   * Checks the specified tape for the tape label command.
   *
   * This method checks if the tape is safe to be labeled and will throw an
   * exception if the specified tape does not ready to be labeled.
   *
   * @param vid The volume identifier of the tape to be checked.
   */
  virtual void checkTapeForLabel(const std::string &vid) = 0;

  /**
   * Returns the number of any files contained in the tape identified by its vid
   * @param vid the vid in which we will the number of files
   * @return the number of files on the tape
   */
  virtual uint64_t getNbFilesOnTape(const std::string &vid) const = 0 ;

  virtual void modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &mediaType) = 0;
  virtual void modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &vendor) = 0;
  virtual void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &logicalLibraryName) = 0;
  virtual void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &tapePoolName) = 0;
  virtual void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &encryptionKeyName) = 0;
  /**
   * Modify the state of the specified tape
   * @param admin, the person or the system who modified the state of the tape
   * @param vid the VID of the tape to change the state
   * @param state the new state
   * @param stateReason the reason why the state changes, if the state is ACTIVE and the stateReason is nullopt, the state will be reset to null
   */
  virtual void modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const cta::optional<std::string> & stateReason) = 0;
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

  /**
   * This method notifies the CTA catalogue to set the specified tape is from CASTOR.
   * This method only for unitTests and MUST never be called in CTA!!!
   *
   * @param vid The volume identifier of the tape.
   */
  virtual void setTapeIsFromCastorInUnitTests(const std::string &vid) = 0;

  virtual void setTapeDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string & reason) = 0;

  virtual void setTapeDirty(const std::string & vid) = 0;

  /**
   * Modifies the tape comment
   * If the comment == cta::nullopt, it will delete the comment from the tape table
   * @param admin the admin who removes the comment
   * @param vid the vid of the tape to remove the comment
   * @param comment the new comment. If comment == cta::nullopt, the comment will be deleted.
   */
  virtual void modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const cta::optional<std::string> &comment) = 0;

  virtual void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) = 0;
  virtual void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &comment) = 0;
  virtual void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) = 0;
  virtual void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) = 0;

   virtual void createMountPolicy(const common::dataStructures::SecurityIdentity &admin, const CreateMountPolicyAttributes & mountPolicy) = 0;

  /**
   * Returns the list of all existing mount policies.
   *
   * @return the list of all existing mount policies.
   */
  virtual std::list<common::dataStructures::MountPolicy> getMountPolicies() const = 0;

  /**
   * Returns the cached list of all existing mount policies.
   *
   * @return the list of all existing mount policies.
   */
  virtual std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const = 0;

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
  virtual void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) = 0;

  virtual void createActivitiesFairShareWeight(const common::dataStructures::SecurityIdentity &admin, const std::string & diskInstanceName, const std::string & acttivity,
    double weight, const std::string & comment) = 0;
  virtual void modifyActivitiesFairShareWeight(const common::dataStructures::SecurityIdentity &admin, const std::string & diskInstanceName, const std::string & acttivity,
    double weight, const std::string & comment) = 0;
  virtual void deleteActivitiesFairShareWeight(const common::dataStructures::SecurityIdentity &admin, const std::string & diskInstanceName, const std::string & acttivity) = 0;
  virtual std::list<common::dataStructures::ActivitiesFairShareWeights> getActivitiesFairShareWeights() const = 0;

  /**
   * Returns all the disk systems within the CTA catalogue.
   *
   * @return The disk systems.
   * requester group.
   */
  virtual disk::DiskSystemList getAllDiskSystems() const = 0;

  /**
   * Creates a disk system.
   *
   * @param admin The administrator.
   * @param name The name of the disk system.
   * @param fileRegexp The regular expression allowing matching destination URLs
   * for this disk system.
   * @param freeSpaceQueryURL The query URL that describes a method to query the
   * free space from the disk system.
   * @param refreshInterval The refresh interval (seconds) defining how long do
   * we use a free space value.
   * @param targetedFreeSpace The targeted free space (margin) based on the free
   * space update latency (inherent to the file system and induced by the refresh
   * interval), and the expected external bandwidth from sources external to CTA.
   * @param comment Comment.
   */
  virtual void createDiskSystem(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &name,
    const std::string &fileRegexp,
    const std::string &freeSpaceQueryURL,
    const uint64_t refreshInterval,
    const uint64_t targetedFreeSpace,
    const uint64_t sleepTime,
    const std::string &comment) = 0;

  /**
   * Deletes a disk system.
   *
   * @param name The name of the disk system.
   */
  virtual void deleteDiskSystem(const std::string &name) = 0;

  virtual void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &fileRegexp) = 0;
  virtual void modifyDiskSystemFreeSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &freeSpaceQueryURL) = 0;
  virtual void modifyDiskSystemRefreshInterval(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t refreshInterval) = 0;
  virtual void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t targetedFreeSpace) = 0;
  virtual void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t sleepTime) = 0;
  virtual void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) = 0;

  typedef CatalogueItor<common::dataStructures::ArchiveFile> ArchiveFileItor;

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
   * Returns the specified archive file. If the search criteria result in more than one tape file being returned
   * an exception is thrown.
   * @param searchCriteria The search criteria.
   * @return The archive file.
   */
  virtual common::dataStructures::ArchiveFile getArchiveFileForDeletion(const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const = 0;

  typedef CatalogueItor<common::dataStructures::FileRecycleLog> FileRecycleLogItor;

  /**
   * Returns all the currently deleted files by looking at the FILE_RECYCLE_LOG table
   *
   * @return The deleted archive files ordered by archive file ID.
   */
  virtual FileRecycleLogItor getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria = RecycleTapeFileSearchCriteria()) const = 0;


  /**
   * Restores the deleted file in the Recycle log that match the criteria passed
   *
   * @param searchCriteria The search criteria
   * @param newFid the new Fid of the archive file (if the archive file must be restored)
   */
  virtual void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid) = 0;


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
  * Deletes a tape file copy
  *
  * @param file The tape file to delete
  * @param reason The reason for deleting the tape file copy
  */
  virtual void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) = 0;

  /**
   * Returns the archive file with the specified unique identifier.
   *
   * This method assumes that the archive file being requested exists and will
   * therefore throw an exception if it does not.
   *
   * Please note that an archive file with no associated tape files is
   * considered not to exist by this method.
   *
   * @param id The unique identifier of the archive file.
   * @return The archive file.
   */
  virtual common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const = 0;

  /**
   * !!!!!!!!!!!!!!!!!!! THIS METHOD SHOULD NOT BE USED !!!!!!!!!!!!!!!!!!!!!!!
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
  virtual void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &instanceName, const uint64_t archiveFileId,
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
   * Checks that the online database schema MAJOR version number matches the schema MAJOR version number defined in version.h
   */
  virtual void verifySchemaVersion() = 0;

  /**
   * Returns the SchemaVersion object corresponding to the catalogue schema version:
   * - SCHEMA_VERSION_MAJOR
   * - SCHEMA_VERSION_MINOR
   * - SCHEMA_VERSION_MAJOR_NEXT (future major version number of the schema in case of upgrade)
   * - SCHEMA_VERSION_MINOR_NEXT (future minor version number of the schema in case of upgrade)
   * - STATUS (UPGRADING or PRODUCTION)
   *
   * @return The SchemaVersion object corresponding to the catalogue schema version
   */
  virtual SchemaVersion getSchemaVersion() const = 0;

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

  /**
   * Returns true if the specified disk system exists.
   *
   * @param name The name identifier of the disk system.
   * @return True if the tape exists.
   */
  virtual bool diskSystemExists(const std::string &name) const = 0;

  /**
   * Updates the disk file ID of the specified archive file.
   *
   * @param archiveFileId The unique identifier of the archive file.
   * @param diskInstance The instance name of the source disk system.
   * @param diskFileId The identifier of the source disk file which is unique
   * within it's host disk system.  Two files from different disk systems may
   * have the same identifier.  The combination of diskInstance and diskFileId
   * must be globally unique, in other words unique within the CTA catalogue.
   */
  virtual void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance,
    const std::string &diskFileId) = 0;

  /**
   * Insert the ArchiveFile and all its tape files in the FILE_RECYCLE_LOG table.
   * There will be one entry on the FILE_RECYCLE_LOG table per deleted tape file
   *
   * @param request the DeleteRequest object that holds information about the file to delete.
   * @param lc the logContext
   */
  virtual void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) = 0;

   /**
   *
   * Deletes the specified archive file and its associated tape copies from the
   * recycle-bin
   *
   * Please note that this method is idempotent.  If the file to be deleted does
   * not exist in the CTA catalogue then this method returns without error.
   *
   * @param archiveFileId The unique identifier of the archive file.
   * @param lc The log context.
   */
  virtual void deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc) = 0;

  /**
   * Deletes all the log entries corresponding to the vid passed in parameter.
   *
   * Please note that this method is idempotent.  If there are no recycle log
   * entries associated to the vid passed in parameter, the method will return
   * without any error.
   *
   * @param vid, the vid of the files to be deleted
   * @param lc, the logContext
   */
  virtual void deleteFilesFromRecycleLog(const std::string & vid, log::LogContext & lc) = 0;

  /**
   * Creates the specified Tape Drive
   * @param tapeDrive Parameters of the Tape Drive.
   */
  virtual void createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) = 0;

  /**
   * Gets the names of all stored Tape Drive
   * @return List of tape drive names
   */
  virtual std::list<std::string> getTapeDriveNames() const = 0;

  /**
   * Gets the information of the specified Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @return Parameters of the Tape Drive.
   */
  virtual optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string &tapeDriveName) const = 0;

  /**
   * Modifies the parameters of the specified Tape Drive
   * @param tapeDrive Parameters of the Tape Drive.
   */
  virtual void modifyTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) = 0;

  /**
   * Deletes the entry of a Tape Drive
   * @param tapeDriveName The name of the tape drive.
   */
  virtual void deleteTapeDrive(const std::string &tapeDriveName) = 0;

  /**
   * Creates a specified parameter of the configuration for a certain Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @param category The category of the parameter.
   * @param keyName The key of the parameter.
   * @param value The value of the parameter.
   * @param source The source from which the parameter was gotten.
   */
  virtual void createDriveConfig(const std::string &tapeDriveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) = 0;

  /**
   * Gets the Key and Names of configurations of all TapeDrives
   * @return Keys and Names of configurations.
   */
  virtual std::list<std::pair<std::string, std::string>> getDriveConfigNamesAndKeys() const = 0;

  /**
   * Modifies a specified parameter of the configuration for a certain Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @param category The category of the parameter.
   * @param keyName The key of the parameter.
   * @param value The value of the parameter.
   * @param source The source from which the parameter was gotten.
   */
  virtual void modifyDriveConfig(const std::string &tapeDriveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) = 0;

  /**
   * Gets a specified parameter of the configuration for a certain Tape Drive
   * @param tapeDriveName The name of the tape drive.
   * @param keyName The key of the parameter.
   * @return Returns the category, value and source of a parameter of the configuarion
   */
  virtual optional<std::tuple<std::string, std::string, std::string>> getDriveConfig( const std::string &tapeDriveName,
    const std::string &keyName) const = 0;

  /**
   * Deletes the entry of a Drive Configuration
   * @param tapeDriveName The name of the tape drive.
   */
  virtual void deleteDriveConfig(const std::string &tapeDriveName, const std::string &keyName) = 0;

  virtual std::map<std::string, uint64_t> getExistingDrivesReservations() const = 0;

  virtual void reserveDiskSpace(const std::string& driveName, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) = 0;

  virtual void addDiskSpaceReservation(const std::string& driveName, const std::string& diskSystemName, uint64_t bytes) = 0;

  virtual void subtractDiskSpaceReservation(const std::string& driveName, const std::string& diskSystemName, uint64_t bytes) = 0;
  
  virtual std::tuple<std::string, uint64_t> getDiskSpaceReservation(const std::string& driveName) = 0;
  
  virtual void releaseDiskSpace(const std::string& driveName, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) = 0;

}; // class Catalogue

} // namespace catalogue
} // namespace cta
