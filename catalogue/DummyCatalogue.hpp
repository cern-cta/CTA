/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <list>
#include <map>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/threading/MutexLocker.hpp"
#include "disk/DiskSystem.hpp"

namespace cta {
namespace catalogue {

/**
 * An empty implementation of the Catalogue used to populate unit tests of the scheduler database
 * as they need a reference to a Catalogue, used in very few situations (requeueing of retrieve
 * requests).
 */
class DummyCatalogue: public Catalogue {
public:
  DummyCatalogue() = default;
  ~DummyCatalogue() override = default;

  void createAdminUser(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) override;
  void createArchiveRoute(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& tapePoolName, const std::string& comment) override;
  void createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const bool isDisabled, const std::string& comment) override;
  void createMountPolicy(const common::dataStructures::SecurityIdentity& admin, const CreateMountPolicyAttributes & mountPolicy) override;
  void createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstanceName, const std::string& requesterGroupName, const std::string& comment) override;
  void createRequesterMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstance, const std::string& requesterName, const std::string& comment) override;
  void createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstance, const std::string& requesterName, const std::string &activityRegex, const std::string& comment) override;
  void createStorageClass(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::StorageClass& storageClass) override;
  void createTape(const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes & tape) override;
  void createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) override;
  void deleteMediaType(const std::string &name) override;
  std::list<MediaTypeWithLogs> getMediaTypes() const override;
  MediaType getMediaTypeByVid(const std::string & vid) const override;
  void modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override;
  void modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &cartridge) override;
  void modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) override;
  void modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) override;
  void modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) override;
  void modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint32_t> &nbWraps) override;
  void modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &minLPos) override;
  void modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &maxLPos) override;
  void modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;
  void createTapePool(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string & vo, const uint64_t nbPartialTapes, const bool encryptionValue, const std::optional<std::string> &supply, const std::string& comment) override;
  void deleteAdminUser(const std::string& username) override;
  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string& instanceName, const uint64_t archiveFileId, log::LogContext &lc) override;
  void deleteArchiveRoute(const std::string& storageClassName, const uint32_t copyNb) override;
  void deleteLogicalLibrary(const std::string& name) override;
  void deleteMountPolicy(const std::string& name) override;
  void deleteRequesterGroupMountRule(const std::string& diskInstanceName, const std::string& requesterGroupName) override;
  void deleteRequesterMountRule(const std::string& diskInstanceName, const std::string& requesterName) override;
  void deleteRequesterActivityMountRule(const std::string& diskInstanceName, const std::string& requesterName, const std::string &activityRegex) override;
  void deleteStorageClass(const std::string& storageClassName) override;
  void deleteTape(const std::string& vid) override;
  void deleteTapePool(const std::string& name) override;
  void filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) override;
  void deleteDiskSystem(const std::string &name) override;
  void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &fileRegexp) override;
  void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t targetedFreeSpace) override;
  void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t sleepTime) override;
  void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;
  void modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName) override;
  void modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceSpaceName) override;
  std::list<common::dataStructures::DiskInstance> getAllDiskInstances() const override;
  void deleteDiskInstance(const std::string &name) override;
  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;
  std::list<common::dataStructures::DiskInstanceSpace> getAllDiskInstanceSpaces() const override;
  void deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) override;
  void modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &comment) override;
  void modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) override;
  void modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) override;
  std::list<common::dataStructures::AdminUser> getAdminUsers() const override;
  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const override;
  ArchiveFileItor getArchiveFilesItor(const TapeFileSearchCriteria& searchCriteria) const;
  FileRecycleLogItor getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria) const;
  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid);
  void deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc);
  void deleteFilesFromRecycleLog(const std::string & vid, log::LogContext & lc);
  void createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive);
  void deleteTapeDrive(const std::string &tapeDriveName);
  void createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category, const std::string &keyName, const std::string &value, const std::string &source);
  std::list<cta::catalogue::Catalogue::DriveConfig> getTapeDriveConfigs() const;
  std::list<std::pair<std::string, std::string>> getTapeDriveConfigNamesAndKeys() const;
  void modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category, const std::string &keyName, const std::string &value, const std::string &source);
  std::optional<std::tuple<std::string, std::string, std::string>> getTapeDriveConfig( const std::string &tapeDriveName, const std::string &keyName) const;
  void deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName);
  std::list<common::dataStructures::ArchiveFile> getFilesForRepack(const std::string &vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const override;
  ArchiveFileItor getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const override;
  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const;
  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName, const std::string &tapePoolName) const override;
  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const;
  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const;
  std::list<common::dataStructures::RequesterMountRule> getRequesterMountRules() const;
  std::list<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const;
  std::list<common::dataStructures::StorageClass> getStorageClasses() const;
  common::dataStructures::StorageClass getStorageClass(const std::string &name) const;
  common::dataStructures::ArchiveFileSummary getTapeFileSummary(const TapeFileSearchCriteria& searchCriteria) const;
  common::dataStructures::ArchiveFile getArchiveFileForDeletion(const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;
  void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) override;
  std::list<TapePool> getTapePools(const TapePoolSearchCriteria &searchCriteria) const;
  std::optional<TapePool> getTapePool(const std::string &tapePoolName) const override;
  std::optional<common::dataStructures::MountPolicy> getMountPolicy(const std::string &mountPolicyName) const override;
  std::list<common::dataStructures::Tape> getTapes(const TapeSearchCriteria& searchCriteria) const;
 // getTapesByVid is implemented below (and works).
  std::map<std::string, std::string> getVidToLogicalLibrary(const std::set<std::string> &vids) const override;
  common::dataStructures::Label::Format getTapeLabelFormat(const std::string& vid) const override;
  std::list<TapeForWriting> getTapesForWriting(const std::string& logicalLibraryName) const;
  bool isAdmin(const common::dataStructures::SecurityIdentity& admin) const;
  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) override;
  void createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) override;
  void deleteVirtualOrganization(const std::string &voName) override;
  std::list<common::dataStructures::VirtualOrganization> getVirtualOrganizations() const override;
  common::dataStructures::VirtualOrganization getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override;
  common::dataStructures::VirtualOrganization getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override;
  void modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) override;
  void modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) override;
  void modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) override;
  void modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) override;
  void modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) override;
  void modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) override;
  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& comment) override;
  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& tapePoolName) override;
  void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override;
  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override;
  void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& disabledReason) override;
  void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) override;
  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minArchiveRequestAge) override;
  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t archivePriority) override;
  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override;
  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minRetrieveRequestAge) override;
  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t retrievePriority) override;
  void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string &activityRegex, const std::string& comment) override;
  void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& comment) override;
  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& comment) override;
  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& mountPolicy) override;
  void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& mountPolicy) override;
  void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string &activityRegex, const std::string& mountPolicy) override;
  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override;
  void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override;
  void modifyStorageClassName(const common::dataStructures::SecurityIdentity& admin, const std::string& currentName, const std::string& newName) override;
  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbCopies) override;
  void modifyTapeComment(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::optional<std::string> &comment) override;
  void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& encryptionKeyName) override;
  void modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& verificationStatus) override;
  void modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const std::optional<common::dataStructures::Tape::State> & prev_state, const std::optional<std::string> & stateReason) override;
  void modifyTapeMediaType(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& mediaType) override;
  void modifyTapeVendor(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& vendor) override;
  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& logicalLibraryName) override;
  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override;
  void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override;
  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbPartialTapes) override;
  void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& supply) override;
  void modifyTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& currentName, const std::string& newName) override;
  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& tapePoolName) override;
  void noSpaceLeftOnTape(const std::string& vid) override;
  void ping() override;
  void verifySchemaVersion() override;
  SchemaVersion getSchemaVersion() const override;
  uint64_t checkAndGetNextArchiveFileId(const std::string &diskInstanceName, const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override;
  common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(const std::string &diskInstanceName,
    const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override;
  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(const std::string& diskInstanceName, const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity& user, const std::optional<std::string>& activity, log::LogContext& lc, const std::optional<std::string> &mountPolicyName) override;
  void reclaimTape(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, cta::log::LogContext & lc) override;
  void checkTapeForLabel(const std::string& vid) override;
  uint64_t getNbFilesOnTape(const std::string& vid) const  override;
  void setTapeDisabled(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string & reason) override;
  void setTapeFull(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool fullValue) override;
  void setTapeDirty(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool dirtyValue) override;
  void setTapeDirty(const std::string & vid) override;
  void setTapeIsFromCastorInUnitTests(const std::string &vid) override;
  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const bool encryptionValue) override;
  bool tapeExists(const std::string& vid) const;
  bool diskSystemExists(const std::string& name) const;
  void tapeLabelled(const std::string& vid, const std::string& drive) override;
  void tapeMountedForArchive(const std::string& vid, const std::string& drive) override;
  void tapeMountedForRetrieve(const std::string& vid, const std::string& drive) override;
  bool tapePoolExists(const std::string& tapePoolName) const;
  void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance, const std::string &diskFileId) override;
  void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) override;
  void modifyArchiveFileStorageClassId(const uint64_t archiveFileId, const std::string &newStorageClassName) const override;

  common::dataStructures::Tape::State getTapeState(const std::string & vid) const;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::string& vid) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string>& vids) const override;

  std::list<common::dataStructures::MountPolicy> getMountPolicies() const override;

  std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const override;

  std::list<std::string> getTapeDriveNames() const override;

  std::optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string &tapeDriveName) const override;

  std::list<common::dataStructures::TapeDrive> getTapeDrives() const override;

  void setDesiredTapeDriveState(const std::string&, const common::dataStructures::DesiredDriveState &desiredState) override;

  void setDesiredTapeDriveStateComment(const std::string& tapeDriveName, const std::string &comment) override;

  void updateTapeDriveStatistics(const std::string& tapeDriveName,
    const std::string& host, const std::string& logicalLibrary,
    const common::dataStructures::TapeDriveStatistics& statistics) override;

  void updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) override;

  std::map<std::string, uint64_t> getDiskSpaceReservations() const override;

  void reserveDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override;

  void releaseDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override;

  /*
   * Implemented for testing disk space reservation logic
   */
  disk::DiskSystemList getAllDiskSystems() const override;

  void createDiskInstance(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  void createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL, const uint64_t refreshInterval, const std::string &comment) override;

  void createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment) override;

  void modifyDiskInstanceSpaceFreeSpace(const std::string &name, const std::string &diskInstance, const uint64_t freeSpace) override;

  // This special funcitons for unit tests should be put in private
  void addEnabledTape(const std::string & vid);
  void addDisabledTape(const std::string & vid);
  void addRepackingTape(const std::string & vid);

private:
  mutable threading::Mutex m_tapeEnablingMutex;
  std::map<std::string, common::dataStructures::Tape::State> m_tapeEnabling;

  common::dataStructures::TapeDrive m_tapeDriveStatus;

  disk::DiskSystemList m_diskSystemList;
  std::map<std::string, common::dataStructures::DiskInstance> m_diskInstances;
  std::map<std::string, common::dataStructures::DiskInstanceSpace> m_diskInstanceSpaces;
};

}  // namespace catalogue
}  // namespace cta.

