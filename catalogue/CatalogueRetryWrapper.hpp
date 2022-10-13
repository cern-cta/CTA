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

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"

namespace cta {

namespace catalogue {

/**
 * Wrapper around a CTA catalogue object that retries a method if a
 * LostConnectionException is thrown.
 */
class CatalogueRetryWrapper: public Catalogue {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param catalogue The catalogue to be wrapped.
   * @param maxTriesToConnect The maximum number of times a single method should
   * try to connect to the database in the event of LostDatabaseConnection
   * exceptions being thrown.
   */
  CatalogueRetryWrapper(log::Logger &log, std::unique_ptr<Catalogue> catalogue, const uint32_t maxTriesToConnect = 3);

  CatalogueRetryWrapper(CatalogueRetryWrapper &) = delete;

  ~CatalogueRetryWrapper() override = default;

  CatalogueRetryWrapper &operator=(const CatalogueRetryWrapper &) = delete;

  void tapeLabelled(const std::string &vid, const std::string &drive) override;

  uint64_t checkAndGetNextArchiveFileId(const std::string &diskInstanceName, const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override;

  common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(const std::string &diskInstanceName,
    const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override;

  std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const override;

  common::dataStructures::Label::Format getTapeLabelFormat(const std::string& vid) const override;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) override;

  void tapeMountedForArchive(const std::string &vid, const std::string &drive) override;

  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(const std::string& diskInstanceName, const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity& user, const std::optional<std::string>& activity, log::LogContext& lc, const std::optional<std::string> &mountPolicyName) override;

  void tapeMountedForRetrieve(const std::string &vid, const std::string &drive) override;

  void noSpaceLeftOnTape(const std::string &vid) override;

  void createAdminUser(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) override;

  void deleteAdminUser(const std::string &username) override;

  std::list<common::dataStructures::AdminUser> getAdminUsers() const override;

  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) override;

  void createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) override;

  void deleteVirtualOrganization(const std::string &voName) override;

  std::list<common::dataStructures::VirtualOrganization> getVirtualOrganizations() const override;

  common::dataStructures::VirtualOrganization getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override;

  common::dataStructures::VirtualOrganization getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override;

  void modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) override;

  void modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) override ;

  void modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) override;

  void modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) override;


  void modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) override;

  void modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) override;

  void createStorageClass(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::StorageClass &storageClass) override;

  void deleteStorageClass(const std::string &storageClassName) override;

  std::list<common::dataStructures::StorageClass> getStorageClasses() const override;

  common::dataStructures::StorageClass getStorageClass(const std::string &name) const override;

  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbCopies) override;

  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  void modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override;

  void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override;

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

  void createTapePool(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue, const std::optional<std::string> &supply, const std::string &comment) override;

  void deleteTapePool(const std::string &name) override;

  std::list<TapePool> getTapePools(const TapePoolSearchCriteria &searchCriteria) const override;

  std::optional<TapePool> getTapePool(const std::string &tapePoolName) const override;

  void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override;

  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbPartialTapes) override;

  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool encryptionValue) override;

  void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &supply) override;

  void modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override;

  void createArchiveRoute(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName, const std::string &comment) override;

  void deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb) override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName, const std::string &tapePoolName) const override;

  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName) override;

  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &comment) override;

  void createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool isDisabled, const std::string &comment) override;

  void deleteLogicalLibrary(const std::string &name) override;

  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const override;

  void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override;

  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &disabledReason) override;

  void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) override;

  void createTape( const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes & tape) override;

  void deleteTape(const std::string &vid) override;

  std::list<common::dataStructures::Tape> getTapes(const TapeSearchCriteria &searchCriteria) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::string& vid) const override;

  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string> &vids) const override;

  std::map<std::string, std::string> getVidToLogicalLibrary(const std::set<std::string> &vids) const override;

  void reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, cta::log::LogContext & lc) override;

  void checkTapeForLabel(const std::string &vid) override;

  uint64_t getNbFilesOnTape(const std::string &vid) const override ;

  void modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &mediaType) override;

  void modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &vendor) override;

  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &logicalLibraryName) override;

  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &tapePoolName) override;

  void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &encryptionKeyName) override;

  void modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &verificationStatus) override;

  void modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const std::optional<std::string> & stateReason) override;

  void setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool fullValue) override;

  void setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool dirtyValue) override;

  void setTapeIsFromCastorInUnitTests(const std::string &vid) override;

  void setTapeDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string & reason) override;

  void setTapeDirty(const std::string & vid) override;

  void modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::optional<std::string> &comment) override;

  void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &mountPolicy) override;

  void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &comment) override;

  void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) override;

  void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &comment) override;

  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) override;

  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) override;

  void createMountPolicy(const common::dataStructures::SecurityIdentity &admin, const CreateMountPolicyAttributes & mountPolicy) override;

  std::list<common::dataStructures::MountPolicy> getMountPolicies() const override;

  std::optional<common::dataStructures::MountPolicy> getMountPolicy(const std::string &mountPolicyName) const override;

  std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const override;

  void deleteMountPolicy(const std::string &name) override;

  void createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName, const std::string &activityRegex, const std::string &comment) override;

  std::list<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const override;

  void deleteRequesterActivityMountRule(const std::string &diskInstanceName, const std::string &requesterName, const std::string &activityRegex) override;

  void createRequesterMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName, const std::string &comment) override;

  std::list<common::dataStructures::RequesterMountRule> getRequesterMountRules() const override;

  void deleteRequesterMountRule(const std::string &diskInstanceName, const std::string &requesterName) override;

  void createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstanceName, const std::string &requesterGroupName, const std::string &comment) override;

  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const override;

  void deleteRequesterGroupMountRule(const std::string &diskInstanceName, const std::string &requesterGroupName) override;

  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t archivePriority) override;

  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minArchiveRequestAge) override;

  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t retrievePriority) override;

  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minRetrieveRequestAge) override;

  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  disk::DiskSystemList getAllDiskSystems() const override;

  void createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment) override;

  void deleteDiskSystem(const std::string &name) override;

  void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &fileRegexp) override;

  void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t targetedFreeSpace) override;

  void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  void modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName) override;

  void modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceSpaceName) override;

  void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t sleepTime) override;

  void createDiskInstance(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  std::list<common::dataStructures::DiskInstance> getAllDiskInstances() const override;

  void deleteDiskInstance(const std::string &name) override;

  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  void createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL, const uint64_t refreshInterval, const std::string &comment) override;

  std::list<common::dataStructures::DiskInstanceSpace> getAllDiskInstanceSpaces() const override;

  void deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) override;

  void modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &comment) override;

  void modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) override;

  void modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) override;

  void modifyDiskInstanceSpaceFreeSpace(const std::string &name, const std::string &diskInstance, const uint64_t freeSpace) override;

  ArchiveFileItor getArchiveFilesItor(const TapeFileSearchCriteria &searchCriteria) const override;

  FileRecycleLogItor getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria) const override;

  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid) override;

  void deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc) override;

  void deleteFilesFromRecycleLog(const std::string & vid, log::LogContext & lc) override;

  std::list<common::dataStructures::ArchiveFile> getFilesForRepack(const std::string &vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const override;

  ArchiveFileItor getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const override;

  common::dataStructures::ArchiveFileSummary getTapeFileSummary(const TapeFileSearchCriteria &searchCriteria) const override;

  common::dataStructures::ArchiveFile getArchiveFileForDeletion(const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override;

  void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) override;

  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const override;

  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &instanceName, const uint64_t archiveFileId, log::LogContext &lc) override;

  bool isAdmin(const common::dataStructures::SecurityIdentity &admin) const override;

  void ping() override;

  void verifySchemaVersion() override;

  SchemaVersion getSchemaVersion() const override;

  bool tapePoolExists(const std::string &tapePoolName) const override;

  bool tapeExists(const std::string &vid) const;

  bool diskSystemExists(const std::string &name) const override;

  void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance, const std::string &diskFileId) override;

  void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) override;

  void createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) override;

  std::list<std::string> getTapeDriveNames() const override;

  std::list<common::dataStructures::TapeDrive> getTapeDrives() const override;

  std::optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string &tapeDriveName) const override;

  void setDesiredTapeDriveState(const std::string& tapeDriveName,
    const common::dataStructures::DesiredDriveState &desiredState) override;

  void setDesiredTapeDriveStateComment(const std::string& tapeDriveName, const std::string &comment) override;

  void updateTapeDriveStatistics(const std::string& tapeDriveName,
    const std::string& host, const std::string& logicalLibrary,
    const common::dataStructures::TapeDriveStatistics& statistics) override;

  void updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) override;

  void deleteTapeDrive(const std::string &tapeDriveName) override;

  void createTapeDriveConfig(const std::string &driveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) override;

  std::list<cta::catalogue::Catalogue::DriveConfig> getTapeDriveConfigs() const override;

  std::list<std::pair<std::string, std::string>> getTapeDriveConfigNamesAndKeys() const override;

  void modifyTapeDriveConfig(const std::string &driveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) override;

  std::optional<std::tuple<std::string, std::string, std::string>> getTapeDriveConfig( const std::string &tapeDriveName,
    const std::string &keyName) const override;

  void deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) override;

  std::map<std::string, uint64_t> getDiskSpaceReservations() const override;

  void reserveDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override;

  void releaseDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override;

protected:
  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger &m_log;

  /**
   * The wrapped catalogue.
   */
  std::unique_ptr<Catalogue> m_catalogue;

  /**
   * The maximum number of times a single method should try to connect to the
   * database in the event of LostDatabaseConnection exceptions being thrown.
   */
  uint32_t m_maxTriesToConnect;
};  // class CatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
