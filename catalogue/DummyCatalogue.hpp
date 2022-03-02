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

#include <iostream>

#include "Catalogue.hpp"
#include "common/make_unique.hpp"

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

  void createAdminUser(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createArchiveRoute(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& tapePoolName, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const bool isDisabled, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createMountPolicy(const common::dataStructures::SecurityIdentity& admin, const CreateMountPolicyAttributes & mountPolicy) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstanceName, const std::string& requesterGroupName, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createRequesterMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstance, const std::string& requesterName, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstance, const std::string& requesterName, const std::string &activityRegex, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createStorageClass(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::StorageClass& storageClass) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createTape(const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes & tape) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteMediaType(const std::string &name) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<MediaTypeWithLogs> getMediaTypes() const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  MediaType getMediaTypeByVid(const std::string & vid) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &cartridge) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const cta::optional<std::uint32_t> &nbWraps) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const cta::optional<std::uint64_t> &minLPos) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const cta::optional<std::uint64_t> &maxLPos) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createTapePool(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string & vo, const uint64_t nbPartialTapes, const bool encryptionValue, const cta::optional<std::string> &supply, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteAdminUser(const std::string& username) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string& instanceName, const uint64_t archiveFileId, log::LogContext &lc) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteArchiveRoute(const std::string& storageClassName, const uint32_t copyNb) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteLogicalLibrary(const std::string& name) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteMountPolicy(const std::string& name) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteRequesterGroupMountRule(const std::string& diskInstanceName, const std::string& requesterGroupName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteRequesterMountRule(const std::string& diskInstanceName, const std::string& requesterName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteRequesterActivityMountRule(const std::string& diskInstanceName, const std::string& requesterName, const std::string &activityRegex) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteStorageClass(const std::string& storageClassName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteTape(const std::string& vid) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteTapePool(const std::string& name) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteDiskSystem(const std::string &name) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &fileRegexp) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t targetedFreeSpace) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t sleepTime) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<common::dataStructures::DiskInstance> getAllDiskInstances() const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void deleteDiskInstance(const std::string &name) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<common::dataStructures::DiskInstanceSpace> getAllDiskInstanceSpaces() const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) override {  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<common::dataStructures::AdminUser> getAdminUsers() const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  ArchiveFileItor getArchiveFilesItor(const TapeFileSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  FileRecycleLogItor getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void deleteFilesFromRecycleLog(const std::string & vid, log::LogContext & lc) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<common::dataStructures::TapeDrive> getTapeDrives() const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void deleteTapeDrive(const std::string &tapeDriveName) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category, const std::string &keyName, const std::string &value, const std::string &source) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<cta::catalogue::Catalogue::DriveConfig> getTapeDriveConfigs() const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<std::pair<std::string, std::string>> getTapeDriveConfigNamesAndKeys() const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category, const std::string &keyName, const std::string &value, const std::string &source) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  optional<std::tuple<std::string, std::string, std::string>> getTapeDriveConfig( const std::string &tapeDriveName, const std::string &keyName) const {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<common::dataStructures::ArchiveFile> getFilesForRepack(const std::string &vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  ArchiveFileItor getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName, const std::string &tapePoolName) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::RequesterMountRule> getRequesterMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::StorageClass> getStorageClasses() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::StorageClass getStorageClass(const std::string &name) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::ArchiveFileSummary getTapeFileSummary(const TapeFileSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::ArchiveFile getArchiveFileForDeletion(const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) override {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  std::list<TapePool> getTapePools(const TapePoolSearchCriteria &searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  cta::optional<TapePool> getTapePool(const std::string &tapePoolName) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::Tape> getTapes(const TapeSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
 // getTapesByVid is implemented below (and works).
  std::map<std::string, std::string> getVidToLogicalLibrary(const std::set<std::string> &vids) const override { throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented"); }
  std::list<TapeForWriting> getTapesForWriting(const std::string& logicalLibraryName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  bool isAdmin(const common::dataStructures::SecurityIdentity& admin) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteVirtualOrganization(const std::string &voName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::VirtualOrganization> getVirtualOrganizations() const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::VirtualOrganization getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::VirtualOrganization getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& storageClassName, const uint32_t copyNb, const std::string& tapePoolName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minArchiveRequestAge) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t archivePriority) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minRetrieveRequestAge) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t retrievePriority) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string &activityRegex, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& mountPolicy) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& mountPolicy) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string &activityRegex, const std::string& mountPolicy) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyStorageClassName(const common::dataStructures::SecurityIdentity& admin, const std::string& currentName, const std::string& newName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbCopies) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeComment(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const cta::optional<std::string> &comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& encryptionKeyName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const cta::optional<std::string> & stateReason) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeMediaType(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& mediaType) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeVendor(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& vendor) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& logicalLibraryName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbPartialTapes) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& supply) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& currentName, const std::string& newName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& tapePoolName) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void noSpaceLeftOnTape(const std::string& vid) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void ping() override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void verifySchemaVersion() override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  SchemaVersion getSchemaVersion() const override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  uint64_t checkAndGetNextArchiveFileId(const std::string &diskInstanceName, const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(const std::string &diskInstanceName,
    const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(const std::string& diskInstanceName, const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity& user, const optional<std::string>& activity, log::LogContext& lc) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void reclaimTape(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, cta::log::LogContext & lc) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void checkTapeForLabel(const std::string& vid) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  uint64_t getNbFilesOnTape(const std::string& vid) const  override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapeDisabled(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string & reason) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapeFull(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool fullValue) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapeDirty(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool dirtyValue) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapeDirty(const std::string & vid) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapeIsFromCastorInUnitTests(const std::string &vid) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const bool encryptionValue) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  bool tapeExists(const std::string& vid) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  bool diskSystemExists(const std::string& name) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void tapeLabelled(const std::string& vid, const std::string& drive) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void tapeMountedForArchive(const std::string& vid, const std::string& drive) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void tapeMountedForRetrieve(const std::string& vid, const std::string& drive) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  bool tapePoolExists(const std::string& tapePoolName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance, const std::string &diskFileId) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) override {throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");}
  // Special functions for unit tests.
  void addEnabledTape(const std::string & vid) {
    threading::MutexLocker lm(m_tapeEnablingMutex);
    m_tapeEnabling[vid]=common::dataStructures::Tape::ACTIVE;
  }
  void addDisabledTape(const std::string & vid) {
    threading::MutexLocker lm(m_tapeEnablingMutex);
    m_tapeEnabling[vid]=common::dataStructures::Tape::DISABLED;
  }
  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string>& vids) const {
    // Minimal implementation of VidToMap for retrieve request unit tests. We just support
    // disabled status for the tapes.
    // If the tape is not listed, it is listed as enabled in the return value.
    threading::MutexLocker lm(m_tapeEnablingMutex);
    common::dataStructures::VidToTapeMap ret;
    for (const auto & v: vids) {
      try {
        ret[v].state = m_tapeEnabling.at(v);
      } catch (std::out_of_range &) {
        ret[v].state = common::dataStructures::Tape::ACTIVE;
      }
    }
    return ret;
  }
  std::list<common::dataStructures::MountPolicy> getMountPolicies() const {
    std::list<common::dataStructures::MountPolicy> mountPolicies;
    common::dataStructures::MountPolicy mp1;
    mp1.name = "mountPolicy";
    mp1.archivePriority = 1;
    mp1.archiveMinRequestAge = 0;
    mp1.retrievePriority = 1;
    mp1.retrieveMinRequestAge = 0;
    mountPolicies.push_back(mp1);

    common::dataStructures::MountPolicy mp2;
    mp2.name = "moreAdvantageous";
    mp2.archivePriority = 2;
    mp2.archiveMinRequestAge = 0;
    mp2.retrievePriority = 2;
    mp2.retrieveMinRequestAge = 0;
    mountPolicies.push_back(mp1);
    return mountPolicies;
  }

  std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const override {
    std::list<common::dataStructures::MountPolicy> mountPolicies;
    common::dataStructures::MountPolicy mp1;
    mp1.name = "mountPolicy";
    mp1.archivePriority = 1;
    mp1.archiveMinRequestAge = 0;
    mp1.retrievePriority = 1;
    mp1.retrieveMinRequestAge = 0;
    mountPolicies.push_back(mp1);

    common::dataStructures::MountPolicy mp2;
    mp2.name = "moreAdvantageous";
    mp2.archivePriority = 2;
    mp2.archiveMinRequestAge = 0;
    mp2.retrievePriority = 2;
    mp2.retrieveMinRequestAge = 0;
    mountPolicies.push_back(mp1);
    return mountPolicies;
  }

  std::list<std::string> getTapeDriveNames() const {
    return {m_tapeDriveStatus.driveName};
  }

  optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string &tapeDriveName) const {
    if (m_tapeDriveStatus.driveName != "") return m_tapeDriveStatus;
    common::dataStructures::TapeDrive tapeDriveStatus;
    const time_t reportTime = time(NULL);

    tapeDriveStatus.driveName = tapeDriveName;
    tapeDriveStatus.host = "Dummy_Host";
    tapeDriveStatus.logicalLibrary = "Dummy_Library";

    tapeDriveStatus.downOrUpStartTime = reportTime;

    tapeDriveStatus.mountType = common::dataStructures::MountType::NoMount;
    tapeDriveStatus.driveStatus = common::dataStructures::DriveStatus::Down;
    tapeDriveStatus.desiredUp = false;
    tapeDriveStatus.desiredForceDown = false;

    tapeDriveStatus.diskSystemName = "Dummy_System";
    tapeDriveStatus.reservedBytes = 0;
    tapeDriveStatus.reservationSessionId = 0;
    

    return tapeDriveStatus;
  }

  void setDesiredTapeDriveState(const std::string&,
      const common::dataStructures::DesiredDriveState &desiredState) override {
    m_tapeDriveStatus.desiredUp = desiredState.up;
    m_tapeDriveStatus.desiredForceDown = desiredState.forceDown;
    m_tapeDriveStatus.reasonUpDown = desiredState.reason;
  }

  void setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
    const std::string &comment) override {
    m_tapeDriveStatus.userComment = comment;
  }

  void updateTapeDriveStatistics(const std::string& tapeDriveName,
    const std::string& host, const std::string& logicalLibrary,
    const common::dataStructures::TapeDriveStatistics& statistics) override {
    m_tapeDriveStatus.driveName = tapeDriveName;
    m_tapeDriveStatus.host = host;
    m_tapeDriveStatus.logicalLibrary = logicalLibrary;
    m_tapeDriveStatus.bytesTransferedInSession = statistics.bytesTransferedInSession;
    m_tapeDriveStatus.filesTransferedInSession = statistics.filesTransferedInSession;
    m_tapeDriveStatus.lastModificationLog = statistics.lastModificationLog;
  }

  void updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) {
    m_tapeDriveStatus = tapeDrive;
  }

  void modifyTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) {
    m_tapeDriveStatus = tapeDrive;
  }

  std::map<std::string, uint64_t> getDiskSpaceReservations() const override {
    std::map<std::string, uint64_t> ret;
    const auto tdNames = getTapeDriveNames();
    for (const auto& driveName : tdNames) {
      const auto tdStatus = getTapeDrive(driveName);
      if (tdStatus.value().diskSystemName) {
        //no need to check key, operator[] initializes missing values at zero for scalar types
        ret[tdStatus.value().diskSystemName.value()] += tdStatus.value().reservedBytes.value();
      }
    }
    return ret;
  }

  void reserveDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override {
    if(diskSpaceReservation.empty()) return;

    log::ScopedParamContainer params(lc);
    params.add("driveName", driveName)
          .add("diskSystem", diskSpaceReservation.begin()->first)
          .add("reservationBytes", diskSpaceReservation.begin()->second)
          .add("mountId", mountId);
    lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): reservation request.");

    auto tdStatus = getTapeDrive(driveName);
    if (!tdStatus) return;

    if (!tdStatus.value().reservationSessionId) {
      tdStatus.value().reservationSessionId = mountId;
      tdStatus.value().reservedBytes = 0;
    }
    
    if (tdStatus.value().reservationSessionId != mountId) {
      tdStatus.value().reservationSessionId = mountId;
      tdStatus.value().reservedBytes = 0;
    }
    
    tdStatus.value().diskSystemName = diskSpaceReservation.begin()->first;
    tdStatus.value().reservedBytes.value() += diskSpaceReservation.begin()->second;
    modifyTapeDrive(tdStatus.value());
  }

  void releaseDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) override {
    if (diskSpaceReservation.empty()) return;

    log::ScopedParamContainer params(lc);
    params.add("driveName", driveName)
          .add("diskSystem", diskSpaceReservation.begin()->first)
          .add("reservationBytes", diskSpaceReservation.begin()->second)
          .add("mountId", mountId);
    lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): reservation release request.");

    auto tdStatus = getTapeDrive(driveName);
    
    if (!tdStatus) return;
    if (!tdStatus.value().reservationSessionId) {
      return;
    }
    if (tdStatus.value().reservationSessionId != mountId) {
      return;
    }
    auto& bytes = diskSpaceReservation.begin()->second;
    if (bytes > tdStatus.value().reservedBytes) throw NegativeDiskSpaceReservationReached(
      "In DriveState::subtractDiskSpaceReservation(): we would reach a negative reservation size.");
    tdStatus.value().diskSystemName = diskSpaceReservation.begin()->first;
    tdStatus.value().reservedBytes.value() -= bytes;
    modifyTapeDrive(tdStatus.value());
  }


  /*
   * Implemented for testing disk space reservation logic
   */
  disk::DiskSystemList getAllDiskSystems() const override {
    return m_diskSystemList;
  }

  void createDiskInstance(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    m_diskInstances[name] = {name, comment, common::dataStructures::EntryLog(), common::dataStructures::EntryLog()};
  }

  void createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL, const uint64_t refreshInterval, const std::string &comment) override {
    m_diskInstanceSpaces[name] = {name, diskInstance, freeSpaceQueryURL, refreshInterval, 0, 0, comment, common::dataStructures::EntryLog(), common::dataStructures::EntryLog()};
  }
  

  void createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp, const uint64_t targetedFreeSpace, const uint64_t sleepTime, const std::string &comment)  override {
    m_diskSystemList.push_back({name, m_diskInstanceSpaces.at(diskInstanceSpaceName), fileRegexp, targetedFreeSpace, sleepTime, common::dataStructures::EntryLog(), common::dataStructures::EntryLog{}, comment});
  }

private:
  mutable threading::Mutex m_tapeEnablingMutex;
  std::map<std::string, common::dataStructures::Tape::State> m_tapeEnabling;

  common::dataStructures::TapeDrive m_tapeDriveStatus;

  disk::DiskSystemList m_diskSystemList;
  std::map<std::string, common::dataStructures::DiskInstance> m_diskInstances;
  std::map<std::string, common::dataStructures::DiskInstanceSpace> m_diskInstanceSpaces;
  
};

}} // namespace cta::catalogue.
