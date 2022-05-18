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

#include "catalogue/Catalogue.hpp"
#include "catalogue/retryOnLostConnection.hpp"

#include <memory>

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
  CatalogueRetryWrapper(log::Logger &log, std::unique_ptr<Catalogue> catalogue, const uint32_t maxTriesToConnect = 3):
    m_log(log),
    m_catalogue(std::move(catalogue)),
    m_maxTriesToConnect(maxTriesToConnect) {
  }

  /**
   * Deletion of the copy constructor.
   */
  CatalogueRetryWrapper(CatalogueRetryWrapper &) = delete;

  /**
   * Destructor.
   */
  ~CatalogueRetryWrapper() override = default;

  /**
   * Deletion of the copy assignment operator.
   */
  CatalogueRetryWrapper &operator=(const CatalogueRetryWrapper &) = delete;

  void tapeLabelled(const std::string &vid, const std::string &drive) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeLabelled(vid, drive);}, m_maxTriesToConnect);
  }

  uint64_t checkAndGetNextArchiveFileId(const std::string &diskInstanceName, const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->checkAndGetNextArchiveFileId(diskInstanceName, storageClassName, user);}, m_maxTriesToConnect);
  }

  common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(const std::string &diskInstanceName,
    const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFileQueueCriteria(diskInstanceName, storageClassName, user);}, m_maxTriesToConnect);
  }

  std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapesForWriting(logicalLibraryName);}, m_maxTriesToConnect);
  }

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->filesWrittenToTape(event);}, m_maxTriesToConnect);
  }

  void tapeMountedForArchive(const std::string &vid, const std::string &drive) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeMountedForArchive(vid, drive);}, m_maxTriesToConnect);
  }

  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(const std::string& diskInstanceName, const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity& user, const std::optional<std::string>& activity, log::LogContext& lc, const std::optional<std::string> &mountPolicyName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->prepareToRetrieveFile(diskInstanceName, archiveFileId, user, activity, lc, mountPolicyName);}, m_maxTriesToConnect);
  }

  void tapeMountedForRetrieve(const std::string &vid, const std::string &drive) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeMountedForRetrieve(vid, drive);}, m_maxTriesToConnect);
  }

  void noSpaceLeftOnTape(const std::string &vid) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->noSpaceLeftOnTape(vid);}, m_maxTriesToConnect);
  }

  void createAdminUser(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createAdminUser(admin, username, comment);}, m_maxTriesToConnect);
  }

  void deleteAdminUser(const std::string &username) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteAdminUser(username);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::AdminUser> getAdminUsers() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getAdminUsers();}, m_maxTriesToConnect);
  }

  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyAdminUserComment(admin, username, comment);}, m_maxTriesToConnect);
  }

  void createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createVirtualOrganization(admin, vo);}, m_maxTriesToConnect);
  }

  void deleteVirtualOrganization(const std::string &voName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteVirtualOrganization(voName);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::VirtualOrganization> getVirtualOrganizations() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getVirtualOrganizations();}, m_maxTriesToConnect);
  }

  common::dataStructures::VirtualOrganization getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getVirtualOrganizationOfTapepool(tapepoolName);}, m_maxTriesToConnect);
  }

  common::dataStructures::VirtualOrganization getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getCachedVirtualOrganizationOfTapepool(tapepoolName);}, m_maxTriesToConnect);
  }

  void modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName, const std::string &newVoName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationName(admin,currentVoName,newVoName);}, m_maxTriesToConnect);
  }

  void modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationReadMaxDrives(admin,voName,readMaxDrives);}, m_maxTriesToConnect);
  }

  void modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationWriteMaxDrives(admin,voName,writeMaxDrives);}, m_maxTriesToConnect);
  }

  void modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationMaxFileSize(admin,voName,maxFileSize);}, m_maxTriesToConnect);
  }


  void modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationComment(admin,voName,comment);}, m_maxTriesToConnect);
  }

  void modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyVirtualOrganizationDiskInstanceName(admin,voName,diskInstance);}, m_maxTriesToConnect);
  }

  void createStorageClass(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::StorageClass &storageClass) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createStorageClass(admin, storageClass);}, m_maxTriesToConnect);
  }

  void deleteStorageClass(const std::string &storageClassName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteStorageClass(storageClassName);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::StorageClass> getStorageClasses() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getStorageClasses();}, m_maxTriesToConnect);
  }

  common::dataStructures::StorageClass getStorageClass(const std::string &name) const override {
   return retryOnLostConnection(m_log, [&]{return m_catalogue->getStorageClass(name);}, m_maxTriesToConnect);
  }

  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbCopies) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassNbCopies(admin, name, nbCopies);}, m_maxTriesToConnect);
  }

  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassComment(admin, name, comment);}, m_maxTriesToConnect);
  }

  void modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassName(admin, currentName, newName);}, m_maxTriesToConnect);
  }

  void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyStorageClassVo(admin, name, vo);}, m_maxTriesToConnect);
  }

  void createMediaType(const common::dataStructures::SecurityIdentity &admin, const MediaType &mediaType) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createMediaType(admin, mediaType);}, m_maxTriesToConnect);
  }

  void deleteMediaType(const std::string &name) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteMediaType(name);}, m_maxTriesToConnect);
  }

  std::list<MediaTypeWithLogs> getMediaTypes() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getMediaTypes();}, m_maxTriesToConnect);
  }

  MediaType getMediaTypeByVid(const std::string & vid) const override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->getMediaTypeByVid(vid);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeName(admin, currentName, newName);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &cartridge) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeCartridge(admin, name, cartridge);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeCapacityInBytes(admin, name, capacityInBytes);}, m_maxTriesToConnect);
  }

  void modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypePrimaryDensityCode(admin, name, primaryDensityCode);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeSecondaryDensityCode(admin, name, secondaryDensityCode);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint32_t> &nbWraps) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeNbWraps(admin, name, nbWraps);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &minLPos) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeMinLPos(admin, name, minLPos);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::optional<std::uint64_t> &maxLPos) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeMaxLPos(admin, name, maxLPos);}, m_maxTriesToConnect);
  }

  void modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMediaTypeComment(admin, name, comment);}, m_maxTriesToConnect);
  }

  void createTapePool(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo, const uint64_t nbPartialTapes, const bool encryptionValue, const std::optional<std::string> &supply, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createTapePool(admin, name, vo, nbPartialTapes, encryptionValue, supply, comment);}, m_maxTriesToConnect);
  }

  void deleteTapePool(const std::string &name) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteTapePool(name);}, m_maxTriesToConnect);
  }

  std::list<TapePool> getTapePools(const TapePoolSearchCriteria &searchCriteria) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapePools(searchCriteria);}, m_maxTriesToConnect);
  }

  std::optional<TapePool> getTapePool(const std::string &tapePoolName) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapePool(tapePoolName);}, m_maxTriesToConnect);
  }

  void modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolVo(admin, name, vo);}, m_maxTriesToConnect);
  }

  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbPartialTapes) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolNbPartialTapes(admin, name, nbPartialTapes);}, m_maxTriesToConnect);
  }

  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolComment(admin, name, comment);}, m_maxTriesToConnect);
  }

  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool encryptionValue) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapePoolEncryption(admin, name, encryptionValue);}, m_maxTriesToConnect);
  }

  void modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &supply) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolSupply(admin, name, supply);}, m_maxTriesToConnect);
  }

  void modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapePoolName(admin, currentName, newName);}, m_maxTriesToConnect);
  }

  void createArchiveRoute(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createArchiveRoute(admin, storageClassName, copyNb, tapePoolName, comment);}, m_maxTriesToConnect);
  }

  void deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteArchiveRoute(storageClassName, copyNb);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveRoutes();}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(const std::string &storageClassName, const std::string &tapePoolName) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveRoutes(storageClassName, tapePoolName);}, m_maxTriesToConnect);
  }

  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyArchiveRouteTapePoolName(admin, storageClassName, copyNb, tapePoolName);}, m_maxTriesToConnect);
  }

  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin, const std::string &storageClassName, const uint32_t copyNb, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyArchiveRouteComment(admin, storageClassName, copyNb, comment);}, m_maxTriesToConnect);
  }

  void createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool isDisabled, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createLogicalLibrary(admin, name, isDisabled, comment);}, m_maxTriesToConnect);
  }

  void deleteLogicalLibrary(const std::string &name) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteLogicalLibrary(name);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getLogicalLibraries();}, m_maxTriesToConnect);
  }

  void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &currentName, const std::string &newName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyLogicalLibraryName(admin, currentName, newName);}, m_maxTriesToConnect);
  }

  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyLogicalLibraryComment(admin, name, comment);}, m_maxTriesToConnect);
  }

  void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &disabledReason) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyLogicalLibraryDisabledReason(admin, name, disabledReason);}, m_maxTriesToConnect);
  }

  void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool disabledValue) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->setLogicalLibraryDisabled(admin, name, disabledValue);}, m_maxTriesToConnect);
  }

  void createTape( const common::dataStructures::SecurityIdentity &admin, const CreateTapeAttributes & tape) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createTape(admin, tape);}, m_maxTriesToConnect);
  }

  void deleteTape(const std::string &vid) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteTape(vid);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::Tape> getTapes(const TapeSearchCriteria &searchCriteria) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapes(searchCriteria);}, m_maxTriesToConnect);
  }

  common::dataStructures::VidToTapeMap getTapesByVid(const std::string& vid) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapesByVid(vid);}, m_maxTriesToConnect);
  }

  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string> &vids) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapesByVid(vids);}, m_maxTriesToConnect);
  }

  std::map<std::string, std::string> getVidToLogicalLibrary(const std::set<std::string> &vids) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getVidToLogicalLibrary(vids);}, m_maxTriesToConnect);
  }

  void reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, cta::log::LogContext & lc) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->reclaimTape(admin, vid,lc);}, m_maxTriesToConnect);
  }

  void checkTapeForLabel(const std::string &vid) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->checkTapeForLabel(vid);}, m_maxTriesToConnect);
  }

  uint64_t getNbFilesOnTape(const std::string &vid) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getNbFilesOnTape(vid);}, m_maxTriesToConnect);
  }

  void modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &mediaType) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeMediaType(admin, vid, mediaType);}, m_maxTriesToConnect);
  }

  void modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &vendor) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeVendor(admin, vid, vendor);}, m_maxTriesToConnect);
  }

  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &logicalLibraryName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeLogicalLibraryName(admin, vid, logicalLibraryName);}, m_maxTriesToConnect);
  }

  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &tapePoolName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeTapePoolName(admin, vid, tapePoolName);}, m_maxTriesToConnect);
  }

  void modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &encryptionKeyName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeEncryptionKeyName(admin, vid, encryptionKeyName);}, m_maxTriesToConnect);
  }
  void modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &verificationStatus) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeVerificationStatus(admin, vid, verificationStatus);}, m_maxTriesToConnect);
  }
  
  void modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const std::optional<std::string> & stateReason) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeState(admin,vid, state, stateReason);}, m_maxTriesToConnect);
  }

  void setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool fullValue) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeFull(admin, vid, fullValue);}, m_maxTriesToConnect);
  }

  void setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool dirtyValue) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeDirty(admin, vid, dirtyValue);}, m_maxTriesToConnect);
  }

  void setTapeIsFromCastorInUnitTests(const std::string &vid) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeIsFromCastorInUnitTests(vid);}, m_maxTriesToConnect);
  }

  void setTapeDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string & reason) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->setTapeDisabled(admin, vid, reason);}, m_maxTriesToConnect);
  }

  void setTapeDirty(const std::string & vid) override {
    return retryOnLostConnection(m_log,[&]{ return m_catalogue->setTapeDirty(vid);}, m_maxTriesToConnect);
  }

  void modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::optional<std::string> &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyTapeComment(admin, vid, comment);}, m_maxTriesToConnect);
  }

  void modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &mountPolicy) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterActivityMountRulePolicy(admin, instanceName, requesterName, activityRegex, mountPolicy);}, m_maxTriesToConnect);
  }

  void modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterActivityMountRuleComment(admin, instanceName, requesterName, activityRegex, comment);}, m_maxTriesToConnect);
  }

  void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterMountRulePolicy(admin, instanceName, requesterName, mountPolicy);}, m_maxTriesToConnect);
  }

  void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesteMountRuleComment(admin, instanceName, requesterName, comment);}, m_maxTriesToConnect);
  }

  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterGroupMountRulePolicy(admin, instanceName, requesterGroupName, mountPolicy);}, m_maxTriesToConnect);
  }

  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyRequesterGroupMountRuleComment(admin, instanceName, requesterGroupName, comment);}, m_maxTriesToConnect);
  }

  void createMountPolicy(const common::dataStructures::SecurityIdentity &admin, const CreateMountPolicyAttributes & mountPolicy) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createMountPolicy(admin, mountPolicy);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::MountPolicy> getMountPolicies() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getMountPolicies();}, m_maxTriesToConnect);
  }

  std::optional<common::dataStructures::MountPolicy> getMountPolicy(const std::string &mountPolicyName) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getMountPolicy(mountPolicyName);}, m_maxTriesToConnect);
  }


  std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getCachedMountPolicies();}, m_maxTriesToConnect);
  }

  void deleteMountPolicy(const std::string &name) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteMountPolicy(name);}, m_maxTriesToConnect);
  }

  void createRequesterActivityMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName, const std::string &activityRegex, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createRequesterActivityMountRule(admin, mountPolicyName, diskInstance, requesterName, activityRegex, comment);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::RequesterActivityMountRule> getRequesterActivityMountRules() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getRequesterActivityMountRules();}, m_maxTriesToConnect);
  }

  void deleteRequesterActivityMountRule(const std::string &diskInstanceName, const std::string &requesterName, const std::string &activityRegex) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteRequesterActivityMountRule(diskInstanceName, requesterName, activityRegex);}, m_maxTriesToConnect);
  }

  void createRequesterMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstance, const std::string &requesterName, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createRequesterMountRule(admin, mountPolicyName, diskInstance, requesterName, comment);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::RequesterMountRule> getRequesterMountRules() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getRequesterMountRules();}, m_maxTriesToConnect);
  }

  void deleteRequesterMountRule(const std::string &diskInstanceName, const std::string &requesterName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteRequesterMountRule(diskInstanceName, requesterName);}, m_maxTriesToConnect);
  }

  void createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity &admin, const std::string &mountPolicyName, const std::string &diskInstanceName, const std::string &requesterGroupName, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createRequesterGroupMountRule(admin, mountPolicyName, diskInstanceName, requesterGroupName, comment);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getRequesterGroupMountRules();}, m_maxTriesToConnect);
  }

  void deleteRequesterGroupMountRule(const std::string &diskInstanceName, const std::string &requesterGroupName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);}, m_maxTriesToConnect);
  }

  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t archivePriority) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyArchivePriority(admin, name, archivePriority);}, m_maxTriesToConnect);
  }

  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minArchiveRequestAge) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyArchiveMinRequestAge(admin, name, minArchiveRequestAge);}, m_maxTriesToConnect);
  }

  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t retrievePriority) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyRetrievePriority(admin, name, retrievePriority);}, m_maxTriesToConnect);
  }

  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minRetrieveRequestAge) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyRetrieveMinRequestAge(admin, name, minRetrieveRequestAge);}, m_maxTriesToConnect);
  }

  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyMountPolicyComment(admin, name, comment);}, m_maxTriesToConnect);
  }

  disk::DiskSystemList getAllDiskSystems() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getAllDiskSystems();}, m_maxTriesToConnect);
  }

  void createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp, const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment)  override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createDiskSystem(admin, name, diskInstanceName, diskInstanceSpaceName, fileRegexp, targetedFreeSpace, sleepTime, comment);}, m_maxTriesToConnect);
  }

  void deleteDiskSystem(const std::string &name) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteDiskSystem(name);}, m_maxTriesToConnect);
  }

  void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &fileRegexp) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemFileRegexp(admin, name, fileRegexp);}, m_maxTriesToConnect);
  }

  void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t targetedFreeSpace) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemTargetedFreeSpace(admin, name, targetedFreeSpace);}, m_maxTriesToConnect);
  }

  void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemComment(admin, name, comment);}, m_maxTriesToConnect);
  }

  void modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemDiskInstanceName(admin, name, diskInstanceName);}, m_maxTriesToConnect);
  }

  void modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstanceSpaceName) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemDiskInstanceSpaceName(admin, name, diskInstanceSpaceName);}, m_maxTriesToConnect);
  }

  void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t sleepTime) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskSystemSleepTime(admin, name, sleepTime);}, m_maxTriesToConnect);
  }

  void createDiskInstance(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment)  override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createDiskInstance(admin, name, comment);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::DiskInstance> getAllDiskInstances() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getAllDiskInstances();}, m_maxTriesToConnect);
  }

  void deleteDiskInstance(const std::string &name) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteDiskInstance(name);}, m_maxTriesToConnect);
  }

  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceComment(admin, name, comment);}, m_maxTriesToConnect);
  }

  void createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL, const uint64_t refreshInterval, const std::string &comment) override{
    return retryOnLostConnection(m_log, [&]{return m_catalogue->createDiskInstanceSpace(admin, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);}, m_maxTriesToConnect);
  }

  std::list<common::dataStructures::DiskInstanceSpace> getAllDiskInstanceSpaces() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getAllDiskInstanceSpaces();}, m_maxTriesToConnect);
  }

  void deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteDiskInstanceSpace(name, diskInstance);}, m_maxTriesToConnect);
  }

  void modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &comment) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceComment(admin, name, diskInstance, comment);}, m_maxTriesToConnect);
  }

  void modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceRefreshInterval(admin, name, diskInstance, refreshInterval);}, m_maxTriesToConnect);
  }

  void modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceQueryURL(admin, name, diskInstance, freeSpaceQueryURL);}, m_maxTriesToConnect);
  }

  void modifyDiskInstanceSpaceFreeSpace(const std::string &name, const std::string &diskInstance, const uint64_t freeSpace) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->modifyDiskInstanceSpaceFreeSpace(name, diskInstance, freeSpace);}, m_maxTriesToConnect); 
  }

  ArchiveFileItor getArchiveFilesItor(const TapeFileSearchCriteria &searchCriteria) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFilesItor(searchCriteria);}, m_maxTriesToConnect);
  }

  FileRecycleLogItor getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getFileRecycleLogItor(searchCriteria);}, m_maxTriesToConnect);
  }

  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->restoreFileInRecycleLog(searchCriteria, newFid);}, m_maxTriesToConnect);
  }

  void deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc){
    return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteFileFromRecycleBin(archiveFileId,lc);},m_maxTriesToConnect);
  }

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
  void deleteFilesFromRecycleLog(const std::string & vid, log::LogContext & lc){
    return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteFilesFromRecycleLog(vid,lc);},m_maxTriesToConnect);
  }

  std::list<common::dataStructures::ArchiveFile> getFilesForRepack(const std::string &vid, const uint64_t startFSeq, const uint64_t maxNbFiles) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getFilesForRepack(vid, startFSeq, maxNbFiles);}, m_maxTriesToConnect);
  }

  ArchiveFileItor getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFilesForRepackItor(vid, startFSeq);}, m_maxTriesToConnect);
  }

  common::dataStructures::ArchiveFileSummary getTapeFileSummary(const TapeFileSearchCriteria &searchCriteria) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getTapeFileSummary(searchCriteria);}, m_maxTriesToConnect);
  }

  common::dataStructures::ArchiveFile getArchiveFileForDeletion(const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFileForDeletion(searchCriteria);}, m_maxTriesToConnect);
  }

  void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->deleteTapeFileCopy(file, reason);}, m_maxTriesToConnect);
  }

  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getArchiveFileById(id);}, m_maxTriesToConnect);
  }

  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &instanceName, const uint64_t archiveFileId, log::LogContext &lc) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(instanceName, archiveFileId, lc);}, m_maxTriesToConnect);
  }

  bool isAdmin(const common::dataStructures::SecurityIdentity &admin) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->isAdmin(admin);}, m_maxTriesToConnect);
  }

  void ping() override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->ping();}, m_maxTriesToConnect);
  }

  void verifySchemaVersion() override{
    return retryOnLostConnection(m_log, [&]{return m_catalogue->verifySchemaVersion();}, m_maxTriesToConnect);
  }

  SchemaVersion getSchemaVersion() const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getSchemaVersion();}, m_maxTriesToConnect);
  }

  bool tapePoolExists(const std::string &tapePoolName) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->tapePoolExists(tapePoolName);}, m_maxTriesToConnect);
  }

  bool tapeExists(const std::string &vid) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->tapeExists(vid);}, m_maxTriesToConnect);
  }

  bool diskSystemExists(const std::string &name) const override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->diskSystemExists(name);}, m_maxTriesToConnect);
  }

  void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance, const std::string &diskFileId) override {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->updateDiskFileId(archiveFileId, diskInstance, diskFileId);}, m_maxTriesToConnect);
  }

  void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->moveArchiveFileToRecycleLog(request,lc);},m_maxTriesToConnect);
  }

  void createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->createTapeDrive(tapeDrive);},m_maxTriesToConnect);
  }

  std::list<std::string> getTapeDriveNames() const override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveNames();},m_maxTriesToConnect);
  }

  std::list<common::dataStructures::TapeDrive> getTapeDrives() const override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDrives();},m_maxTriesToConnect);
  }

  std::optional<common::dataStructures::TapeDrive> getTapeDrive(const std::string &tapeDriveName) const override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDrive(tapeDriveName);},m_maxTriesToConnect);
  }

  void setDesiredTapeDriveState(const std::string& tapeDriveName,
    const common::dataStructures::DesiredDriveState &desiredState) override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->setDesiredTapeDriveState(tapeDriveName, desiredState);}, m_maxTriesToConnect);
  }

  void setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
    const std::string &comment) override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->setDesiredTapeDriveStateComment(tapeDriveName, comment);}, m_maxTriesToConnect);
  }

  void updateTapeDriveStatistics(const std::string& tapeDriveName,
    const std::string& host, const std::string& logicalLibrary,
    const common::dataStructures::TapeDriveStatistics& statistics) override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->updateTapeDriveStatistics(tapeDriveName, host, logicalLibrary, statistics);}, m_maxTriesToConnect);
  }

  void updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->updateTapeDriveStatus(tapeDrive);}, m_maxTriesToConnect);
  }

  void deleteTapeDrive(const std::string &tapeDriveName) override {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteTapeDrive(tapeDriveName);},m_maxTriesToConnect);
  }

  void createTapeDriveConfig(const std::string &driveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->createTapeDriveConfig(driveName, category, keyName, value, source);},m_maxTriesToConnect);
  }

  std::list<cta::catalogue::Catalogue::DriveConfig> getTapeDriveConfigs() const {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveConfigs();},m_maxTriesToConnect);
  }

  std::list<std::pair<std::string, std::string>> getTapeDriveConfigNamesAndKeys() const {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveConfigNamesAndKeys();},m_maxTriesToConnect);
  }

  void modifyTapeDriveConfig(const std::string &driveName, const std::string &category,
    const std::string &keyName, const std::string &value, const std::string &source) {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->modifyTapeDriveConfig(driveName, category, keyName, value, source);},m_maxTriesToConnect);
  }

  std::optional<std::tuple<std::string, std::string, std::string>> getTapeDriveConfig( const std::string &tapeDriveName,
    const std::string &keyName) const {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->getTapeDriveConfig(tapeDriveName, keyName);},m_maxTriesToConnect);
  }

  void deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {
    return retryOnLostConnection(m_log,[&]{return m_catalogue->deleteTapeDriveConfig(tapeDriveName, keyName);},m_maxTriesToConnect);
  }

  std::map<std::string, uint64_t> getDiskSpaceReservations() const {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->getDiskSpaceReservations();}, m_maxTriesToConnect);
  }

  void reserveDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->reserveDiskSpace(driveName, mountId, diskSpaceReservation, lc);}, m_maxTriesToConnect);
  }

  void releaseDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
    return retryOnLostConnection(m_log, [&]{return m_catalogue->releaseDiskSpace(driveName, mountId, diskSpaceReservation, lc);}, m_maxTriesToConnect);
  }

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

}; // class CatalogueRetryWrapper

} // namespace catalogue
} // namespace cta
