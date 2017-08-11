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

#include "Catalogue.hpp"

namespace cta {

namespace catalogue {

/**
 * An empty implementation of the Catalogue used to populate unit tests of the scheduler database
 * as they need a reference to a Catalogue, used in very few situations (requeueing of retrieve
 * requests).
 */
class DummyCatalogue: public Catalogue {
public:
  DummyCatalogue(log::Logger &l): Catalogue(l) {}
  virtual ~DummyCatalogue() { }
  void createAdminHost(const common::dataStructures::SecurityIdentity& admin, const std::string& hostName, const std::string& comment) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }

  void createAdminUser(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createArchiveRoute(const common::dataStructures::SecurityIdentity& admin, const std::string& diskInstanceName, const std::string& storageClassName, const uint64_t copyNb, const std::string& tapePoolName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createMountPolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t archivePriority, const uint64_t minArchiveRequestAge, const uint64_t retrievePriority, const uint64_t minRetrieveRequestAge, const uint64_t maxDrivesAllowed, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createRequesterGroupMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstanceName, const std::string& requesterGroupName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createRequesterMountRule(const common::dataStructures::SecurityIdentity& admin, const std::string& mountPolicyName, const std::string& diskInstance, const std::string& requesterName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createStorageClass(const common::dataStructures::SecurityIdentity& admin, const common::dataStructures::StorageClass& storageClass) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createTape(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& logicalLibraryName, const std::string& tapePoolName, const uint64_t capacityInBytes, const bool disabled, const bool full, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void createTapePool(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteAdminHost(const std::string& hostName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteAdminUser(const std::string& username) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteArchiveFile(const std::string& instanceName, const uint64_t archiveFileId) override { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteArchiveRoute(const std::string& diskInstanceName, const std::string& storageClassName, const uint64_t copyNb) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteLogicalLibrary(const std::string& name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteMountPolicy(const std::string& name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteRequesterGroupMountRule(const std::string& diskInstanceName, const std::string& requesterGroupName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteRequesterMountRule(const std::string& diskInstanceName, const std::string& requesterName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteStorageClass(const std::string& diskInstanceName, const std::string& storageClassName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteTape(const std::string& vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void deleteTapePool(const std::string& name) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void filesWrittenToTape(const std::set<TapeFileWritten>& event) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::AdminHost> getAdminHosts() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::AdminUser> getAdminUsers() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  ArchiveFileItor getArchiveFiles(const TapeFileSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::MountPolicy> getMountPolicies() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::RequesterMountRule> getRequesterMountRules() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::StorageClass> getStorageClasses() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::ArchiveFileSummary getTapeFileSummary(const TapeFileSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::TapePool> getTapePools() const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  std::list<common::dataStructures::Tape> getTapes(const TapeSearchCriteria& searchCriteria) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
 // getTapesByVid is implemented below (and works).
  std::list<TapeForWriting> getTapesForWriting(const std::string& logicalLibraryName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  bool isAdmin(const common::dataStructures::SecurityIdentity& admin) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyAdminHostComment(const common::dataStructures::SecurityIdentity& admin, const std::string& hostName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin, const std::string& username, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& storageClassName, const uint64_t copyNb, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& storageClassName, const uint64_t copyNb, const std::string& tapePoolName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minArchiveRequestAge) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t archivePriority) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t maxDrivesAllowed) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t minRetrieveRequestAge) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t retrievePriority) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterGroupName, const std::string& mountPolicy) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& requesterName, const std::string& mountPolicy) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity& admin, const std::string& instanceName, const std::string& name, const uint64_t nbCopies) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const uint64_t capacityInBytes) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeComment(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& encryptionKey) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& logicalLibraryName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const std::string& comment) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const uint64_t nbPartialTapes) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const std::string& tapePoolName) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void noSpaceLeftOnTape(const std::string& vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void ping() { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::ArchiveFileQueueCriteria prepareForNewFile(const std::string& diskInstanceName, const std::string& storageClassName, const common::dataStructures::UserIdentity& user) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(const std::string& instanceName, const uint64_t archiveFileId, const common::dataStructures::UserIdentity& user, log::LogContext &lc) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void reclaimTape(const common::dataStructures::SecurityIdentity& admin, const std::string& vid) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapeDisabled(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool disabledValue) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapeFull(const common::dataStructures::SecurityIdentity& admin, const std::string& vid, const bool fullValue) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity& admin, const std::string& name, const bool encryptionValue) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  bool tapeExists(const std::string& vid) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void tapeLabelled(const std::string& vid, const std::string& drive, const bool lbpIsOn) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void tapeMountedForArchive(const std::string& vid, const std::string& drive) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  void tapeMountedForRetrieve(const std::string& vid, const std::string& drive) { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  bool tapePoolExists(const std::string& tapePoolName) const { throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented"); }
  
  // Special functions for unit tests.
  void addEnabledTape(const std::string & vid) {
    threading::MutexLocker lm(m_tapeEnablingMutex);
    m_tapeEnabling[vid]=true;
  }
  void addDisabledTape(const std::string & vid) {
    threading::MutexLocker lm(m_tapeEnablingMutex);
    m_tapeEnabling[vid]=false;
  }
  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string>& vids) const {
    // Minimal implementation of VidToMap for retrieve request unit tests. We just support
    // disabled status for the tapes.
    // If the tape is not listed, it is listed as enabled in the return value.
    threading::MutexLocker lm(m_tapeEnablingMutex);
    common::dataStructures::VidToTapeMap ret;
    for (const auto & v: vids) {
      try {
        ret[v].disabled = !m_tapeEnabling.at(v);
      } catch (std::out_of_range &) {
        ret[v].disabled = false;
      }
    }
    return ret;
  }
private:
  mutable threading::Mutex m_tapeEnablingMutex;
  std::map<std::string, bool> m_tapeEnabling;
};

}} // namespace cta::catalogue.
