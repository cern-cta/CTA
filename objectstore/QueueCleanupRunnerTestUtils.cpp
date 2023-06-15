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

#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif

#include <exception>
#include <gtest/gtest.h>
#include <uuid/uuid.h>

#include "objectstore/ObjectStoreFixture.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/GarbageCollector.hpp"
#include "scheduler/Scheduler.hpp"

namespace unitTests {

/**
 * Create Objectstore RetrieveRequest and insert them in a list that could be used to queue with the Algorithms
 * @param requests the list of RetrieveRequests that will be queued in the objectstore
 * @param requestPtrs the pointers of the RetrieveRequests that will be queued in the objectstore
 * @param be objectstore backend
 * @param agentRef the current agent that queues
 * @param startFseq allows to set the FSeq of the first file to be queued (in case this method is called multiple times)
 */
void fillRetrieveRequestsForCleanupRunner(
  typename cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue,
                                                 cta::objectstore::RetrieveQueueToTransfer>::InsertedElement::list&
    requests,
  uint32_t requestNr,
  std::list<std::unique_ptr<cta::objectstore::RetrieveRequest>>&
    requestPtrs,                     //List to avoid memory leak on ArchiveQueueAlgorithms test
  std::set<std::string>& tapeNames,  // List of tapes that will contain a replica
  std::string& activeCopyTape,
  cta::objectstore::BackendVFS& be,
  cta::objectstore::AgentReference& agentRef,
  uint64_t startFseq) {
  using namespace cta::objectstore;
  for (size_t i = 0; i < requestNr; i++) {
    std::string rrAddr = agentRef.nextId("RetrieveRequest");
    agentRef.addToOwnership(rrAddr, be);
    cta::common::dataStructures::MountPolicy mp;
    cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
    rqc.archiveFile.archiveFileID = 123456789L;
    rqc.archiveFile.diskFileId = "eos://diskFile";
    rqc.archiveFile.checksumBlob.insert(cta::checksum::NONE, "");
    rqc.archiveFile.creationTime = 0;
    rqc.archiveFile.reconciliationTime = 0;
    rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    rqc.archiveFile.diskInstance = "eoseos";
    rqc.archiveFile.fileSize = 1000 + i;
    rqc.archiveFile.storageClass = "sc";
    uint32_t currentCopyNb = 0;
    uint32_t activeCopyNr = 0;
    for (auto& tapeName : tapeNames) {
      cta::common::dataStructures::TapeFile tf;
      tf.blockId = 0;
      tf.fileSize = 1;
      tf.copyNb = currentCopyNb;
      tf.creationTime = time(nullptr);
      tf.fSeq = startFseq;
      tf.vid = tapeName;
      rqc.archiveFile.tapeFiles.push_back(tf);

      if (activeCopyTape == tapeName) {
        activeCopyNr = currentCopyNb;
      }
      currentCopyNb++;
    }
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    requestPtrs.emplace_back(new cta::objectstore::RetrieveRequest(rrAddr, be));
    requests.emplace_back(ContainerAlgorithms<RetrieveQueue, RetrieveQueueToTransfer>::InsertedElement {
      requestPtrs.back().get(), activeCopyNr, startFseq++, 667, mp, std::nullopt, std::nullopt});
    auto& rr = *requests.back().retrieveRequest;
    rr.initialize();
    rr.setRetrieveFileQueueCriteria(rqc);
    cta::common::dataStructures::RetrieveRequest sReq;
    sReq.archiveFileID = rqc.archiveFile.archiveFileID;
    sReq.creationLog.time = time(nullptr);
    rr.setSchedulerRequest(sReq);
    rr.addJob(activeCopyNr, 1, 1, 1);
    rr.setOwner(agentRef.getAgentAddress());
    rr.setActiveCopyNumber(activeCopyNr);
    rr.insert();
  }
}

}  // namespace unitTests