
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

#include "RootEntry.hpp"
#include "AgentReference.hpp"
#include "Agent.hpp"
#include "common/log/DummyLogger.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"
#include "catalogue/DummyCatalogue.hpp"
#include "BackendVFS.hpp"
#include "common/make_unique.hpp"
#include "ArchiveQueueAlgorithms.hpp"
#include "RetrieveQueueAlgorithms.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif

#include <gtest/gtest.h>

namespace unitTests {

TEST(ObjectStore, ArchiveQueueAlgorithms) {
  using namespace cta::objectstore;
  // We will need a log object 
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::catalogue::DummyCatalogue catalogue;
  cta::log::LogContext lc(dl);
  // Here we check for the ability to detect dead (but empty agents)
  // and clean them up.
  BackendVFS be;
  AgentReference agentRef("unitTestGarbageCollector", dl);
  Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  agent.initialize();
  agent.insertAndRegisterSelf(lc);
  ContainerAlgorithms<ArchiveQueue>::InsertedElement::list requests;
  for (size_t i=0; i<10; i++) {
    std::string arAddr = agentRef.nextId("ArchiveRequest");
    agentRef.addToOwnership(arAddr, be);
    cta::common::dataStructures::MountPolicy mp;
    // This will be a copy number 1.
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = 123456789L;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumType = "";
    aFile.checksumValue = "";
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = 667;
    aFile.storageClass = "sc";
    requests.emplace_back(ContainerAlgorithms<ArchiveQueue>::InsertedElement{cta::make_unique<ArchiveRequest>(arAddr, be), 1, aFile, mp});
    auto & ar=*requests.back().archiveRequest;
    auto copyNb = requests.back().copyNb;
    ar.initialize();
    ar.setArchiveFile(aFile);
    ar.addJob(copyNb, "TapePool0", agentRef.getAgentAddress(), 1, 1);
    ar.setMountPolicy(mp);
    ar.setArchiveReportURL("");
    ar.setArchiveErrorReportURL("");
    ar.setRequester(cta::common::dataStructures::UserIdentity("user0", "group0"));
    ar.setSrcURL("root://eoseos/myFile");
    ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    ar.insert();
  }
  ContainerAlgorithms<ArchiveQueue> archiveAlgos(be, agentRef);
  archiveAlgos.referenceAndSwitchOwnership("Tapepool", QueueType::JobsToTransfer, requests, lc);
  // Now get the requests back
  ContainerTraits<ArchiveQueue>::PopCriteria popCriteria;
  popCriteria.bytes = std::numeric_limits<decltype(popCriteria.bytes)>::max();
  popCriteria.files = 100;
  auto popedJobs = archiveAlgos.popNextBatch("Tapepool", QueueType::JobsToTransfer, popCriteria, lc);
  ASSERT_EQ(popedJobs.summary.files, 10);
}

TEST(ObjectStore, RetrieveQueueAlgorithms) {
  using namespace cta::objectstore;
  // We will need a log object 
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::catalogue::DummyCatalogue catalogue;
  cta::log::LogContext lc(dl);
  // Here we check for the ability to detect dead (but empty agents)
  // and clean them up.
  BackendVFS be;
  AgentReference agentRef("unitTestGarbageCollector", dl);
  Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  EntryLogSerDeser el("user0",
      "unittesthost", time(NULL));
  ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  agent.initialize();
  agent.insertAndRegisterSelf(lc);
  ContainerAlgorithms<RetrieveQueue>::InsertedElement::list requests;
  for (size_t i=0; i<10; i++) {
    std::string rrAddr = agentRef.nextId("RetrieveRequest");
    agentRef.addToOwnership(rrAddr, be);
    cta::common::dataStructures::MountPolicy mp;
    cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
    rqc.archiveFile.archiveFileID = 123456789L;
    rqc.archiveFile.diskFileId = "eos://diskFile";
    rqc.archiveFile.checksumType = "";
    rqc.archiveFile.checksumValue = "";
    rqc.archiveFile.creationTime = 0;
    rqc.archiveFile.reconciliationTime = 0;
    rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    rqc.archiveFile.diskInstance = "eoseos";
    rqc.archiveFile.fileSize = 1000 + i;
    rqc.archiveFile.storageClass = "sc";
    rqc.archiveFile.tapeFiles[1].blockId=0;
    rqc.archiveFile.tapeFiles[1].compressedSize=1;
    rqc.archiveFile.tapeFiles[1].compressedSize=1;
    rqc.archiveFile.tapeFiles[1].copyNb=1;
    rqc.archiveFile.tapeFiles[1].creationTime=time(nullptr);
    rqc.archiveFile.tapeFiles[1].fSeq=i;
    rqc.archiveFile.tapeFiles[1].vid="Tape0";
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.maxDrivesAllowed = 1;
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    requests.emplace_back(ContainerAlgorithms<RetrieveQueue>::InsertedElement{cta::make_unique<RetrieveRequest>(rrAddr, be), 1, i, 667, mp,
        serializers::RetrieveJobStatus::RJS_ToTransfer});
    auto & rr=*requests.back().retrieveRequest;
    rr.initialize();
    rr.setRetrieveFileQueueCriteria(rqc);
    cta::common::dataStructures::RetrieveRequest sReq;
    sReq.archiveFileID = rqc.archiveFile.archiveFileID;
    sReq.creationLog.time=time(nullptr);
    rr.setSchedulerRequest(sReq);
    rr.addJob(1, 1, 1);
    rr.setOwner(agentRef.getAgentAddress());
    rr.setActiveCopyNumber(1);
    rr.insert();
  }
  ContainerAlgorithms<RetrieveQueue> retrieveAlgos(be, agentRef);
  try {
    retrieveAlgos.referenceAndSwitchOwnership("VID", QueueType::JobsToTransfer, agentRef.getAgentAddress(), requests, lc);
  } catch (ContainerTraits<RetrieveQueue>::OwnershipSwitchFailure & ex) {
    for (auto & e: ex.failedElements) {
      try {
        throw e.failure;
      } catch (std::exception & e) {
        std::cout << e.what() << std::endl;
      }
    }
  }
}

}