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

void fillRetrieveRequests(
  typename cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue,cta::objectstore::RetrieveQueueToTransferForUser>::InsertedElement::list &requests,
  std::list<std::unique_ptr<cta::objectstore::RetrieveRequest> >& requestPtrs, //List to avoid memory leak on ArchiveQueueAlgorithms test
  cta::objectstore::BackendVFS &be,
  cta::objectstore::AgentReference &agentRef)
{
  using namespace cta::objectstore;

  for(size_t i = 0; i < 10; ++i)
  {
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
    {
      cta::common::dataStructures::TapeFile tf;
      tf.blockId = 0;
      tf.fileSize = 1;
      tf.copyNb = 1;
      tf.creationTime = time(nullptr);
      tf.fSeq = i;
      tf.vid = "Tape0";
      rqc.archiveFile.tapeFiles.push_back(tf);
    }
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.maxDrivesAllowed = 1;
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    requestPtrs.emplace_back(new cta::objectstore::RetrieveRequest(rrAddr, be));
    requests.emplace_back(ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransferForUser>::InsertedElement{
      requestPtrs.back().get(), 1, i, 667, mp, serializers::RetrieveJobStatus::RJS_ToTransferForUser, cta::nullopt
    });
    auto &rr = *requests.back().retrieveRequest;
    rr.initialize();
    rr.setRetrieveFileQueueCriteria(rqc);
    cta::common::dataStructures::RetrieveRequest sReq;
    sReq.archiveFileID = rqc.archiveFile.archiveFileID;
    sReq.creationLog.time = time(nullptr);
    rr.setSchedulerRequest(sReq);
    rr.addJob(1, 1, 1, 1);
    rr.setOwner(agentRef.getAgentAddress());
    rr.setActiveCopyNumber(1);
    rr.insert();
  }
}

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
  ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransferForUser>::InsertedElement::list requests;
  std::list<std::unique_ptr<cta::objectstore::ArchiveRequest>> archiveRequests;
  for (size_t i=0; i<10; i++) {
    std::string arAddr = agentRef.nextId("ArchiveRequest");
    agentRef.addToOwnership(arAddr, be);
    cta::common::dataStructures::MountPolicy mp;
    // This will be a copy number 1.
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = 123456789L;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumBlob.insert(cta::checksum::NONE, "");
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = 667;
    aFile.storageClass = "sc";
    archiveRequests.emplace_back(new cta::objectstore::ArchiveRequest(arAddr, be));
    requests.emplace_back(ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransferForUser>::InsertedElement{archiveRequests.back().get(), 1, aFile, mp,
        cta::nullopt});
    auto & ar=*requests.back().archiveRequest;
    auto copyNb = requests.back().copyNb;
    ar.initialize();
    ar.setArchiveFile(aFile);
    ar.addJob(copyNb, "TapePool0", agentRef.getAgentAddress(), 1, 1, 1);
    ar.setMountPolicy(mp);
    ar.setArchiveReportURL("");
    ar.setArchiveErrorReportURL("");
    ar.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
    ar.setSrcURL("root://eoseos/myFile");
    ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    ar.insert();
  }
  ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransferForUser> archiveAlgos(be, agentRef);
  archiveAlgos.referenceAndSwitchOwnership("Tapepool", requests, lc);
  // Now get the requests back
  ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PopCriteria popCriteria;
  popCriteria.bytes = std::numeric_limits<decltype(popCriteria.bytes)>::max();
  popCriteria.files = 100;
  auto poppedJobs = archiveAlgos.popNextBatch("Tapepool", popCriteria, lc);
  ASSERT_EQ(poppedJobs.summary.files, 10);
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

  // Here we check for the ability to detect dead (but empty agents) and clean them up
  BackendVFS be;
  AgentReference agentRef("unitTestGarbageCollector", dl);
  Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  EntryLogSerDeser el("user0", "unittesthost", time(NULL));
  ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  agent.initialize();
  agent.insertAndRegisterSelf(lc);
  std::list<std::unique_ptr<RetrieveRequest> > requestsPtrs;
  ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransferForUser>::InsertedElement::list requests;
  fillRetrieveRequests(requests, requestsPtrs, be, agentRef); //memory leak here

  {
    // Second agent to test referenceAndSwitchOwnershipIfNecessary
    BackendVFS be2;
    AgentReference agentRef2("Agent 2", dl);
    Agent agent2(agentRef2.getAgentAddress(), be2);
    // Create the root entry
    RootEntry re2(be2);
    re2.initialize();
    re2.insert();
    // Create the agent register
    EntryLogSerDeser el2("user0", "unittesthost", time(NULL));
    ScopedExclusiveLock rel2(re2);
    re2.addOrGetAgentRegisterPointerAndCommit(agentRef2, el2, lc);
    rel2.release();
    agent2.initialize();
    agent2.insertAndRegisterSelf(lc);
    ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransferForUser>::InsertedElement::list requests2;
    std::list<std::unique_ptr<RetrieveRequest> > requestsPtrs2;
    fillRetrieveRequests(requests2, requestsPtrs2,be2, agentRef2);

    auto a1 = agentRef2.getAgentAddress();
    auto a2 = agentRef2.getAgentAddress();
    ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransferForUser> retrieveAlgos2(be2, agentRef2);
    retrieveAlgos2.referenceAndSwitchOwnershipIfNecessary("VID",
      a2, a1, requests2, lc);
  }

  ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransferForUser> retrieveAlgos(be, agentRef);
  try {
    ASSERT_EQ(requests.size(), 10);

    retrieveAlgos.referenceAndSwitchOwnership("VID", 
      agentRef.getAgentAddress(), requests, lc);

    // Now get the requests back
    ContainerTraits<RetrieveQueue,RetrieveQueueToTransferForUser>::PopCriteria popCriteria;
    popCriteria.bytes = std::numeric_limits<decltype(popCriteria.bytes)>::max();
    popCriteria.files = 100;
    auto poppedJobs = retrieveAlgos.popNextBatch("VID", popCriteria, lc);
    ASSERT_EQ(poppedJobs.summary.files, 10);

    // Validate that the summary has the same information as the popped elements
    ContainerTraits<RetrieveQueue,RetrieveQueueToTransferForUser>::PoppedElementsSummary s;
    for(auto &e: poppedJobs.elements) {
      s += ContainerTraits<RetrieveQueue,RetrieveQueueToTransferForUser>::getElementSummary(e);
    }
    ASSERT_EQ(s, poppedJobs.summary);
  } catch (ContainerTraits<RetrieveQueue,RetrieveQueueToTransferForUser>::OwnershipSwitchFailure & ex) {
    for (auto & e: ex.failedElements) {
      try {
        throw e.failure;
      } catch(std::exception &e) {
        std::cout << e.what() << std::endl;
      }
    }
  }
}

}
