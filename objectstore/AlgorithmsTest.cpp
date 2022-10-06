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

#include <gtest/gtest.h>

#include "Agent.hpp"
#include "AgentReference.hpp"
#include "ArchiveQueueAlgorithms.hpp"
#include "BackendVFS.hpp"
#include "catalogue/DummyCatalogue.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/log/DummyLogger.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif
#include "objectstore/RetrieveQueueAlgorithms.hpp"
#include "RetrieveQueueAlgorithms.hpp"
#include "RootEntry.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"

namespace unitTests {

/**
 * Create Objectstore RetrieveRequest and insert them in a list that could be used to queue with the Algorithms
 * @param requests the list of RetrieveRequests that will be queued in the objectstore
 * @param requestPtrs the pointers of the RetrieveRequests that will be queued in the objectstore
 * @param be objectstore backend
 * @param agentRef the current agent that queues
 * @param startFseq allows to set the FSeq of the first file to be queued (in case this method is called multiple times)
 */
void fillRetrieveRequests(
  typename cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue,cta::objectstore::RetrieveQueueToTransfer>::InsertedElement::list &requests,
  std::list<std::unique_ptr<cta::objectstore::RetrieveRequest> >& requestPtrs, //List to avoid memory leak on ArchiveQueueAlgorithms test
  cta::objectstore::BackendVFS &be,
  cta::objectstore::AgentReference &agentRef, uint64_t startFseq = 0)
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
      tf.fSeq = startFseq;
      tf.vid = "Tape0";
      rqc.archiveFile.tapeFiles.push_back(tf);
    }
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    requestPtrs.emplace_back(new cta::objectstore::RetrieveRequest(rrAddr, be));
    requests.emplace_back(ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer>::InsertedElement{
      requestPtrs.back().get(), 1, startFseq++, 667, mp, std::nullopt, std::nullopt
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

/**
 * Create Objectstore ArchiveRequest and insert them in a list that could be used to queue with the Algorithms
 * @param requests the list of ArchiveRequests that will be queued in the objectstore
 * @param requestPtrs the pointers of the ArchiveRequests that will be queued in the objectstore
 * @param be objectstore backend
 * @param agentRef the current agent that queues
 */
void fillArchiveRequests(typename cta::objectstore::ContainerAlgorithms<cta::objectstore::ArchiveQueue,cta::objectstore::ArchiveQueueToTransferForUser>::InsertedElement::list &requests,
  std::list<std::unique_ptr<cta::objectstore::ArchiveRequest> >& requestPtrs, //List to avoid memory leak on ArchiveQueueAlgorithms test
  cta::objectstore::BackendVFS &be,
  cta::objectstore::AgentReference &agentRef){
  using namespace cta::objectstore;
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
    requestPtrs.emplace_back(new cta::objectstore::ArchiveRequest(arAddr, be));
    requests.emplace_back(ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransferForUser>::InsertedElement{requestPtrs.back().get(), 1, aFile, mp,
        std::nullopt});
    auto & ar=*requests.back().archiveRequest;
    auto copyNb = requests.back().copyNb;
    ar.initialize();
    ar.setArchiveFile(aFile);
    ar.addJob(copyNb, "tapepool", agentRef.getAgentAddress(), 1, 1, 1);
    ar.setMountPolicy(mp);
    ar.setArchiveReportURL("");
    ar.setArchiveErrorReportURL("");
    ar.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
    ar.setSrcURL("root://eoseos/myFile");
    ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    ar.insert();
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
      "unittesthost", time(nullptr));
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
        std::nullopt});
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

  for(auto & ar: archiveRequests){
    cta::objectstore::ScopedExclusiveLock sel(*ar);
    ar->fetch();
    ASSERT_TRUE(ar->getJobOwner(1).find("ArchiveQueueToTransferForUser-Tapepool-unitTestGarbageCollector") != std::string::npos);
  }
  // Now get the requests back
  ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PopCriteria popCriteria;
  popCriteria.bytes = std::numeric_limits<decltype(popCriteria.bytes)>::max();
  popCriteria.files = 100;
  auto poppedJobs = archiveAlgos.popNextBatch("Tapepool", popCriteria, lc);
  ASSERT_EQ(poppedJobs.summary.files, 10);
  for(auto & ar: archiveRequests){
    cta::objectstore::ScopedExclusiveLock sel(*ar);
    ar->fetch();
    ASSERT_EQ(agentRef.getAgentAddress(),ar->getJobOwner(1));
  }
}

TEST(ObjectStore, ArchiveQueueAlgorithmsWithDeletedJobsInQueue) {
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
      "unittesthost", time(nullptr));
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
    aFile.creationTime = i; //so we can check the requests popped were correct
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = 667;
    aFile.storageClass = "sc";
    archiveRequests.emplace_back(new cta::objectstore::ArchiveRequest(arAddr, be));
    requests.emplace_back(ContainerAlgorithms<ArchiveQueue,ArchiveQueueToTransferForUser>::InsertedElement{archiveRequests.back().get(), 1, aFile, mp,
        std::nullopt});
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

  {
    //Delete one in tree requests from the queue
    int i = 0;
    for(auto & ar: archiveRequests){
      cta::objectstore::ScopedExclusiveLock sel(*ar);
      ar->fetch();
      if ((i % 3) == 1) {
        ar->remove();
      }
      i++;
    }
  }

  {
    // Get first batch of four requests
    ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PopCriteria popCriteria;
    popCriteria.bytes = 667 * 4;
    popCriteria.files = 100; // never reached
    auto poppedJobs = archiveAlgos.popNextBatch("Tapepool", popCriteria, lc);
    ASSERT_EQ(poppedJobs.elements.size(), 4);
    ASSERT_EQ(poppedJobs.summary.files, 4);
    ASSERT_EQ(poppedJobs.summary.bytes, 667 * 4);
    int expected_times[] = {0, 2, 3, 5};
    int i = 0;
    for (auto &job: poppedJobs.elements) {
      ASSERT_EQ(job.archiveFile.creationTime, expected_times[i]);
      ASSERT_EQ(job.bytes, 667);
      i++;
    }
  }

  {
    // Get a second batch of remaining tree requests
    ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PopCriteria popCriteria;
    popCriteria.bytes = 667 * 3;
    popCriteria.files = 100; // never reached
    auto poppedJobs = archiveAlgos.popNextBatch("Tapepool", popCriteria, lc);
    ASSERT_EQ(poppedJobs.elements.size(), 3);
    ASSERT_EQ(poppedJobs.summary.files, 3);
    ASSERT_EQ(poppedJobs.summary.bytes, 667 * 3);
    int expected_times[] = {6, 8, 9};
    int i = 0;
    for (auto &job: poppedJobs.elements) {
      ASSERT_EQ(job.archiveFile.creationTime, expected_times[i]);
      ASSERT_EQ(job.bytes, 667);
      i++;
    }
  }
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
  //Agent1 for queueing
  AgentReference agentRef("unitTestGarbageCollector", dl);
  Agent agent(agentRef.getAgentAddress(), be);

  //Agent2 for popping
  AgentReference agentRef2("Agent2", dl);
  Agent agent2(agentRef2.getAgentAddress(), be);
  // Create the root entry
  RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef2, el, lc);
  rel.release();
  agent.initialize();
  agent.insertAndRegisterSelf(lc);
  agent2.initialize();
  agent2.insertAndRegisterSelf(lc);
  std::list<std::unique_ptr<RetrieveRequest> > requestsPtrs;
  ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer>::InsertedElement::list requests;
  fillRetrieveRequests(requests, requestsPtrs, be, agentRef); //memory leak here
  auto a1 = agentRef.getAgentAddress();
  auto a2 = agentRef2.getAgentAddress();

  {
    ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer>::InsertedElement::list requests2;

    ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> queueRetrieveAlgo(be, agentRef2);
    queueRetrieveAlgo.referenceAndSwitchOwnership("VID",
      a1, requests, lc);
    //Test that the owner of these requests is the queue with VID and Agent2
    for(auto &request: requestsPtrs){
      cta::objectstore::RetrieveRequest rr(request->getAddressIfSet(),be);
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      ASSERT_TRUE(rr.getOwner().find("RetrieveQueueToTransferForUser-VID-Agent2") != std::string::npos);
    }
  }

  ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> popRetrieveAlgos(be, agentRef);
  try {
    ASSERT_EQ(requests.size(), 10);

    // Now get the requests back
    ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::PopCriteria popCriteria;
    popCriteria.bytes = std::numeric_limits<decltype(popCriteria.bytes)>::max();
    popCriteria.files = 100;
    auto poppedJobs = popRetrieveAlgos.popNextBatch("VID", popCriteria, lc);
    ASSERT_EQ(poppedJobs.summary.files, 10);

    // Validate that the summary has the same information as the popped elements
    ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::PoppedElementsSummary s;
    for(auto &e: poppedJobs.elements) {
      s += ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::getElementSummary(e);
    }
    //Check that the popped jobs owner is now the agent1 and not the queue
    for(auto & elt: poppedJobs.elements){
      cta::objectstore::RetrieveRequest rr(elt.retrieveRequest->getAddressIfSet(),be);
      cta::objectstore::ScopedExclusiveLock sel(rr);
      rr.fetch();
      ASSERT_EQ(a1, rr.getOwner());
    }
    ASSERT_EQ(s, poppedJobs.summary);
  } catch (ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::OwnershipSwitchFailure & ex) {
    for (auto & e: ex.failedElements) {
      try {
        throw e.failure;
      } catch(std::exception &e) {
        std::cout << e.what() << std::endl;
      }
    }
  }
}

TEST(ObjectStore, RetrieveQueueAlgorithmsUpdatesOldestJobQueueTime) {
  using cta::common::dataStructures::JobQueueType;
  using cta::objectstore::RetrieveQueue;
  using cta::objectstore::RetrieveQueueToTransfer;
  using cta::objectstore::RetrieveRequest;
  using cta::objectstore::ContainerAlgorithms;
  // We will need a log object
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::catalogue::DummyCatalogue catalogue;
  cta::log::LogContext lc(dl);

  // Here we check for the ability to detect dead (but empty agents) and clean them up
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestGarbageCollector", dl);
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  agent.initialize();
  agent.insertAndRegisterSelf(lc);

  std::string vid = "Tape0";

  ContainerAlgorithms<RetrieveQueue, RetrieveQueueToTransfer> retrieveAlgos(be, agentRef);
  std::string retrieveQueueAddress;
  std::unique_ptr<cta::objectstore::RetrieveQueue> rq;
  time_t firstBatchOldestJobStartTime;
  {
    std::list<std::unique_ptr<RetrieveRequest> > requestsPtrs;
    ContainerAlgorithms<RetrieveQueue, RetrieveQueueToTransfer>::InsertedElement::list requests;
    fillRetrieveRequests(requests, requestsPtrs, be, agentRef, 0);

    // Insert a first batch of 10 requests
    ASSERT_EQ(requests.size(), 10);

    retrieveAlgos.referenceAndSwitchOwnership(vid,
      agentRef.getAgentAddress(), requests, lc);

    re.fetchNoLock();

    retrieveQueueAddress = re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser);
    rq.reset(new cta::objectstore::RetrieveQueue(retrieveQueueAddress, be));
    rq->fetchNoLock();
    // Get the first batch oldestAge
    firstBatchOldestJobStartTime = rq->getJobsSummary().oldestJobStartTime;
  }
  // Create another batch of 10 requests
  {
    std::list<std::unique_ptr<RetrieveRequest> > requestsPtrs;
    ContainerAlgorithms<RetrieveQueue, RetrieveQueueToTransfer>::InsertedElement::list requests;
    fillRetrieveRequests(requests, requestsPtrs, be, agentRef, 10);
    ASSERT_EQ(requests.size(), 10);
    // Sleep 1 second before requeueing
    ::sleep(1);
    // Requeue
    retrieveAlgos.referenceAndSwitchOwnership(vid,
      agentRef.getAgentAddress(), requests, lc);
    rq->fetchNoLock();
    uint64_t secondBatchOldestJobStartTime;
    secondBatchOldestJobStartTime = rq->getJobsSummary().oldestJobStartTime;
    // As we did not pop, the first inserted batch of jobs is the oldest one
    ASSERT_EQ(firstBatchOldestJobStartTime, secondBatchOldestJobStartTime);
  }

  // Now pop the first 10 batches of jobs --> the queue oldestjobstarttime should be equal
  // to the second batch oldestjobstarttime
  cta::objectstore::ContainerTraits<RetrieveQueue, RetrieveQueueToTransfer>::PopCriteria popCriteria;
  popCriteria.bytes = std::numeric_limits<decltype(popCriteria.bytes)>::max();
  popCriteria.files = 10;
  auto poppedJobs = retrieveAlgos.popNextBatch(vid, popCriteria, lc);
  ASSERT_EQ(poppedJobs.summary.files, 10);

  // The new oldestJobStartTime should be equal to the jobstarttime of the first job of the second batch
  rq->fetchNoLock();
  time_t oldestJobStartTime = rq->getJobsSummary().oldestJobStartTime;

  ASSERT_TRUE(oldestJobStartTime > firstBatchOldestJobStartTime);
}

TEST(ObjectStore, ArchiveQueueAlgorithmsUpdatesOldestJobQueueTime) {
  using cta::common::dataStructures::JobQueueType;
  using cta::objectstore::ArchiveQueue;
  using cta::objectstore::ArchiveQueueToTransferForUser;
  using cta::objectstore::ArchiveRequest;
  using cta::objectstore::ContainerAlgorithms;
  // We will need a log object
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger dl("dummy", "unitTest");
#else
  cta::log::DummyLogger dl("dummy", "unitTest");
#endif
  cta::catalogue::DummyCatalogue catalogue;
  cta::log::LogContext lc(dl);

  // Here we check for the ability to detect dead (but empty agents) and clean them up
  cta::objectstore::BackendVFS be;
  cta::objectstore::AgentReference agentRef("unitTestArchiveQueueAlgorithms", dl);
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::EntryLogSerDeser el("user0", "unittesthost", time(nullptr));
  cta::objectstore::ScopedExclusiveLock rel(re);
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  agent.initialize();
  agent.insertAndRegisterSelf(lc);

  std::string tapepool = "tapepool";

  ContainerAlgorithms<ArchiveQueue, ArchiveQueueToTransferForUser> archiveAlgos(be, agentRef);
  std::string archiveQueueAddress;
  std::unique_ptr<cta::objectstore::ArchiveQueue> aq;
  time_t firstBatchOldestJobStartTime;
  {
    std::list<std::unique_ptr<ArchiveRequest> > requestsPtrs;
    ContainerAlgorithms<ArchiveQueue, ArchiveQueueToTransferForUser>::InsertedElement::list requests;
    fillArchiveRequests(requests, requestsPtrs, be, agentRef);

    // Insert a first batch of 10 requests
    ASSERT_EQ(requests.size(), 10);

    archiveAlgos.referenceAndSwitchOwnership(tapepool,
      agentRef.getAgentAddress(), requests, lc);

    re.fetchNoLock();

    archiveQueueAddress = re.getArchiveQueueAddress(tapepool, JobQueueType::JobsToTransferForUser);
    aq.reset(new cta::objectstore::ArchiveQueue(archiveQueueAddress, be));
    aq->fetchNoLock();
    // Get the first batch oldestAge
    firstBatchOldestJobStartTime = aq->getJobsSummary().oldestJobStartTime;
  }
  // Create another batch of 10 requests
  {
    std::list<std::unique_ptr<ArchiveRequest> > requestsPtrs;
    ContainerAlgorithms<ArchiveQueue, ArchiveQueueToTransferForUser>::InsertedElement::list requests;
    fillArchiveRequests(requests, requestsPtrs, be, agentRef);
    ASSERT_EQ(requests.size(), 10);
    // Sleep 1 second before requeueing
    ::sleep(1);
    // Requeue
    archiveAlgos.referenceAndSwitchOwnership(tapepool,
      agentRef.getAgentAddress(), requests, lc);
    aq->fetchNoLock();
    uint64_t secondBatchOldestJobStartTime;
    secondBatchOldestJobStartTime = aq->getJobsSummary().oldestJobStartTime;
    // As we did not pop, the first inserted batch of jobs is the oldest one
    ASSERT_EQ(firstBatchOldestJobStartTime, secondBatchOldestJobStartTime);
  }

  // Now pop the first 10 batches of jobs --> the queue oldestjobstarttime should be equal to the
  // second batch oldestjobstarttime
  cta::objectstore::ContainerTraits<ArchiveQueue, ArchiveQueueToTransferForUser>::PopCriteria popCriteria;
  popCriteria.files = 10;
  popCriteria.bytes = std::numeric_limits<decltype(popCriteria.bytes)>::max();
  auto poppedJobs = archiveAlgos.popNextBatch(tapepool, popCriteria, lc);
  ASSERT_EQ(poppedJobs.summary.files, 10);

  // The new oldestJobStartTime should be equal to the jobstarttime of the first job of the second batch
  aq->fetchNoLock();
  time_t oldestJobStartTime = aq->getJobsSummary().oldestJobStartTime;

  ASSERT_TRUE(oldestJobStartTime > firstBatchOldestJobStartTime);
}

}
