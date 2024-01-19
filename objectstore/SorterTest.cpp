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

#include <memory>

#include "BackendVFS.hpp"
#include "common/exception/Exception.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/log/DummyLogger.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"
#include "common/log/StdoutLogger.hpp"
#include "GarbageCollector.hpp"
#include "RootEntry.hpp"
#include "Agent.hpp"
#include "AgentReference.hpp"
#include "AgentRegister.hpp"
#include "DriveRegister.hpp"
#include "ArchiveRequest.hpp"
#include "RetrieveRequest.hpp"
#include "ArchiveQueue.hpp"
#include "RetrieveQueue.hpp"
#include "EntryLogSerDeser.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "Sorter.hpp"

#include "ObjectStoreFixture.hpp"

namespace unitTests {

TEST_F(ObjectStore,SorterInsertArchiveRequest){
  //cta::log::StdoutLogger dl("dummy", "unitTest", "configFilename");
  cta::log::DummyLogger dl("dummy", "unitTest", "configFilename");
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  cta::objectstore::BackendVFS be;
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
    cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(nullptr));
  cta::objectstore::ScopedExclusiveLock rel(re);
  // Create the agent for objects creation
  cta::objectstore::AgentReference agentRef("unitTestCreateEnv", dl);
  // Finish root creation.
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // continue agent creation.
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);

  //Create the agent of the Sorter
  cta::objectstore::AgentReference agentRefSorter("agentRefSorter", dl);
  cta::objectstore::Agent agentSorter(agentRefSorter.getAgentAddress(), be);
  agentSorter.initialize();
  agentSorter.setTimeout_us(0);
  agentSorter.insertAndRegisterSelf(lc);

  std::string archiveRequestID = agentRef.nextId("ArchiveRequest");
  agentRef.addToOwnership(archiveRequestID,be);
  cta::objectstore::ArchiveRequest ar(archiveRequestID,be);
  ar.initialize();
  cta::common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = 123456789L;
  aFile.diskFileId = "eos://diskFile";
  aFile.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  aFile.creationTime = 0;
  aFile.reconciliationTime = 0;
  aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  aFile.diskInstance = "eoseos";
  aFile.fileSize = 667;
  aFile.storageClass = "sc";
  ar.setArchiveFile(aFile);
  ar.addJob(1, "TapePool0", agentRef.getAgentAddress(), 1, 1, 1);
  ar.addJob(2, "TapePool1", agentRef.getAgentAddress(), 1, 1, 1);
  ar.addJob(3,"TapePool0",agentRef.getAgentAddress(),1,1,1);
  ar.setJobStatus(1,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForTransfer);
  ar.setJobStatus(3,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportToUserForTransfer);
  cta::common::dataStructures::MountPolicy mp;
  ar.setMountPolicy(mp);
  ar.setArchiveReportURL("");
  ar.setArchiveErrorReportURL("");
  ar.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
  ar.setSrcURL("root://eoseos/myFile");
  ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
  ar.insert();
  cta::objectstore::ScopedExclusiveLock atfrl(ar);
  ar.fetch();

  cta::objectstore::Sorter sorter(agentRefSorter,be,catalogue);
  std::shared_ptr<cta::objectstore::ArchiveRequest> arPtr = std::make_shared<cta::objectstore::ArchiveRequest>(ar);
  sorter.insertArchiveRequest(arPtr,agentRef,lc);
  atfrl.release();
  //Get the future
  cta::objectstore::Sorter::MapArchive allArchiveJobs = sorter.getAllArchive();
  std::list<std::tuple<cta::objectstore::SorterArchiveJob,std::future<void>>> allFutures;

  for(auto& kv: allArchiveJobs){
    for(auto& job: kv.second){
      allFutures.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
    }
  }
  sorter.flushAll(lc);
  for(auto& future: allFutures){
    ASSERT_NO_THROW(std::get<1>(future).get());
  }

  cta::objectstore::ScopedExclusiveLock sel(re);
  re.fetch();

  using cta::common::dataStructures::JobQueueType;
  {
    //Get the archiveQueueToReport
    std::string archiveQueueToReport = re.getArchiveQueueAddress("TapePool0", JobQueueType::JobsToReportToUser);
    cta::objectstore::ArchiveQueue aq(archiveQueueToReport,be);

    //Fetch the queue so that we can get the archiveRequests from it
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    ASSERT_EQ(aq.dumpJobs().size(),2);
    for(auto &job: aq.dumpJobs()){
      ASSERT_TRUE(job.copyNb == 1 || job.copyNb == 3);
      ASSERT_EQ(job.size,667);
      cta::objectstore::ArchiveRequest archiveRequest(job.address,be);
      archiveRequest.fetchNoLock();
      cta::common::dataStructures::ArchiveFile archiveFile = archiveRequest.getArchiveFile();

      ASSERT_EQ(archiveFile.archiveFileID,aFile.archiveFileID);

      ASSERT_EQ(archiveFile.diskFileId,aFile.diskFileId);
      ASSERT_EQ(archiveFile.checksumBlob,aFile.checksumBlob);
      ASSERT_EQ(archiveFile.creationTime,aFile.creationTime);
      ASSERT_EQ(archiveFile.reconciliationTime,aFile.reconciliationTime);
      ASSERT_EQ(archiveFile.diskFileInfo,aFile.diskFileInfo);
      ASSERT_EQ(archiveFile.fileSize,aFile.fileSize);
      ASSERT_EQ(archiveFile.storageClass,aFile.storageClass);
    }
  }

  {
    //Get the archiveQueueToTransfer
    std::string archiveQueueToTransfer = re.getArchiveQueueAddress("TapePool1", JobQueueType::JobsToTransferForUser);
    cta::objectstore::ArchiveQueue aq(archiveQueueToTransfer,be);

    //Fetch the queue so that we can get the archiveRequests from it
    cta::objectstore::ScopedExclusiveLock aql(aq);
    aq.fetch();
    ASSERT_EQ(aq.dumpJobs().size(),1);
    ASSERT_EQ(aq.getTapePool(),"TapePool1");
    for(auto &job: aq.dumpJobs()){
      ASSERT_EQ(job.copyNb,2);
      ASSERT_EQ(job.size,667);
      cta::objectstore::ArchiveRequest archiveRequest(job.address,be);
      archiveRequest.fetchNoLock();
      cta::common::dataStructures::ArchiveFile archiveFile = archiveRequest.getArchiveFile();

      ASSERT_EQ(archiveFile.archiveFileID,aFile.archiveFileID);

      ASSERT_EQ(archiveFile.diskFileId,aFile.diskFileId);
      ASSERT_EQ(archiveFile.checksumBlob,aFile.checksumBlob);
      ASSERT_EQ(archiveFile.creationTime,aFile.creationTime);
      ASSERT_EQ(archiveFile.reconciliationTime,aFile.reconciliationTime);
      ASSERT_EQ(archiveFile.diskFileInfo,aFile.diskFileInfo);
      ASSERT_EQ(archiveFile.fileSize,aFile.fileSize);
      ASSERT_EQ(archiveFile.storageClass,aFile.storageClass);
    }
  }

  ASSERT_EQ(sorter.getAllArchive().size(),0);
}

TEST_F(ObjectStore,SorterInsertRetrieveRequest){

  using namespace cta::objectstore;

  //cta::log::StdoutLogger dl("dummy", "unitTest", "configFilename");
  cta::log::DummyLogger dl("dummy", "unitTest", "configFilename");
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  cta::objectstore::BackendVFS be;
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(nullptr));
  cta::objectstore::ScopedExclusiveLock rel(re);
  // Create the agent for objects creation
  cta::objectstore::AgentReference agentRef("unitTestCreateEnv", dl);
  // Finish root creation.
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // continue agent creation.
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);

  //Create the agent of the Sorter
  cta::objectstore::AgentReference agentRefSorter("agentRefSorter", dl);
  cta::objectstore::Agent agentSorter(agentRefSorter.getAgentAddress(), be);
  agentSorter.initialize();
  agentSorter.setTimeout_us(0);
  agentSorter.insertAndRegisterSelf(lc);

  std::string retrieveRequestID = agentRef.nextId("RetrieveRequest");
  agentRef.addToOwnership(retrieveRequestID,be);
  cta::objectstore::RetrieveRequest rr(retrieveRequestID,be);
  rr.initialize();
  cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
  rqc.archiveFile.archiveFileID = 123456789L;
  rqc.archiveFile.diskFileId = "eos://diskFile";
  rqc.archiveFile.checksumBlob.insert(cta::checksum::NONE, "");
  rqc.archiveFile.creationTime = 0;
  rqc.archiveFile.reconciliationTime = 0;
  rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  rqc.archiveFile.diskInstance = "eoseos";
  rqc.archiveFile.fileSize = 1000;
  rqc.archiveFile.storageClass = "sc";
  {
    cta::common::dataStructures::TapeFile tf;
    tf.blockId=0;
    tf.fileSize=1;
    tf.copyNb=1;
    tf.creationTime=time(nullptr);
    tf.fSeq=2;
    tf.vid="Tape0";
    rqc.archiveFile.tapeFiles.push_back(tf);
  }
  {
    cta::common::dataStructures::TapeFile tf;
    tf.blockId=0;
    tf.fileSize=2;
    tf.copyNb=2;
    tf.creationTime=time(nullptr);
    tf.fSeq=2;
    tf.vid="Tape1";
    rqc.archiveFile.tapeFiles.push_back(tf);
  }
  rqc.mountPolicy.archiveMinRequestAge = 1;
  rqc.mountPolicy.archivePriority = 1;
  rqc.mountPolicy.creationLog.time = time(nullptr);
  rqc.mountPolicy.lastModificationLog.time = time(nullptr);
  rqc.mountPolicy.retrieveMinRequestAge = 1;
  rqc.mountPolicy.retrievePriority = 1;
  rr.setRetrieveFileQueueCriteria(rqc);

  // Make sure job 1 will get queued by failing the other one.
  rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_Failed);

  cta::common::dataStructures::RetrieveRequest sReq;
  sReq.archiveFileID = rqc.archiveFile.archiveFileID;
  sReq.creationLog.time=time(nullptr);
  rr.setSchedulerRequest(sReq);

  rr.setOwner(agent.getAddressIfSet());
  rr.setActiveCopyNumber(0);
  rr.insert();

  cta::objectstore::Sorter sorter(agentRefSorter,be,catalogue);
  std::shared_ptr<cta::objectstore::RetrieveRequest> retrieveRequest = std::make_shared<cta::objectstore::RetrieveRequest>(rr);

  {
    cta::objectstore::ScopedExclusiveLock rrl(*retrieveRequest);
    retrieveRequest->fetch();

    sorter.insertRetrieveRequest(retrieveRequest,agentRef,std::nullopt,lc);
    rrl.release();
    cta::objectstore::Sorter::MapRetrieve allRetrieveJobs = sorter.getAllRetrieve();
    std::list<std::tuple<cta::objectstore::Sorter::RetrieveJob,std::future<void>>> allFutures;
    for(auto& kv: allRetrieveJobs){
      for(auto& job: kv.second){
        allFutures.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
      }
    }
    sorter.flushAll(lc);
    for(auto& future: allFutures){
      ASSERT_NO_THROW(std::get<1>(future).get());
    }

    allFutures.clear();

    typedef ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> Algo;
    Algo algo(be,agentRef);

    typename Algo::PopCriteria criteria;
    criteria.files = 1;
    criteria.bytes = 1000;
    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("Tape0",criteria,lc);
    ASSERT_EQ(elements.elements.size(),1);

    auto& elt = elements.elements.front();

    cta::common::dataStructures::ArchiveFile aFile = elt.archiveFile;
    ASSERT_EQ(aFile.archiveFileID,rqc.archiveFile.archiveFileID);
    ASSERT_EQ(aFile.diskFileId,rqc.archiveFile.diskFileId);
    ASSERT_EQ(aFile.checksumBlob,rqc.archiveFile.checksumBlob);
    ASSERT_EQ(aFile.creationTime,rqc.archiveFile.creationTime);
    ASSERT_EQ(aFile.reconciliationTime,rqc.archiveFile.reconciliationTime);
    ASSERT_EQ(aFile.diskFileInfo,rqc.archiveFile.diskFileInfo);
    ASSERT_EQ(aFile.fileSize,rqc.archiveFile.fileSize);
    ASSERT_EQ(aFile.storageClass,rqc.archiveFile.storageClass);
    ASSERT_EQ(elt.copyNb,1);
    ASSERT_EQ(elt.bytes,1000);
    ASSERT_EQ(elt.reportType,cta::SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired);
    ASSERT_EQ(elt.rr.archiveFileID,aFile.archiveFileID);
  }
  {
    ScopedExclusiveLock sel(*retrieveRequest);
    retrieveRequest->fetch();
    // Make sure now copy 2 will get queued.
    retrieveRequest->setJobStatus(1,cta::objectstore::serializers::RetrieveJobStatus::RJS_Failed);
    retrieveRequest->setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToTransfer);
    retrieveRequest->commit();

    ASSERT_NO_THROW(sorter.insertRetrieveRequest(retrieveRequest,agentRef,std::optional<uint32_t>(2),lc));

    sel.release();

    cta::objectstore::Sorter::MapRetrieve allRetrieveJobs = sorter.getAllRetrieve();
    std::list<std::tuple<cta::objectstore::Sorter::RetrieveJob,std::future<void>>> allFutures;
    ASSERT_EQ(allRetrieveJobs.size(),1);
    for(auto& kv: allRetrieveJobs){
      for(auto& job: kv.second){
        allFutures.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
      }
    }
    sorter.flushAll(lc);
    for(auto& future: allFutures){
      ASSERT_NO_THROW(std::get<1>(future).get());
    }

    ASSERT_EQ(sorter.getAllRetrieve().size(),0);
    typedef ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> Algo;
    Algo algo(be,agentRef);

    typename Algo::PopCriteria criteria;
    criteria.files = 1;
    criteria.bytes = 1000;
    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("Tape1",criteria,lc);
    ASSERT_EQ(elements.elements.size(),1);
    auto& elt = elements.elements.front();
    cta::common::dataStructures::ArchiveFile aFile = elt.archiveFile;
    ASSERT_EQ(aFile.archiveFileID,rqc.archiveFile.archiveFileID);
    ASSERT_EQ(aFile.diskFileId,rqc.archiveFile.diskFileId);
    ASSERT_EQ(aFile.checksumBlob,rqc.archiveFile.checksumBlob);
    ASSERT_EQ(aFile.creationTime,rqc.archiveFile.creationTime);
    ASSERT_EQ(aFile.reconciliationTime,rqc.archiveFile.reconciliationTime);
    ASSERT_EQ(aFile.diskFileInfo,rqc.archiveFile.diskFileInfo);
    ASSERT_EQ(aFile.fileSize,rqc.archiveFile.fileSize);
    ASSERT_EQ(aFile.storageClass,rqc.archiveFile.storageClass);
    ASSERT_EQ(elt.copyNb,2);
    ASSERT_EQ(elt.archiveFile.tapeFiles.at(2).fileSize,2);
    ASSERT_EQ(elt.bytes,1000);
    ASSERT_EQ(elt.reportType,cta::SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired);
    ASSERT_EQ(elt.rr.archiveFileID,aFile.archiveFileID);
  }
  {
    ScopedExclusiveLock sel(*retrieveRequest);
    retrieveRequest->fetch();
    // We should be forbidden to force queueing a non-exsiting copy number.
    ASSERT_THROW(sorter.insertRetrieveRequest(retrieveRequest,agentRef,std::optional<uint32_t>(4),lc),cta::exception::Exception);

    retrieveRequest->setJobStatus(1,serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
    retrieveRequest->setJobStatus(2,serializers::RetrieveJobStatus::RJS_Failed);
    retrieveRequest->commit();

    // We should be forbidden to requeue a request if no copy is in status ToTranfer.
    ASSERT_THROW(sorter.insertRetrieveRequest(retrieveRequest,agentRef,std::nullopt,lc),cta::exception::Exception);

    sel.release();
  }
}

TEST_F(ObjectStore,SorterInsertDifferentTypesOfRequests){

  using namespace cta::objectstore;

  //cta::log::StdoutLogger dl("dummy", "unitTest", "configFilename");
  cta::log::DummyLogger dl("dummy", "unitTest", "configFilename");
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call ArchiveRequests's garbage collector
  cta::objectstore::BackendVFS be;
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(nullptr));
  cta::objectstore::ScopedExclusiveLock rel(re);
  // Create the agent for objects creation
  cta::objectstore::AgentReference agentRef("unitTestCreateEnv", dl);
  // Finish root creation.
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // continue agent creation.
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);

  //Create the agent of the Sorter
  cta::objectstore::AgentReference agentRefSorter("agentRefSorter", dl);
  cta::objectstore::Agent agentSorter(agentRefSorter.getAgentAddress(), be);
  agentSorter.initialize();
  agentSorter.setTimeout_us(0);
  agentSorter.insertAndRegisterSelf(lc);

  /**
   * Creation of retrieve requests
   */
  std::string retrieveRequestID = agentRef.nextId("RetrieveRequest");
  agentRef.addToOwnership(retrieveRequestID,be);
  cta::objectstore::RetrieveRequest rr(retrieveRequestID,be);
  rr.initialize();
  cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
  rqc.archiveFile.archiveFileID = 1L;
  rqc.archiveFile.diskFileId = "eos://diskFile1";
  rqc.archiveFile.checksumBlob.insert(cta::checksum::NONE, "");
  rqc.archiveFile.creationTime = 0;
  rqc.archiveFile.reconciliationTime = 0;
  rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  rqc.archiveFile.diskInstance = "eoseos";
  rqc.archiveFile.fileSize = 1000;
  rqc.archiveFile.storageClass = "sc";
  {
    cta::common::dataStructures::TapeFile tf;
    tf.blockId=0;
    tf.fileSize=1;
    tf.copyNb=1;
    tf.creationTime=time(nullptr);
    tf.fSeq=1;
    tf.vid="Tape0";
    rqc.archiveFile.tapeFiles.push_back(tf);
  }
  rqc.mountPolicy.archiveMinRequestAge = 1;
  rqc.mountPolicy.archivePriority = 1;
  rqc.mountPolicy.creationLog.time = time(nullptr);
  rqc.mountPolicy.lastModificationLog.time = time(nullptr);
  rqc.mountPolicy.retrieveMinRequestAge = 1;
  rqc.mountPolicy.retrievePriority = 1;
  rr.setRetrieveFileQueueCriteria(rqc);

  cta::common::dataStructures::RetrieveRequest sReq;
  sReq.archiveFileID = rqc.archiveFile.archiveFileID;
  sReq.creationLog.time=time(nullptr);
  rr.setSchedulerRequest(sReq);

  rr.setOwner(agent.getAddressIfSet());
  rr.setActiveCopyNumber(0);
  rr.insert();

  std::string retrieveRequestID2 = agentRef.nextId("RetrieveRequest");
  agentRef.addToOwnership(retrieveRequestID2,be);
  cta::objectstore::RetrieveRequest rr2(retrieveRequestID2,be);
  rr2.initialize();
  cta::common::dataStructures::RetrieveFileQueueCriteria rqc2;
  rqc2.archiveFile.archiveFileID = 2L;
  rqc2.archiveFile.diskFileId = "eos://diskFile2";
  rqc2.archiveFile.checksumBlob.insert(cta::checksum::NONE, "");
  rqc2.archiveFile.creationTime = 0;
  rqc2.archiveFile.reconciliationTime = 0;
  rqc2.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  rqc2.archiveFile.diskInstance = "eoseos";
  rqc2.archiveFile.fileSize = 1000;
  rqc2.archiveFile.storageClass = "sc";
  {
    cta::common::dataStructures::TapeFile tf;
    tf.blockId=0;
    tf.fileSize=1;
    tf.copyNb=2;
    tf.creationTime=time(nullptr);
    tf.fSeq=2;
    tf.vid="Tape0";
    rqc2.archiveFile.tapeFiles.push_back(tf);
  }
  rqc2.mountPolicy.archiveMinRequestAge = 1;
  rqc2.mountPolicy.archivePriority = 1;
  rqc2.mountPolicy.creationLog.time = time(nullptr);
  rqc2.mountPolicy.lastModificationLog.time = time(nullptr);
  rqc2.mountPolicy.retrieveMinRequestAge = 1;
  rqc2.mountPolicy.retrievePriority = 1;
  rr2.setRetrieveFileQueueCriteria(rqc2);

  cta::common::dataStructures::RetrieveRequest sReq2;
  sReq2.archiveFileID = rqc2.archiveFile.archiveFileID;
  sReq2.creationLog.time=time(nullptr);
  rr2.setSchedulerRequest(sReq2);

  rr2.setOwner(agent.getAddressIfSet());
  rr2.setActiveCopyNumber(0);
  rr2.insert();

  cta::objectstore::Sorter sorter(agentRefSorter,be,catalogue);

  std::shared_ptr<cta::objectstore::RetrieveRequest> retrieveRequest = std::make_shared<cta::objectstore::RetrieveRequest>(rr);

  {
    ScopedExclusiveLock sel(*retrieveRequest);
    retrieveRequest->fetch();

    ASSERT_NO_THROW(sorter.insertRetrieveRequest(retrieveRequest,agentRef,std::nullopt,lc));

    sel.release();
  }

  std::shared_ptr<cta::objectstore::RetrieveRequest> retrieveRequest2 = std::make_shared<cta::objectstore::RetrieveRequest>(rr2);
  {
    ScopedExclusiveLock sel(*retrieveRequest2);
    retrieveRequest2->fetch();

    ASSERT_NO_THROW(sorter.insertRetrieveRequest(retrieveRequest2,agentRef,std::nullopt,lc));

    sel.release();
  }

  cta::objectstore::Sorter::MapRetrieve allRetrieveJobs = sorter.getAllRetrieve();
  std::list<std::tuple<cta::objectstore::Sorter::RetrieveJob,std::future<void>>> allFuturesRetrieve;
  ASSERT_EQ(allRetrieveJobs.size(),1);
  for(auto& kv: allRetrieveJobs){
    for(auto& job: kv.second){
      allFuturesRetrieve.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
    }
  }

  /**
   * Creation of Archive Requests
   */
  std::string archiveRequestID = agentRef.nextId("ArchiveRequest");
  agentRef.addToOwnership(archiveRequestID,be);
  cta::objectstore::ArchiveRequest ar(archiveRequestID,be);
  ar.initialize();
  cta::common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = 3L;
  aFile.diskFileId = "eos://diskFile";
  aFile.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  aFile.creationTime = 0;
  aFile.reconciliationTime = 0;
  aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  aFile.diskInstance = "eoseos";
  aFile.fileSize = 667;
  aFile.storageClass = "sc";
  ar.setArchiveFile(aFile);
  ar.addJob(1, "TapePool0", agentRef.getAgentAddress(), 1, 1, 1);
  cta::common::dataStructures::MountPolicy mp;
  ar.setMountPolicy(mp);
  ar.setArchiveReportURL("");
  ar.setArchiveErrorReportURL("");
  ar.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
  ar.setSrcURL("root://eoseos/myFile");
  ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
  ar.insert();

  std::string archiveRequestID2 = agentRef.nextId("ArchiveRequest");
  agentRef.addToOwnership(archiveRequestID2,be);
  cta::objectstore::ArchiveRequest ar2(archiveRequestID2,be);
  ar2.initialize();
  cta::common::dataStructures::ArchiveFile aFile2;
  aFile2.archiveFileID = 4L;
  aFile2.diskFileId = "eos://diskFile";
  aFile2.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  aFile2.creationTime = 0;
  aFile2.reconciliationTime = 0;
  aFile2.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  aFile2.diskInstance = "eoseos";
  aFile2.fileSize = 667;
  aFile2.storageClass = "sc";
  ar2.setArchiveFile(aFile2);
  ar2.addJob(2, "TapePool0", agentRef.getAgentAddress(), 1, 1, 1);
  ar2.addJob(3,"TapePool0", agentRef.getAgentAddress(), 1, 1, 1);
  ar2.setJobStatus(3,serializers::ArchiveJobStatus::AJS_ToReportToRepackForSuccess);
  ar2.setMountPolicy(mp);
  ar2.setArchiveReportURL("");
  ar2.setArchiveErrorReportURL("");
  ar2.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
  ar2.setSrcURL("root://eoseos/myFile");
  ar2.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
  ar2.insert();

  std::shared_ptr<ArchiveRequest> archiveRequest = std::make_shared<ArchiveRequest>(ar);
  {
    ScopedExclusiveLock sel(*archiveRequest);
    archiveRequest->fetch();

    ASSERT_NO_THROW(sorter.insertArchiveRequest(archiveRequest,agentRef,lc));
    sel.release();
  }

  std::shared_ptr<ArchiveRequest> archiveRequest2 = std::make_shared<ArchiveRequest>(ar2);
  {
    ScopedExclusiveLock sel(*archiveRequest2);
    archiveRequest2->fetch();

    ASSERT_NO_THROW(sorter.insertArchiveRequest(archiveRequest2,agentRef,lc));
    sel.release();
  }

  cta::objectstore::Sorter::MapArchive allArchiveJobs = sorter.getAllArchive();
  std::list<std::tuple<cta::objectstore::SorterArchiveJob,std::future<void>>> allFuturesArchive;
  ASSERT_EQ(allArchiveJobs.size(),2);
  for(auto& kv: allArchiveJobs){
    for(auto& job: kv.second){
      allFuturesArchive.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
    }
  }

  ASSERT_NO_THROW(sorter.flushAll(lc));

  for(auto& future: allFuturesRetrieve){
    ASSERT_NO_THROW(std::get<1>(future).get());
  }

  for(auto& future: allFuturesArchive){
    ASSERT_NO_THROW(std::get<1>(future).get());
  }

  {
    //Test the Retrieve Jobs
    typedef ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> Algo;
    Algo algo(be,agentRef);
    typename Algo::PopCriteria criteria;
    criteria.files = 2;
    criteria.bytes = 2000;

    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("Tape0",criteria,lc);
    ASSERT_EQ(elements.elements.size(),2);

    auto& elt = elements.elements.front();
    ASSERT_EQ(elt.copyNb,1);
    ASSERT_EQ(elt.archiveFile.tapeFiles.at(1).vid,"Tape0");
    ASSERT_EQ(elt.archiveFile.tapeFiles.at(1).fSeq,1);

    auto& elt2 = elements.elements.back();
    ASSERT_EQ(elt2.copyNb,2);
    ASSERT_EQ(elt2.archiveFile.tapeFiles.at(2).vid,"Tape0");
    ASSERT_EQ(elt2.archiveFile.tapeFiles.at(2).fSeq,2);

  }
  {
    //Test the Archive Jobs
    typedef ContainerAlgorithms<ArchiveQueue, ArchiveQueueToTransferForUser> Algo;
    Algo algo(be,agentRef);
    typename Algo::PopCriteria criteria;
    criteria.files = 2;
    criteria.bytes = 2000;

    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("TapePool0",criteria,lc);
    ASSERT_EQ(elements.elements.size(),2);

    auto &elt = elements.elements.front();

    ASSERT_EQ(elt.copyNb,1);
    ASSERT_EQ(elt.archiveFile.archiveFileID,3L);

    auto& elt2 = elements.elements.back();
    ASSERT_EQ(elt2.copyNb,2);
    ASSERT_EQ(elt2.archiveFile.archiveFileID,4L);
  }
  {
    //Test ArchiveJobToTransferForRepack
    typedef ContainerAlgorithms<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess> Algo;
    Algo algo(be,agentRef);
    typename Algo::PopCriteria criteria;
    criteria.files = 1;

    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("TapePool0",criteria,lc);
    ASSERT_EQ(elements.elements.size(),1);

    auto& elt = elements.elements.front();
    ASSERT_EQ(elt.copyNb,3);
    ASSERT_EQ(elt.archiveFile.archiveFileID,4L);
  }
}

TEST_F(ObjectStore,SorterInsertArchiveRequestNotFetched){

  using namespace cta::objectstore;

  //cta::log::StdoutLogger dl("dummy", "unitTest", "configFilename");
  cta::log::DummyLogger dl("dummy", "unitTest", "configFilename");
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call ArchiveRequests's garbage collector
  cta::objectstore::BackendVFS be;
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(nullptr));
  cta::objectstore::ScopedExclusiveLock rel(re);
  // Create the agent for objects creation
  cta::objectstore::AgentReference agentRef("unitTestCreateEnv", dl);
  // Finish root creation.
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // continue agent creation.
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);

  //Create the agent of the Sorter
  cta::objectstore::AgentReference agentRefSorter("agentRefSorter", dl);
  cta::objectstore::Agent agentSorter(agentRefSorter.getAgentAddress(), be);
  agentSorter.initialize();
  agentSorter.setTimeout_us(0);
  agentSorter.insertAndRegisterSelf(lc);

  /**
  * Creation of Archive Requests
  */
  std::string archiveRequestID = agentRef.nextId("ArchiveRequest");
  agentRef.addToOwnership(archiveRequestID,be);
  cta::objectstore::ArchiveRequest ar(archiveRequestID,be);
  ar.initialize();
  cta::common::dataStructures::ArchiveFile aFile;
  aFile.archiveFileID = 3L;
  aFile.diskFileId = "eos://diskFile";
  aFile.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  aFile.creationTime = 0;
  aFile.reconciliationTime = 0;
  aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  aFile.diskInstance = "eoseos";
  aFile.fileSize = 667;
  aFile.storageClass = "sc";
  ar.setArchiveFile(aFile);
  ar.addJob(1, "TapePool0", agentRef.getAgentAddress(), 1, 1, 1);
  ar.addJob(2, "TapePool0", agentRef.getAgentAddress(), 1, 1, 1);
  cta::common::dataStructures::MountPolicy mp;
  ar.setMountPolicy(mp);
  ar.setArchiveReportURL("");
  ar.setArchiveErrorReportURL("");
  ar.setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
  ar.setSrcURL("root://eoseos/myFile");
  ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
  ar.insert();



  SorterArchiveRequest request;
  std::list<SorterArchiveJob>& jobs = request.archiveJobs;
  jobs.emplace_back();
  SorterArchiveJob& job1 = jobs.back();
  job1.archiveRequest = std::make_shared<cta::objectstore::ArchiveRequest>(ar);
  job1.archiveFile = aFile;
  job1.jobDump.copyNb = 1;
  job1.jobDump.tapePool = "TapePool0";
  job1.jobDump.owner = agentRef.getAgentAddress();
  job1.jobDump.status = serializers::ArchiveJobStatus::AJS_ToTransferForUser;
  job1.jobQueueType = cta::common::dataStructures::JobQueueType::JobsToTransferForUser;
  job1.mountPolicy = mp;
  job1.mountPolicy.creationLog = cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr));
  job1.previousOwner = &agentRef;

  jobs.emplace_back();
  SorterArchiveJob& job2 = jobs.back();
  job2.archiveRequest = std::make_shared<cta::objectstore::ArchiveRequest>(ar);
  job2.archiveFile = aFile;
  job2.jobDump.copyNb = 2;
  job2.jobDump.tapePool = "TapePool0";
  job2.jobDump.owner = agentRef.getAgentAddress();
  job2.jobDump.status = serializers::ArchiveJobStatus::AJS_ToTransferForUser;
  job2.jobQueueType = cta::common::dataStructures::JobQueueType::JobsToTransferForUser;
  job2.mountPolicy = mp;
  job2.mountPolicy.creationLog = cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr));
  job2.previousOwner = &agentRef;

  Sorter sorter(agentRefSorter,be,catalogue);
  sorter.insertArchiveRequest(request,agentRef,lc);

  cta::objectstore::Sorter::MapArchive allArchiveJobs = sorter.getAllArchive();
  std::list<std::tuple<cta::objectstore::SorterArchiveJob,std::future<void>>> allFuturesArchive;
  ASSERT_EQ(allArchiveJobs.size(),1);
  for(auto& kv: allArchiveJobs){
    for(auto& job: kv.second){
      allFuturesArchive.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
    }
  }

  sorter.flushAll(lc);

  for(auto& future: allFuturesArchive){
    ASSERT_NO_THROW(std::get<1>(future).get());
  }

  {
    //Test the Archive Jobs
    typedef ContainerAlgorithms<ArchiveQueue, ArchiveQueueToTransferForUser> Algo;
    Algo algo(be,agentRef);
    typename Algo::PopCriteria criteria;
    criteria.files = 2;
    criteria.bytes = 2000;

    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("TapePool0",criteria,lc);
    ASSERT_EQ(elements.elements.size(),2);

    auto &elt = elements.elements.front();

    ASSERT_EQ(elt.copyNb,1);
    ASSERT_EQ(elt.archiveFile.archiveFileID,3L);
    ASSERT_EQ(elt.archiveFile.checksumBlob,cta::checksum::ChecksumBlob(cta::checksum::ADLER32, "1234"));
    ASSERT_EQ(elt.archiveFile.diskInstance,"eoseos");
    ASSERT_EQ(elt.archiveFile.diskFileId,"eos://diskFile");

    auto &elt2 = elements.elements.back();

    ASSERT_EQ(elt2.copyNb,2);
    ASSERT_EQ(elt2.archiveFile.archiveFileID,3L);
    ASSERT_EQ(elt2.archiveFile.checksumBlob,cta::checksum::ChecksumBlob(cta::checksum::ADLER32, "1234"));
    ASSERT_EQ(elt.archiveFile.diskInstance,"eoseos");
    ASSERT_EQ(elt.archiveFile.diskFileId,"eos://diskFile");
  }

}

TEST_F(ObjectStore,SorterInsertRetrieveRequestNotFetched){
  using namespace cta::objectstore;

  //cta::log::StdoutLogger dl("dummy", "unitTest", "configFilename");
  cta::log::DummyLogger dl("dummy", "unitTest", "configFilename");
  cta::log::LogContext lc(dl);
  // We need a dummy catalogue
  cta::catalogue::DummyCatalogue catalogue;
  // Here we check that can successfully call ArchiveRequests's garbage collector
  cta::objectstore::BackendVFS be;
  // Create the root entry
  cta::objectstore::RootEntry re(be);
  re.initialize();
  re.insert();
  // Create the agent register
  cta::objectstore::EntryLogSerDeser el("user0",
      "unittesthost", time(nullptr));
  cta::objectstore::ScopedExclusiveLock rel(re);
  // Create the agent for objects creation
  cta::objectstore::AgentReference agentRef("unitTestCreateEnv", dl);
  // Finish root creation.
  re.addOrGetAgentRegisterPointerAndCommit(agentRef, el, lc);
  rel.release();
  // continue agent creation.
  cta::objectstore::Agent agent(agentRef.getAgentAddress(), be);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);

  //Create the agent of the Sorter
  cta::objectstore::AgentReference agentRefSorter("agentRefSorter", dl);
  cta::objectstore::Agent agentSorter(agentRefSorter.getAgentAddress(), be);
  agentSorter.initialize();
  agentSorter.setTimeout_us(0);
  agentSorter.insertAndRegisterSelf(lc);

  /**
   * Creation of retrieve requests
   */
  std::string retrieveRequestID = agentRef.nextId("RetrieveRequest");
  agentRef.addToOwnership(retrieveRequestID,be);
  cta::objectstore::RetrieveRequest rr(retrieveRequestID,be);
  rr.initialize();
  cta::common::dataStructures::RetrieveFileQueueCriteria rqc;
  rqc.archiveFile.archiveFileID = 1L;
  rqc.archiveFile.diskFileId = "eos://diskFile";
  rqc.archiveFile.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  rqc.archiveFile.creationTime = 0;
  rqc.archiveFile.reconciliationTime = 0;
  rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  rqc.archiveFile.diskInstance = "eoseos";
  rqc.archiveFile.fileSize = 1000;
  rqc.archiveFile.storageClass = "sc";
  {
    cta::common::dataStructures::TapeFile tf;
    tf.blockId=0;
    tf.fileSize=1;
    tf.copyNb=1;
    tf.creationTime=time(nullptr);
    tf.fSeq=1;
    tf.vid="Tape0";
    rqc.archiveFile.tapeFiles.push_back(tf);
  }
  rqc.mountPolicy.archiveMinRequestAge = 1;
  rqc.mountPolicy.archivePriority = 1;
  rqc.mountPolicy.creationLog.time = time(nullptr);
  rqc.mountPolicy.lastModificationLog.time = time(nullptr);
  rqc.mountPolicy.retrieveMinRequestAge = 1;
  rqc.mountPolicy.retrievePriority = 1;
  rr.setRetrieveFileQueueCriteria(rqc);

  cta::common::dataStructures::RetrieveRequest sReq;
  sReq.archiveFileID = rqc.archiveFile.archiveFileID;
  sReq.creationLog.time=time(nullptr);
  rr.setSchedulerRequest(sReq);

  rr.setOwner(agent.getAddressIfSet());
  rr.setActiveCopyNumber(0);
  rr.insert();

  Sorter::SorterRetrieveRequest request;
  request.archiveFile = rqc.archiveFile;

  for(auto& tf: request.archiveFile.tapeFiles){
    Sorter::RetrieveJob& job = request.retrieveJobs[tf.copyNb];
    job.fSeq = tf.fSeq;
    job.fileSize = rqc.archiveFile.fileSize;
    job.jobDump.copyNb = tf.copyNb;
    job.jobDump.status = serializers::RetrieveJobStatus::RJS_ToTransfer;
    job.jobQueueType = cta::common::dataStructures::JobQueueType::JobsToTransferForUser;
    job.mountPolicy = rqc.mountPolicy;
    job.previousOwner = &agentRef;
    job.retrieveRequest = std::make_shared<cta::objectstore::RetrieveRequest>(rr);
  }

  Sorter sorter(agentRefSorter,be,catalogue);
  sorter.insertRetrieveRequest(request,agentRef,std::nullopt,lc);

  cta::objectstore::Sorter::MapRetrieve allRetrieveJobs = sorter.getAllRetrieve();
  std::list<std::tuple<cta::objectstore::Sorter::RetrieveJob,std::future<void>>> allFuturesRetrieve;
  ASSERT_EQ(allRetrieveJobs.size(),1);
  for(auto& kv: allRetrieveJobs){
    for(auto& job: kv.second){
      allFuturesRetrieve.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
    }
  }

  ASSERT_NO_THROW(sorter.flushAll(lc));

  for(auto& future: allFuturesRetrieve){
    ASSERT_NO_THROW(std::get<1>(future).get());
  }

  {
    //Test the Retrieve Jobs
    typedef ContainerAlgorithms<RetrieveQueue,RetrieveQueueToTransfer> Algo;
    Algo algo(be,agentRef);
    typename Algo::PopCriteria criteria;
    criteria.files = 2;
    criteria.bytes = 2000;

    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("Tape0",criteria,lc);
    ASSERT_EQ(elements.elements.size(),1);

    auto& elt = elements.elements.front();
    ASSERT_EQ(elt.copyNb,1);
    ASSERT_EQ(elt.archiveFile.tapeFiles.at(1).vid,"Tape0");
    ASSERT_EQ(elt.archiveFile.tapeFiles.at(1).fSeq,1);
    ASSERT_EQ(elt.archiveFile.checksumBlob,cta::checksum::ChecksumBlob(cta::checksum::ADLER32, "1234"));
    ASSERT_EQ(elt.archiveFile.diskInstance,"eoseos");
    ASSERT_EQ(elt.archiveFile.fileSize,1000);
    ASSERT_EQ(elt.archiveFile.storageClass,"sc");
  }

}

}
