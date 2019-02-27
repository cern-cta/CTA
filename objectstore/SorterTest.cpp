/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#include <gtest/gtest.h>
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
#include "catalogue/DummyCatalogue.hpp"
#include "Sorter.hpp"
#include <memory>

namespace unitTests {
  
TEST(ObjectStore,SorterInsertArchiveRequest){
  cta::log::StdoutLogger dl("dummy", "unitTest");
  //cta::log::DummyLogger dl("dummy", "unitTest");
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
      "unittesthost", time(NULL));
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
  aFile.checksumType = "checksumType";
  aFile.checksumValue = "checksumValue";
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
  ar.setJobStatus(1,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportForTransfer);
  ar.setJobStatus(3,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportForTransfer);
  cta::common::dataStructures::MountPolicy mp;
  ar.setMountPolicy(mp);
  ar.setArchiveReportURL("");
  ar.setArchiveErrorReportURL("");
  ar.setRequester(cta::common::dataStructures::UserIdentity("user0", "group0"));
  ar.setSrcURL("root://eoseos/myFile");
  ar.setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
  ar.insert();
  cta::objectstore::ScopedExclusiveLock atfrl(ar);
  ar.fetch();
  auto jobs = ar.dumpJobs();
  cta::objectstore::Sorter sorter(agentRefSorter,be,catalogue);
  std::shared_ptr<cta::objectstore::ArchiveRequest> arPtr = std::make_shared<cta::objectstore::ArchiveRequest>(ar);
  sorter.insertArchiveRequest(arPtr,agentRef,lc);
  atfrl.release();
  //Get the future
  cta::objectstore::Sorter::MapArchive allArchiveJobs = sorter.getAllArchive();
  std::list<std::tuple<cta::objectstore::Sorter::ArchiveJob,std::future<void>>> allFutures;
  
  for(auto& kv: allArchiveJobs){
    for(auto& job: kv.second){
      allFutures.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
    }
  }
  sorter.flushOneArchive(lc);
  sorter.flushOneArchive(lc);
  for(auto& future: allFutures){
    ASSERT_NO_THROW(std::get<1>(future).get());
  }

  cta::objectstore::ScopedExclusiveLock sel(re);
  re.fetch();

  {
    //Get the archiveQueueToReport
    std::string archiveQueueToReport = re.getArchiveQueueAddress("TapePool0",cta::objectstore::JobQueueType::JobsToReportToUser);
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
      ASSERT_EQ(archiveFile.checksumType,aFile.checksumType);
      ASSERT_EQ(archiveFile.checksumValue,aFile.checksumValue);
      ASSERT_EQ(archiveFile.creationTime,aFile.creationTime);
      ASSERT_EQ(archiveFile.reconciliationTime,aFile.reconciliationTime);
      ASSERT_EQ(archiveFile.diskFileInfo,aFile.diskFileInfo);
      ASSERT_EQ(archiveFile.fileSize,aFile.fileSize);
      ASSERT_EQ(archiveFile.storageClass,aFile.storageClass);
    }
  }

  {
    //Get the archiveQueueToTransfer
    std::string archiveQueueToTransfer = re.getArchiveQueueAddress("TapePool1",cta::objectstore::JobQueueType::JobsToTransfer);
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
      ASSERT_EQ(archiveFile.checksumType,aFile.checksumType);
      ASSERT_EQ(archiveFile.checksumValue,aFile.checksumValue);
      ASSERT_EQ(archiveFile.creationTime,aFile.creationTime);
      ASSERT_EQ(archiveFile.reconciliationTime,aFile.reconciliationTime);
      ASSERT_EQ(archiveFile.diskFileInfo,aFile.diskFileInfo);
      ASSERT_EQ(archiveFile.fileSize,aFile.fileSize);
      ASSERT_EQ(archiveFile.storageClass,aFile.storageClass);
    }
  }
  
  ASSERT_EQ(sorter.getAllArchive().size(),0);
}
  
TEST(ObjectStore,SorterInsertRetrieveRequest){
  
  using namespace cta::objectstore;
  
  cta::log::StdoutLogger dl("dummy", "unitTest");
  //cta::log::DummyLogger dl("dummy", "unitTest");
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
      "unittesthost", time(NULL));
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
  rqc.archiveFile.checksumType = "";
  rqc.archiveFile.checksumValue = "";
  rqc.archiveFile.creationTime = 0;
  rqc.archiveFile.reconciliationTime = 0;
  rqc.archiveFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
  rqc.archiveFile.diskInstance = "eoseos";
  rqc.archiveFile.fileSize = 1000;
  rqc.archiveFile.storageClass = "sc";
  rqc.archiveFile.tapeFiles[1].blockId=0;
  rqc.archiveFile.tapeFiles[1].compressedSize=1;
  rqc.archiveFile.tapeFiles[1].compressedSize=1;
  rqc.archiveFile.tapeFiles[1].copyNb=1;
  rqc.archiveFile.tapeFiles[1].creationTime=time(nullptr);
  rqc.archiveFile.tapeFiles[1].fSeq=2;
  rqc.archiveFile.tapeFiles[1].vid="Tape0";
  rqc.archiveFile.tapeFiles[2].blockId=0;
  rqc.archiveFile.tapeFiles[2].compressedSize=2;
  rqc.archiveFile.tapeFiles[2].compressedSize=2;
  rqc.archiveFile.tapeFiles[2].copyNb=2;
  rqc.archiveFile.tapeFiles[2].creationTime=time(nullptr);
  rqc.archiveFile.tapeFiles[2].fSeq=2;
  rqc.archiveFile.tapeFiles[2].vid="Tape1";
  rqc.mountPolicy.archiveMinRequestAge = 1;
  rqc.mountPolicy.archivePriority = 1;
  rqc.mountPolicy.creationLog.time = time(nullptr);
  rqc.mountPolicy.lastModificationLog.time = time(nullptr);
  rqc.mountPolicy.maxDrivesAllowed = 1;
  rqc.mountPolicy.retrieveMinRequestAge = 1;
  rqc.mountPolicy.retrievePriority = 1;
  rr.setRetrieveFileQueueCriteria(rqc);
  
  rr.setJobStatus(2,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
  
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

    sorter.insertRetrieveRequest(retrieveRequest,agentRef,cta::nullopt,lc);
    rrl.release();
    cta::objectstore::Sorter::MapRetrieve allRetrieveJobs = sorter.getAllRetrieve();
    std::list<std::tuple<cta::objectstore::Sorter::RetrieveJob,std::future<void>>> allFutures;
    for(auto& kv: allRetrieveJobs){
      for(auto& job: kv.second){
        allFutures.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
      }
    }
    sorter.flushOneRetrieve(lc);
    sorter.flushOneRetrieve(lc);
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
    ASSERT_EQ(aFile.checksumType,rqc.archiveFile.checksumType);
    ASSERT_EQ(aFile.checksumValue,rqc.archiveFile.checksumValue);
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

    sorter.insertRetrieveRequest(retrieveRequest,agentRef,cta::optional<uint32_t>(2),lc);

    sel.release();

    cta::objectstore::Sorter::MapRetrieve allRetrieveJobs = sorter.getAllRetrieve();
    std::list<std::tuple<cta::objectstore::Sorter::RetrieveJob,std::future<void>>> allFutures;
    ASSERT_EQ(allRetrieveJobs.size(),1);
    for(auto& kv: allRetrieveJobs){
      for(auto& job: kv.second){
        allFutures.emplace_back(std::make_tuple(std::get<0>(job->jobToQueue),std::get<1>(job->jobToQueue).get_future()));
      }
    }
    sorter.flushOneRetrieve(lc);
    sorter.flushOneRetrieve(lc);
    for(auto& future: allFutures){
      ASSERT_NO_THROW(std::get<1>(future).get());
    }
    
    ASSERT_EQ(sorter.getAllRetrieve().size(),0);
    typedef ContainerAlgorithms<RetrieveQueue,RetrieveQueueToReportToRepackForSuccess> Algo;
    Algo algo(be,agentRef);
    
    typename Algo::PopCriteria criteria;
    criteria.files = 1;
    typename Algo::PoppedElementsBatch elements = algo.popNextBatch("Tape1",criteria,lc);
    ASSERT_EQ(elements.elements.size(),1);
    auto& elt = elements.elements.front();
    
    cta::common::dataStructures::ArchiveFile aFile = elt.archiveFile;
    ASSERT_EQ(aFile.archiveFileID,rqc.archiveFile.archiveFileID);
    ASSERT_EQ(aFile.diskFileId,rqc.archiveFile.diskFileId);
    ASSERT_EQ(aFile.checksumType,rqc.archiveFile.checksumType);
    ASSERT_EQ(aFile.checksumValue,rqc.archiveFile.checksumValue);
    ASSERT_EQ(aFile.creationTime,rqc.archiveFile.creationTime);
    ASSERT_EQ(aFile.reconciliationTime,rqc.archiveFile.reconciliationTime);
    ASSERT_EQ(aFile.diskFileInfo,rqc.archiveFile.diskFileInfo);
    ASSERT_EQ(aFile.fileSize,rqc.archiveFile.fileSize);
    ASSERT_EQ(aFile.storageClass,rqc.archiveFile.storageClass);
    ASSERT_EQ(elt.copyNb,2);
    ASSERT_EQ(elt.archiveFile.tapeFiles[2].compressedSize,2);
    ASSERT_EQ(elt.bytes,1000);
    ASSERT_EQ(elt.reportType,cta::SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired);
    ASSERT_EQ(elt.rr.archiveFileID,aFile.archiveFileID);
  }
  {
    ScopedExclusiveLock sel(*retrieveRequest);
    retrieveRequest->fetch();
    
    ASSERT_THROW(sorter.insertRetrieveRequest(retrieveRequest,agentRef,cta::optional<uint32_t>(4),lc),cta::exception::Exception);

    retrieveRequest->setJobStatus(1,serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
    
    ASSERT_THROW(sorter.insertRetrieveRequest(retrieveRequest,agentRef,cta::nullopt,lc),cta::exception::Exception);
    
    sel.release();
  }
}
  
}

