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
    ar.setJobStatus(1,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToReportForTransfer);
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
    for(auto& j: jobs){
      sorter.insertArchiveJob(arPtr,j,lc);
    }
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
      ASSERT_EQ(aq.dumpJobs().size(),1);
      ASSERT_EQ(aq.getTapePool(),"TapePool0");
      for(auto &job: aq.dumpJobs()){
        ASSERT_EQ(job.copyNb,1);
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
  }
  
  
}

