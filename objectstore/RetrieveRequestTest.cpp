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

#include "common/log/DummyLogger.hpp"
#include "BackendVFS.hpp"
#include "RootEntry.hpp"
#include "AgentReference.hpp"
#include "Agent.hpp"
#include "RetrieveRequest.hpp"
#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif
#include "catalogue/DummyCatalogue.hpp"
#include "Algorithms.hpp"
#include "RetrieveQueueAlgorithms.hpp"
#include "RetrieveQueue.hpp"

namespace unitTests {
  using namespace cta::objectstore;
  
  TEST(RetrieveRequest, IndividualGarbageCollectionRetrieveRequestToReportToRepackForSuccess) {
    #ifdef STDOUT_LOGGING
      cta::log::StdoutLogger dl("dummy", "unitTest");
    #else
      cta::log::DummyLogger dl("dummy", "unitTest");
    #endif
    cta::log::LogContext lc(dl);
    // We need a dummy catalogue
    cta::catalogue::DummyCatalogue catalogue;
    // Here we check that can successfully call RetrieveRequests's garbage collector
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
    // Create an agent to garbage be collected
    cta::objectstore::AgentReference agrA("unitTestAgentA", dl);
    cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
    agA.initialize();
    agA.setTimeout_us(0);
    agA.insertAndRegisterSelf(lc);
    
    //Create the RetrieveRequest
    std::string atfrAddr = agrA.nextId("RetrieveRequest");
    agrA.addToOwnership(atfrAddr, be);
    
    cta::objectstore::RetrieveRequest rr(atfrAddr, be);
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
      tf.fSeq=1;
      tf.vid="Tape0";
      rqc.archiveFile.tapeFiles.push_back(tf);
    }
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.maxDrivesAllowed = 1;
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    rr.setRetrieveFileQueueCriteria(rqc);
    cta::common::dataStructures::RetrieveRequest sReq;
    sReq.archiveFileID = rqc.archiveFile.archiveFileID;
    sReq.creationLog.time=time(nullptr);
    rr.setSchedulerRequest(sReq);
    rr.addJob(1, 1, 1, 1);
    rr.setJobStatus(1,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
    rr.setOwner(agA.getAddressIfSet());
    rr.setActiveCopyNumber(0);
    cta::objectstore::RetrieveRequest::RepackInfo ri;
    ri.isRepack = true;
    ri.fSeq = 1;
    ri.fileBufferURL = "testFileBufferURL";
    ri.repackRequestAddress = "repackRequestAddress";
    rr.setRepackInfo(ri);
    rr.insert();
    
    //Calling the individual garbage collection of the RetrieveRequest
    cta::objectstore::ScopedExclusiveLock sel(rr);
    ASSERT_NO_THROW(rr.garbageCollect(rr.getOwner(),agentRef,lc,catalogue));
    
    //The Retrieve Request should now be queued in the RetrieveQueueToReportToRepackForSuccess
    
    typedef cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue,cta::objectstore::RetrieveQueueToReportToRepackForSuccess> RQTRTRFSAlgo;
    RQTRTRFSAlgo algo(be,agentRef);
    RQTRTRFSAlgo::PopCriteria criteria;
    criteria.files = 1;
    auto jobs = algo.popNextBatch(ri.repackRequestAddress, criteria, lc);
    ASSERT_FALSE(jobs.elements.empty());
    
    ASSERT_EQ(123456789L,jobs.elements.front().archiveFile.archiveFileID);
    ASSERT_EQ(1,jobs.elements.front().archiveFile.tapeFiles.at(1).fSeq);
    ASSERT_EQ("Tape0",jobs.elements.front().archiveFile.tapeFiles.at(1).vid);
  }
  
  TEST(RetrieveRequest, IndividualGarbageCollectionRetrieveRequestToReportToRepackForFailure) {
    #ifdef STDOUT_LOGGING
      cta::log::StdoutLogger dl("dummy", "unitTest");
    #else
      cta::log::DummyLogger dl("dummy", "unitTest");
    #endif
    cta::log::LogContext lc(dl);
    // We need a dummy catalogue
    cta::catalogue::DummyCatalogue catalogue;
    // Here we check that can successfully call RetrieveRequests's garbage collector
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
    // Create an agent to garbage be collected
    cta::objectstore::AgentReference agrA("unitTestAgentA", dl);
    cta::objectstore::Agent agA(agrA.getAgentAddress(), be);
    agA.initialize();
    agA.setTimeout_us(0);
    agA.insertAndRegisterSelf(lc);
    
    //Create the RetrieveRequest
    std::string atfrAddr = agrA.nextId("RetrieveRequest");
    agrA.addToOwnership(atfrAddr, be);
    
    cta::objectstore::RetrieveRequest rr(atfrAddr, be);
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
      tf.fSeq=1;
      tf.vid="Tape0";
      rqc.archiveFile.tapeFiles.push_back(tf);
    }
    rqc.mountPolicy.archiveMinRequestAge = 1;
    rqc.mountPolicy.archivePriority = 1;
    rqc.mountPolicy.creationLog.time = time(nullptr);
    rqc.mountPolicy.lastModificationLog.time = time(nullptr);
    rqc.mountPolicy.maxDrivesAllowed = 1;
    rqc.mountPolicy.retrieveMinRequestAge = 1;
    rqc.mountPolicy.retrievePriority = 1;
    rr.setRetrieveFileQueueCriteria(rqc);
    cta::common::dataStructures::RetrieveRequest sReq;
    sReq.archiveFileID = rqc.archiveFile.archiveFileID;
    sReq.creationLog.time=time(nullptr);
    rr.setSchedulerRequest(sReq);
    rr.addJob(1, 1, 1, 1);
    rr.setJobStatus(1,cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForFailure);
    rr.setOwner(agA.getAddressIfSet());
    rr.setActiveCopyNumber(0);
    cta::objectstore::RetrieveRequest::RepackInfo ri;
    ri.isRepack = true;
    ri.fSeq = 1;
    ri.fileBufferURL = "testFileBufferURL";
    ri.repackRequestAddress = "repackRequestAddress";
    rr.setRepackInfo(ri);
    rr.insert();
    
    //Calling the individual garbage collection of the RetrieveRequest
    cta::objectstore::ScopedExclusiveLock sel(rr);
    ASSERT_NO_THROW(rr.garbageCollect(rr.getOwner(),agentRef,lc,catalogue));
    
    //The Retrieve Request should now be queued in the RetrieveQueueToReportToRepackForFailure
    
    typedef cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue,cta::objectstore::RetrieveQueueToReportToRepackForFailure> RQTRTRFFAlgo;
    RQTRTRFFAlgo algo(be,agentRef);
    RQTRTRFFAlgo::PopCriteria criteria;
    criteria.files = 1;
    auto jobs = algo.popNextBatch(ri.repackRequestAddress, criteria, lc);
    ASSERT_FALSE(jobs.elements.empty());
    
    ASSERT_EQ(123456789L,jobs.elements.front().archiveFile.archiveFileID);
    ASSERT_EQ(1,jobs.elements.front().archiveFile.tapeFiles.at(1).fSeq);
    ASSERT_EQ("Tape0",jobs.elements.front().archiveFile.tapeFiles.at(1).vid);
  }
}