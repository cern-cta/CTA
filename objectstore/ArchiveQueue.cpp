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

#include <google/protobuf/util/json_util.h>

#include "AgentReference.hpp"
#include "ArchiveQueue.hpp"
#include "ArchiveQueueShard.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "EntryLogSerDeser.hpp"
#include "GenericObject.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include "RootEntry.hpp"
#include "ValueCountMap.hpp"

namespace cta::objectstore {

ArchiveQueue::ArchiveQueue(const std::string& address, Backend& os):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(os, address) { }

ArchiveQueue::ArchiveQueue(Backend& os):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(os) { }

ArchiveQueue::ArchiveQueue(GenericObject& go):
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

std::string ArchiveQueue::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

void ArchiveQueue::initialize(const std::string& name) {
  // Setup underlying object
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>::initialize();
  // Setup the object so it's valid
  m_payload.set_tapepool(name);
  // set the archive jobs counter to zero
  m_payload.set_archivejobstotalsize(0);
  m_payload.set_archivejobscount(0);
  m_payload.set_oldestjobcreationtime(0);
  m_payload.set_youngestjobcreationtime(0);
  // set the initial summary map rebuild count to zero
  m_payload.set_mapsrebuildcount(0);
  // This object is good to go (to storage)
  m_payloadInterpreted = true;
}

void ArchiveQueue::commit() {
  if (!checkMapsAndShardsCoherency()) {
    rebuild();
  }
  ObjectOps<serializers::ArchiveQueue, serializers::ArchiveQueue_t>::commit();
}

bool ArchiveQueue::checkMapsAndShardsCoherency() {
  checkPayloadReadable();
  uint64_t bytesFromShardPointers = 0;
  uint64_t jobsExpectedFromShardsPointers = 0;
  // Add up shard summaries
  for (auto & aqs: m_payload.archivequeueshards()) {
    bytesFromShardPointers += aqs.shardbytescount();
    jobsExpectedFromShardsPointers += aqs.shardjobscount();
  }
  uint64_t totalBytes = m_payload.archivejobstotalsize();
  uint64_t totalJobs = m_payload.archivejobscount();
  // The sum of shards should be equal to the summary
  if (totalBytes != bytesFromShardPointers ||
      totalJobs != jobsExpectedFromShardsPointers)
    return false;
  // Check that we have coherent queue summaries
  ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
  ValueCountMapUint64 minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  if (priorityMap.total() != m_payload.archivejobscount() ||
      minArchiveRequestAgeMap.total() != m_payload.archivejobscount() ||
      mountPolicyNameMap.total() != m_payload.archivejobscount()
    )
    return false;
  return true;
}

void ArchiveQueue::rebuild() {
  checkPayloadWritable();
  // Something is off with the queue. We will hence rebuild it. The rebuild of the
  // queue will consist in:
  // 1) Attempting to read all shards in parallel. Absent shards are possible, and will
  // mean we have dangling pointers.
  // 2) Rebuild the summaries from the shards.
  // As a side note, we do not go as far as validating the pointers to jobs within th
  // shards, as this is already handled as access goes.
  std::list<ArchiveQueueShard> shards;
  std::list<std::unique_ptr<ArchiveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;

  // Get the summaries structures ready
  ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
  priorityMap.clear();
  ValueCountMapUint64 minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
  minArchiveRequestAgeMap.clear();
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  mountPolicyNameMap.clear();
  for (auto & sa: m_payload.archivequeueshards()) {
    shards.emplace_back(ArchiveQueueShard(sa.address(), m_objectStore));
    shardsFetchers.emplace_back(shards.back().asyncLockfreeFetch());
  }
  auto s = shards.begin();
  auto sf = shardsFetchers.begin();
  uint64_t totalJobs=0;
  uint64_t totalBytes=0;
  time_t oldestJobCreationTime=std::numeric_limits<time_t>::max();
  time_t youngestJobCreationTime=std::numeric_limits<time_t>::min();

  while (s != shards.end()) {
    // Each shard could be gone or be empty
    bool shardObjectNotFound = false;
    try {
      (*sf)->wait();
    } catch (cta::exception::NoSuchObject & ex) {
      shardObjectNotFound = true;
    }
    if (shardObjectNotFound || s->dumpJobs().empty()) {
      // Remove the shard from the list
      auto aqs = m_payload.mutable_archivequeueshards()->begin();
      while (aqs != m_payload.mutable_archivequeueshards()->end()) {
        if (aqs->address() == s->getAddressIfSet()) {
          aqs = m_payload.mutable_archivequeueshards()->erase(aqs);
        } else {
          aqs++;
        }
      }
      goto nextShard;
    }
    {
      // The shard is still around, let's compute its summaries.
      uint64_t jobs = 0;
      uint64_t size = 0;
      for (auto & j: s->dumpJobs()) {
        jobs++;
        size += j.size;
        priorityMap.incCount(j.priority);
        minArchiveRequestAgeMap.incCount(j.minArchiveRequestAge);
        mountPolicyNameMap.incCount(j.mountPolicyName);
        if (j.startTime < oldestJobCreationTime) oldestJobCreationTime = j.startTime;
        if (j.startTime > youngestJobCreationTime) youngestJobCreationTime = j.startTime;
      }
      // Add the summary to total.
      totalJobs+=jobs;
      totalBytes+=size;
      // And store the value in the shard pointers.
      auto maqs = m_payload.mutable_archivequeueshards();
      for (auto & aqsp: *maqs) {
        if (aqsp.address() == s->getAddressIfSet()) {
          aqsp.set_shardjobscount(jobs);
          aqsp.set_shardbytescount(size);
          goto shardUpdated;
        }
      }
      {
        // We had to update a shard and did not find it. This is an error.
        throw exception::Exception(std::string ("In ArchiveQueue::rebuild(): failed to record summary for shard " + s->getAddressIfSet()));
      }
    shardUpdated:;
      // We still need to check if the shard itself is coherent (we have an opportunity to
      // match its summary with the jobs total we just recomputed.
      if (size != s->getJobsSummary().bytes) {
        ArchiveQueueShard aqs(s->getAddressIfSet(), m_objectStore);
        m_exclusiveLock->includeSubObject(aqs);
        aqs.fetch();
        aqs.rebuild();
        aqs.commit();
      }
    }
  nextShard:;
    s++;
    sf++;
  }
  m_payload.set_archivejobscount(totalJobs);
  m_payload.set_archivejobstotalsize(totalBytes);
  m_payload.set_oldestjobcreationtime(oldestJobCreationTime);
  m_payload.set_youngestjobcreationtime(youngestJobCreationTime);
  m_payload.set_mapsrebuildcount(m_payload.mapsrebuildcount()+1);
  // We went through all the shard, re-updated the summaries, removed references to
  // gone shards. Done.
}

void ArchiveQueue::recomputeOldestAndYoungestJobCreationTime(){
  checkPayloadWritable();

  std::list<ArchiveQueueShard> shards;
  std::list<std::unique_ptr<ArchiveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;

  for (auto & sa: m_payload.archivequeueshards()) {
    shards.emplace_back(ArchiveQueueShard(sa.address(), m_objectStore));
    shardsFetchers.emplace_back(shards.back().asyncLockfreeFetch());
  }

  auto s = shards.begin();
  auto sf = shardsFetchers.begin();
  time_t oldestJobCreationTime=std::numeric_limits<time_t>::max();
  time_t youngestJobCreationTime=std::numeric_limits<time_t>::min();

  while (s != shards.end()) {
    // Each shard could be gone or be empty
    bool shardObjectNotFound = false;
    try {
      (*sf)->wait();
    } catch (cta::exception::NoSuchObject & ex) {
      shardObjectNotFound = true;
    }
    if (shardObjectNotFound || s->dumpJobs().empty()) {
      // Remove the shard from the list
      auto aqs = m_payload.mutable_archivequeueshards()->begin();
      while (aqs != m_payload.mutable_archivequeueshards()->end()) {
        if (aqs->address() == s->getAddressIfSet()) {
          aqs = m_payload.mutable_archivequeueshards()->erase(aqs);
        } else {
          aqs++;
        }
      }
      goto nextShard;
    }
    {
      // The shard is still around, let's compute its oldest job
      for (auto & j: s->dumpJobs()) {
        if (j.startTime < oldestJobCreationTime) oldestJobCreationTime = j.startTime;
        if (j.startTime > youngestJobCreationTime) youngestJobCreationTime = j.startTime;
      }
    }
    nextShard:;
    s++;
    sf++;
  }
  if(oldestJobCreationTime != std::numeric_limits<time_t>::max()){
    m_payload.set_oldestjobcreationtime(oldestJobCreationTime);
  }
  if (youngestJobCreationTime != std::numeric_limits<time_t>::min()){
    m_payload.set_youngestjobcreationtime(youngestJobCreationTime);
  }
}


bool ArchiveQueue::isEmpty() {
  checkPayloadReadable();
  // Check we have no archive jobs pending
  if (m_payload.archivequeueshards_size())
    return false;
  // If we made it to here, it seems the pool is indeed empty.
  return true;
}

void ArchiveQueue::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkPayloadWritable();
  // If the agent is not anymore the owner of the object, then only the very
  // last operation of the tape pool creation failed. We have nothing to do.
  if (presumedOwner != m_header.owner())
    return;
  // If the owner is still the agent, there are 2 possibilities
  // 1) The tape pool is referenced in the root entry, and then nothing is needed
  // besides setting the tape pool's owner to the root entry's address in
  // order to enable its usage. Before that, it was considered as a dangling
  // pointer.
  {
    RootEntry re(m_objectStore);
    ScopedSharedLock rel (re);
    re.fetch();
    auto tpd=re.dumpArchiveQueues(common::dataStructures::JobQueueType::JobsToTransferForUser);
    for (auto tp=tpd.begin(); tp!=tpd.end(); tp++) {
      if (tp->address == getAddressIfSet()) {
        setOwner(re.getAddressIfSet());
        commit();
        return;
      }
    }
  }
  // 2) The tape pool is not referenced by the root entry. It is then effectively
  // not accessible and should be discarded.
  if (!isEmpty()) {
    throw (NotEmpty("Trying to garbage collect a non-empty ArchiveQueue: internal error"));
  }
  remove();
  log::ScopedParamContainer params(lc);
  params.add("archiveQueueObject", getAddressIfSet());
  lc.log(log::INFO, "In ArchiveQueue::garbageCollect(): Garbage collected and removed archive queue object.");
}

void ArchiveQueue::setTapePool(const std::string& name) {
  checkPayloadWritable();
  m_payload.set_tapepool(name);
}

std::string ArchiveQueue::getTapePool() {
  checkPayloadReadable();
  return m_payload.tapepool();
}

void ArchiveQueue::setContainerId(const std::string& name) {
  checkPayloadWritable();
  m_payload.set_tapepool(name);
}

std::string ArchiveQueue::getContainerId(){
 checkPayloadReadable();
 return m_payload.tapepool();
}

void ArchiveQueue::addJobsAndCommit(std::list<JobToAdd> & jobsToAdd, AgentReference & agentReference, log::LogContext & lc) {
  checkPayloadWritable();
  // Before adding the jobs, we have to decide how to lay them out in the shards.
  // We are here in FIFO mode, so the algorithm is just 1) complete the current last
  // shard, if it did not reach the maximum size
  // 2) create new shard(s) as needed.
  //
  //  First implementation is shard by shard. A batter, parallel one could be implemented,
  // but the performance gain should be marginal as most of the time we will be dealing
  // with a single shard.

  auto nextJob = jobsToAdd.begin();
  while (nextJob != jobsToAdd.end()) {
    // If we're here, the is at least a job to add.
    // Let's find a shard for it/them. It can be either the last (incomplete) shard or
    // a new shard to create. In all case, we will max out the shard, jobs list permitting.
    // If we do fill up the shard, we'll go through another round here.
    // Is there a last shard, and is it not full?
    ArchiveQueueShard aqs(m_objectStore);
    serializers::ArchiveQueueShardPointer * aqsp = nullptr;
    bool newShard=false;
    uint64_t shardCount = m_payload.archivequeueshards_size();
    if (shardCount && m_payload.archivequeueshards(shardCount - 1).shardjobscount() < c_maxShardSize) {
      auto & shardPointer=m_payload.archivequeueshards(shardCount - 1);
      aqs.setAddress(shardPointer.address());
      // include-locking does not check existence of the object in the object store.
      // we will find out on fetch. If we fail, we have to rebuild.
      m_exclusiveLock->includeSubObject(aqs);
      try {
        aqs.fetch();
      } catch (cta::exception::NoSuchObject & ex) {
        log::ScopedParamContainer params (lc);
        params.add("archiveQueueObject", getAddressIfSet())
              .add("shardNumber", shardCount - 1)
              .add("shardObject", shardPointer.address());
        lc.log(log::ERR, "In ArchiveQueue::addJobsAndCommit(): shard not present. Rebuilding queue.");
        rebuild();
        commit();
        continue;
      }
      // Validate that the shard is as expected from the pointer. If not we need to
      // rebuild the queue and restart the shard selection.
      auto shardSummary = aqs.getJobsSummary();
      if (shardPointer.shardbytescount() != shardSummary.bytes ||
          shardPointer.shardjobscount() != shardSummary.jobs) {
        log::ScopedParamContainer params(lc);
        params.add("archiveQueueObject", getAddressIfSet())
              .add("shardNumber", shardCount - 1)
              .add("shardObject", shardPointer.address())
              .add("shardReportedBytes", shardSummary.bytes)
              .add("shardReportedJobs", shardSummary.jobs)
              .add("expectedBytes", shardPointer.shardbytescount())
              .add("expectedJobs", shardPointer.shardjobscount());
        lc.log(log::ERR, "In ArchiveQueue::addJobsAndCommit(): mismatch found. Rebuilding the queue.");
        rebuild();
        commit();
        continue;
      }
      // The shard looks good. We will now proceed with the addition of individual jobs.
      aqsp = m_payload.mutable_archivequeueshards(shardCount - 1);
    } else {
      // We need a new shard. Just add it (in memory).
      newShard = true;
      aqsp = m_payload.add_archivequeueshards();
      // Create the shard in memory.
      std::stringstream shardName;
      shardName << "ArchiveQueueShard-" << m_payload.tapepool();
      aqs.setAddress(agentReference.nextId(shardName.str()));
      aqs.initialize(getAddressIfSet());
      // Reference the shard in the pointer, and initialized counters.
      aqsp->set_address(aqs.getAddressIfSet());
      aqsp->set_shardbytescount(0);
      aqsp->set_shardjobscount(0);
    }
    // We can now add the individual jobs, commit the main queue and then insert or commit the shard.
    {
      // As the queue could be rebuilt on each shard round, we get access to the
      // value maps here
      ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
      ValueCountMapUint64 minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
      ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
      while (nextJob != jobsToAdd.end() && aqsp->shardjobscount() < c_maxShardSize) {
        // Update stats and global counters.
        priorityMap.incCount(nextJob->policy.archivePriority);
        minArchiveRequestAgeMap.incCount(nextJob->policy.archiveMinRequestAge);
        mountPolicyNameMap.incCount(nextJob->policy.name);
        if (m_payload.archivejobscount()) {
          if ((uint64_t)nextJob->startTime < m_payload.oldestjobcreationtime())
            m_payload.set_oldestjobcreationtime(nextJob->startTime);
          if ((uint64_t)nextJob->startTime > m_payload.youngestjobcreationtime())
            m_payload.set_youngestjobcreationtime(nextJob->startTime);
          m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() + nextJob->fileSize);
        } else {
          m_payload.set_archivejobstotalsize(nextJob->fileSize);
          m_payload.set_oldestjobcreationtime(nextJob->startTime);
          m_payload.set_youngestjobcreationtime(nextJob->startTime);
        }
        m_payload.set_archivejobscount(m_payload.archivejobscount()+1);
        // Add the job to shard, update pointer counts and queue summary.
        aqsp->set_shardjobscount(aqs.addJob(*nextJob));
        aqsp->set_shardbytescount(aqsp->shardbytescount() + nextJob->fileSize);
        // And move to the next job
        nextJob++;
      }
    }
    // We will now commit this shard (and the queue) before moving to the next.
    // Commit in the right order:
    // 1) Get the shard on storage. Could be either insert or commit.
    if (newShard) {
      aqs.insert();
    } else {
      aqs.commit();
    }
    // 2) commit the queue so the shard is referenced.
    commit();
  } // end of loop over all objects.
}

auto ArchiveQueue::getJobsSummary() -> JobsSummary {
  checkPayloadReadable();
  JobsSummary ret;
  ret.jobs = m_payload.archivejobscount();
  ret.bytes = m_payload.archivejobstotalsize();
  ret.oldestJobStartTime = m_payload.oldestjobcreationtime();
  ret.youngestJobStartTime = m_payload.youngestjobcreationtime();
  if (ret.jobs) {
    ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
    ret.priority = priorityMap.maxValue();
    ValueCountMapUint64 minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
    ret.minArchiveRequestAge = minArchiveRequestAgeMap.minValue();
    ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
    ret.mountPolicyCountMap = mountPolicyNameMap.getMap();
  } else {
    ret.priority = 0;
    ret.minArchiveRequestAge = 0;
  }
  return ret;
}

ArchiveQueue::AdditionSummary ArchiveQueue::addJobsIfNecessaryAndCommit(std::list<JobToAdd>& jobsToAdd,
    AgentReference & agentReference, log::LogContext & lc) {
  checkPayloadWritable();
  // First get all the shards of the queue to understand which jobs to add.
  std::list<ArchiveQueueShard> shards;
  std::list<std::unique_ptr<ArchiveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;

  for (auto & sa: m_payload.archivequeueshards()) {
    shards.emplace_back(ArchiveQueueShard(sa.address(), m_objectStore));
    shardsFetchers.emplace_back(shards.back().asyncLockfreeFetch());
  }
  std::list<std::list<JobDump>> shardsDumps;
  auto s = shards.begin();
  auto sf = shardsFetchers.begin();

  while (s!= shards.end()) {
    try {
      (*sf)->wait();
    } catch (cta::exception::NoSuchObject & ex) {
      goto nextShard;
    }
    shardsDumps.emplace_back(std::list<JobDump>());
    for (auto & j: s->dumpJobs()) {
      shardsDumps.back().emplace_back(JobDump({j.size, j.address, j.copyNb}));
    }
  nextShard:
    s++;
    sf++;
  }

  // Now filter the jobs to add
  AdditionSummary ret;
  std::list<JobToAdd> jobsToReallyAdd;
  for (auto & jta: jobsToAdd) {
    for (auto & sd: shardsDumps) {
      for (auto & sjd: sd) {
        if (sjd.address == jta.archiveRequestAddress)
          goto found;
      }
    }
    jobsToReallyAdd.emplace_back(jta);
    ret.bytes += jta.fileSize;
    ret.files++;
  found:;
  }

  // We can now proceed with the standard addition.
  addJobsAndCommit(jobsToReallyAdd, agentReference, lc);
  return ret;
}

void ArchiveQueue::removeJobsAndCommit(const std::list<std::string>& jobsToRemove, log::LogContext & lc) {
  checkPayloadWritable();
  ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
  ValueCountMapUint64 minArchiveRequestAgeMap(m_payload.mutable_minarchiverequestagemap());
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  // Make a working copy of the jobs to remove. We will progressively trim this local list.
  auto localJobsToRemove = jobsToRemove;
  // The jobs are expected to be removed from the front shards first.
  // Remove jobs until there are no more jobs or no more shards.
  ssize_t shardIndex=0;
  auto * mutableArchiveQueueShards= m_payload.mutable_archivequeueshards();
  while (localJobsToRemove.size() && shardIndex <  mutableArchiveQueueShards->size()) {
    auto * shardPointer = mutableArchiveQueueShards->Mutable(shardIndex);
    // Get hold of the shard
    ArchiveQueueShard aqs(shardPointer->address(), m_objectStore);
    m_exclusiveLock->includeSubObject(aqs);
    try {
      aqs.fetch();
    } catch(const cta::exception::NoSuchObject &) {
      // If the shard's gone, so should the pointer be gone... Push it to the end of the queue and trim it.
      log::ScopedParamContainer params(lc);
      params.add("archiveQueueObject", getAddressIfSet());
      params.add("archiveQueueShardObject", shardPointer->address());
      lc.log(log::ERR, "In ArchiveQueue::removeJobsAndCommit(): Shard object is missing. Rebuilding queue.");
      rebuild();
      commit();
      continue;
    }
    // Remove jobs from shard
    auto removalResult = aqs.removeJobs(localJobsToRemove);
    // Commit shard changes. If it has been drained, it will be deleted afterwards.
    aqs.commit();

    // We still need to update the tracking queue side.
    // Update stats and remove the jobs from the todo list.
    for (auto & j: removalResult.removedJobs) {
      priorityMap.decCount(j.priority);
      minArchiveRequestAgeMap.decCount(j.minArchiveRequestAge);
      mountPolicyNameMap.decCount(j.mountPolicyName);
    }
    // In all cases, we should update the global statistics.
    m_payload.set_archivejobscount(m_payload.archivejobscount() - removalResult.jobsRemoved);
    m_payload.set_archivejobstotalsize(m_payload.archivejobstotalsize() - removalResult.bytesRemoved);
    // If the shard is still around, we shall update its pointer's stats too.
    if (removalResult.jobsAfter) {
      // Also update the shard pointers's stats. In case of mismatch, we will trigger a rebuild.
      shardPointer->set_shardbytescount(shardPointer->shardbytescount() - removalResult.bytesRemoved);
      shardPointer->set_shardjobscount(shardPointer->shardjobscount() - removalResult.jobsRemoved);
      if (shardPointer->shardbytescount() != removalResult.bytesAfter
          || shardPointer->shardjobscount() != removalResult.jobsAfter) {
        rebuild();
      }
      // We will commit when exiting anyway...
      shardIndex++;
    } else {
      // Shard's gone, so should the pointer. Push it to the end of the queue and
      // trim it.
      for (auto i=shardIndex; i<mutableArchiveQueueShards->size()-1; i++) {
        mutableArchiveQueueShards->SwapElements(i, i+1);
      }
      mutableArchiveQueueShards->RemoveLast();
    }
    // We should also trim the removed jobs from our list.
    localJobsToRemove.remove_if(
      [&removalResult](const std::string & ja){
        return std::count_if(removalResult.removedJobs.begin(), removalResult.removedJobs.end(),
          [&ja](ArchiveQueueShard::JobInfo & j) {
            return j.address == ja;
          }
        );
      }
    ); // end of remove_if
    // And commit the queue (once per shard should not hurt performance).
    recomputeOldestAndYoungestJobCreationTime();
    commit();
    // Only remove the drained shard after the queue updates have been committed,
    // in order to avoid a dangling shard address
    if (!removalResult.jobsAfter) {
      aqs.remove();
    }
  }
}

auto ArchiveQueue::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  // Go read the shards in parallel...
  std::list<JobDump> ret;
  std::list<ArchiveQueueShard> shards;
  std::list<std::unique_ptr<ArchiveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;
  for (auto & sa: m_payload.archivequeueshards()) {
    shards.emplace_back(ArchiveQueueShard(sa.address(), m_objectStore));
    shardsFetchers.emplace_back(shards.back().asyncLockfreeFetch());
  }
  auto s = shards.begin();
  auto sf = shardsFetchers.begin();
  while (s != shards.end()) {
    try {
      (*sf)->wait();
    } catch (cta::exception::NoSuchObject & ex) {
      // We are possibly in read only mode, so we cannot rebuild.
      // Just skip this shard.
      goto nextShard;
    }
    for (auto & j: s->dumpJobs()) {
      ret.emplace_back(JobDump{j.size, j.address, j.copyNb});
    }
  nextShard:
    s++; sf++;
  }
  return ret;
}

auto ArchiveQueue::getCandidateList(uint64_t maxBytes, uint64_t maxFiles, std::set<std::string> archiveRequestsToSkip, log::LogContext & lc) -> CandidateJobList {
  checkPayloadReadable();
  CandidateJobList ret;
  for (auto & aqsp: m_payload.archivequeueshards()) {
    // We need to go through all shard pointers unconditionally to count what is left (see else part)
    if (ret.candidateBytes < maxBytes && ret.candidateFiles < maxFiles) {
      // Fetch the shard
      ArchiveQueueShard aqs(aqsp.address(), m_objectStore);
      try {
        aqs.fetchNoLock();
      } catch(const cta::exception::NoSuchObject &) {
        // If the shard's gone we are not getting any pointers from it...
        log::ScopedParamContainer params(lc);
        params.add("archiveQueueObject", getAddressIfSet());
        params.add("archiveQueueShardObject", aqsp.address());
        lc.log(log::ERR, "In ArchiveQueue::getCandidateList(): Shard object is missing. Ignoring shard.");
        continue;
      }
      auto shardCandidates = aqs.getCandidateJobList(maxBytes - ret.candidateBytes, maxFiles - ret.candidateFiles, archiveRequestsToSkip);
      ret.candidateBytes += shardCandidates.candidateBytes;
      ret.candidateFiles += shardCandidates.candidateFiles;
      // We overwrite the remaining values each time as the previous
      // shards have exhaustied their candidate lists.
      ret.remainingBytesAfterCandidates = shardCandidates.remainingBytesAfterCandidates;
      ret.remainingFilesAfterCandidates = shardCandidates.remainingFilesAfterCandidates;
      ret.candidates.splice(ret.candidates.end(), shardCandidates.candidates);
    } else {
      // We are done with finding candidates. We just need to count what is left in the non-visited shards.
      ret.remainingBytesAfterCandidates += aqsp.shardbytescount();
      ret.remainingFilesAfterCandidates += aqsp.shardjobscount();
    }
  }
  return ret;
}

auto ArchiveQueue::getCandidateSummary() -> CandidateJobList {
  checkPayloadReadable();
  CandidateJobList ret;
  for(auto & aqsp: m_payload.archivequeueshards()) {
    ret.candidateBytes += aqsp.shardbytescount();
    ret.candidateFiles += aqsp.shardjobscount();
  }
  return ret;
}

} // namespace cta::objectstore
