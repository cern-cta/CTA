/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <google/protobuf/util/json_util.h>

#include "AgentReference.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "EntryLogSerDeser.hpp"
#include "GenericObject.hpp"
#include "RetrieveActivityCountMap.hpp"
#include "RetrieveQueue.hpp"
#include "RetrieveQueueShard.hpp"
#include "ValueCountMap.hpp"

namespace cta { namespace objectstore {

RetrieveQueue::RetrieveQueue(const std::string& address, Backend& os):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(os, address) { }

RetrieveQueue::RetrieveQueue(GenericObject& go):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(go.objectStore()){
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

RetrieveQueue::RetrieveQueue(Backend& os):
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>(os) { }

void RetrieveQueue::initialize(const std::string &vid) {
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>::initialize();
  // Set the reguired fields
  m_payload.set_oldestjobcreationtime(0);
  m_payload.set_youngestjobcreationtime(0);
  m_payload.set_retrievejobstotalsize(0);
  m_payload.set_retrievejobscount(0);
  m_payload.set_vid(vid);
  m_payload.set_mapsrebuildcount(0);
  m_payload.set_maxshardsize(m_maxShardSize);
  m_payloadInterpreted = true;
}

bool RetrieveQueue::checkMapsAndShardsCoherency() {
  checkPayloadReadable();
  uint64_t bytesFromShardPointers = 0;
  uint64_t jobsExpectedFromShardsPointers = 0;
  // Add up shard summaries
  for (auto & aqs: m_payload.retrievequeueshards()) {
    bytesFromShardPointers += aqs.shardbytescount();
    jobsExpectedFromShardsPointers += aqs.shardjobscount();
  }
  uint64_t totalBytes = m_payload.retrievejobstotalsize();
  uint64_t totalJobs = m_payload.retrievejobscount();
  // The sum of shards should be equal to the summary
  if (totalBytes != bytesFromShardPointers ||
      totalJobs != jobsExpectedFromShardsPointers)
    return false;
  // Check that we have coherent queue summaries
  ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
  ValueCountMapUint64 minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  if (priorityMap.total() != m_payload.retrievejobscount() ||
      minRetrieveRequestAgeMap.total() != m_payload.retrievejobscount() ||
      mountPolicyNameMap.total() != m_payload.retrievejobscount()
    )
    return false;
  return true;
}

void RetrieveQueue::rebuild() {
  checkPayloadWritable();
  // Something is off with the queue. We will hence rebuild it. The rebuild of the
  // queue will consist in:
  // 1) Attempting to read all shards in parallel. Absent shards are possible, and will
  // mean we have dangling pointers.
  // 2) Rebuild the summaries from the shards.
  // As a side note, we do not go as far as validating the pointers to jobs within the
  // shards, as this is already handled as access goes.
  std::list<RetrieveQueueShard> shards;
  std::list<std::unique_ptr<RetrieveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;

  // Get the summaries structures ready
  ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
  priorityMap.clear();
  ValueCountMapUint64 minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
  minRetrieveRequestAgeMap.clear();
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  mountPolicyNameMap.clear();
  for (auto & sa: m_payload.retrievequeueshards()) {
    shards.emplace_back(RetrieveQueueShard(sa.address(), m_objectStore));
    shardsFetchers.emplace_back(shards.back().asyncLockfreeFetch());
  }
  auto s = shards.begin();
  auto sf = shardsFetchers.begin();
  uint64_t totalJobs=0;
  uint64_t totalBytes=0;
  time_t oldestJobCreationTime=std::numeric_limits<time_t>::max();
  time_t youngestJobCreationTime=std::numeric_limits<time_t>::min();
  
  while (s != shards.end()) {
    // Each shard could be gone
    try {
      (*sf)->wait();
    } catch (cta::exception::NoSuchObject & ex) {
      // Remove the shard from the list
      auto aqs = m_payload.mutable_retrievequeueshards()->begin();
      while (aqs != m_payload.mutable_retrievequeueshards()->end()) {
        if (aqs->address() == s->getAddressIfSet()) {
          aqs = m_payload.mutable_retrievequeueshards()->erase(aqs);
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
      uint64_t minFseq = std::numeric_limits<uint64_t>::max();
      uint64_t maxFseq = std::numeric_limits<uint64_t>::min();
      for (auto & j: s->dumpJobs()) {
        jobs++;
        size += j.size;
        priorityMap.incCount(j.priority);
        minRetrieveRequestAgeMap.incCount(j.minRetrieveRequestAge);
        mountPolicyNameMap.incCount(j.mountPolicyName);
        if (j.startTime < oldestJobCreationTime) oldestJobCreationTime = j.startTime;
        if (j.startTime > youngestJobCreationTime) youngestJobCreationTime = j.startTime;
        if (j.fSeq < minFseq) minFseq = j.fSeq;
        if (j.fSeq > maxFseq) maxFseq = j.fSeq;
      }
      // Add the summary to total.
      totalJobs+=jobs;
      totalBytes+=size;
      // And store the value in the shard pointers.
      auto mrqs = m_payload.mutable_retrievequeueshards();
      for (auto & rqsp: *mrqs) {
        if (rqsp.address() == s->getAddressIfSet()) {
          rqsp.set_shardjobscount(jobs);
          rqsp.set_shardbytescount(size);
          rqsp.set_maxfseq(maxFseq);
          rqsp.set_minfseq(minFseq);
          goto shardUpdated;
        }
      }
      {
        // We had to update a shard and did not find it. This is an error.
        throw exception::Exception(std::string ("In RetrieveQueue::rebuild(): failed to record summary for shard " + s->getAddressIfSet()));
      }
    shardUpdated:;
      // We still need to check if the shard itself is coherent (we have an opportunity to
      // match its summary with the jobs total we just recomputed.
      if (size != s->getJobsSummary().bytes) {
        RetrieveQueueShard rqs(s->getAddressIfSet(), m_objectStore);
        m_exclusiveLock->includeSubObject(rqs);
        rqs.fetch();
        rqs.rebuild();
        rqs.commit();
      }
    }
  nextShard:;
    s++;
    sf++;
  }
  m_payload.set_retrievejobscount(totalJobs);
  m_payload.set_retrievejobstotalsize(totalBytes);
  m_payload.set_oldestjobcreationtime(oldestJobCreationTime);
  m_payload.set_youngestjobcreationtime(youngestJobCreationTime);
  // We went through all the shard, re-updated the summaries, removed references to
  // gone shards. Done.}
}


void RetrieveQueue::commit() {
  if (!checkMapsAndShardsCoherency()) {
    rebuild();
    m_payload.set_mapsrebuildcount(m_payload.mapsrebuildcount()+1);
  }
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>::commit();
}

void RetrieveQueue::getPayloadFromHeader() {
  ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>::getPayloadFromHeader();
  m_maxShardSize = m_payload.maxshardsize();
}

bool RetrieveQueue::isEmpty() {
  checkPayloadReadable();
  return !m_payload.retrievejobstotalsize() && !m_payload.retrievequeueshards_size();
}

void RetrieveQueue::removeIfEmpty(log::LogContext & lc) {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In RetrieveQueue::removeIfEmpty: trying to remove an tape with retrieves queued");
  }
  remove();
  log::ScopedParamContainer params(lc);
  params.add("retrieveQueueObject", getAddressIfSet());
  lc.log(log::INFO, "In RetrieveQueue::removeIfEmpty(): removed the queue.");
}

std::string RetrieveQueue::getVid() {
  checkPayloadReadable();
  return m_payload.vid();
}

void RetrieveQueue::resetSleepForFreeSpaceStartTime() {
  checkPayloadWritable();
  m_payload.clear_sleep_for_free_space_since();
  m_payload.clear_disk_system_slept_for();
}

void RetrieveQueue::setSleepForFreeSpaceStartTimeAndName(time_t time, const std::string & diskSystemName, uint64_t sleepTime) {
  checkPayloadWritable();
  m_payload.set_sleep_for_free_space_since((uint64_t)time);
  m_payload.set_disk_system_slept_for(diskSystemName);
  m_payload.set_sleep_time(sleepTime);
}

std::string RetrieveQueue::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

void RetrieveQueue::updateShardLimits(uint64_t fSeq, ShardForAddition & sfa) {
  if (fSeq < sfa.minFseq) sfa.minFseq=fSeq;
  if (fSeq > sfa.maxFseq) sfa.maxFseq=fSeq;
}

/** Add a jobs to a shard, spliting it if necessary*/
void RetrieveQueue::addJobToShardAndMaybeSplit(RetrieveQueue::JobToAdd & jobToAdd,
    std::list<ShardForAddition>::iterator & shardForAddition, std::list<ShardForAddition> & shardList) {
  // Is the shard still small enough? We will not double split shards (we suppose insertion size << shard size cap).
  // We will also no split a new shard.
  if (   shardForAddition->jobsCount < m_maxShardSize
      || shardForAddition->fromSplit || shardForAddition->newShard) {
    // We just piggy back here. No need to increase range, we are within it.
    shardForAddition->jobsCount++;
    shardForAddition->jobsToAdd.emplace_back(jobToAdd);
    updateShardLimits(jobToAdd.fSeq, *shardForAddition);
  } else {
    // The shard is full. We need to split it (and can). We will cut the shard range in
    // 2 equal parts, not forgetting to redistribute the existing jobs to add accordinglyGarbageCollectorRetrieveRequest
    // Create the new shard
    auto newSfa = shardList.insert(shardForAddition, ShardForAddition());
    // The new shard size can only be estimated, but we will update it to the actual value as we update the shard.
    // The new shard is inserted before the old one, so the old one will keep the high
    // half and new shard gets the bottom half.
    uint64_t shardRange = shardForAddition->maxFseq - shardForAddition->minFseq;
    newSfa->minFseq = shardForAddition->minFseq;
    newSfa->maxFseq = shardForAddition->minFseq + shardRange/2;
    newSfa->jobsCount = shardForAddition->jobsCount/2;
    newSfa->splitSource = &*shardForAddition;
    newSfa->fromSplit = true;
    newSfa->newShard = true;
    shardForAddition->minFseq = shardForAddition->minFseq + shardRange/2 + 1;
    shardForAddition->jobsCount = shardForAddition->jobsCount/2;
    shardForAddition->toSplit = true;
    shardForAddition->splitDestination = &*newSfa;
    // Transfer jobs to add to new shard if needed
    for (auto jta2=shardForAddition->jobsToAdd.begin(); jta2!=shardForAddition->jobsToAdd.end();) {
      if (jta2->fSeq <= newSfa->maxFseq) {
        newSfa->jobsToAdd.emplace_back(*jta2);
        jta2 = shardForAddition->jobsToAdd.erase(jta2);
      } else {
        jta2++;
      }
    }
    // We can finally add our job to one of the two shards from the split
    if (jobToAdd.fSeq >= shardForAddition->minFseq) {
      shardForAddition->jobsToAdd.emplace_back(jobToAdd);
      shardForAddition->jobsCount++;
      updateShardLimits(jobToAdd.fSeq, *shardForAddition);
    } else {
      newSfa->jobsToAdd.emplace_back(jobToAdd);
      newSfa->jobsCount++;
      updateShardLimits(jobToAdd.fSeq, *newSfa);
    }
  }
}

void RetrieveQueue::addJobsAndCommit(std::list<JobToAdd> & jobsToAdd, AgentReference & agentReference, log::LogContext & lc) {
  checkPayloadWritable();
  if (jobsToAdd.empty()) return;
  // Keep track of the mounting criteria
  ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
  ValueCountMapUint64 minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  RetrieveActivityCountMap retrieveActivityCountMap(m_payload.mutable_activity_map());
  // We need to figure out which job will be added to which shard.
  // We might have to split shards if they would become too big.
  // For a given jobs, there a 4 possible cases:
  // - Before first shard
  // - Within a shard
  // - Between 2 shards
  // - After last shard
  // We can classify the previous into 2 use cases
  // - Within a shard
  // - Outside of a shard
  // In the case we land within a shard, we either have to use the existing one, or
  // to split it and choose the half we are going to use.
  // In case we land outside of a shard, we have 1 or 2 adjacent shards. If any of the
  // one or 2 is not full, we will add the job to it.
  // otherwise, a new shard will be added to contain the job.
  // We need to pre-plan the insertion with all the incoming jobs before doing the real action.
  // 1) Initialize the shard list from the shard pointers. The shards are supposed to be
  // sorted by fseq and to contain non-overlapping segments of fSeq ranges. We will tweak the
  // extracted values from the object store to achieve this condition.
  std::list<ShardForAddition> shardsForAddition;
  for (auto & rqsp: m_payload.retrievequeueshards()) {
    shardsForAddition.emplace_back(ShardForAddition());
    auto & sfa = shardsForAddition.back();
    sfa.minFseq = rqsp.minfseq();
    sfa.maxFseq = rqsp.maxfseq();
    sfa.jobsCount = rqsp.shardjobscount();
    sfa.address = rqsp.address();
  }
  // After extracting the pointers, we ensure the fSeqs are in order. We go from first to last,
  // and whichever fSeq is used will push forward the limits of the following shards.
  uint64_t highestFseqSoFar=0;
  for (auto & sfa: shardsForAddition) {
    sfa.minFseq = std::max(highestFseqSoFar, sfa.minFseq);
    highestFseqSoFar = sfa.minFseq;
    sfa.maxFseq = std::max(highestFseqSoFar, sfa.maxFseq);
    highestFseqSoFar = sfa.maxFseq+1;
  }
  // We now try to fit the jobs to the right shards
  for (auto & jta: jobsToAdd) {
    // If there is no shard, let's create the initial one.
    if (shardsForAddition.empty()) {
      shardsForAddition.emplace_back(ShardForAddition());
      auto & sfa=shardsForAddition.back();
      sfa.newShard=true;
      sfa.maxFseq = sfa.minFseq = jta.fSeq;
      sfa.jobsCount=1;
      sfa.jobsToAdd.emplace_back(jta);
      goto jobInserted;
    }
    // Find where the job lands in the shards
    for (auto sfa=shardsForAddition.begin(); sfa != shardsForAddition.end(); sfa++) {
      if (sfa->minFseq > jta.fSeq) {
        // Are we before the current shard? (for example, before first shard)
        addJobToShardAndMaybeSplit(jta, sfa, shardsForAddition);
        goto jobInserted;
      } else if (jta.fSeq >= sfa->minFseq && jta.fSeq <= sfa->maxFseq) {
        // Is it within this shard?
        addJobToShardAndMaybeSplit(jta, sfa, shardsForAddition);
        goto jobInserted;
      } else if (sfa != shardsForAddition.end() && std::next(sfa) != shardsForAddition.end()) {
        // Are we between shards?
        auto nextSfa=std::next(sfa);
        if (jta.fSeq > sfa->maxFseq && jta.fSeq < nextSfa->minFseq) {
          if (sfa->jobsCount < nextSfa->jobsCount) {
            addJobToShardAndMaybeSplit(jta, sfa, shardsForAddition);
          } else {
            addJobToShardAndMaybeSplit(jta, nextSfa, shardsForAddition);
          }
          goto jobInserted;
        }
      } else if (std::next(sfa) == shardsForAddition.end() && sfa->maxFseq < jta.fSeq) {
        // Are we after the last shard?
        addJobToShardAndMaybeSplit(jta, sfa, shardsForAddition);
        goto jobInserted;
      }
    }
    // Still not inserted? Now we run out of options. Segfault to ease debugging.
    {
      throw cta::exception::Exception("In RetrieveQueue::addJobsAndCommit(): could not find an appropriate shard for job");
    }
    jobInserted:;
  }

  {
    // Number the shards.
    size_t shardIndex=0;
    for (auto & shard: shardsForAddition) shard.shardIndex=shardIndex++;
  }

  // Jobs are now planned for insertions in their respective (and potentially
  // new) shards.
  // We will iterate shard by shard.
  // TODO: shard creation and update could be parallelized (to some extent as we
  // have shard to shard dependencies with the splits), but as a first implementation
  // we just go iteratively.
  for (auto & shard: shardsForAddition) {
    uint64_t addedJobs = 0, addedBytes = 0, transferedInSplitJobs = 0, transferedInSplitBytes = 0;
    // Variables which will allow the shard/pointer updates in all cases.
    cta::objectstore::serializers::RetrieveQueueShardPointer * shardPointer = nullptr, * splitFromShardPointer = nullptr;
    RetrieveQueueShard rqs(m_objectStore), rqsSplitFrom(m_objectStore);
    if (shard.newShard) {
      // Irrespective of the case, we need to create the shard.
      std::stringstream shardName;
      shardName << "RetrieveQueueShard-" << m_payload.vid();
      rqs.setAddress(agentReference.nextId(shardName.str()));
      rqs.initialize(getAddressIfSet());
      // We also need to create the pointer, and insert it to the right spot.
      shardPointer = m_payload.mutable_retrievequeueshards()->Add();
      // Pre-update the shard pointer.
      shardPointer->set_address(rqs.getAddressIfSet());
      shardPointer->set_maxfseq(0);
      shardPointer->set_minfseq(0);
      shardPointer->set_shardbytescount(0);
      shardPointer->set_shardjobscount(0);
      shard.creationDone = true;
      shard.address = rqs.getAddressIfSet();
      // Move the shard pointer to its intended location.
      size_t currentShardPosition=m_payload.retrievequeueshards_size() - 1;
      while (currentShardPosition != shard.shardIndex) {
        m_payload.mutable_retrievequeueshards()->SwapElements(currentShardPosition-1,currentShardPosition);
        currentShardPosition--;
      }
      // Make sure the pointer is the right one after move.
      shardPointer = m_payload.mutable_retrievequeueshards(shard.shardIndex);
      // If necessary, fill up this new shard with jobs from the split one.
      if (shard.fromSplit) {
        rqsSplitFrom.setAddress(shard.splitSource->address);
        splitFromShardPointer=m_payload.mutable_retrievequeueshards(shard.splitSource->shardIndex);
        m_exclusiveLock->includeSubObject(rqsSplitFrom);
        rqsSplitFrom.fetch();
        auto jobsFromSource=rqsSplitFrom.dumpJobsToAdd();
        std::list<std::string> jobsToTransferAddresses;
        for (auto &j: jobsFromSource) {
          RetrieveQueueShard::JobsToAddSet jtas;
          if (j.fSeq >= shard.minFseq && j.fSeq <= shard.maxFseq) {
            jtas.insert(j);
            addedJobs++;
            addedBytes+=j.fileSize;
            jobsToTransferAddresses.emplace_back(j.retrieveRequestAddress);
          }
          rqs.addJobsBatch(jtas);
        }
        auto removalResult = rqsSplitFrom.removeJobs(jobsToTransferAddresses);
        transferedInSplitBytes += removalResult.bytesRemoved;
        transferedInSplitJobs += removalResult.jobsRemoved;
        // We update the shard pointer with fseqs to allow validations, but the actual
        //values will be updated as the shard itself is populated.
        shardPointer->set_maxfseq(shard.maxFseq);
        shardPointer->set_minfseq(shard.minFseq);
        shardPointer->set_shardjobscount(shard.jobsCount);
        shardPointer->set_shardbytescount(1);
        splitFromShardPointer->set_minfseq(shard.splitSource->minFseq);
        splitFromShardPointer->set_maxfseq(shard.splitSource->maxFseq);
        splitFromShardPointer->set_shardjobscount(shard.splitSource->jobsCount);
        shardPointer->set_shardbytescount(1);
        // We are all set (in memory) for the shard from which we split.
        shard.splitDone = true;
        shard.splitSource->splitDone = true;
      }
      // We can now fill up the shard (outside of this if/else).
    } else {
      rqs.setAddress(shard.address);
      m_exclusiveLock->includeSubObject(rqs);
      rqs.fetch();
      shardPointer=m_payload.mutable_retrievequeueshards(shard.shardIndex);
    }
    // ... add the jobs to the shard (in memory)
    RetrieveQueueShard::JobsToAddSet jtas;
    for (auto j:shard.jobsToAdd) {
      jtas.insert(j);
      addedJobs++;
      addedBytes+=j.fileSize;
      priorityMap.incCount(j.policy.retrievePriority);
      minRetrieveRequestAgeMap.incCount(j.policy.retrieveMinRequestAge);
      mountPolicyNameMap.incCount(j.policy.name);
      if (j.activityDescription) {
        retrieveActivityCountMap.incCount(j.activityDescription.value());
      }
      // oldestjobcreationtime is initialized to 0 when
      if (m_payload.oldestjobcreationtime()) {
        if ((uint64_t)j.startTime < m_payload.oldestjobcreationtime())
          m_payload.set_oldestjobcreationtime(j.startTime);
      } else {
        m_payload.set_oldestjobcreationtime(j.startTime);
      }
      // youngestjobcreationtime has a default value of 0 when it is not initialized
      if (m_payload.youngestjobcreationtime()) {
        if ((uint64_t)j.startTime > m_payload.youngestjobcreationtime())
          m_payload.set_youngestjobcreationtime(j.startTime);
      } else {
        m_payload.set_youngestjobcreationtime(j.startTime);
      }
    }
    rqs.addJobsBatch(jtas);
    // ... update the shard pointer
    auto shardSummary = rqs.getJobsSummary();
    shardPointer->set_maxfseq(shardSummary.maxFseq);
    shardPointer->set_minfseq(shardSummary.minFseq);
    shardPointer->set_shardbytescount(shardSummary.bytes);
    shardPointer->set_shardjobscount(shardSummary.jobs);
    // ... and finally commit the queue (first! there is potentially a new shard to
    // pre-reference before inserting) and shards as is appropriate.
    // Update global summaries
    m_payload.set_retrievejobscount(m_payload.retrievejobscount() + addedJobs - transferedInSplitJobs);
    m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize() + addedBytes - transferedInSplitBytes);
    // If we are creating a new shard, we have to do a blind commit: the
    // stats for shard we are splitting from could not be accounted for properly
    // and the new shard is not yet inserted yet.
    if (shard.fromSplit)
      ObjectOps<serializers::RetrieveQueue, serializers::RetrieveQueue_t>::commit();
    else {
      // in other cases, we should have a coherent state.
      commit();
    }
    shard.comitted = true;

    if (shard.newShard) {
      rqs.insert();
      if (shard.fromSplit)
        rqsSplitFrom.commit();
    }
    else rqs.commit();
  }
}

auto RetrieveQueue::addJobsIfNecessaryAndCommit(std::list<JobToAdd> & jobsToAdd,
    AgentReference & agentReference, log::LogContext & lc)
-> AdditionSummary {
  checkPayloadWritable();
  // First get all the shards of the queue to understand which jobs to add.
  std::list<RetrieveQueueShard> shards;
  std::list<std::unique_ptr<RetrieveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;

  for (auto & sp: m_payload.retrievequeueshards()) {
    shards.emplace_back(RetrieveQueueShard(sp.address(), m_objectStore));
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
      shardsDumps.back().emplace_back(JobDump({j.address, j.copyNb, j.size, j.activityDescription, j.diskSystemName}));
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
        if (sjd.address == jta.retrieveRequestAddress)
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


RetrieveQueue::JobsSummary RetrieveQueue::getJobsSummary() {
  checkPayloadReadable();
  JobsSummary ret;
  ret.bytes = m_payload.retrievejobstotalsize();
  ret.jobs = m_payload.retrievejobscount();
  ret.oldestJobStartTime = m_payload.oldestjobcreationtime();
  ret.youngestJobStartTime = m_payload.youngestjobcreationtime();
  if (ret.jobs) {
    ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
    ret.priority = priorityMap.maxValue();
    ValueCountMapUint64 minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
    ret.minRetrieveRequestAge = minRetrieveRequestAgeMap.minValue();
    ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
    ret.mountPolicyCountMap = mountPolicyNameMap.getMap();
    RetrieveActivityCountMap retrieveActivityCountMap(m_payload.mutable_activity_map());
    for (auto ra: retrieveActivityCountMap.getActivities(ret.priority)) {
      ret.activityCounts.push_back({ra.diskInstanceName, ra.activity, ra.weight, ra.count});
    }
    if (m_payload.has_sleep_for_free_space_since()) {
      JobsSummary::SleepInfo si;
      si.diskSystemSleptFor = m_payload.disk_system_slept_for();
      si.sleepStartTime = m_payload.sleep_for_free_space_since();
      si.sleepTime = m_payload.sleep_time();
      ret.sleepInfo = si;
    }
  } else {
    ret.priority = 0;
    ret.minRetrieveRequestAge = 0;
  }
  return ret;
}

auto RetrieveQueue::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  // Go read the shards in parallel...
  std::list<JobDump> ret;
  std::list<RetrieveQueueShard> shards;
  std::list<std::unique_ptr<RetrieveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;
  for (auto & sa: m_payload.retrievequeueshards()) {
    shards.emplace_back(RetrieveQueueShard(sa.address(), m_objectStore));
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
      ret.emplace_back(JobDump{j.address, j.copyNb, j.size, j.activityDescription, j.diskSystemName});
    }
  nextShard:
    s++; sf++;
  }
  return ret;
}

auto RetrieveQueue::getCandidateList(uint64_t maxBytes, uint64_t maxFiles, const std::set<std::string> & retrieveRequestsToSkip, const std::set<std::string> & diskSystemsToSkip) -> CandidateJobList {
  checkPayloadReadable();
  CandidateJobList ret;
  for(auto & rqsp: m_payload.retrievequeueshards()) {
    // We need to go through all shard pointers unconditionnaly to count what is left (see else part)
    if (ret.candidateBytes < maxBytes && ret.candidateFiles < maxFiles) {
      // Fetch the shard
      RetrieveQueueShard rqs(rqsp.address(), m_objectStore);
      rqs.fetchNoLock();
      auto shardCandidates = rqs.getCandidateJobList(maxBytes - ret.candidateBytes, maxFiles - ret.candidateFiles,
          retrieveRequestsToSkip, diskSystemsToSkip);
      ret.candidateBytes += shardCandidates.candidateBytes;
      ret.candidateFiles += shardCandidates.candidateFiles;
      // We overwrite the remaining values each time as the previous
      // shards have exhaustied their candidate lists.
      ret.remainingBytesAfterCandidates = shardCandidates.remainingBytesAfterCandidates;
      ret.remainingFilesAfterCandidates = shardCandidates.remainingFilesAfterCandidates;
      ret.candidates.splice(ret.candidates.end(), shardCandidates.candidates);
    } else {
      // We are done with finding candidates. We just need to count what is left in the non-visited shards.
      ret.remainingBytesAfterCandidates += rqsp.shardbytescount();
      ret.remainingFilesAfterCandidates += rqsp.shardjobscount();
    }
  }
  return ret;
}

auto RetrieveQueue::getCandidateSummary() -> CandidateJobList {
  checkPayloadReadable();
  CandidateJobList ret;
  for(auto & rqsp: m_payload.retrievequeueshards()) {
    ret.candidateBytes += rqsp.shardbytescount();
    ret.candidateFiles += rqsp.shardjobscount();
  }
  return ret;
}

auto RetrieveQueue::getMountPolicyNames() -> std::list<std::string> {
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  auto mountPolicyCountMap = mountPolicyNameMap.getMap();

  std::list<std::string> mountPolicyNames;

  for(const auto &mountPolicyCount: mountPolicyCountMap) {
    if (mountPolicyCount.second != 0) {
      mountPolicyNames.push_back(mountPolicyCount.first);
    }
  }
  return mountPolicyNames;
}

void RetrieveQueue::removeJobsAndCommit(const std::list<std::string>& jobsToRemove) {
  checkPayloadWritable();
  ValueCountMapUint64 priorityMap(m_payload.mutable_prioritymap());
  ValueCountMapUint64 minRetrieveRequestAgeMap(m_payload.mutable_minretrieverequestagemap());
  ValueCountMapString mountPolicyNameMap(m_payload.mutable_mountpolicynamemap());
  RetrieveActivityCountMap retrieveActivityCountMap(m_payload.mutable_activity_map());
  // Make a working copy of the jobs to remove. We will progressively trim this local list.
  auto localJobsToRemove = jobsToRemove;
  // The jobs are expected to be removed from the front shards first (poped in order)
  // Remove jobs until there are no more jobs or no more shards.
  ssize_t shardIndex=0;
  auto * mutableRetrieveQueueShards= m_payload.mutable_retrievequeueshards();
  while (localJobsToRemove.size() && shardIndex <  mutableRetrieveQueueShards->size()) {
    auto * shardPointer = mutableRetrieveQueueShards->Mutable(shardIndex);
    // Get hold of the shard
    RetrieveQueueShard rqs(shardPointer->address(), m_objectStore);
    m_exclusiveLock->includeSubObject(rqs);
    rqs.fetch();
    // Remove jobs from shard
    auto removalResult = rqs.removeJobs(localJobsToRemove);
    // If the shard is drained, remove, otherwise commit. We update the pointer afterwards.
    if (removalResult.jobsAfter) {
      rqs.commit();
    } else {
      rqs.remove();
    }
    // We still need to update the tracking queue side.
    // Update stats and remove the jobs from the todo list.
    bool needToRebuild = false;
    time_t oldestJobCreationTime = m_payload.oldestjobcreationtime();
    time_t youngestJobCreationTime = m_payload.youngestjobcreationtime();
    for (auto & j: removalResult.removedJobs) {
      priorityMap.decCount(j.priority);
      minRetrieveRequestAgeMap.decCount(j.minRetrieveRequestAge);
      mountPolicyNameMap.decCount(j.mountPolicyName);
      if(j.startTime <= oldestJobCreationTime){
        //the job we remove was the oldest one, we should rebuild the queue
        //to update the oldestjobcreationtime counter
        needToRebuild = true;
      }
      if (j.startTime >= youngestJobCreationTime) {
        //the job we remove was the youngest one, we should rebuild the queue
        //to update the youngestjobcreationtime counter
        needToRebuild = true;
      }
      if (j.activityDescription) {
        // We have up a partial activity description, but this is enough to decCount.
        RetrieveActivityDescription activityDescription;
        activityDescription.priority = j.priority;
        activityDescription.diskInstanceName = j.activityDescription.value().diskInstanceName;
        activityDescription.activity = j.activityDescription.value().activity;
        retrieveActivityCountMap.decCount(activityDescription);
      }
    }
    // In all cases, we should update the global statistics.
    m_payload.set_retrievejobscount(m_payload.retrievejobscount() - removalResult.jobsRemoved);
    m_payload.set_retrievejobstotalsize(m_payload.retrievejobstotalsize() - removalResult.bytesRemoved);
    // If the shard is still around, we shall update its pointer's stats too.
    if (removalResult.jobsAfter) {
      // Also update the shard pointers's stats. In case of mismatch, we will trigger a rebuild.
      shardPointer->set_shardbytescount(shardPointer->shardbytescount() - removalResult.bytesRemoved);
      shardPointer->set_shardjobscount(shardPointer->shardjobscount() - removalResult.jobsRemoved);

      if (!needToRebuild && (shardPointer->shardbytescount() != removalResult.bytesAfter
          || shardPointer->shardjobscount() != removalResult.jobsAfter)) {
        rebuild();
      }
      // We will commit when exiting anyway...
      shardIndex++;
    } else {
      // Shard's gone, so should the pointer. Push it to the end of the queue and
      // trim it.
      for (auto i=shardIndex; i<mutableRetrieveQueueShards->size()-1; i++) {
        mutableRetrieveQueueShards->SwapElements(i, i+1);
      }
      mutableRetrieveQueueShards->RemoveLast();
    }
    // We should also trim the removed jobs from our list.
    localJobsToRemove.remove_if(
      [&removalResult](const std::string & ja){
        return std::count_if(removalResult.removedJobs.begin(), removalResult.removedJobs.end(),
          [&ja](RetrieveQueueShard::JobInfo & j) {
            return j.address == ja;
          }
        );
      }
    ); // end of remove_if
    // And commit the queue (once per shard should not hurt performance).
    if(needToRebuild){
      rebuild();
    }
    commit();
  }
}

void RetrieveQueue::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  throw cta::exception::Exception("In RetrieveQueue::garbageCollect(): not implemented");
}

void RetrieveQueue::setShardSize(uint64_t shardSize) {
  checkPayloadWritable();
  m_payload.set_maxshardsize(shardSize);
}

uint64_t RetrieveQueue::getShardCount() {
  checkPayloadReadable();
  return m_payload.retrievequeueshards_size();
}


}} // namespace cta::objectstore
