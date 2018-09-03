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

#include "RetrieveQueueAlgorithms.hpp"
#include "common/make_unique.hpp"

namespace cta { namespace objectstore {

template<>
const std::string ContainerTraits<RetrieveQueue_t,RetrieveQueue>::c_containerTypeName = "RetrieveQueue";

template<>
const std::string ContainerTraits<RetrieveQueue_t,RetrieveQueue>::c_identifierType = "vid";

// ContainerTraitsTypes

template<>
void ContainerTraitsTypes<RetrieveQueue_t,RetrieveQueue>::PoppedElementsSummary::
addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params) const {
  params.add("filesAdded", files - previous.files)
        .add("bytesAdded", bytes - previous.bytes)
        .add("filesBefore", previous.files)
        .add("bytesBefore", previous.bytes)
        .add("filesAfter", files)
        .add("bytesAfter", bytes);
}

template<>
void ContainerTraitsTypes<RetrieveQueue_t,RetrieveQueue>::ContainerSummary::
addDeltaToLog(const ContainerSummary &previous, log::ScopedParamContainer &params) const {
  params.add("queueFilesBefore", previous.files)
        .add("queueBytesBefore", previous.bytes)
        .add("queueFilesAfter", files)
        .add("queueBytesAfter", bytes);
}

template<>
auto ContainerTraits<RetrieveQueue_t,RetrieveQueue>::PopCriteria::
operator-=(const PoppedElementsSummary &pes) -> PopCriteria & {
  bytes -= pes.bytes;
  files -= pes.files;
  return *this;
}

template<>
void ContainerTraitsTypes<RetrieveQueue_t,RetrieveQueue>::PoppedElementsList::
insertBack(PoppedElementsList &&insertedList) {
  for (auto &e: insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

template<>
void ContainerTraitsTypes<RetrieveQueue_t,RetrieveQueue>::PoppedElementsList::insertBack(PoppedElement &&e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

template<>
void ContainerTraitsTypes<RetrieveQueue_t,RetrieveQueue>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) const {
  params.add("bytes", summary.bytes)
        .add("files", summary.files);
}

// ContainerTraits

template<>
void ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
getLockedAndFetched(Container &cont, ScopedExclusiveLock &aqL, AgentReference &agRef,
  const ContainerIdentifier &contId, QueueType queueType, log::LogContext &lc)
{
  Helpers::getLockedAndFetchedQueue<Container>(cont, aqL, agRef, contId, queueType, lc);
}

template<>
void ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
getLockedAndFetchedNoCreate(Container &cont, ScopedExclusiveLock &contLock,
  const ContainerIdentifier &cId, QueueType queueType, log::LogContext &lc)
{
  // Try and get access to a queue.
  size_t attemptCount = 0;

retry:
  objectstore::RootEntry re(cont.m_objectStore);
  re.fetchNoLock();
  std::string rqAddress;
  auto rql = re.dumpRetrieveQueues(queueType);
  for (auto &rqp : rql) {
    if (rqp.vid == cId)
      rqAddress = rqp.address;
  }
  if(rqAddress.empty()) throw NoSuchContainer("In ContainerTraits<RetrieveQueue_t,RetrieveQueue>::getLockedAndFetchedNoCreate(): no such retrieve queue");

  // try and lock the retrieve queue. Any failure from here on means the end of the getting jobs.
  cont.setAddress(rqAddress);
  try {
    if(contLock.isLocked()) contLock.release();
    contLock.lock(cont);
    cont.fetch();
  } catch(cta::exception::Exception & ex) {
    // The queue is now absent. We can remove its reference in the root entry.
    // A new queue could have been added in the meantime, and be non-empty.
    // We will then fail to remove from the RootEntry (non-fatal).
    ScopedExclusiveLock rexl(re);
    re.fetch();
    try {
      re.removeRetrieveQueueAndCommit(cId, queueType, lc);
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue_t,RetrieveQueue>::getLockedAndFetchedNoCreate(): dereferenced missing queue from root entry");
    } catch (RootEntry::RetrieveQueueNotEmpty &ex) {
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue_t,RetrieveQueue>::getLockedAndFetchedNoCreate(): could not dereference missing queue from root entry");
    } catch (RootEntry::NoSuchRetrieveQueue &ex) {
      // Somebody removed the queue in the meantime. Barely worth mentioning.
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::DEBUG, "In ContainerTraits<RetrieveQueue_t,RetrieveQueue>::getLockedAndFetchedNoCreate(): could not dereference missing queue from root entry: already done.");
    }
    attemptCount++;
    goto retry;
  }
}

template<>
void ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
addReferencesAndCommit(Container &cont, InsertedElement::list &elemMemCont, AgentReference &agentRef,
  log::LogContext &lc)
{
  std::list<RetrieveQueue::JobToAdd> jobsToAdd;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    jobsToAdd.push_back({e.copyNb, e.fSeq, rr.getAddressIfSet(), e.filesize, e.policy, ::time(nullptr)});
  }
  cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
}

template<>
void ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
addReferencesIfNecessaryAndCommit(Container& cont, InsertedElement::list& elemMemCont,
  AgentReference& agentRef, log::LogContext& lc)
{
  std::list<RetrieveQueue::JobToAdd> jobsToAdd;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    jobsToAdd.push_back({e.copyNb, e.fSeq, rr.getAddressIfSet(), e.filesize, e.policy, ::time(nullptr)});
  }
  cont.addJobsIfNecessaryAndCommit(jobsToAdd, agentRef, lc);
}

template<>
void ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
removeReferencesAndCommit(Container &cont, OpFailure<InsertedElement>::list &elementsOpFailures)
{
  std::list<std::string> elementsToRemove;
  for (auto &eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->retrieveRequest->getAddressIfSet());
  }
  cont.removeJobsAndCommit(elementsToRemove);
}

template<>
void ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
removeReferencesAndCommit(Container &cont, std::list<ElementAddress> &elementAddressList) {
  cont.removeJobsAndCommit(elementAddressList);
}

template<>
auto ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
getContainerSummary(Container &cont) -> ContainerSummary {
  ContainerSummary ret(cont.getJobsSummary());
  return ret;
}

template<>
auto ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
switchElementsOwnership(InsertedElement::list &elemMemCont, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t,
  log::LogContext &lc) -> OpFailure<InsertedElement>::list
{
  std::list<std::unique_ptr<RetrieveRequest::AsyncJobOwnerUpdater>> updaters;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    updaters.emplace_back(rr.asyncUpdateJobOwner(e.copyNb, contAddress, previousOwnerAddress));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);
  auto u = updaters.begin();
  auto e = elemMemCont.begin();
  OpFailure<InsertedElement>::list ret;
  while (e != elemMemCont.end()) {
    try {
      u->get()->wait();
    } catch (...) {
      ret.push_back(OpFailure<InsertedElement>());
      ret.back().element = &(*e);
      ret.back().failure = std::current_exception();
    }
    u++;
    e++;
  }
  timingList.insertAndReset("asyncUpdateCompletionTime", t);
  return ret;
}

template<>
auto ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elemtsToSkip,
  log::LogContext &lc) -> PoppedElementsBatch
{
  PoppedElementsBatch ret;

  auto candidateJobsFromQueue = cont.getCandidateList(unfulfilledCriteria.bytes, unfulfilledCriteria.files, elemtsToSkip);
  for(auto &cjfq : candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement{
       cta::make_unique<RetrieveRequest>(cjfq.address, cont.m_objectStore),
       cjfq.copyNb,
       cjfq.size,
       common::dataStructures::ArchiveFile(),
       common::dataStructures::RetrieveRequest()
    });
    ret.summary.bytes += cjfq.size;
    ret.summary.files++;
  }

  return ret;
}

template<>
auto ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
getElementSummary(const PoppedElement &poppedElement) -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.bytes = poppedElement.bytes;
  ret.files = 1;
  return ret;
}

template<>
auto ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
switchElementsOwnership(PoppedElementsBatch &poppedElementBatch, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t,
  log::LogContext &lc) -> OpFailure<PoppedElement>::list
{
  // Asynchronously get the Retrieve requests
  std::list<std::unique_ptr<RetrieveRequest::AsyncJobOwnerUpdater>> updaters;
  for(auto &e : poppedElementBatch.elements) {
    RetrieveRequest &rr = *e.retrieveRequest;
    updaters.emplace_back(rr.asyncUpdateJobOwner(e.copyNb, contAddress, previousOwnerAddress));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);

  // Process the Retrieve requests as they come in
  OpFailure<PoppedElement>::list ret;

  for(auto el = std::make_pair(updaters.begin(), poppedElementBatch.elements.begin());
      el.first != updaters.end(); ++el.first, ++el.second)
  {
    auto &u = *(el.first);
    auto &e = *(el.second);

    try {
      u.get()->wait();
      e.archiveFile = u.get()->getArchiveFile();
      e.rr = u.get()->getRetrieveRequest();
    } catch(...) {
      ret.push_back(OpFailure<PoppedElement>(&e, std::current_exception()));
    }
  }
  timingList.insertAndReset("asyncUpdateCompletionTime", t);

  return ret;
}

template<>
void ContainerTraits<RetrieveQueue_t,RetrieveQueue>::
trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock, const ContainerIdentifier &cId,
  log::LogContext &lc)
{
  if(cont.isEmpty())
  {
    // The current implementation is done unlocked
    contLock.release();
    try {
      // The queue should be removed as it is empty
      RootEntry re(cont.m_objectStore);
      ScopedExclusiveLock rexl(re);
      re.fetch();
      re.removeRetrieveQueueAndCommit(cId, QueueType::JobsToTransfer, lc);
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue_t,RetrieveQueue>::trimContainerIfNeeded(): deleted empty queue");
    } catch(cta::exception::Exception &ex) {
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue_t,RetrieveQueue>::trimContainerIfNeeded(): could not delete a presumably empty queue");
    }
  }
}

}} // namespace cta::objectstore
