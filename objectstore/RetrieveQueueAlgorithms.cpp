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
const std::string ContainerTraits<RetrieveQueue>::c_containerTypeName = "RetrieveQueue";

template<>
const std::string ContainerTraits<RetrieveQueue>::c_identifierType = "vid";

// ContainerTraitsTypes

void ContainerTraitsTypes<RetrieveQueue>::PoppedElementsSummary::
addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params) {
  throw std::runtime_error("1 Not implemented.");
#if 0
  params.add("filesAdded", files - previous.files)
        .add("bytesAdded", bytes - previous.bytes)
        .add("filesBefore", previous.files)
        .add("bytesBefore", previous.bytes)
        .add("filesAfter", files)
        .add("bytesAfter", bytes);
#endif
}

void ContainerTraitsTypes<RetrieveQueue>::ContainerSummary::
addDeltaToLog(const ContainerSummary &previous, log::ScopedParamContainer &params) {
  throw std::runtime_error("2 Not implemented.");
#if 0
  params.add("queueJobsBefore", previous.jobs)
        .add("queueBytesBefore", previous.bytes)
        .add("queueJobsAfter", jobs)
        .add("queueBytesAfter", bytes);
#endif
}

auto ContainerTraits<RetrieveQueue>::PopCriteria::
operator-=(const PoppedElementsSummary &pes) -> PopCriteria & {
  bytes -= pes.bytes;
  files -= pes.files;
  return *this;
}

void ContainerTraitsTypes<RetrieveQueue>::PoppedElementsList::
insertBack(PoppedElementsList &&insertedList) {
  for (auto &e: insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

void ContainerTraitsTypes<RetrieveQueue>::PoppedElementsList::insertBack(PoppedElement &&e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

void ContainerTraitsTypes<RetrieveQueue>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) {
  params.add("bytes", summary.bytes)
        .add("files", summary.files);
}

// ContainerTraits

template<>
void ContainerTraits<RetrieveQueue>::
getLockedAndFetched(Container &cont, ScopedExclusiveLock &aqL, AgentReference &agRef,
  const ContainerIdentifier &contId, log::LogContext &lc)
{
  Helpers::getLockedAndFetchedQueue<Container>(cont, aqL, agRef, contId, QueueType::LiveJobs, lc);
}

template<>
void ContainerTraits<RetrieveQueue>::
getLockedAndFetchedNoCreate(Container &cont, ScopedExclusiveLock &contLock, const ContainerIdentifier &cId, log::LogContext &lc)
{
  // Try and get access to a queue.
  size_t attemptCount = 0;

retry:
  objectstore::RootEntry re(cont.m_objectStore);
  re.fetchNoLock();
  std::string rqAddress;
  auto rql = re.dumpRetrieveQueues(QueueType::LiveJobs);
  for (auto &rqp : rql) {
    if (rqp.vid == cId)
      rqAddress = rqp.address;
  }
  if(rqAddress.empty()) throw NoSuchContainer("In ContainerTraits<RetrieveQueue>::getLockedAndFetchedNoCreate(): no such retrieve queue");

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
      re.removeRetrieveQueueAndCommit(cId, QueueType::LiveJobs, lc);
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue>::getLockedAndFetchedNoCreate(): dereferenced missing queue from root entry");
    } catch (RootEntry::RetrieveQueueNotEmpty &ex) {
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue>::getLockedAndFetchedNoCreate(): could not dereference missing queue from root entry");
    } catch (RootEntry::NoSuchRetrieveQueue &ex) {
      // Somebody removed the queue in the meantime. Barely worth mentioning.
      log::ScopedParamContainer params(lc);
      params.add("vid", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::DEBUG, "In ContainerTraits<RetrieveQueue>::getLockedAndFetchedNoCreate(): could not dereference missing queue from root entry: already done.");
    }
    attemptCount++;
    goto retry;
  }
}

template<>
void ContainerTraits<RetrieveQueue>::
addReferencesAndCommit(Container &cont, InsertedElement::list &elemMemCont, AgentReference &agentRef,
  log::LogContext &lc)
{
  throw std::runtime_error("4 Not implemented.");
  std::list<RetrieveQueue::JobToAdd> jobsToAdd;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    jobsToAdd.push_back({e.copyNb, e.fSeq, rr.getAddressIfSet(), e.filesize, e.policy, ::time(nullptr)});
  }
  cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
#if 0
  std::list<ArchiveQueue::JobToAdd> jobsToAdd;
  for (auto & e: elemMemCont) {
    ElementDescriptor jd;
    jd.copyNb = e.copyNb;
    jd.tapePool = cont.getTapePool();
    jd.owner = cont.getAddressIfSet();
    ArchiveRequest & ar = *e.archiveRequest;
    jobsToAdd.push_back({jd, ar.getAddressIfSet(), e.archiveFile.archiveFileID, e.archiveFile.fileSize,
        e.mountPolicy, time(nullptr)});
  }
  cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
#endif
}

template<>
void ContainerTraits<RetrieveQueue>::addReferencesIfNecessaryAndCommit(Container& cont,
  InsertedElement::list& elemMemCont, AgentReference& agentRef, log::LogContext& lc)
{
  throw std::runtime_error("5 Not implemented.");
#if 0
  std::list<ArchiveQueue::JobToAdd> jobsToAdd;
  for (auto & e: elemMemCont) {
    ElementDescriptor jd;
    jd.copyNb = e.copyNb;
    jd.tapePool = cont.getTapePool();
    jd.owner = cont.getAddressIfSet();
    ArchiveRequest & ar = *e.archiveRequest;
    jobsToAdd.push_back({jd, ar.getAddressIfSet(), e.archiveFile.archiveFileID, e.archiveFile.fileSize,
        e.mountPolicy, time(nullptr)});
  }
  cont.addJobsIfNecessaryAndCommit(jobsToAdd, agentRef, lc);
#endif
}

template<>
void ContainerTraits<RetrieveQueue>::
removeReferencesAndCommit(Container &cont, OpFailure<InsertedElement>::list &elementsOpFailures)
{
  std::list<std::string> elementsToRemove;
  for (auto &eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->retrieveRequest->getAddressIfSet());
  }
  cont.removeJobsAndCommit(elementsToRemove);
}

template<>
void ContainerTraits<RetrieveQueue>::
removeReferencesAndCommit(Container &cont, std::list<ElementAddress> &elementAddressList) {
  cont.removeJobsAndCommit(elementAddressList);
}

template<>
auto ContainerTraits<RetrieveQueue>::
getContainerSummary(Container &cont) -> ContainerSummary {
  throw std::runtime_error("6 Not implemented.");
  ContainerSummary ret;
#if 0
  ret.JobsSummary::operator=(cont.getJobsSummary());
#endif
  return ret;
}

template<>
auto ContainerTraits<RetrieveQueue>::
switchElementsOwnership(InsertedElement::list &elemMemCont, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t,
  log::LogContext &lc) -> OpFailure<InsertedElement>::list
{
  std::list<std::unique_ptr<RetrieveRequest::AsyncOwnerUpdater>> updaters;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    auto copyNb = e.copyNb;
    updaters.emplace_back(rr.asyncUpdateOwner(copyNb, contAddress, previousOwnerAddress));
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
auto ContainerTraits<RetrieveQueue>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elemtsToSkip,
  log::LogContext &lc) -> PoppedElementsBatch
{
  throw std::runtime_error("7 Not implemented.");
  PoppedElementsBatch ret;
#if 0
  auto candidateJobsFromQueue=cont.getCandidateList(unfulfilledCriteria.bytes, unfulfilledCriteria.files, elemtsToSkip);
  for (auto &cjfq: candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement{cta::make_unique<ArchiveRequest>(cjfq.address, cont.m_objectStore), cjfq.copyNb, cjfq.size,
    common::dataStructures::ArchiveFile(), "", "", "", });
    ret.summary.bytes += cjfq.size;
    ret.summary.files++;
  }
#endif
  return ret;
}

template<>
auto ContainerTraits<RetrieveQueue>::
getElementSummary(const PoppedElement &poppedElement) -> PoppedElementsSummary {
  throw std::runtime_error("8 Not implemented.");
  PoppedElementsSummary ret;
#if 0
  ret.bytes = poppedElement.bytes;
  ret.files = 1;
#endif
  return ret;
}

template<>
auto ContainerTraits<RetrieveQueue>::
switchElementsOwnership(PoppedElementsBatch &poppedElementBatch, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t,
  log::LogContext &lc) -> OpFailure<PoppedElement>::list
{
  throw std::runtime_error("9 Not implemented.");
#if 0
  std::list<std::unique_ptr<ArchiveRequest::AsyncJobOwnerUpdater>> updaters;
  for (auto & e: popedElementBatch.elements) {
    ArchiveRequest & ar = *e.archiveRequest;
    auto copyNb = e.copyNb;
    updaters.emplace_back(ar.asyncUpdateJobOwner(copyNb, contAddress, previousOwnerAddress));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);
  auto u = updaters.begin();
  auto e = popedElementBatch.elements.begin();
#endif
  OpFailure<PoppedElement>::list ret;
#if 0
  while (e != popedElementBatch.elements.end()) {
    try {
      u->get()->wait();
      e->archiveFile = u->get()->getArchiveFile();
      e->archiveReportURL = u->get()->getArchiveReportURL();
      e->errorReportURL = u->get()->getArchiveErrorReportURL();
      e->srcURL = u->get()->getSrcURL();
    } catch (...) {
      ret.push_back(OpFailure<PoppedElement>());
      ret.back().element = &(*e);
      ret.back().failure = std::current_exception();
    }
    u++;
    e++;
  }
  timingList.insertAndReset("asyncUpdateCompletionTime", t);
#endif
  return ret;
}

template<>
void ContainerTraits<RetrieveQueue>::
trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock, const ContainerIdentifier &cId,
  log::LogContext &lc)
{
  throw std::runtime_error("10 Not implemented.");
#if 0
  if(cont.isEmpty()) {
    // The current implementation is done unlocked.
    contLock.release();
    try {
      // The queue should be removed as it is empty.
      RootEntry re(cont.m_objectStore);
      ScopedExclusiveLock rexl(re);
      re.fetch();
      re.removeArchiveQueueAndCommit(cId, QueueType::LiveJobs, lc);
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ContainerTraits<ArchiveQueue>::trimContainerIfNeeded(): deleted empty queue");
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ContainerTraits<ArchiveQueue>::trimContainerIfNeeded(): could not delete a presumably empty queue");
    }
    //queueRemovalTime += localQueueRemovalTime = t.secs(utils::Timer::resetCounter);
  }
#endif
}

}} // namespace cta::objectstore
