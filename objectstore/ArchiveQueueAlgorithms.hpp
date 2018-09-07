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

#pragma once
#include "Algorithms.hpp"
#include "ArchiveQueue.hpp"
#include "common/make_unique.hpp"
#include "common/optional.hpp"

namespace cta { namespace objectstore {

// Partial specialisation of ArchiveQueue traits

template<typename C>
struct ContainerTraits<ArchiveQueue,C>
{ 
  struct ContainerSummary : public ArchiveQueue::JobsSummary {
    void addDeltaToLog(ContainerSummary&, log::ScopedParamContainer&);
  };

  struct InsertedElement {
    ArchiveRequest* archiveRequest;
    uint16_t copyNb;
    cta::common::dataStructures::ArchiveFile archiveFile;
    cta::optional<cta::common::dataStructures::MountPolicy> mountPolicy;
    cta::optional<serializers::ArchiveJobStatus> newStatus;
    typedef std::list<InsertedElement> list;
  };

  typedef ArchiveRequest::JobDump ElementDescriptor;

  struct PoppedElement {
    std::unique_ptr<ArchiveRequest> archiveRequest;
    uint16_t copyNb;
    uint64_t bytes;
    common::dataStructures::ArchiveFile archiveFile;
    std::string srcURL;
    std::string archiveReportURL;
    std::string errorReportURL;
    std::string latestError;
    SchedulerDatabase::ArchiveJob::ReportType reportType;
  };
  struct PoppedElementsSummary;
  struct PopCriteria {
    PopCriteria(uint64_t f = 0, uint64_t b = 0) : files(f), bytes(b) {}
    PopCriteria& operator-=(const PoppedElementsSummary&);
    uint64_t files;
    uint64_t bytes;
  };
  struct PoppedElementsSummary {
    uint64_t bytes = 0;
    uint64_t files = 0;
    bool operator< (const PopCriteria & pc) {
      // This returns false if bytes or files are equal but the other value is less. Is that the intended behaviour?
      return bytes < pc.bytes && files < pc.files;
    }
    PoppedElementsSummary& operator+=(const PoppedElementsSummary &other) {
      bytes += other.bytes;
      files += other.files;
      return *this;
    }
    void addDeltaToLog(const PoppedElementsSummary&, log::ScopedParamContainer&);
  };
  struct PoppedElementsList : public std::list<PoppedElement> {
    void insertBack(PoppedElementsList&&);
    void insertBack(PoppedElement&&);
  };
  struct PoppedElementsBatch {
    PoppedElementsList elements;
    PoppedElementsSummary summary;
    void addToLog(log::ScopedParamContainer&);
  };

  typedef ArchiveQueue                                Container;
  typedef std::string                                 ContainerAddress;
  typedef std::string                                 ElementAddress;
  typedef std::string                                 ContainerIdentifier;
  typedef std::list<std::unique_ptr<InsertedElement>> ElementMemoryContainer;
  typedef std::list<ElementDescriptor>                ElementDescriptorContainer;
  typedef std::set<ElementAddress>                    ElementsToSkipSet;

  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

  template<typename Element>
  struct OpFailure {
    Element *element;
    std::exception_ptr failure;
    typedef std::list<OpFailure> list;

    OpFailure() {}
    OpFailure(Element *e, const std::exception_ptr &f) : element(e), failure(f) {}
  };

  struct OwnershipSwitchFailure: public cta::exception::Exception {
    OwnershipSwitchFailure(const std::string & message): cta::exception::Exception(message) {};
    typename OpFailure<InsertedElement>::list failedElements;
  };

  template<typename Element>
  static ElementAddress getElementAddress(const Element &e) {
    return e.archiveRequest->getAddressIfSet();
  }
  
  static ContainerSummary getContainerSummary(Container &cont);
  static void trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void getLockedAndFetched(Container &cont, ScopedExclusiveLock &contLock, AgentReference &agRef,
    const ContainerIdentifier &cId, QueueType queueType, log::LogContext &lc);
  static void getLockedAndFetchedNoCreate(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, QueueType queueType, log::LogContext &lc);
  static void addReferencesAndCommit(Container &cont, typename InsertedElement::list &elemMemCont,
    AgentReference &agentRef, log::LogContext &lc);
  static void addReferencesIfNecessaryAndCommit(Container &cont, typename InsertedElement::list &elemMemCont,
    AgentReference &agentRef, log::LogContext &lc);
  static void removeReferencesAndCommit(Container &cont, typename OpFailure<InsertedElement>::list &elementsOpFailures);
  static void removeReferencesAndCommit(Container &cont, std::list<ElementAddress> &elementAddressList);

  static typename OpFailure<InsertedElement>::list
  switchElementsOwnership(typename InsertedElement::list &elemMemCont, const ContainerAddress &contAddress,
    const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t, log::LogContext &lc);

  static typename OpFailure<PoppedElement>::list
  switchElementsOwnership(PoppedElementsBatch &poppedElementBatch, const ContainerAddress &contAddress,
    const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t, log::LogContext &lc);

  static PoppedElementsSummary getElementSummary(const PoppedElement &);
  static PoppedElementsBatch getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria,
    ElementsToSkipSet &elemtsToSkip, log::LogContext &lc);

  static const std::string c_containerTypeName;
  static const std::string c_identifierType;

private:
  static void trimContainerIfNeeded(Container &cont, QueueType queueType, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
};



// ArchiveQueue partial specialisations for ContainerTraits.
//
// Add a full specialisation to override for a specific ArchiveQueue type.

template<typename C>
void ContainerTraits<ArchiveQueue,C>::ContainerSummary::
addDeltaToLog(ContainerSummary &previous, log::ScopedParamContainer &params) {
  params.add("queueJobsBefore", previous.jobs)
        .add("queueBytesBefore", previous.bytes)
        .add("queueJobsAfter", jobs)
        .add("queueBytesAfter", bytes);
}

template<typename C>
auto ContainerTraits<ArchiveQueue,C>::PopCriteria::
operator-=(const PoppedElementsSummary &pes) -> PopCriteria& {
  bytes -= pes.bytes;
  files -= pes.files;
  return *this;
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::PoppedElementsSummary::
addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params) {
  params.add("filesAdded", files - previous.files)
        .add("bytesAdded", bytes - previous.bytes)
        .add("filesBefore", previous.files)
        .add("bytesBefore", previous.bytes)
        .add("filesAfter", files)
        .add("bytesAfter", bytes);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::PoppedElementsList::
insertBack(PoppedElementsList &&insertedList) {
  for (auto &e: insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::PoppedElementsList::
insertBack(PoppedElement &&e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) {
  params.add("bytes", summary.bytes)
        .add("files", summary.files);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
trimContainerIfNeeded(Container& cont, QueueType queueType, ScopedExclusiveLock & contLock,
  const ContainerIdentifier & cId, log::LogContext& lc)
{
  if (cont.isEmpty()) {
    // The current implementation is done unlocked.
    contLock.release();
    try {
      // The queue should be removed as it is empty.
      RootEntry re(cont.m_objectStore);
      ScopedExclusiveLock rexl(re);
      re.fetch();
      re.removeArchiveQueueAndCommit(cId, queueType, lc);
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ContainerTraits<ArchiveQueue_t,ArchiveQueue>::trimContainerIfNeeded(): deleted empty queue");
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ContainerTraits<ArchiveQueue_t,ArchiveQueue>::trimContainerIfNeeded(): could not delete a presumably empty queue");
    }
    //queueRemovalTime += localQueueRemovalTime = t.secs(utils::Timer::resetCounter);
  }
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
getLockedAndFetched(Container& cont, ScopedExclusiveLock& aqL, AgentReference& agRef,
  const ContainerIdentifier& contId, QueueType queueType, log::LogContext& lc)
{
  Helpers::getLockedAndFetchedQueue<Container>(cont, aqL, agRef, contId, queueType, lc);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
getLockedAndFetchedNoCreate(Container& cont, ScopedExclusiveLock& contLock, const ContainerIdentifier& cId,
  QueueType queueType, log::LogContext& lc)
{
  // Try and get access to a queue.
  size_t attemptCount = 0;
  retry:
  objectstore::RootEntry re(cont.m_objectStore);
  re.fetchNoLock();
  std::string aqAddress;
  auto aql = re.dumpArchiveQueues(queueType);
  for (auto & aqp : aql) {
    if (aqp.tapePool == cId)
      aqAddress = aqp.address;
  }
  if (!aqAddress.size()) throw NoSuchContainer("In ContainerTraits<ArchiveQueue,C>::getLockedAndFetchedNoCreate(): no such archive queue");
  // try and lock the archive queue. Any failure from here on means the end of the getting jobs.
  cont.setAddress(aqAddress);
  //findQueueTime += localFindQueueTime = t.secs(utils::Timer::resetCounter);
  try {
    if (contLock.isLocked()) contLock.release();
    contLock.lock(cont);
    cont.fetch();
    //lockFetchQueueTime += localLockFetchQueueTime = t.secs(utils::Timer::resetCounter);
  } catch (cta::exception::Exception & ex) {
    // The queue is now absent. We can remove its reference in the root entry.
    // A new queue could have been added in the mean time, and be non-empty.
    // We will then fail to remove from the RootEntry (non-fatal).
    ScopedExclusiveLock rexl(re);
    re.fetch();
    try {
      re.removeArchiveQueueAndCommit(cId, queueType, lc);
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ArchiveMount::getNextJobBatch(): de-referenced missing queue from root entry");
    } catch (RootEntry::ArchiveQueueNotEmpty & ex) {
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ArchiveMount::getNextJobBatch(): could not de-referenced missing queue from root entry");
    } catch (RootEntry::NoSuchArchiveQueue & ex) {
      // Somebody removed the queue in the mean time. Barely worth mentioning.
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::DEBUG, "In ArchiveMount::getNextJobBatch(): could not de-referenced missing queue from root entry: already done.");
    }
    //emptyQueueCleanupTime += localEmptyCleanupQueueTime = t.secs(utils::Timer::resetCounter);
    attemptCount++;
    goto retry;
  }
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
addReferencesAndCommit(Container& cont, typename InsertedElement::list& elemMemCont, AgentReference& agentRef,
  log::LogContext& lc)
{
  std::list<ArchiveQueue::JobToAdd> jobsToAdd;
  for (auto & e: elemMemCont) {
    ElementDescriptor jd;
    jd.copyNb = e.copyNb;
    jd.tapePool = cont.getTapePool();
    jd.owner = cont.getAddressIfSet();
    ArchiveRequest & ar = *e.archiveRequest;
    cta::common::dataStructures::MountPolicy mp;
    if (e.mountPolicy) 
      mp=*e.mountPolicy;
    else
      mp=cta::common::dataStructures::MountPolicy();
    jobsToAdd.push_back({jd, ar.getAddressIfSet(), e.archiveFile.archiveFileID, e.archiveFile.fileSize,
        mp, time(nullptr)});
  }
  cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
addReferencesIfNecessaryAndCommit(Container& cont, typename InsertedElement::list& elemMemCont, AgentReference& agentRef,
  log::LogContext& lc)
{
  std::list<ArchiveQueue::JobToAdd> jobsToAdd;
  for (auto &e : elemMemCont) {
    ElementDescriptor jd;
    jd.copyNb = e.copyNb;
    jd.tapePool = cont.getTapePool();
    jd.owner = cont.getAddressIfSet();
    ArchiveRequest & ar = *e.archiveRequest;
    cta::common::dataStructures::MountPolicy mp = e.mountPolicy ? *e.mountPolicy : cta::common::dataStructures::MountPolicy();
    jobsToAdd.push_back({jd, ar.getAddressIfSet(), e.archiveFile.archiveFileID, e.archiveFile.fileSize, mp, time(nullptr)});
  }
  cont.addJobsIfNecessaryAndCommit(jobsToAdd, agentRef, lc);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
removeReferencesAndCommit(Container& cont, typename OpFailure<InsertedElement>::list& elementsOpFailures) {
  std::list<std::string> elementsToRemove;
  for (auto &eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->archiveRequest->getAddressIfSet());
  }
  cont.removeJobsAndCommit(elementsToRemove);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
removeReferencesAndCommit(Container& cont, std::list<ElementAddress>& elementAddressList) {
  cont.removeJobsAndCommit(elementAddressList);
}

template<typename C>
auto ContainerTraits<ArchiveQueue,C>::
switchElementsOwnership(typename InsertedElement::list& elemMemCont, const ContainerAddress& contAddress,
  const ContainerAddress& previousOwnerAddress, log::TimingList& timingList, utils::Timer &t, log::LogContext& lc)
  -> typename OpFailure<InsertedElement>::list
{
  std::list<std::unique_ptr<ArchiveRequest::AsyncJobOwnerUpdater>> updaters;
  for (auto & e: elemMemCont) {
    ArchiveRequest & ar = *e.archiveRequest;
    auto copyNb = e.copyNb;
    updaters.emplace_back(ar.asyncUpdateJobOwner(copyNb, contAddress, previousOwnerAddress, cta::nullopt));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);
  auto u = updaters.begin();
  auto e = elemMemCont.begin();
  typename OpFailure<InsertedElement>::list ret;
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

template<typename C>
auto ContainerTraits<ArchiveQueue,C>::
switchElementsOwnership(PoppedElementsBatch &poppedElementBatch, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t, log::LogContext &lc)
  -> typename OpFailure<PoppedElement>::list
{
  std::list<std::unique_ptr<ArchiveRequest::AsyncJobOwnerUpdater>> updaters;
  for (auto & e: poppedElementBatch.elements) {
    ArchiveRequest & ar = *e.archiveRequest;
    auto copyNb = e.copyNb;
    updaters.emplace_back(ar.asyncUpdateJobOwner(copyNb, contAddress, previousOwnerAddress, cta::nullopt));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);
  auto u = updaters.begin();
  auto e = poppedElementBatch.elements.begin();
  typename OpFailure<PoppedElement>::list ret;
  while (e != poppedElementBatch.elements.end()) {
    try {
      u->get()->wait();
      e->archiveFile = u->get()->getArchiveFile();
      e->archiveReportURL = u->get()->getArchiveReportURL();
      e->errorReportURL = u->get()->getArchiveErrorReportURL();
      e->srcURL = u->get()->getSrcURL();
      switch(u->get()->getJobStatus()) {
        case serializers::ArchiveJobStatus::AJS_ToReportForTransfer:
          e->reportType = SchedulerDatabase::ArchiveJob::ReportType::CompletionReport;
          break;
        case serializers::ArchiveJobStatus::AJS_ToReportForFailure:
          e->reportType = SchedulerDatabase::ArchiveJob::ReportType::FailureReport;
          break;
        default:
          e->reportType = SchedulerDatabase::ArchiveJob::ReportType::NoReportRequired;
          break;
      }
    } catch (...) {
      ret.push_back(OpFailure<PoppedElement>());
      ret.back().element = &(*e);
      ret.back().failure = std::current_exception();
    }
    u++;
    e++;
  }
  timingList.insertAndReset("asyncUpdateCompletionTime", t);
  return ret;
}

template<typename C>
auto ContainerTraits<ArchiveQueue,C>::
getElementSummary(const PoppedElement& poppedElement) -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.bytes = poppedElement.bytes;
  ret.files = 1;
  return ret;
}

template<typename C>
auto ContainerTraits<ArchiveQueue,C>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elemtsToSkip,
  log::LogContext& lc) -> PoppedElementsBatch
{
  PoppedElementsBatch ret;
  auto candidateJobsFromQueue=cont.getCandidateList(unfulfilledCriteria.bytes, unfulfilledCriteria.files, elemtsToSkip);
  for (auto &cjfq: candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement{cta::make_unique<ArchiveRequest>(cjfq.address, cont.m_objectStore), cjfq.copyNb, cjfq.size,
    common::dataStructures::ArchiveFile(), "", "", "", "", SchedulerDatabase::ArchiveJob::ReportType::NoReportRequired });
    ret.summary.bytes += cjfq.size;
    ret.summary.files++;
  }
  return ret;
}

}} // namespace cta::objectstore
