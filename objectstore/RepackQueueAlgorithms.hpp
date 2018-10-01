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
#include "RepackQueue.hpp"
#include "common/make_unique.hpp"
#include "common/optional.hpp"

namespace cta { namespace objectstore {

// Partial specialisation of RepackQueue traits

template<typename C>
struct ContainerTraits<RepackQueue,C>
{ 
  struct ContainerSummary : public RepackQueue::RequestsSummary {
    std::string repackRequestAddress;
    void addDeltaToLog(ContainerSummary&, log::ScopedParamContainer&);
  };
  
  typedef cta::objectstore::RepackQueueType QueueType;

  struct InsertedElement {
    std::unique_ptr<RepackRequest> repackRequest;
    typedef std::list<InsertedElement> list;
  };

  typedef std::string ElementDescriptor;

  struct PoppedElement {
    std::unique_ptr<RepackRequest> repackRequest;
    std::string vid;
    serializers::RepackRequestStatus status;
  };
  struct PoppedElementsSummary;
  struct PopCriteria {
    uint64_t requests;
    PopCriteria() : requests(1) {}
    PopCriteria& operator-=(const PoppedElementsSummary&);
  };
  struct PoppedElementsSummary {
    uint64_t requests;
    PoppedElementsSummary(uint64_t r = 0) : requests(r) {}
    bool operator< (const PopCriteria & pc) {
      return requests < pc.requests;
    }
    PoppedElementsSummary& operator+=(const PoppedElementsSummary &other) {
      requests += other.requests;
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

  typedef RepackQueue                                 Container;
  typedef std::string                                 ContainerAddress;
  typedef std::string                                 ElementAddress;
  typedef cta::nullopt_t                              ContainerIdentifier;
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
    return e.repackRequest->getAddressIfSet();
  }
  
  static ContainerSummary getContainerSummary(Container &cont);
  static void trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void getLockedAndFetched(Container &cont, ScopedExclusiveLock &contLock, AgentReference &agRef,
    const ContainerIdentifier &cId, RepackQueueType queueType, log::LogContext &lc);
  static void getLockedAndFetchedNoCreate(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, RepackQueueType queueType, log::LogContext &lc);
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
  static void trimContainerIfNeeded(Container &cont, JobQueueType queueType, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
};



// RepackQueue partial specialisations for ContainerTraits.
//
// Add a full specialisation to override for a specific RepackQueue type.

template<typename C>
void ContainerTraits<RepackQueue,C>::ContainerSummary::
addDeltaToLog(ContainerSummary &previous, log::ScopedParamContainer &params) {
  params.add("queueRequestsBefore", previous.requests)
        .add("queueRequestsAfter", requests);
}

template<typename C>
auto ContainerTraits<RepackQueue,C>::PopCriteria::
operator-=(const PoppedElementsSummary &pes) -> PopCriteria& {
  requests -= pes.requests;
  return *this;
}

template<typename C>
void ContainerTraits<RepackQueue,C>::PoppedElementsSummary::
addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params) {
  params.add("requestsAdded", requests - previous.requests)
        .add("requestsBefore", previous.requests)
        .add("requestsAfter", requests);
}

template<typename C>
void ContainerTraits<RepackQueue,C>::PoppedElementsList::
insertBack(PoppedElementsList &&insertedList) {
  for (auto &e: insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

template<typename C>
void ContainerTraits<RepackQueue,C>::PoppedElementsList::
insertBack(PoppedElement &&e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

template<typename C>
void ContainerTraits<RepackQueue,C>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) {
  params.add("requests", summary.requests);
}

template<typename C>
void ContainerTraits<RepackQueue,C>::
trimContainerIfNeeded(Container& cont, JobQueueType queueType, ScopedExclusiveLock & contLock,
  const ContainerIdentifier & cId, log::LogContext& lc)
{
  // Repack queues are one per status, so we do not need to trim them.
}

template<typename C>
void ContainerTraits<RepackQueue,C>::
getLockedAndFetched(Container& cont, ScopedExclusiveLock& aqL, AgentReference& agRef,
  const ContainerIdentifier& contId, RepackQueueType queueType, log::LogContext& lc)
{
  Helpers::getLockedAndFetchedRepackQueue(cont, aqL, agRef, queueType, lc);
}

template<typename C>
void ContainerTraits<RepackQueue,C>::
getLockedAndFetchedNoCreate(Container& cont, ScopedExclusiveLock& contLock, const ContainerIdentifier& cId,
  RepackQueueType queueType, log::LogContext& lc)
{
  // Try and get access to a queue.
  size_t attemptCount = 0;
  retry:
  objectstore::RootEntry re(cont.m_objectStore);
  re.fetchNoLock();
  std::string rpkQAddress;
  try {
    rpkQAddress = re.getRepackQueueAddress(queueType);
  } catch (RootEntry::NotAllocated &) {
    throw NoSuchContainer("In ContainerTraits<RepackQueue,C>::getLockedAndFetchedNoCreate(): no such repack queue");
  }
  // try and lock the archive queue. Any failure from here on means the end of the getting jobs.
  cont.setAddress(rpkQAddress);
  //findQueueTime += localFindQueueTime = t.secs(utils::Timer::resetCounter);
  try {
    if (contLock.isLocked()) contLock.release();
    contLock.lock(cont);
    cont.fetch();
    //lockFetchQueueTime += localLockFetchQueueTime = t.secs(utils::Timer::resetCounter);
  } catch (Backend::NoSuchObject & ex) {
    // The queue is now absent. We can remove its reference in the root entry.
    // A new queue could have been added in the mean time, and be non-empty.
    // We will then fail to remove from the RootEntry (non-fatal).
    ScopedExclusiveLock rexl(re);
    re.fetch();
    try {
      re.removeRepackQueueAndCommit(queueType, lc);
      log::ScopedParamContainer params(lc);
      params.add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ArchiveMount::getNextJobBatch(): de-referenced missing queue from root entry");
    } catch (RootEntry::RepackQueueNotEmpty & ex) {
      log::ScopedParamContainer params(lc);
      params.add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ArchiveMount::getNextJobBatch(): could not de-referenced missing queue from root entry");
    } catch (RootEntry::NoSuchRepackQueue & ex) {
      // Somebody removed the queue in the mean time. Barely worth mentioning.
      log::ScopedParamContainer params(lc);
      params.add("queueObject", cont.getAddressIfSet());
      lc.log(log::DEBUG, "In ArchiveMount::getNextJobBatch(): could not de-referenced missing queue from root entry: already done.");
    }
    //emptyQueueCleanupTime += localEmptyCleanupQueueTime = t.secs(utils::Timer::resetCounter);
    attemptCount++;
    goto retry;
  }
}

template<typename C>
void ContainerTraits<RepackQueue,C>::
addReferencesAndCommit(Container& cont, typename InsertedElement::list& elemMemCont, AgentReference& agentRef,
  log::LogContext& lc)
{
  std::list<std::string> requestsToAdd;
  for (auto & e: elemMemCont) requestsToAdd.emplace_back(e.repackRequest->getAddressIfSet());
  cont.addRequestsAndCommit(requestsToAdd, lc);
}

template<typename C>
void ContainerTraits<RepackQueue,C>::
addReferencesIfNecessaryAndCommit(Container& cont, typename InsertedElement::list& elemMemCont, AgentReference& agentRef,
  log::LogContext& lc)
{
  std::list<std::string> requestsToAdd;
  for (auto & e: elemMemCont) requestsToAdd.emplace_back(e.address);
  cont.addRequestsIfNecessaryAndCommit(requestsToAdd, lc);
}

template<typename C>
void ContainerTraits<RepackQueue,C>::
removeReferencesAndCommit(Container& cont, typename OpFailure<InsertedElement>::list& elementsOpFailures) {
  std::list<std::string> elementsToRemove;
  for (auto &eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->repackRequest->getAddressIfSet());
  }
  cont.removeRequestsAndCommit(elementsToRemove);
}

template<typename C>
void ContainerTraits<RepackQueue,C>::
removeReferencesAndCommit(Container& cont, std::list<ElementAddress>& elementAddressList) {
  cont.removeRequestsAndCommit(elementAddressList);
}

template<typename C>
auto ContainerTraits<RepackQueue,C>::
switchElementsOwnership(typename InsertedElement::list& elemMemCont, const ContainerAddress& contAddress,
  const ContainerAddress& previousOwnerAddress, log::TimingList& timingList, utils::Timer &t, log::LogContext& lc)
  -> typename OpFailure<InsertedElement>::list
{
  std::list<std::unique_ptr<RepackRequest::AsyncOwnerUpdater>> updaters;
  for (auto & e: elemMemCont) {
    RepackRequest & repr = *e.repackRequest;
    updaters.emplace_back(repr.asyncUpdateOwner(contAddress, previousOwnerAddress, cta::nullopt));
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
auto ContainerTraits<RepackQueue,C>::
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
auto ContainerTraits<RepackQueue,C>::
getElementSummary(const PoppedElement& poppedElement) -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.requests = 1;
  return ret;
}



// RepackQueue full specialisations for ContainerTraits.

template<>
struct ContainerTraits<RepackQueue,RepackQueuePending>::PopCriteria {
  uint64_t files;
  uint64_t bytes;
  PopCriteria(uint64_t f = 0, uint64_t b = 0) : files(f), bytes(b) {}
  template<typename PoppedElementsSummary_t>
  PopCriteria& operator-=(const PoppedElementsSummary_t &pes) {
    bytes -= pes.bytes;
    files -= pes.files;
    return *this;
  }
};

template<>
struct ContainerTraits<RepackQueue,RepackQueuePending>::PoppedElementsSummary {
  uint64_t files;
  uint64_t bytes;
  PoppedElementsSummary(uint64_t f = 0, uint64_t b = 0) : files(f), bytes(b) {}
  bool operator< (const PopCriteria & pc) {
    // This returns false if bytes or files are equal but the other value is less. Is that the intended behaviour?
    return bytes < pc.bytes && files < pc.files;
  }
  PoppedElementsSummary& operator+=(const PoppedElementsSummary &other) {
    bytes += other.bytes;
    files += other.files;
    return *this;
  }
  void addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params) {
    params.add("filesAdded", files - previous.files)
          .add("bytesAdded", bytes - previous.bytes)
          .add("filesBefore", previous.files)
          .add("bytesBefore", previous.bytes)
          .add("filesAfter", files)
          .add("bytesAfter", bytes);
  }
};

}} // namespace cta::objectstore
