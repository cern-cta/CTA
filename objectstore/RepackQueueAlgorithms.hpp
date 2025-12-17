/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Algorithms.hpp"
#include "RepackQueue.hpp"
#include "RepackRequest.hpp"
#include "common/exception/NoSuchObject.hpp"

#include <optional>

namespace cta::objectstore {

// Partial specialisation of RepackQueue traits

template<typename C>
struct ContainerTraits<RepackQueue, C> {
  struct ContainerSummary : public RepackQueue::RequestsSummary {
    using RepackQueue::RequestsSummary::RequestsSummary;
    void addDeltaToLog(ContainerSummary&, log::ScopedParamContainer&);
  };

  struct QueueType;

  struct InsertedElement {
    std::unique_ptr<RepackRequest> repackRequest;
    std::optional<serializers::RepackRequestStatus> newStatus;
    using list = std::list<InsertedElement>;
  };

  using ElementDescriptor = std::string;

  struct PoppedElement {
    std::unique_ptr<RepackRequest> repackRequest;
    common::dataStructures::RepackInfo repackInfo;
  };
  struct PoppedElementsSummary;

  struct PopCriteria {
    uint64_t requests = 1;
    PopCriteria() = default;
    PopCriteria& operator-=(const PoppedElementsSummary&);
  };

  struct PoppedElementsSummary {
    uint64_t requests;

    explicit PoppedElementsSummary(uint64_t r = 0) : requests(r) {}

    bool operator<(const PopCriteria& pc) { return requests < pc.requests; }

    PoppedElementsSummary& operator+=(const PoppedElementsSummary& other) {
      requests += other.requests;
      return *this;
    }

    PoppedElementsSummary& operator-=(const PoppedElementsSummary& other) {
      requests -= other.requests;
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

  using Container = RepackQueue;
  using ContainerAddress = std::string;
  using ElementAddress = std::string;
  using ContainerIdentifier = std::nullopt_t;
  using ElementMemoryContainer = std::list<std::unique_ptr<InsertedElement>>;
  using ElementDescriptorContainer = std::list<ElementDescriptor>;
  using ElementsToSkipSet = std::set<ElementAddress>;
  using ElementStatus = serializers::RepackRequestStatus;

  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

  template<typename Element>
  struct OpFailure {
    Element* element = nullptr;
    std::exception_ptr failure;
    using list = std::list<OpFailure>;

    OpFailure() = default;

    OpFailure(Element* e, const std::exception_ptr& f) : element(e), failure(f) {}
  };

  struct OwnershipSwitchFailure : public cta::exception::Exception {
    explicit OwnershipSwitchFailure(const std::string& message) : cta::exception::Exception(message) {};
    typename OpFailure<InsertedElement>::list failedElements;
  };

  template<typename Element>
  static ElementAddress getElementAddress(const Element& e) {
    return e.repackRequest->getAddressIfSet();
  }

  static ContainerSummary getContainerSummary(Container& cont);
  static bool trimContainerIfNeeded(Container& cont,
                                    ScopedExclusiveLock& contLock,
                                    const ContainerIdentifier& cId,
                                    log::LogContext& lc);
  static void getLockedAndFetched(Container& cont,
                                  ScopedExclusiveLock& contLock,
                                  AgentReference& agRef,
                                  const ContainerIdentifier& cId,
                                  log::LogContext& lc);
  static void getLockedAndFetchedNoCreate(Container& cont,
                                          ScopedExclusiveLock& contLock,
                                          const ContainerIdentifier& cId,
                                          log::LogContext& lc);
  static void addReferencesAndCommit(Container& cont,
                                     typename InsertedElement::list& elemMemCont,
                                     AgentReference& agentRef,
                                     log::LogContext& lc);
  static void addReferencesIfNecessaryAndCommit(Container& cont,
                                                typename InsertedElement::list& elemMemCont,
                                                AgentReference& agentRef,
                                                log::LogContext& lc);
  static void removeReferencesAndCommit(Container& cont,
                                        typename OpFailure<InsertedElement>::list& elementsOpFailures,
                                        log::LogContext& lc);
  static void
  removeReferencesAndCommit(Container& cont, std::list<ElementAddress>& elementAddressList, log::LogContext& lc);

  static typename OpFailure<InsertedElement>::list switchElementsOwnership(typename InsertedElement::list& elemMemCont,
                                                                           const ContainerAddress& contAddress,
                                                                           const ContainerAddress& previousOwnerAddress,
                                                                           log::TimingList& timingList,
                                                                           utils::Timer& t,
                                                                           log::LogContext& lc);

  static typename OpFailure<PoppedElement>::list switchElementsOwnership(PoppedElementsBatch& poppedElementBatch,
                                                                         const ContainerAddress& contAddress,
                                                                         const ContainerAddress& previousOwnerAddress,
                                                                         log::TimingList& timingList,
                                                                         utils::Timer& t,
                                                                         log::LogContext& lc);

  static typename OpFailure<PoppedElement>::list
  switchElementsOwnershipAndStatus(PoppedElementsBatch& poppedElementBatch,
                                   const ContainerAddress& contAddress,
                                   const ContainerAddress& previousOwnerAddress,
                                   log::TimingList& timingList,
                                   utils::Timer& t,
                                   log::LogContext& lc,
                                   std::optional<ElementStatus>& newStatus = std::nullopt);

  static PoppedElementsSummary getElementSummary(const PoppedElement&);
  static PoppedElementsBatch getPoppingElementsCandidates(Container& cont,
                                                          PopCriteria& unfulfilledCriteria,
                                                          ElementsToSkipSet& elemtsToSkip,
                                                          log::LogContext& lc);

  static const std::string c_containerTypeName;
  static const std::string c_identifierType;
};

// RepackQueue partial specialisations for ContainerTraits.
//
// Add a full specialisation to override for a specific RepackQueue type.

template<typename C>
void ContainerTraits<RepackQueue, C>::ContainerSummary::addDeltaToLog(ContainerSummary& previous,
                                                                      log::ScopedParamContainer& params) {
  params.add("queueRequestsBefore", previous.requests).add("queueRequestsAfter", requests);
}

template<typename C>
auto ContainerTraits<RepackQueue, C>::getPoppingElementsCandidates(Container& cont,
                                                                   PopCriteria& unfulfilledCriteria,
                                                                   ElementsToSkipSet& elemtsToSkip,
                                                                   log::LogContext& lc) -> PoppedElementsBatch {
  PoppedElementsBatch ret;
  auto candidateReqsFromQueue = cont.getCandidateList(unfulfilledCriteria.requests, elemtsToSkip);
  for (auto& crfq : candidateReqsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement());
    PoppedElement& elem = ret.elements.back();
    elem.repackRequest = std::make_unique<RepackRequest>(crfq.address, cont.m_objectStore);
    elem.repackInfo.status = common::dataStructures::RepackInfo::Status::Undefined;
    elem.repackInfo.type = common::dataStructures::RepackInfo::Type::Undefined;
    elem.repackInfo.vid = "";
    ret.summary.requests++;
  }
  return ret;
}

template<typename C>
auto ContainerTraits<RepackQueue, C>::PopCriteria::operator-=(const PoppedElementsSummary& pes) -> PopCriteria& {
  requests -= pes.requests;
  return *this;
}

template<typename C>
void ContainerTraits<RepackQueue, C>::PoppedElementsSummary::addDeltaToLog(const PoppedElementsSummary& previous,
                                                                           log::ScopedParamContainer& params) {
  params.add("requestsAdded", requests - previous.requests)
    .add("requestsBefore", previous.requests)
    .add("requestsAfter", requests);
}

template<typename C>
void ContainerTraits<RepackQueue, C>::PoppedElementsList::insertBack(PoppedElementsList&& insertedList) {
  for (auto& e : insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

template<typename C>
void ContainerTraits<RepackQueue, C>::PoppedElementsList::insertBack(PoppedElement&& e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

template<typename C>
void ContainerTraits<RepackQueue, C>::PoppedElementsBatch::addToLog(log::ScopedParamContainer& params) {
  params.add("requests", summary.requests);
}

template<typename C>
bool ContainerTraits<RepackQueue, C>::trimContainerIfNeeded(Container& cont,
                                                            ScopedExclusiveLock& contLock,
                                                            const ContainerIdentifier& cId,
                                                            log::LogContext& lc) {
  // Repack queues are one per status, so we do not need to trim them.
  if (!cont.isEmpty()) {
    return false;
  }
  return true;
}

template<typename C>
void ContainerTraits<RepackQueue, C>::getLockedAndFetched(Container& cont,
                                                          ScopedExclusiveLock& aqL,
                                                          AgentReference& agRef,
                                                          const ContainerIdentifier& contId,
                                                          log::LogContext& lc) {
  ContainerTraits<RepackQueue, C>::QueueType queueType;
  Helpers::getLockedAndFetchedRepackQueue(cont, aqL, agRef, queueType.value, lc);
}

template<typename C>
void ContainerTraits<RepackQueue, C>::getLockedAndFetchedNoCreate(Container& cont,
                                                                  ScopedExclusiveLock& contLock,
                                                                  const ContainerIdentifier& cId,
                                                                  log::LogContext& lc) {
  // Try and get access to a queue.
  size_t attemptCount = 0;
retry:
  objectstore::RootEntry re(cont.m_objectStore);
  re.fetchNoLock();
  std::string rpkQAddress;
  ContainerTraits<RepackQueue, C>::QueueType queueType;
  try {
    rpkQAddress = re.getRepackQueueAddress(queueType.value);
  } catch (RootEntry::NoSuchRepackQueue&) {
    throw NoSuchContainer("In ContainerTraits<RepackQueue,C>::getLockedAndFetchedNoCreate(): no such repack queue");
  }
  // try and lock the repack queue. Any failure from here on means the end of the getting jobs.
  cont.setAddress(rpkQAddress);
  try {
    if (contLock.isLocked()) {
      contLock.release();
    }
    contLock.lock(cont);
    cont.fetch();
  } catch (cta::exception::NoSuchObject&) {
    // The queue is now absent. We can remove its reference in the root entry.
    // A new queue could have been added in the mean time, and be non-empty.
    // We will then fail to remove from the RootEntry (non-fatal).
    ScopedExclusiveLock rexl(re);
    re.fetch();
    try {
      re.removeRepackQueueAndCommit(queueType.value, lc);
      log::ScopedParamContainer params(lc);
      params.add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO,
             "In ContainerTraits<RepackQueue,C>::getLockedAndFetchedNoCreate(): de-referenced missing queue from root "
             "entry");
    } catch (RootEntry::RepackQueueNotEmpty& ex) {
      log::ScopedParamContainer params(lc);
      params.add("queueObject", cont.getAddressIfSet()).add("exceptionMessage", ex.getMessageValue());
      lc.log(log::INFO,
             "In ContainerTraits<RepackQueue,C>::getLockedAndFetchedNoCreate(): could not de-referenced missing queue "
             "from root entry");
    } catch (RootEntry::NoSuchRepackQueue&) {
      // Somebody removed the queue in the mean time. Barely worth mentioning.
      log::ScopedParamContainer params(lc);
      params.add("queueObject", cont.getAddressIfSet());
      lc.log(log::DEBUG,
             "In ContainerTraits<RepackQueue,C>::getLockedAndFetchedNoCreate(): could not de-referenced missing queue "
             "from root entry: already done.");
    }
    attemptCount++;
    // Unlock and reset the address so we can reuse the in-memory object with potentially ane address.
    if (contLock.isLocked()) {
      contLock.release();
    }
    cont.resetAddress();
    goto retry;
  }
}

template<typename C>
void ContainerTraits<RepackQueue, C>::addReferencesAndCommit(Container& cont,
                                                             typename InsertedElement::list& elemMemCont,
                                                             AgentReference& agentRef,
                                                             log::LogContext& lc) {
  std::list<std::string> requestsToAdd;
  for (auto& e : elemMemCont) {
    requestsToAdd.emplace_back(e.repackRequest->getAddressIfSet());
  }
  cont.addRequestsAndCommit(requestsToAdd, lc);
}

template<typename C>
void ContainerTraits<RepackQueue, C>::addReferencesIfNecessaryAndCommit(Container& cont,
                                                                        typename InsertedElement::list& elemMemCont,
                                                                        AgentReference& agentRef,
                                                                        log::LogContext& lc) {
  std::list<std::string> requestsToAdd;
  for (auto& e : elemMemCont) {
    requestsToAdd.emplace_back(e.address);
  }
  cont.addRequestsIfNecessaryAndCommit(requestsToAdd, lc);
}

template<typename C>
void ContainerTraits<RepackQueue, C>::removeReferencesAndCommit(
  Container& cont,
  typename OpFailure<InsertedElement>::list& elementsOpFailures,
  log::LogContext& lc) {
  std::list<std::string> elementsToRemove;
  for (auto& eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->repackRequest->getAddressIfSet());
  }
  cont.removeRequestsAndCommit(elementsToRemove);
}

template<typename C>
void ContainerTraits<RepackQueue, C>::removeReferencesAndCommit(Container& cont,
                                                                std::list<ElementAddress>& elementAddressList,
                                                                log::LogContext& lc) {
  cont.removeRequestsAndCommit(elementAddressList);
}

template<typename C>
auto ContainerTraits<RepackQueue, C>::switchElementsOwnership(typename InsertedElement::list& elemMemCont,
                                                              const ContainerAddress& contAddress,
                                                              const ContainerAddress& previousOwnerAddress,
                                                              log::TimingList& timingList,
                                                              utils::Timer& t,
                                                              log::LogContext& lc) ->
  typename OpFailure<InsertedElement>::list {
  std::list<std::unique_ptr<RepackRequest::AsyncOwnerAndStatusUpdater>> updaters;
  for (auto& e : elemMemCont) {
    RepackRequest& repr = *e.repackRequest;
    updaters.emplace_back(repr.asyncUpdateOwnerAndStatus(contAddress, previousOwnerAddress, std::nullopt));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);
  auto u = updaters.begin();
  auto e = elemMemCont.begin();
  typename OpFailure<InsertedElement>::list ret;
  while (e != elemMemCont.end()) {
    try {
      u->get()->wait();
    } catch (...) {
      ret.emplace_back();
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
auto ContainerTraits<RepackQueue, C>::switchElementsOwnership(PoppedElementsBatch& poppedElementBatch,
                                                              const ContainerAddress& contAddress,
                                                              const ContainerAddress& previousOwnerAddress,
                                                              log::TimingList& timingList,
                                                              utils::Timer& t,
                                                              log::LogContext& lc) ->
  typename OpFailure<PoppedElement>::list {
  std::list<std::unique_ptr<RepackRequest::AsyncOwnerAndStatusUpdater>> updaters;
  for (auto& e : poppedElementBatch.elements) {
    RepackRequest& repackRequest = *e.repackRequest;
    updaters.emplace_back(repackRequest.asyncUpdateOwnerAndStatus(contAddress, previousOwnerAddress, std::nullopt));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);

  typename OpFailure<PoppedElement>::list ret;

  for (auto el = std::make_pair(updaters.begin(), poppedElementBatch.elements.begin()); el.first != updaters.end();
       ++el.first, ++el.second) {
    auto& updater = *(el.first);
    auto& element = *(el.second);
    try {
      updater.get()->wait();
      element.repackInfo = updater.get()->getInfo();
    } catch (...) {
      ret.emplace_back(&element, std::current_exception());
    }
  }
  timingList.insertAndReset("asyncUpdateCompletionTime", t);
  return ret;
}

template<typename C>
auto ContainerTraits<RepackQueue, C>::switchElementsOwnershipAndStatus(PoppedElementsBatch& poppedElementBatch,
                                                                       const ContainerAddress& contAddress,
                                                                       const ContainerAddress& previousOwnerAddress,
                                                                       log::TimingList& timingList,
                                                                       utils::Timer& t,
                                                                       log::LogContext& lc,
                                                                       std::optional<ElementStatus>& newStatus) ->
  typename OpFailure<PoppedElement>::list {
  std::list<std::unique_ptr<RepackRequest::AsyncOwnerAndStatusUpdater>> updaters;
  for (auto& e : poppedElementBatch.elements) {
    RepackRequest& rr = *e.repackRequest;
    updaters.emplace_back(rr.asyncUpdateOwnerAndStatus(contAddress, previousOwnerAddress, newStatus));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);
  auto u = updaters.begin();
  auto e = poppedElementBatch.elements.begin();
  typename OpFailure<PoppedElement>::list ret;
  while (e != poppedElementBatch.elements.end()) {
    try {
      u->get()->wait();
      e->repackInfo = u->get()->getInfo();
    } catch (...) {
      ret.emplace_back();
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
auto ContainerTraits<RepackQueue, C>::getElementSummary(const PoppedElement& poppedElement) -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.requests = 1;
  return ret;
}

template<>
struct ContainerTraits<RepackQueue, RepackQueuePending>::QueueType {
  common::dataStructures::RepackQueueType value = common::dataStructures::RepackQueueType::Pending;
};

template<>
struct ContainerTraits<RepackQueue, RepackQueueToExpand>::QueueType {
  common::dataStructures::RepackQueueType value = common::dataStructures::RepackQueueType::ToExpand;
};
}  // namespace cta::objectstore
