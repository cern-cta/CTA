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

#pragma once

#include "Algorithms.hpp"
#include "ArchiveQueue.hpp"
#include "common/dataStructures/JobQueueType.hpp"

#include <optional>

namespace cta::objectstore {

// Partial specialisation of ArchiveQueue traits

template<typename C>
struct ContainerTraits<ArchiveQueue,C>
{
  struct ContainerSummary : public ArchiveQueue::JobsSummary {
    void addDeltaToLog(ContainerSummary&, log::ScopedParamContainer&);
  };

  struct QueueType;

  struct InsertedElement {
    ArchiveRequest* archiveRequest;
    uint32_t copyNb;
    cta::common::dataStructures::ArchiveFile archiveFile;
    std::optional<cta::common::dataStructures::MountPolicy> mountPolicy;
    std::optional<serializers::ArchiveJobStatus> newStatus;
    typedef std::list<InsertedElement> list;
    bool operator==(InsertedElement & other){
      return archiveRequest->getAddressIfSet() == other.archiveRequest->getAddressIfSet() && copyNb == other.copyNb;
    }
  };

  typedef ArchiveRequest::JobDump ElementDescriptor;

  struct PoppedElement {
    std::unique_ptr<ArchiveRequest> archiveRequest;
    uint32_t copyNb;
    uint64_t bytes;
    common::dataStructures::ArchiveFile archiveFile;
    std::string srcURL;
    std::string archiveReportURL;
    std::string errorReportURL;
    std::string latestError;
    SchedulerDatabase::ArchiveJob::ReportType reportType;
    ArchiveRequest::RepackInfo repackInfo;
    std::map<uint32_t, serializers::ArchiveJobStatus> archiveJobsStatusMap;
  };
  struct PoppedElementsSummary;
  struct PopCriteria {
    uint64_t files;
    explicit PopCriteria(uint64_t f = 0) : files(f) {}
    PopCriteria& operator-=(const PoppedElementsSummary&);
  };
  struct PoppedElementsSummary {
    uint64_t files;
    explicit PoppedElementsSummary(uint64_t f = 0) : files(f) {}
    bool operator< (const PopCriteria & pc) {
      return files < pc.files;
    }
    PoppedElementsSummary& operator+=(const PoppedElementsSummary &other) {
      files += other.files;
      return *this;
    }
    PoppedElementsSummary& operator-=(const PoppedElementsSummary &other) {
      files -= other.files;
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
  typedef serializers::ArchiveJobStatus               ElementStatus;

  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

  template<typename Element>
  struct OpFailure {
    Element *element = nullptr;
    std::exception_ptr failure;
    typedef std::list<OpFailure> list;

    OpFailure() = default;
    OpFailure(Element *e, const std::exception_ptr &f) : element(e), failure(f) {}
  };

  struct OwnershipSwitchFailure: public cta::exception::Exception {
    explicit OwnershipSwitchFailure(const std::string& message) : cta::exception::Exception(message) {};
    typename OpFailure<InsertedElement>::list failedElements;
  };

  template<typename Element>
  static ElementAddress getElementAddress(const Element &e) {
    return e.archiveRequest->getAddressIfSet();
  }

  static ContainerSummary getContainerSummary(Container &cont);
  static bool trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void getLockedAndFetched(Container &cont, ScopedExclusiveLock &contLock, AgentReference &agRef,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void getLockedAndFetchedNoCreate(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void addReferencesAndCommit(Container &cont, typename InsertedElement::list &elemMemCont,
    AgentReference &agentRef, log::LogContext &lc);
  static void addReferencesIfNecessaryAndCommit(Container &cont, typename InsertedElement::list &elemMemCont,
    AgentReference &agentRef, log::LogContext &lc);
  static void removeReferencesAndCommit(Container &cont, typename OpFailure<InsertedElement>::list &elementsOpFailures, log::LogContext & lc);
  static void removeReferencesAndCommit(Container &cont, std::list<ElementAddress> &elementAddressList, log::LogContext & lc);

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
  files -= pes.files;
  return *this;
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::PoppedElementsSummary::
addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params) {
  params.add("filesAdded", files - previous.files)
        .add("filesBefore", previous.files)
        .add("filesAfter", files);
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
  params.add("files", summary.files);
}

template<typename C>
bool ContainerTraits<ArchiveQueue,C>::
trimContainerIfNeeded(Container& cont, ScopedExclusiveLock & contLock,
  const ContainerIdentifier & cId, log::LogContext& lc)
{
  log::TimingList tl;
  cta::utils::Timer t;
  if (!cont.isEmpty())  return false;
  // The current implementation is done unlocked.
  contLock.release();
  tl.insertAndReset("queueUnlockTime",t);
  try {
    // The queue should be removed as it is empty.
    ContainerTraits<ArchiveQueue,C>::QueueType queueType;
    RootEntry re(cont.m_objectStore);
    ScopedExclusiveLock rexl(re);
    tl.insertAndReset("rootEntryLockTime",t);
    re.fetch();
    tl.insertAndReset("rootEntryFetchTime",t);
    re.removeArchiveQueueAndCommit(cId, queueType.value, lc);
    tl.insertAndReset("rootEntryRemoveArchiveQueueAndCommitTime",t);
    log::ScopedParamContainer params(lc);
    params.add("tapepool", cId)
          .add("queueObject", cont.getAddressIfSet());
    tl.addToLog(params);
    lc.log(log::INFO, "In ContainerTraits<ArchiveQueue_t,ArchiveQueue>::trimContainerIfNeeded(): deleted empty queue");
  } catch (cta::exception::Exception &ex) {
    log::ScopedParamContainer params(lc);
    params.add("tapepool", cId)
          .add("queueObject", cont.getAddressIfSet())
          .add("Message", ex.getMessageValue());
    tl.addToLog(params);
    lc.log(log::INFO, "In ContainerTraits<ArchiveQueue_t,ArchiveQueue>::trimContainerIfNeeded(): could not delete a presumably empty queue");
  }
  //queueRemovalTime += localQueueRemovalTime = t.secs(utils::Timer::resetCounter);
  return true;
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
getLockedAndFetched(Container& cont, ScopedExclusiveLock& aqL, AgentReference& agRef,
  const ContainerIdentifier& contId, log::LogContext& lc)
{
  ContainerTraits<ArchiveQueue,C>::QueueType queueType;
  Helpers::getLockedAndFetchedJobQueue<Container>(cont, aqL, agRef, contId, queueType.value, lc);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
getLockedAndFetchedNoCreate(Container& cont, ScopedExclusiveLock& contLock, const ContainerIdentifier& cId, log::LogContext& lc)
{
  // Try and get access to a queue.
  size_t attemptCount = 0;
  log::TimingList tl;
  retry:
  cta::utils::Timer t;
  objectstore::RootEntry re(cont.m_objectStore);
  re.fetchNoLock();
  tl.insertAndReset("rootEntryFetchNoLockTime",t);
  std::string aqAddress;
  ContainerTraits<ArchiveQueue,C>::QueueType queueType;
  auto aql = re.dumpArchiveQueues(queueType.value);
  tl.insertAndReset("rootEntryDumpArchiveQueueTime",t);
  for (auto & aqp : aql) {
    if (aqp.tapePool == cId)
      aqAddress = aqp.address;
  }
  if (!aqAddress.size()) throw NoSuchContainer("In ContainerTraits<ArchiveQueue,C>::getLockedAndFetchedNoCreate(): no such archive queue");
  // try and lock the archive queue. Any failure from here on means the end of the getting jobs.
  cont.setAddress(aqAddress);
  //findQueueTime += localFindQueueTime = t.secs(utils::Timer::resetCounter);
  try {
    if (contLock.isLocked()) {
      contLock.release();
      tl.insertAndReset("queueUnlockTime",t);
    }
    t.reset();
    contLock.lock(cont);
    tl.insertAndReset("queueLockTime",t);
    cont.fetch();
    tl.insertAndReset("queueFetchTime",t);
    //lockFetchQueueTime += localLockFetchQueueTime = t.secs(utils::Timer::resetCounter);
  } catch (cta::exception::Exception & ex) {
    // The queue is now absent. We can remove its reference in the root entry.
    // A new queue could have been added in the mean time, and be non-empty.
    // We will then fail to remove from the RootEntry (non-fatal).
    ScopedExclusiveLock rexl(re);
    tl.insertAndReset("rootEntryLockTime",t);
    re.fetch();
    tl.insertAndReset("rootEntryFetchTime",t);
    try {
      re.removeArchiveQueueAndCommit(cId, queueType.value, lc);
      tl.insertAndReset("rootEntryRemoveArchiveQueueAndCommitTime",t);
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet());
      tl.addToLog(params);
      lc.log(log::INFO, "In ContainerTraits<ArchiveQueue,C>::getLockedAndFetchedNoCreate(): de-referenced missing queue from root entry");
    } catch (RootEntry::ArchiveQueueNotEmpty & ex) {
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      tl.addToLog(params);
      lc.log(log::INFO, "In ContainerTraits<ArchiveQueue,C>::getLockedAndFetchedNoCreate(): could not de-referenced missing queue from root entry");
    } catch (RootEntry::NoSuchArchiveQueue & ex) {
      // Somebody removed the queue in the mean time. Barely worth mentioning.
      log::ScopedParamContainer params(lc);
      params.add("tapepool", cId)
            .add("queueObject", cont.getAddressIfSet());
      tl.addToLog(params);
      lc.log(log::DEBUG, "In ContainerTraits<ArchiveQueue,C>::getLockedAndFetchedNoCreate(): could not de-referenced missing queue from root entry: already done.");
    }
    //emptyQueueCleanupTime += localEmptyCleanupQueueTime = t.secs(utils::Timer::resetCounter);
    attemptCount++;
    // Unlock and reset the address so we can reuse the in-memory object with potentially ane address.
    if (contLock.isLocked()) contLock.release();
    cont.resetAddress();
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
removeReferencesAndCommit(Container& cont, typename OpFailure<InsertedElement>::list& elementsOpFailures, log::LogContext & lc) {
  std::list<std::string> elementsToRemove;
  for (auto &eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->archiveRequest->getAddressIfSet());
  }
  cont.removeJobsAndCommit(elementsToRemove, lc);
}

template<typename C>
void ContainerTraits<ArchiveQueue,C>::
removeReferencesAndCommit(Container& cont, std::list<ElementAddress>& elementAddressList, log::LogContext & lc) {
  cont.removeJobsAndCommit(elementAddressList, lc);
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
    updaters.emplace_back(ar.asyncUpdateJobOwner(copyNb, contAddress, previousOwnerAddress, e.newStatus));
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
    updaters.emplace_back(ar.asyncUpdateJobOwner(copyNb, contAddress, previousOwnerAddress, std::nullopt));
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
      e->repackInfo = u->get()->getRepackInfo();
      e->archiveJobsStatusMap = u->get()->getJobsStatusMap();
      switch(e->archiveJobsStatusMap[e->copyNb]) {
        case serializers::ArchiveJobStatus::AJS_ToReportToUserForTransfer:
          e->reportType = SchedulerDatabase::ArchiveJob::ReportType::CompletionReport;
          break;
        case serializers::ArchiveJobStatus::AJS_ToReportToUserForFailure:
          e->reportType = SchedulerDatabase::ArchiveJob::ReportType::FailureReport;
          e->latestError = u->get()->getLastestError();
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
  ret.files = 1;
  return ret;
}



// ArchiveQueue full specialisations for ContainerTraits.

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PopCriteria {
  uint64_t files;
  uint64_t bytes;
  PopCriteria(uint64_t f = 0, uint64_t b = 0) : files(f), bytes(b) {} // cppcheck-suppress uninitMemberVar
  template<typename PoppedElementsSummary_t>
  PopCriteria& operator-=(const PoppedElementsSummary_t &pes) {
    bytes -= pes.bytes;
    files -= pes.files;
    return *this;
  }
};

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PoppedElementsSummary {
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
  PoppedElementsSummary& operator-=(const PoppedElementsSummary &other) {
    bytes -= other.bytes;
    files -= other.files;
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

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForRepack>::PopCriteria:
  public ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PopCriteria {
  using ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PopCriteria::PopCriteria;
};

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForRepack>::PoppedElementsSummary:
  public ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PoppedElementsSummary {
  using ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PoppedElementsSummary::PoppedElementsSummary;
};

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::QueueType {
    common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToTransferForUser;
};

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueFailed>::QueueType {
    common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::FailedJobs;
};

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueToReportForUser>::QueueType {
    common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToReportToUser;
};

template<>
struct ContainerTraits<ArchiveQueue, ArchiveQueueToTransferForRepack>::QueueType{
  common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToTransferForRepack;
};

template<>
struct ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>::QueueType{
  common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess;
};

template<>
struct ContainerTraits<ArchiveQueue, ArchiveQueueToReportToRepackForFailure>::QueueType{
  common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToReportToRepackForFailure;
};

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elementsToSkip,
  log::LogContext &lc) -> PoppedElementsBatch;

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::
getElementSummary(const PoppedElement &poppedElement) -> PoppedElementsSummary;

} // namespace cta::objectstore
