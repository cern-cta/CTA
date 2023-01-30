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
#include "common/dataStructures/JobQueueType.hpp"
#include "RetrieveQueue.hpp"

namespace cta { namespace objectstore {

template<typename C>
struct ContainerTraits<RetrieveQueue,C>
{
  struct ContainerSummary : public RetrieveQueue::JobsSummary {
    ContainerSummary() : RetrieveQueue::JobsSummary() {}
    ContainerSummary(const RetrieveQueue::JobsSummary &c) :
      RetrieveQueue::JobsSummary({c.jobs,c.bytes,c.oldestJobStartTime,c.youngestJobStartTime,c.priority,
          c.minRetrieveRequestAge,c.mountPolicyCountMap,c.activityCounts,std::nullopt}) {}
    void addDeltaToLog(const ContainerSummary&, log::ScopedParamContainer&) const;
  };

  struct QueueType;

  struct InsertedElement {
    RetrieveRequest *retrieveRequest;
    uint32_t copyNb;
    uint64_t fSeq;
    uint64_t filesize;
    cta::common::dataStructures::MountPolicy policy;
    std::optional<std::string> activity;
    std::optional<std::string> diskSystemName;
    typedef std::list<InsertedElement> list;
  };

  typedef RetrieveRequest::JobDump ElementDescriptor;

  struct PoppedElement {
    std::unique_ptr<RetrieveRequest> retrieveRequest;
    uint32_t copyNb;
    uint64_t bytes;
    common::dataStructures::ArchiveFile archiveFile;
    common::dataStructures::RetrieveRequest rr;
    std::string errorReportURL;
    SchedulerDatabase::RetrieveJob::ReportType reportType;
    RetrieveRequest::RepackInfo repackInfo;
    std::optional<std::string> activity;
    std::optional<std::string> diskSystemName;
  };
  struct PoppedElementsSummary;
  struct PopCriteria {
    uint64_t files;
    PopCriteria(uint64_t f = 0) : files(f) {}
    PopCriteria& operator-=(const PoppedElementsSummary&);
  };
  struct PoppedElementsSummary {
    uint64_t files;
    PoppedElementsSummary(uint64_t f = 0) : files(f) {}
    bool operator==(const PoppedElementsSummary &pes) const {
      return files == pes.files;
    }
    bool operator<(const PopCriteria &pc) const {
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
    void addDeltaToLog(const PoppedElementsSummary&, log::ScopedParamContainer&) const;
  };
  struct PoppedElementsList : public std::list<PoppedElement> {
    void insertBack(PoppedElementsList&&);
    void insertBack(PoppedElement&&);
  };
  struct PoppedElementsBatch {
    PoppedElementsList elements;
    PoppedElementsSummary summary;
    void addToLog(log::ScopedParamContainer&) const;
  };

  typedef RetrieveQueue                               Container;
  typedef std::string                                 ContainerAddress;
  typedef std::string                                 ElementAddress;
  typedef std::string                                 ContainerIdentifier;
  typedef std::list<std::unique_ptr<InsertedElement>> ElementMemoryContainer;
  typedef std::list<ElementDescriptor>                ElementDescriptorContainer;
  typedef std::set<ElementAddress>                    ElementsToSkipSet;
  typedef serializers::RetrieveJobStatus              ElementStatus;

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
    return e.retrieveRequest->getAddressIfSet();
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
};



// RetrieveQueue partial specialisations for ContainerTraits.
//
// Add a full specialisation to override for a specific RetrieveQueue type.

template<typename C>
void ContainerTraits<RetrieveQueue,C>::ContainerSummary::
addDeltaToLog(const ContainerSummary &previous, log::ScopedParamContainer &params) const {
  params.add("queueFilesBefore", previous.jobs)
        .add("queueBytesBefore", previous.bytes)
        .add("queueFilesAfter", jobs)
        .add("queueBytesAfter", bytes);
}

template<typename C>
auto ContainerTraits<RetrieveQueue,C>::PopCriteria::
operator-=(const PoppedElementsSummary &pes) -> PopCriteria & {
  files -= pes.files;
  return *this;
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::PoppedElementsSummary::
addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params) const {
  params.add("filesAdded", files - previous.files)
        .add("filesBefore", previous.files)
        .add("filesAfter", files);
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::PoppedElementsList::
insertBack(PoppedElementsList &&insertedList) {
  for (auto &e: insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::PoppedElementsList::insertBack(PoppedElement &&e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) const {
  params.add("files", summary.files);
}

// ContainerTraits

template<typename C>
void ContainerTraits<RetrieveQueue,C>::
getLockedAndFetched(Container &cont, ScopedExclusiveLock &aqL, AgentReference &agRef,
  const ContainerIdentifier &contId, log::LogContext &lc)
{
  ContainerTraits<RetrieveQueue,C>::QueueType queueType;
  Helpers::getLockedAndFetchedJobQueue<Container>(cont, aqL, agRef, contId, queueType.value, lc);
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::
getLockedAndFetchedNoCreate(Container &cont, ScopedExclusiveLock &contLock,
  const ContainerIdentifier &cId, log::LogContext &lc)
{
  // Try and get access to a queue.
  size_t attemptCount = 0;

retry:
  objectstore::RootEntry re(cont.m_objectStore);
  re.fetchNoLock();
  std::string rqAddress;
  ContainerTraits<RetrieveQueue,C>::QueueType queueType;
  auto rql = re.dumpRetrieveQueues(queueType.value);
  for (auto &rqp : rql) {
    if (rqp.vid == cId)
      rqAddress = rqp.address;
  }
  if(rqAddress.empty()) throw NoSuchContainer("In ContainerTraits<RetrieveQueue,C>::getLockedAndFetchedNoCreate(): no such retrieve queue");

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
      re.removeRetrieveQueueAndCommit(cId, queueType.value, lc);
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue,C>::getLockedAndFetchedNoCreate(): dereferenced missing queue from root entry");
    } catch (RootEntry::RetrieveQueueNotEmpty &ex) {
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("Message", ex.getMessageValue());
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue,C>::getLockedAndFetchedNoCreate(): could not dereference missing queue from root entry");
    } catch (RootEntry::NoSuchRetrieveQueue &ex) {
      // Somebody removed the queue in the meantime. Barely worth mentioning.
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", cId)
            .add("queueObject", cont.getAddressIfSet());
      lc.log(log::DEBUG, "In ContainerTraits<RetrieveQueue,C>::getLockedAndFetchedNoCreate(): could not dereference missing queue from root entry: already done.");
    }
    attemptCount++;
    // Unlock and reset the address so we can reuse the in-memory object with potentially ane address.
    if (contLock.isLocked()) contLock.release();
    cont.resetAddress();
    goto retry;
  }
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::
addReferencesAndCommit(Container &cont, typename InsertedElement::list &elemMemCont, AgentReference &agentRef,
  log::LogContext &lc)
{
  std::list<RetrieveQueue::JobToAdd> jobsToAdd;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    jobsToAdd.push_back({e.copyNb, e.fSeq, rr.getAddressIfSet(), e.filesize, e.policy, ::time(nullptr), e.activity, e.diskSystemName});
  }
  cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::
addReferencesIfNecessaryAndCommit(Container& cont, typename InsertedElement::list& elemMemCont,
  AgentReference& agentRef, log::LogContext& lc)
{
  std::list<RetrieveQueue::JobToAdd> jobsToAdd;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    jobsToAdd.push_back({e.copyNb, e.fSeq, rr.getAddressIfSet(), e.filesize, e.policy, rr.getCreationTime(), e.activity, e.diskSystemName});
  }
  cont.addJobsIfNecessaryAndCommit(jobsToAdd, agentRef, lc);
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::
removeReferencesAndCommit(Container &cont, typename OpFailure<InsertedElement>::list &elementsOpFailures)
{
  std::list<std::string> elementsToRemove;
  for (auto &eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->retrieveRequest->getAddressIfSet());
  }
  cont.removeJobsAndCommit(elementsToRemove);
}

template<typename C>
void ContainerTraits<RetrieveQueue,C>::
removeReferencesAndCommit(Container &cont, std::list<ElementAddress> &elementAddressList) {
  cont.removeJobsAndCommit(elementAddressList);
}

template<typename C>
auto ContainerTraits<RetrieveQueue,C>::
getContainerSummary(Container &cont) -> ContainerSummary {
  ContainerSummary ret(cont.getJobsSummary());
  return ret;
}

template<typename C>
auto ContainerTraits<RetrieveQueue,C>::
switchElementsOwnership(typename InsertedElement::list &elemMemCont, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t,
  log::LogContext &lc) -> typename OpFailure<InsertedElement>::list
{
  std::list<std::unique_ptr<RetrieveRequest::AsyncJobOwnerUpdater>> updaters;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    updaters.emplace_back(rr.asyncUpdateJobOwner(e.copyNb, contAddress, previousOwnerAddress));
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
auto ContainerTraits<RetrieveQueue,C>::
getElementSummary(const PoppedElement &poppedElement) -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.files = 1;
  return ret;
}

template<typename C>
auto ContainerTraits<RetrieveQueue,C>::
switchElementsOwnership(PoppedElementsBatch &poppedElementBatch, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t,
  log::LogContext &lc) -> typename OpFailure<PoppedElement>::list
{
  // Asynchronously get the Retrieve requests
  std::list<std::unique_ptr<RetrieveRequest::AsyncJobOwnerUpdater>> updaters;
  for(auto &e : poppedElementBatch.elements) {
    RetrieveRequest &rr = *e.retrieveRequest;
    updaters.emplace_back(rr.asyncUpdateJobOwner(e.copyNb, contAddress, previousOwnerAddress));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);

  // Process the Retrieve requests as they come in
  typename OpFailure<PoppedElement>::list ret;

  for(auto el = std::make_pair(updaters.begin(), poppedElementBatch.elements.begin());
      el.first != updaters.end(); ++el.first, ++el.second)
  {
    auto &u = *(el.first);
    auto &e = *(el.second);

    try {
      u.get()->wait();
      e.archiveFile = u.get()->getArchiveFile();
      e.rr = u.get()->getRetrieveRequest();
      e.repackInfo = u.get()->getRepackInfo();
      e.activity = u.get()->getActivity();
      e.diskSystemName = u.get()->getDiskSystemName();
      switch(u.get()->getJobStatus()) {
        case serializers::RetrieveJobStatus::RJS_ToReportToUserForFailure:
          e.reportType = SchedulerDatabase::RetrieveJob::ReportType::FailureReport;
          break;
        default:
          e.reportType = SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired;
      }
    } catch(...) {
      ret.push_back(OpFailure<PoppedElement>(&e, std::current_exception()));
    }
  }
  timingList.insertAndReset("asyncUpdateCompletionTime", t);

  return ret;
}

template<typename C>
bool ContainerTraits<RetrieveQueue,C>::
trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc)
{
  if(!cont.isEmpty()) {
    auto si = cont.getJobsSummary().sleepInfo;
    if (si) {
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", cId)
            .add("queueObject", cont.getAddressIfSet())
            .add("diskSystemSleptFor", si.value().diskSystemSleptFor);
      lc.log(log::INFO, "In ContainerTraits<RetrieveQueue,C>::trimContainerIfNeeded(): non-empty queue is sleeping");
      // We fake the fact that we trimed the queue for compatibility with previous algorithms (a sleeping queue is like gone at this point).
      return true;
    }
    return false;
  }
  // The current implementation is done unlocked
  contLock.release();
  try {
    // The queue should be removed as it is empty
    ContainerTraits<RetrieveQueue,C>::QueueType queueType;
    RootEntry re(cont.m_objectStore);
    ScopedExclusiveLock rexl(re);
    re.fetch();
    re.removeRetrieveQueueAndCommit(cId, queueType.value, lc);
    log::ScopedParamContainer params(lc);
    params.add("tapeVid", cId)
          .add("queueObject", cont.getAddressIfSet());
    lc.log(log::INFO, "In ContainerTraits<RetrieveQueue,C>::trimContainerIfNeeded(): deleted empty queue");
  } catch(cta::exception::Exception &ex) {
    log::ScopedParamContainer params(lc);
    params.add("tapeVid", cId)
          .add("queueObject", cont.getAddressIfSet())
          .add("Message", ex.getMessageValue());
    lc.log(log::INFO, "In ContainerTraits<RetrieveQueue,C>::trimContainerIfNeeded(): could not delete a presumably empty queue");
    if(typeid(ex) == typeid(cta::objectstore::RootEntry::RetrieveQueueNotEmpty)){
      return false;
    }
  }
  return true;
}



// RetrieveQueue full specialisations for ContainerTraits.

template<>
struct ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::PopCriteria {
  uint64_t files;
  uint64_t bytes;
  PopCriteria(uint64_t f = 0, uint64_t b = 0) : files(f), bytes(b) {} // cppcheck-suppress uninitMemberVar
  template<typename PoppedElementsSummary_t>
  PopCriteria& operator-=(const PoppedElementsSummary_t &pes) {
    bytes -= pes.bytes;
    files -= pes.files;
    return *this;
  }
  struct DiskSystemToSkip {
    std::string name;
    uint64_t sleepTime;
    bool operator<(const DiskSystemToSkip& o) const { return name < o.name; }
  };
  std::set<DiskSystemToSkip> diskSystemsToSkip;
};

template<>
struct ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::PoppedElementsSummary {
  uint64_t files;
  uint64_t bytes;
  bool diskSystemFull = false;
  std::string fullDiskSystem;
  PoppedElementsSummary(uint64_t f = 0, uint64_t b = 0) : files(f), bytes(b) {}  // cppcheck-suppress uninitMemberVar
  bool operator==(const PoppedElementsSummary &pes) const {
    return bytes == pes.bytes && files == pes.files;
  }
  bool operator<(const PopCriteria &pc) const {
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

template<typename C>
auto ContainerTraits<RetrieveQueue,C>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elementsToSkip,
  log::LogContext &lc) -> PoppedElementsBatch
{
  PoppedElementsBatch ret;

  auto candidateJobsFromQueue = cont.getCandidateList(std::numeric_limits<uint64_t>::max(), unfulfilledCriteria.files,
    elementsToSkip,
    // This parameter is needed only in the specialized version:
    // auto ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::getPoppingElementsCandidates
    // We provide an empty set here.
    std::set<std::string>()
  );
  for(auto &cjfq : candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement{
      std::make_unique<RetrieveRequest>(cjfq.address, cont.m_objectStore),
      cjfq.copyNb,
      cjfq.size,
      common::dataStructures::ArchiveFile(),
      common::dataStructures::RetrieveRequest(),
      "", SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired,
      RetrieveRequest::RepackInfo(), cjfq.activity, cjfq.diskSystemName
    });
    ret.summary.files++;
  }
  return ret;
}

template<typename C>
const std::string ContainerTraits<RetrieveQueue,C>::c_identifierType = "tapeVid";

template<>
struct ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::QueueType{
    common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToTransferForUser;
};

template<>
struct ContainerTraits<RetrieveQueue,RetrieveQueueFailed>::QueueType{
    common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::FailedJobs;
};

template<>
struct ContainerTraits<RetrieveQueue,RetrieveQueueToReportForUser>::QueueType{
    common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToReportToUser;
};

template<>
struct ContainerTraits<RetrieveQueue,RetrieveQueueToReportToRepackForSuccess>::QueueType{
  common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToReportToRepackForSuccess;
};

template<>
struct ContainerTraits<RetrieveQueue,RetrieveQueueToReportToRepackForFailure>::QueueType{
  common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToReportToRepackForFailure;
};

template<>
struct ContainerTraits<RetrieveQueue, RetrieveQueueToTransferForRepack>::QueueType{
  common::dataStructures::JobQueueType value = common::dataStructures::JobQueueType::JobsToTransferForRepack;
};

template<>
auto ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elementsToSkip,
  log::LogContext &lc) -> PoppedElementsBatch;

template<>
auto ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::
getElementSummary(const PoppedElement &poppedElement) -> PoppedElementsSummary;

}} // namespace cta::objectstore
