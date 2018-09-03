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
#include "common/optional.hpp"

namespace cta { namespace objectstore {

template<typename C>
struct ContainerTraitsTypes<ArchiveQueue_t,C>
{ 
  struct ContainerSummary: public C::JobsSummary {
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
};



#if 0
template<typename Element>
template<typename C>
auto ContainerTraits<ArchiveQueue_t,C>::getElementAddress(const Element &e) {
  return e.archiveRequest->getAddressIfSet();
}
#endif

}} // namespace cta::objectstore

#if 0
  typedef std::set<ElementAddress> ElementsToSkipSet;
  
  static PoppedElementsSummary getElementSummary(const PoppedElement &);
  
  static PoppedElementsBatch getPoppingElementsCandidates(Container & cont, PopCriteria & unfulfilledCriteria,
      ElementsToSkipSet & elemtsToSkip, log::LogContext & lc);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

  template <class t_PoppedElementsBatch>
  static OpFailure<PoppedElement>::list switchElementsOwnership(t_PoppedElementsBatch & popedElementBatch,
      const ContainerAddress & contAddress, const ContainerAddress & previousOwnerAddress, log::TimingList& timingList, utils::Timer & t,
      log::LogContext & lc) {
    std::list<std::unique_ptr<ArchiveRequest::AsyncJobOwnerUpdater>> updaters;
    for (auto & e: popedElementBatch.elements) {
      ArchiveRequest & ar = *e.archiveRequest;
      auto copyNb = e.copyNb;
      updaters.emplace_back(ar.asyncUpdateJobOwner(copyNb, contAddress, previousOwnerAddress, cta::nullopt));
    }
    timingList.insertAndReset("asyncUpdateLaunchTime", t);
    auto u = updaters.begin();
    auto e = popedElementBatch.elements.begin();
    OpFailure<PoppedElement>::list ret;
    while (e != popedElementBatch.elements.end()) {
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
  
  static void trimContainerIfNeeded (Container& cont, ScopedExclusiveLock & contLock, const ContainerIdentifyer & cId, log::LogContext& lc) {
    trimContainerIfNeeded(cont, QueueType::JobsToTransfer, contLock, cId, lc);
  }
protected:
  static void trimContainerIfNeeded (Container& cont, QueueType queueType, ScopedExclusiveLock & contLock, const ContainerIdentifyer & cId, log::LogContext& lc);
  
};

template<>
class ContainerTraits<ArchiveQueueToReport>: public ContainerTraits<ArchiveQueue> {
public:
  class PoppedElementsSummary;
  class PopCriteria {
  public:
    PopCriteria& operator-= (const PoppedElementsSummary &);
    uint64_t files = 0;
  };
  class PoppedElementsSummary {
  public:
    uint64_t files = 0;
    bool operator< (const PopCriteria & pc) {
      return files < pc.files;
    }
    PoppedElementsSummary& operator+= (const PoppedElementsSummary & other) {
      files += other.files;
      return *this;
    }
    void addDeltaToLog(const PoppedElementsSummary&, log::ScopedParamContainer &);
  };

  class PoppedElementsBatch {
  public:
    PoppedElementsList elements;
    PoppedElementsSummary summary;
    void addToLog(log::ScopedParamContainer &);
  };
  
  static PoppedElementsSummary getElementSummary(const PoppedElement &);
  
  static PoppedElementsBatch getPoppingElementsCandidates(Container & cont, PopCriteria & unfulfilledCriteria,
      ElementsToSkipSet & elemtsToSkip, log::LogContext & lc);
  
  static void trimContainerIfNeeded (Container& cont, ScopedExclusiveLock & contLock, const ContainerIdentifyer & cId, log::LogContext& lc) {
    ContainerTraits<ArchiveQueue>::trimContainerIfNeeded(cont, QueueType::JobsToReport, contLock, cId, lc);
  }
};
>>>>>>> reportQueues
#endif
