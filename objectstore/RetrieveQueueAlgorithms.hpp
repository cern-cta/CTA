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

#pragma once

#include "Algorithms.hpp"
#include "RetrieveQueue.hpp"

namespace cta { namespace objectstore {

template<>
struct ContainerTraitsTypes<RetrieveQueue>
{
  struct ContainerSummary : public RetrieveQueue::JobsSummary {
    ContainerSummary() : RetrieveQueue::JobsSummary() {}
    ContainerSummary(const RetrieveQueue::JobsSummary &c) : RetrieveQueue::JobsSummary() {}
    void addDeltaToLog(const ContainerSummary&, log::ScopedParamContainer&) const;
  };

  struct InsertedElement {
    std::unique_ptr<RetrieveRequest> retrieveRequest;
    uint16_t copyNb;
    uint64_t fSeq;
    uint64_t filesize;
    cta::common::dataStructures::MountPolicy policy;
    serializers::RetrieveJobStatus status;
    typedef std::list<InsertedElement> list;
  };

  typedef RetrieveRequest::JobDump ElementDescriptor;

  struct PoppedElement {
    std::unique_ptr<RetrieveRequest> retrieveRequest;
    uint16_t copyNb;
    uint64_t bytes;
    common::dataStructures::ArchiveFile archiveFile;
    common::dataStructures::RetrieveRequest rr;
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
    bool operator==(const PoppedElementsSummary &pes) const {
      return bytes == pes.bytes && files == pes.files;
    }
#if 0
=======
  typedef std::list<ElementDescriptor>                           ElementDescriptorContainer;
  
  template <class Element>
  static ElementAddress getElementAddress(const Element & e) { return e.retrieveRequest->getAddressIfSet(); }
  
  static void getLockedAndFetched(Container & cont, ScopedExclusiveLock & aqL, AgentReference & agRef,
      const ContainerIdentifyer & contId, QueueType queueType, log::LogContext & lc) {
    Helpers::getLockedAndFetchedQueue<Container>(cont, aqL, agRef, contId, queueType, lc);
  }
  
  static void addReferencesAndCommit(Container & cont, InsertedElement::list & elemMemCont,
      AgentReference & agentRef, log::LogContext & lc) {
    std::list<RetrieveQueue::JobToAdd> jobsToAdd;
    for (auto & e: elemMemCont) {
      RetrieveRequest & rr = *e.retrieveRequest;
      jobsToAdd.push_back({e.copyNb, e.fSeq, rr.getAddressIfSet(), e.filesize, e.policy, ::time(nullptr)});
    }
    cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
  }
  
  static void removeReferencesAndCommit(Container & cont, OpFailure<InsertedElement>::list & elementsOpFailures) {
    std::list<std::string> elementsToRemove;
    for (auto & eof: elementsOpFailures) {
      elementsToRemove.emplace_back(eof.element->retrieveRequest->getAddressIfSet());
>>>>>>> reportQueues
#endif
    bool operator<(const PopCriteria &pc) const {
      return bytes < pc.bytes && files < pc.files;
    }
    PoppedElementsSummary& operator+=(const PoppedElementsSummary &other) {
      bytes += other.bytes;
      files += other.files;
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
};


template<>
template<typename Element>
ContainerTraits<RetrieveQueue>::ElementAddress ContainerTraits<RetrieveQueue>::
getElementAddress(const Element &e) {
  return e.retrieveRequest->getAddressIfSet();
}

}} // namespace cta::objectstore
