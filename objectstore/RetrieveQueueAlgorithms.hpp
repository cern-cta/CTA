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
#include "RetrieveQueue.hpp"

namespace cta { namespace objectstore {

template <>
class ContainerTraits<RetrieveQueue> {
public:
  typedef RetrieveQueue                                          Container;
  typedef std::string                                            ContainerAddress;
  typedef std::string                                            ElementAddress;
  typedef std::string                                            ContainerIdentifyer;
  struct InsertedElement {
    std::unique_ptr<RetrieveRequest> retrieveRequest;
    uint16_t copyNb;
    uint64_t fSeq;
    uint64_t filesize;
    cta::common::dataStructures::MountPolicy policy;
    serializers::RetrieveJobStatus status;
    typedef std::list<InsertedElement> list;
  };
  
  template <class Element>
  struct OpFailure {
    Element * element;
    std::exception_ptr failure;
    typedef std::list<OpFailure> list;
  };
  typedef RetrieveRequest::JobDump                               ElementDescriptor;
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
    }
    cont.removeJobsAndCommit(elementsToRemove);
  }
  
  static OpFailure<InsertedElement>::list switchElementsOwnership(InsertedElement::list & elemMemCont, const ContainerAddress & contAddress,
      const ContainerAddress & previousOwnerAddress, log::TimingList& timingList, utils::Timer & t, log::LogContext & lc) {
    std::list<std::unique_ptr<RetrieveRequest::AsyncOwnerUpdater>> updaters;
    for (auto & e: elemMemCont) {
      RetrieveRequest & rr = *e.retrieveRequest;
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
   
  class OwnershipSwitchFailure: public cta::exception::Exception {
  public:
    OwnershipSwitchFailure(const std::string & message): cta::exception::Exception(message) {};
    OpFailure<InsertedElement>::list failedElements;
  };
  class PoppedElementsSummary;
  class PopCriteria {
  public:
    PopCriteria();
    PopCriteria& operator-= (const PoppedElementsSummary &);
  };
  class PoppedElementsList {
  public:
    PoppedElementsList();
    void insertBack(PoppedElementsList &&);
  };
  class PoppedElementsSummary {
  public:
    PoppedElementsSummary();
    bool operator< (const PopCriteria &);
    PoppedElementsSummary& operator+= (const PoppedElementsSummary &);
  };
  class PoppedElementsBatch {
  public:
    PoppedElementsBatch();
    PoppedElementsList elements;
    PoppedElementsSummary summary;
  };
  
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

};

}} // namespace cta::objectstore