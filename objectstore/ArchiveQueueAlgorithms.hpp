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

#include "Algorithms.hpp"
#include "ArchiveQueue.hpp"

namespace cta { namespace objectstore {

template <>
class ContainerTraits<ArchiveQueue> {
public:
  typedef ArchiveQueue                                           Container;
  typedef std::string                                            ContainerAddress;
  typedef std::string                                            ElementAddress;
  typedef std::string                                            ContainerIdentifyer;
  struct Element {
    std::unique_ptr<ArchiveRequest> archiveRequest;
    uint16_t copyNb;
    cta::common::dataStructures::ArchiveFile archiveFile;
    cta::common::dataStructures::MountPolicy mountPolicy;
  };
  typedef std::list<Element>                                     ElementMemoryContainer;
  struct ElementOpFailure {
    Element * element;
    std::exception_ptr failure;
  };
  typedef std::list<ElementOpFailure>                            ElementOpFailureContainer;
  typedef ArchiveRequest::JobDump                                ElementDescriptor;
  typedef std::list<ElementDescriptor>                           ElementDescriptorContainer;
  
  static ElementAddress getElementAddress(const Element & e) { return e.archiveRequest->getAddressIfSet(); }
  
  static void getLockedAndFetched(Container & cont, ScopedExclusiveLock & aqL, AgentReference & agRef, const ContainerIdentifyer & tapePool,
    log::LogContext & lc) {
    Helpers::getLockedAndFetchedQueue<ArchiveQueue>(cont, aqL, agRef, tapePool, QueueType::LiveJobs, lc);
  }
  
  static void addReferencesAndCommit(Container & cont, ElementMemoryContainer & elemMemCont,
      AgentReference & agentRef, log::LogContext & lc) {
    std::list<ArchiveQueue::JobToAdd> jobsToAdd;
    for (auto & e: elemMemCont) {
      ArchiveRequest::JobDump jd;
      jd.copyNb = e.copyNb;
      jd.tapePool = cont.getTapePool();
      jd.owner = cont.getAddressIfSet();
      ArchiveRequest & ar = *e.archiveRequest;
      jobsToAdd.push_back({jd, ar.getAddressIfSet(), e.archiveFile.archiveFileID, e.archiveFile.fileSize,
          e.mountPolicy, time(NULL)});
    }
    cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
  }
  
  static void removeReferencesAndCommit(Container & cont, ElementOpFailureContainer & elementsOpFailures) {
    std::list<std::string> elementsToRemove;
    for (auto & eof: elementsOpFailures) {
      elementsToRemove.emplace_back(eof.element->archiveRequest->getAddressIfSet());
    }
    cont.removeJobsAndCommit(elementsToRemove);
  }
  
  static ElementOpFailureContainer switchElementsOwnership(ElementMemoryContainer & elemMemCont, Container & cont,
      const ContainerAddress & previousOwnerAddress, log::LogContext & lc) {
    std::list<std::unique_ptr<ArchiveRequest::AsyncJobOwnerUpdater>> updaters;
    for (auto & e: elemMemCont) {
      ArchiveRequest & ar = *e.archiveRequest;
      auto copyNb = e.copyNb;
      updaters.emplace_back(ar.asyncUpdateJobOwner(copyNb, cont.getAddressIfSet(), previousOwnerAddress));
    }
    auto u = updaters.begin();
    auto e = elemMemCont.begin();
    ElementOpFailureContainer ret;
    while (e != elemMemCont.end()) {
      try {
        u->get()->wait();
      } catch (...) {
        ret.push_back(ElementOpFailure());
        ret.back().element = &(*e);
        ret.back().failure = std::current_exception();
      }
      u++;
      e++;
    }
    return ret;
 }
   
  class OwnershipSwitchFailure: public cta::exception::Exception {
  public:
    OwnershipSwitchFailure(const std::string & message): cta::exception::Exception(message) {};
    ElementOpFailureContainer failedElements;
  };
};

}} // namespace cta::objectstore