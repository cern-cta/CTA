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

namespace cta { namespace objectstore {

template<>
struct ContainerTraitsTypes<ArchiveQueue>
{ 
  class ContainerSummary: public ArchiveQueue::JobsSummary {
  public:
    void addDeltaToLog(ContainerSummary&, log::ScopedParamContainer&);
  };
  
  struct InsertedElement {
    std::shared_ptr<ArchiveRequest> archiveRequest;
    uint16_t copyNb;
    cta::common::dataStructures::ArchiveFile archiveFile;
    cta::common::dataStructures::MountPolicy mountPolicy;
    typedef std::list<InsertedElement> list;
  };

  typedef ArchiveRequest::JobDump ElementDescriptor;

  struct PoppedElement {
    std::unique_ptr<ArchiveRequest> archiveRequest;
    uint16_t copyNb;
    uint64_t bytes;
    common::dataStructures::ArchiveFile archiveFile;
    std::string archiveReportURL;
    std::string errorReportURL;
    std::string srcURL;
  };
  struct PoppedElementsSummary;
  struct PopCriteria {
    PopCriteria& operator-=(const PoppedElementsSummary&);
    uint64_t bytes = 0;
    uint64_t files = 0;
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


template<>
template<typename Element>
ContainerTraits<ArchiveQueue>::ElementAddress ContainerTraits<ArchiveQueue>::
getElementAddress(const Element &e) {
  return e.archiveRequest->getAddressIfSet();
}





#if 0
template <>
class ContainerTraits<ArchiveQueue> {
public:
  typedef ArchiveQueue                                           Container;
  typedef std::string                                            ContainerAddress;
  typedef std::string                                            ElementAddress;
  typedef std::string                                            ContainerIdentifier;
  static const std::string                    c_containerTypeName; //= "ArchiveQueue";
  static const std::string                    c_identifyerType; // = "tapepool";
  
  template <class Element>
  struct OpFailure {
    Element * element;
    std::exception_ptr failure;
    typedef std::list<OpFailure> list;
  };
  
  typedef std::list<ElementDescriptor>                           ElementDescriptorContainer;
  
  static void getLockedAndFetched(Container & cont, ScopedExclusiveLock & aqL, AgentReference & agRef, const ContainerIdentifier & contId,
    log::LogContext & lc);
  
  static void getLockedAndFetchedNoCreate(Container & cont, ScopedExclusiveLock & contLock, const ContainerIdentifier & cId,
    log::LogContext & lc);
  
  static void addReferencesAndCommit(Container & cont, InsertedElement::list & elemMemCont,
      AgentReference & agentRef, log::LogContext & lc);
  
  static void addReferencesIfNecessaryAndCommit(Container & cont, InsertedElement::list & elemMemCont,
      AgentReference & agentRef, log::LogContext & lc);
  
  class OwnershipSwitchFailure: public cta::exception::Exception {
  public:
    OwnershipSwitchFailure(const std::string & message): cta::exception::Exception(message) {};
    OpFailure<InsertedElement>::list failedElements;
  };
  
  typedef std::set<ElementAddress> ElementsToSkipSet;
  
  
  static PoppedElementsBatch getPoppingElementsCandidates(Container & cont, PopCriteria & unfulfilledCriteria,
      ElementsToSkipSet & elemtsToSkip, log::LogContext & lc);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

};
#endif

}} // namespace cta::objectstore
