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

void ContainerTraitsTypes<ArchiveQueue>::PoppedElementsSummary::
addDeltaToLog(const PoppedElementsSummary &previous, log::ScopedParamContainer &params)
{
  params.add("filesAdded", files - previous.files)
        .add("bytesAdded", bytes - previous.bytes)
        .add("filesBefore", previous.files)
        .add("bytesBefore", previous.bytes)
        .add("filesAfter", files)
        .add("bytesAfter", bytes);
}

void ContainerTraitsTypes<ArchiveQueue>::ContainerSummary::
addDeltaToLog(ContainerSummary& previous, log::ScopedParamContainer& params)
{
  params.add("queueJobsBefore", previous.jobs)
        .add("queueBytesBefore", previous.bytes)
        .add("queueJobsAfter", jobs)
        .add("queueBytesAfter", bytes);
}

auto ContainerTraits<ArchiveQueue>::PopCriteria::
operator-=(const PoppedElementsSummary &pes) -> PopCriteria & {
  bytes -= pes.bytes;
  files -= pes.files;
  return *this;
}


void ContainerTraitsTypes<ArchiveQueue>::PoppedElementsList::
insertBack(PoppedElementsList &&insertedList) {
  for (auto &e: insertedList) {
    std::list<PoppedElement>::emplace_back(std::move(e));
  }
}

void ContainerTraitsTypes<ArchiveQueue>::PoppedElementsList::insertBack(PoppedElement &&e) {
  std::list<PoppedElement>::emplace_back(std::move(e));
}

void ContainerTraitsTypes<ArchiveQueue>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) {
  params.add("bytes", summary.bytes)
        .add("files", summary.files);
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
  
  static ContainerSummary getContainerSummary(Container &cont);
  
  template <class Element>
  struct OpFailure {
    Element * element;
    std::exception_ptr failure;
    typedef std::list<OpFailure> list;
  };
  
  typedef std::list<ElementDescriptor>                           ElementDescriptorContainer;
  
  template <class Element>
  static ElementAddress getElementAddress(const Element & e) { return e.archiveRequest->getAddressIfSet(); }
  
  static void getLockedAndFetched(Container & cont, ScopedExclusiveLock & aqL, AgentReference & agRef, const ContainerIdentifier & contId,
    log::LogContext & lc);
  
  static void getLockedAndFetchedNoCreate(Container & cont, ScopedExclusiveLock & contLock, const ContainerIdentifier & cId,
    log::LogContext & lc);
  
  static void addReferencesAndCommit(Container & cont, InsertedElement::list & elemMemCont,
      AgentReference & agentRef, log::LogContext & lc);
  
  static void addReferencesIfNecessaryAndCommit(Container & cont, InsertedElement::list & elemMemCont,
      AgentReference & agentRef, log::LogContext & lc);
  
  static void removeReferencesAndCommit(Container & cont, OpFailure<InsertedElement>::list & elementsOpFailures);
  
  static void removeReferencesAndCommit(Container & cont, std::list<ElementAddress>& elementAddressList);
  
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
