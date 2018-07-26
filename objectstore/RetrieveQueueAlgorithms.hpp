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
  struct ContainerSummary {
    void addDeltaToLog(const ContainerSummary&, log::ScopedParamContainer&);
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
    common::dataStructures::ArchiveFile archiveFile;
  };
  struct PoppedElementsSummary;
  struct PopCriteria {
    PopCriteria(uint64_t f = 0, uint64_t b = 0) : files(f), bytes(b) {}
    PopCriteria& operator-=(const PoppedElementsSummary&);
    uint64_t files;
    uint64_t bytes;
  };
  struct PoppedElementsSummary {
    //PoppedElementsSummary();
    bool operator<(const PopCriteria&);
    PoppedElementsSummary& operator+=(const PoppedElementsSummary&);
    //PoppedElementsSummary(const PoppedElementsSummary&);
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
ContainerTraits<RetrieveQueue>::ElementAddress ContainerTraits<RetrieveQueue>::
getElementAddress(const Element &e) {
  return e.retrieveRequest->getAddressIfSet();
}

}} // namespace cta::objectstore
