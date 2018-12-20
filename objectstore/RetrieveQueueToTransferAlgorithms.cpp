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

#include "RetrieveQueueAlgorithms.hpp"

namespace cta { namespace objectstore {

template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::c_containerTypeName = "RetrieveQueueToTransfer";

template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::c_identifierType = "tapeVid";

template<>
auto ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::
getElementSummary(const PoppedElement &poppedElement) -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.bytes = poppedElement.bytes;
  ret.files = 1;
  return ret;
}

template<>
void ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) const {
  params.add("bytes", summary.bytes)
        .add("files", summary.files);
}

template<>
auto ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elemtsToSkip,
  log::LogContext &lc) -> PoppedElementsBatch
{
  PoppedElementsBatch ret;

  auto candidateJobsFromQueue = cont.getCandidateList(unfulfilledCriteria.bytes, unfulfilledCriteria.files, elemtsToSkip);
  for(auto &cjfq : candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement{
      cta::make_unique<RetrieveRequest>(cjfq.address, cont.m_objectStore),
      cjfq.copyNb,
      cjfq.size,
      common::dataStructures::ArchiveFile(),
      common::dataStructures::RetrieveRequest()
    });
    ret.summary.bytes += cjfq.size;
    ret.summary.files++;
  }
  return ret;
}

}} // namespace cta::objectstore
