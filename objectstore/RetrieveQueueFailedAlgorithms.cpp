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

#include "RetrieveQueueAlgorithms.hpp"

namespace cta { namespace objectstore {

template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueFailed>::c_containerTypeName = "RetrieveQueueFailed";

#if 0
template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueFailed>::c_identifierType = "vid";

template<>
auto ContainerTraits<RetrieveQueue,RetrieveQueueFailed>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elemtsToSkip,
  log::LogContext &lc) -> PoppedElementsBatch
{
  PoppedElementsBatch ret;

  auto candidateJobsFromQueue = cont.getCandidateList(std::numeric_limits<uint64_t>::max(), unfulfilledCriteria.files, elemtsToSkip);
  for(auto &cjfq : candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement{
      cta::make_unique<RetrieveRequest>(cjfq.address, cont.m_objectStore),
      cjfq.copyNb,
      cjfq.size,
      common::dataStructures::ArchiveFile(),
      common::dataStructures::RetrieveRequest(),
      "",
      SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired
    });
    ret.summary.files++;
  }
  return ret;
}

template<>
void ContainerTraits<RetrieveQueue,RetrieveQueueFailed>::
trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock, const ContainerIdentifier &cId,
  log::LogContext &lc)
{
  trimContainerIfNeeded(cont, QueueType::FailedJobs, contLock, cId, lc);
}
#endif

}} // namespace cta::objectstore
