/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "RetrieveQueueAlgorithms.hpp"

namespace cta { namespace objectstore {

template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueToReportForUser>::c_containerTypeName = "RetrieveQueueToReportForUser";

#if 0
template<>
const std::string ContainerTraits<RetrieveQueue,RetrieveQueueToReportForUser>::c_identifierType = "vid";

template<>
auto ContainerTraits<RetrieveQueue,RetrieveQueueToReportForUser>::
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
      SchedulerDatabase::RetrieveJob::ReportType::Report
    });
    ret.summary.files++;
  }
  return ret;
}

template<>
void ContainerTraits<RetrieveQueue,RetrieveQueueToReportForUser>::
trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock, const ContainerIdentifier &cId,
  log::LogContext &lc)
{
  trimContainerIfNeeded(cont, QueueType::JobsToReport, contLock, cId, lc);
}
#endif

}} // namespace cta::objectstore
