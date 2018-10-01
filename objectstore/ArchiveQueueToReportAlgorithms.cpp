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

#include "ArchiveQueueAlgorithms.hpp"
#include "common/Timer.hpp"
#include "common/make_unique.hpp"

namespace cta { namespace objectstore {

// ArchiveQueueToReport full specialisations for ContainerTraits.

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToReport>::c_containerTypeName = "ArchiveQueueToReport";

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToReport>::c_identifierType = "tapepool";

template<>
void ContainerTraits<ArchiveQueue,ArchiveQueueToReport>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) {
  params.add("files", summary.files);
}

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToReport>::
getPoppingElementsCandidates(Container& cont, PopCriteria& unfulfilledCriteria, ElementsToSkipSet& elemtsToSkip,
  log::LogContext& lc) -> PoppedElementsBatch
{
  PoppedElementsBatch ret;
  auto candidateJobsFromQueue=cont.getCandidateList(std::numeric_limits<uint64_t>::max(), unfulfilledCriteria.files, elemtsToSkip);
  for (auto &cjfq: candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement());
    PoppedElement & elem = ret.elements.back();
    elem.archiveRequest = cta::make_unique<ArchiveRequest>(cjfq.address, cont.m_objectStore);
    elem.copyNb = cjfq.copyNb;
    elem.bytes = cjfq.size;
    elem.archiveFile = common::dataStructures::ArchiveFile();
    elem.srcURL = "";
    elem.archiveReportURL = "";
    elem.errorReportURL = "";
    elem.latestError = "";
    elem.reportType = SchedulerDatabase::ArchiveJob::ReportType::Report;
    ret.summary.files++;
  }
  return ret;
}

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToReport>::PopCriteria::
operator-=(const PoppedElementsSummary& pes) -> PopCriteria & {
  files -= pes.files;
  return *this;
}

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToReport>::
getContainerSummary(Container& cont) -> ContainerSummary {
  ContainerSummary ret;
  ret.JobsSummary::operator=(cont.getJobsSummary());
  return ret;
}

template<>
void ContainerTraits<ArchiveQueue,ArchiveQueueToReport>::
trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock, const ContainerIdentifier &cId,
  log::LogContext &lc)
{
  trimContainerIfNeeded(cont, JobQueueType::JobsToReport, contLock, cId, lc);
}

}} // namespace cta::objectstore
