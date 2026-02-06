/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ArchiveQueueAlgorithms.hpp"
#include "common/utils/Timer.hpp"

namespace cta::objectstore {

// ArchiveQueueToReport full specialisations for ContainerTraits.

template<>
const std::string ContainerTraits<ArchiveQueue, ArchiveQueueToReportForUser>::c_containerTypeName =
  "ArchiveQueueToReportForUser";

template<>
const std::string ContainerTraits<ArchiveQueue, ArchiveQueueToReportForUser>::c_identifierType = "tapepool";

template<>
void ContainerTraits<ArchiveQueue, ArchiveQueueToReportForUser>::PoppedElementsBatch::addToLog(
  log::ScopedParamContainer& params) {
  params.add("files", summary.files);
}

template<>
auto ContainerTraits<ArchiveQueue, ArchiveQueueToReportForUser>::getPoppingElementsCandidates(
  Container& cont,
  PopCriteria& unfulfilledCriteria,
  ElementsToSkipSet& elemtsToSkip,
  log::LogContext& lc) -> PoppedElementsBatch {
  PoppedElementsBatch ret;
  auto candidateJobsFromQueue =
    cont.getCandidateList(std::numeric_limits<uint64_t>::max(), unfulfilledCriteria.files, elemtsToSkip, lc);
  for (auto& cjfq : candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement());
    PoppedElement& elem = ret.elements.back();
    elem.archiveRequest = std::make_unique<ArchiveRequest>(cjfq.address, cont.m_objectStore);
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
auto ContainerTraits<ArchiveQueue, ArchiveQueueToReportForUser>::PopCriteria::operator-=(
  const PoppedElementsSummary& pes) -> PopCriteria& {
  files -= pes.files;
  return *this;
}

template<>
auto ContainerTraits<ArchiveQueue, ArchiveQueueToReportForUser>::getContainerSummary(Container& cont)
  -> ContainerSummary {
  ContainerSummary ret;
  ret.JobsSummary::operator=(cont.getJobsSummary());
  return ret;
}

}  // namespace cta::objectstore
