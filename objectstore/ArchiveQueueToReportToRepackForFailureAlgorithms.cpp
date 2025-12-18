/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "ArchiveQueueAlgorithms.hpp"

namespace cta::objectstore {

template<>
const std::string ContainerTraits<ArchiveQueue, ArchiveQueueToReportToRepackForFailure>::c_containerTypeName =
  "ArchiveQueueToReportToRepackForFailure";

template<>
const std::string ContainerTraits<ArchiveQueue, ArchiveQueueToReportToRepackForFailure>::c_identifierType = "tapepool";

template<>
auto ContainerTraits<ArchiveQueue, ArchiveQueueToReportToRepackForFailure>::getContainerSummary(Container& cont)
  -> ContainerSummary {
  ContainerSummary ret;
  ret.JobsSummary::operator=(cont.getJobsSummary());
  return ret;
}

template<>
auto ContainerTraits<ArchiveQueue, ArchiveQueueToReportToRepackForFailure>::getPoppingElementsCandidates(
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

}  // namespace cta::objectstore
