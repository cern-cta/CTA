/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/Timer.hpp"
#include "objectstore/ArchiveQueueAlgorithms.hpp"

namespace cta::objectstore {

// ArchiveQueue full specialisations for ContainerTraits.

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::c_containerTypeName = "ArchiveQueueToTransferForUser";

template<>
const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::c_identifierType = "tapepool";

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::
getContainerSummary(Container& cont) -> ContainerSummary {
  ContainerSummary ret;
  ret.JobsSummary::operator=(cont.getJobsSummary());
  return ret;
}

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::
getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria, ElementsToSkipSet &elemtsToSkip,
  log::LogContext& lc) -> PoppedElementsBatch
{
  PoppedElementsBatch ret;
  auto candidateJobsFromQueue=cont.getCandidateList(unfulfilledCriteria.bytes, unfulfilledCriteria.files, elemtsToSkip, lc);
  for (auto &cjfq: candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement{std::make_unique<ArchiveRequest>(cjfq.address, cont.m_objectStore), cjfq.copyNb, cjfq.size,
    common::dataStructures::ArchiveFile(), "", "", "", "", SchedulerDatabase::ArchiveJob::ReportType::NoReportRequired,
    ArchiveRequest::RepackInfo(), {}});
    ret.summary.bytes += cjfq.size;
    ret.summary.files++;
  }
  return ret;
}

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::
getElementSummary(const PoppedElement& poppedElement) -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.bytes = poppedElement.bytes;
  ret.files = 1;
  return ret;
}

template<>
void ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>::PoppedElementsBatch::
addToLog(log::ScopedParamContainer &params) {
  params.add("bytes", summary.bytes)
        .add("files", summary.files);
}

template<>
auto ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForRepack>::
getContainerSummary(Container& cont) -> ContainerSummary {
  ContainerSummary ret;
  ret.JobsSummary::operator=(cont.getJobsSummary());
  return ret;
}

} // namespace cta::objectstore
