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
#include "ArchiveQueueAlgorithms.hpp"

namespace cta { namespace objectstore {
  template<>
  const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>::c_containerTypeName = "ArchiveQueueToReportToRepackForSuccess";
  
  template<>
  const std::string ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>::c_identifierType = "tapepool";
  
  template<>
  auto ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>::
  getContainerSummary(Container& cont) -> ContainerSummary {
    ContainerSummary ret;
    ret.JobsSummary::operator=(cont.getJobsSummary());
    return ret;
  }
  
    template<>
  auto ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>::
  getPoppingElementsCandidates(Container& cont, PopCriteria& unfulfilledCriteria, ElementsToSkipSet& elemtsToSkip,
    log::LogContext& lc) -> PoppedElementsBatch
  {
    PoppedElementsBatch ret;
    auto candidateJobsFromQueue=cont.getCandidateList(std::numeric_limits<uint64_t>::max(), unfulfilledCriteria.files, elemtsToSkip, lc);
    for (auto &cjfq: candidateJobsFromQueue.candidates) {
      ret.elements.emplace_back(PoppedElement());
      PoppedElement & elem = ret.elements.back();
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

}}