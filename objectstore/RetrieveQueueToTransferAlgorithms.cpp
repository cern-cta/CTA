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

namespace cta {
namespace objectstore {

template<>
const std::string ContainerTraits<RetrieveQueue, RetrieveQueueToTransfer>::c_containerTypeName =
  "RetrieveQueueToTransfer";

template<>
auto ContainerTraits<RetrieveQueue, RetrieveQueueToTransfer>::getElementSummary(const PoppedElement& poppedElement)
  -> PoppedElementsSummary {
  PoppedElementsSummary ret;
  ret.bytes = poppedElement.bytes;
  ret.files = 1;
  return ret;
}

template<>
void ContainerTraits<RetrieveQueue, RetrieveQueueToTransfer>::PoppedElementsBatch::addToLog(
  log::ScopedParamContainer& params) const {
  params.add("bytes", summary.bytes).add("files", summary.files);
}

template<>
auto ContainerTraits<RetrieveQueue, RetrieveQueueToTransfer>::getPoppingElementsCandidates(
  Container& cont,
  PopCriteria& unfulfilledCriteria,
  ElementsToSkipSet& elementsToSkip,
  log::LogContext& lc) -> PoppedElementsBatch {
  PoppedElementsBatch ret;

  std::set<std::string> diskSystemsToSkipNames;
  for (auto& ds : unfulfilledCriteria.diskSystemsToSkip) {
    diskSystemsToSkipNames.insert(ds.name);
  }
  auto candidateJobsFromQueue =
    cont.getCandidateList(unfulfilledCriteria.bytes, unfulfilledCriteria.files, elementsToSkip, diskSystemsToSkipNames);
  if (unfulfilledCriteria.diskSystemsToSkip.size() && candidateJobsFromQueue.candidates.empty() &&
      cont.getJobsSummary().jobs && elementsToSkip.empty()) {
    // We failed to find any candidates from a non empty queue, from which there are no individual elements to skip.
    // this means the it is time to sleep this queue.
    // We log and return empty. Caller will stop trying to pop after that.
    cont.setSleepForFreeSpaceStartTimeAndName(::time(nullptr), unfulfilledCriteria.diskSystemsToSkip.begin()->name,
                                              unfulfilledCriteria.diskSystemsToSkip.begin()->sleepTime);
    cont.commit();
    log::ScopedParamContainer params(lc);
    params.add("tapeVid", cont.getVid())
      .add("queueObject", cont.getAddressIfSet())
      .add("diskSystemName", unfulfilledCriteria.diskSystemsToSkip.begin()->name)
      .add("sleepTime", unfulfilledCriteria.diskSystemsToSkip.begin()->sleepTime);
    lc.log(log::WARNING, "In ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>::getPoppingElementsCandidates(): "
                         "sleeping queue due to disk system full.");
    return ret;
  }
  for (auto& cjfq : candidateJobsFromQueue.candidates) {
    ret.elements.emplace_back(PoppedElement {std::make_unique<RetrieveRequest>(cjfq.address, cont.m_objectStore),
                                             cjfq.copyNb, cjfq.size, common::dataStructures::ArchiveFile(),
                                             common::dataStructures::RetrieveRequest(), "",
                                             SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired,
                                             RetrieveRequest::RepackInfo(), cjfq.activity, cjfq.diskSystemName});
    ret.summary.bytes += cjfq.size;
    ret.summary.files++;
  }
  return ret;
}

}  // namespace objectstore
}  // namespace cta
