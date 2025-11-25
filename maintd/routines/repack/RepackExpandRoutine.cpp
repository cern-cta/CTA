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

#include "common/exception/NoSuchObject.hpp"
#include "RepackExpandRoutine.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintd {

RepackExpandRoutine::RepackExpandRoutine(cta::log::LogContext& lc, cta::Scheduler& scheduler, int maxRequestsToToExpand)
    : m_lc(lc),
      m_scheduler(scheduler),
      m_repackMaxRequestsToToExpand(maxRequestsToToExpand) {
  log::ScopedParamContainer params(m_lc);
  params.add("maxRequestsToToExpand", maxRequestsToToExpand);
  m_lc.log(cta::log::INFO, "Created RepackExpandRoutine");
}

void RepackExpandRoutine::execute() {
  utils::Timer t;
  log::TimingList timingList;
  // First expand any request to expand
  // Next promote requests to ToExpand if needed

  //Putting pending repack request into the RepackQueueToExpand queue
  m_scheduler.promoteRepackRequestsToToExpand(m_lc, m_repackMaxRequestsToToExpand);

  //Retrieve the first repack request from the RepackQueueToExpand queue
  const auto repackRequest = m_scheduler.getNextRepackRequestToExpand();

  if (repackRequest == nullptr) {
    // Nothing to do
    return;
  }
  //We have a RepackRequest that has the status ToExpand, expand it
  try {
    try {
      m_scheduler.expandRepackRequest(repackRequest, timingList, t, m_lc);
      m_lc.log(log::INFO, "In RepackExpandRoutine::execute(): finished expanding a repack request.");
    } catch (const ExpandRepackRequestException& ex) {
      log::ScopedParamContainer spc(m_lc);
      spc.add("vid", repackRequest->getRepackInfo().vid);
      m_lc.log(log::ERR, ex.getMessageValue());
      repackRequest->fail();
    } catch (const cta::exception::Exception& e) {
      log::ScopedParamContainer spc(m_lc);
      spc.add("vid", repackRequest->getRepackInfo().vid);
      m_lc.log(log::ERR, e.getMessageValue());
      repackRequest->fail();
      throw;
    }
  } catch (const cta::exception::NoSuchObject&) {
    //In case the repack request is deleted during expansion, avoid a segmentation fault
    m_lc.log(log::WARNING, "In RepackExpandRoutine::execute(), RepackRequest object does not exist in the objectstore");
  }
}

std::string RepackExpandRoutine::getName() const {
  return "RepackExpandRoutine";
}

}  // namespace cta::maintd
