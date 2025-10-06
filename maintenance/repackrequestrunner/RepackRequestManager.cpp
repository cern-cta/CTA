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
#include "RepackReportThread.hpp"
#include "RepackRequestManager.hpp"
#include "scheduler/Scheduler.hpp"

namespace cta::maintenance {

void RepackRequestManager::executeRunner(cta::log::LogContext &lc) {
  // We give ourselves a budget of 30s for those operations...
  utils::Timer t;
  log::TimingList timingList;
  // First expand any request to expand
  // Next promote requests to ToExpand if needed
  lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): before promoteRepackRequestsToToExpand()");

  //Putting pending repack request into the RepackQueueToExpand queue
  m_scheduler.promoteRepackRequestsToToExpand(lc, m_repackMaxRequestsToToExpand);
  lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): after promoteRepackRequestsToToExpand()");

  //Retrieve the first repack request from the RepackQueueToExpand queue
  if(const auto repackRequest = m_scheduler.getNextRepackRequestToExpand(); repackRequest != nullptr) {
    lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): after getNextRepackRequestToExpand()");
    //We have a RepackRequest that has the status ToExpand, expand it
    try{
      try{
        m_scheduler.expandRepackRequest(repackRequest,timingList,t,lc);
        lc.log(log::DEBUG,"In RepackRequestManager::executeRunner(): after expandRepackRequest()");
      } catch (const ExpandRepackRequestException& ex){
        log::ScopedParamContainer spc(lc);
        spc.add("vid",repackRequest->getRepackInfo().vid);
        lc.log(log::ERR,ex.getMessageValue());
        repackRequest->fail();
      } catch (const cta::exception::Exception &e){
        log::ScopedParamContainer spc(lc);
        spc.add("vid",repackRequest->getRepackInfo().vid);
        lc.log(log::ERR,e.getMessageValue());
        repackRequest->fail();
        throw;
      }
    } catch (const cta::exception::NoSuchObject &){
      //In case the repack request is deleted during expansion, avoid a segmentation fault of the tapeserver
      lc.log(log::WARNING,"In RepackRequestManager::executeRunner(), RepackRequest object does not exist in the objectstore");
    }
  }


  RetrieveSuccessesRepackReportThread rsrrt(m_scheduler,lc,m_reportingSoftTimeout);
  rsrrt.run();
  ArchiveSuccessesRepackReportThread asrrt(m_scheduler,lc,m_reportingSoftTimeout);
  asrrt.run();
  RetrieveFailedRepackReportThread rfrrt(m_scheduler,lc,m_reportingSoftTimeout);
  rfrrt.run();
  ArchiveFailedRepackReportThread afrrt(m_scheduler,lc,m_reportingSoftTimeout);
  afrrt.run();
}
} // namespace cta::maintenance
