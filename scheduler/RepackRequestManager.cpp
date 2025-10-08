/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/NoSuchObject.hpp"
#include "RepackReportThread.hpp"
#include "RepackRequestManager.hpp"
#include "Scheduler.hpp"

namespace cta {

void RepackRequestManager::runOnePass(log::LogContext &lc,
                                      const size_t repackMaxRequestsToExpand) {
  // We give ourselves a budget of 30s for those operations...
  utils::Timer t;
  log::TimingList timingList;
  // First expand any request to expand
  // Next promote requests to ToExpand if needed
  lc.log(log::DEBUG,"In RepackRequestManager::runOnePass(): before promoteRepackRequestsToToExpand()");

  //Putting pending repack request into the RepackQueueToExpand queue
  m_scheduler.promoteRepackRequestsToToExpand(lc, repackMaxRequestsToExpand);
  lc.log(log::DEBUG,"In RepackRequestManager::runOnePass(): after promoteRepackRequestsToToExpand()");

  //Retrieve the first repack request from the RepackQueueToExpand queue
  if(const auto repackRequest = m_scheduler.getNextRepackRequestToExpand(); repackRequest != nullptr) {
    lc.log(log::DEBUG,"In RepackRequestManager::runOnePass(): after getNextRepackRequestToExpand()");
    //We have a RepackRequest that has the status ToExpand, expand it
    try{
      try{
        m_scheduler.expandRepackRequest(repackRequest,timingList,t,lc);
        lc.log(log::DEBUG,"In RepackRequestManager::runOnePass(): after expandRepackRequest()");
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
      lc.log(log::WARNING,"In RepackRequestManager::runOnePass(), RepackRequest object does not exist in the objectstore");
    }
  }


  RetrieveSuccessesRepackReportThread rsrrt(m_scheduler,lc);
  rsrrt.run();
  ArchiveSuccessesRepackReportThread asrrt(m_scheduler,lc);
  asrrt.run();
  RetrieveFailedRepackReportThread rfrrt(m_scheduler,lc);
  rfrrt.run();
  ArchiveFailedRepackReportThread afrrt(m_scheduler,lc);
  afrrt.run();
}
} // namespace cta
