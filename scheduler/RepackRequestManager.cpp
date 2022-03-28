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
#include "common/make_unique.hpp"
#include "OStoreDB/OStoreDB.hpp"
#include "RepackReportThread.hpp"
#include "RepackRequestManager.hpp"
#include "Scheduler.hpp"

namespace cta {

void RepackRequestManager::runOnePass(log::LogContext& lc) {
  // We give ourselves a budget of 30s for those operations...
  utils::Timer t;
  log::TimingList timingList;
  // First expand any request to expand
  // Next promote requests to ToExpand if needed

  //Putting pending repack request into the RepackQueueToExpand queue
  m_scheduler.promoteRepackRequestsToToExpand(lc);

  {
    //Retrieve the first repack request from the RepackQueueToExpand queue
    std::unique_ptr<cta::RepackRequest> repackRequest = m_scheduler.getNextRepackRequestToExpand();
    if(repackRequest != nullptr){
      //We have a RepackRequest that has the status ToExpand, expand it
      try{
        try{
          m_scheduler.expandRepackRequest(repackRequest,timingList,t,lc);
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
          throw(e);
        }
      } catch (const cta::exception::NoSuchObject &ex){
        //In case the repack request is deleted during expansion, avoid a segmentation fault of the tapeserver
        lc.log(log::WARNING,"In RepackRequestManager::runOnePass(), RepackRequest object does not exist in the objectstore");
      }
    }
  }

  {
    RetrieveSuccessesRepackReportThread rsrrt(m_scheduler,lc);
    rsrrt.run();
    ArchiveSuccessesRepackReportThread asrrt(m_scheduler,lc);
    asrrt.run();
    RetrieveFailedRepackReportThread rfrrt(m_scheduler,lc);
    rfrrt.run();
    ArchiveFailedRepackReportThread afrrt(m_scheduler,lc);
    afrrt.run();
  }

}

} // namespace cta
