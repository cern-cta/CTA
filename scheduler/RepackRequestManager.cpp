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

#include "RepackRequestManager.hpp"
#include "Scheduler.hpp"
#include "common/make_unique.hpp"
#include "OStoreDB/OStoreDB.hpp"
#include "RepackReportThread.hpp"

namespace cta {
  
void RepackRequestManager::runOnePass(log::LogContext& lc) {
  // We give ourselves a budget of 30s for those operations...
  utils::Timer t;
  log::TimingList timingList;
  // First expand any request to expand
  // TODO: implement expansion
  // Next promote requests to ToExpand if needed
  
  //Putting pending repack request into the RepackQueueToExpand queue
  m_scheduler.promoteRepackRequestsToToExpand(lc);
  
  {  
    //Retrieve the first repack request from the RepackQueueToExpand queue
    std::unique_ptr<cta::RepackRequest> repackRequest = m_scheduler.getNextRepackRequestToExpand();
    if(repackRequest != nullptr){
      //We have a RepackRequest that has the status ToExpand, expand it
      try{
        m_scheduler.expandRepackRequest(repackRequest,timingList,t,lc);
      } catch(const cta::exception::Exception &e){
        lc.log(log::ERR,e.what());
        repackRequest->fail();
        throw(e);
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
    // Do all round of repack subrequest reporting (heavy lifting is done internally).
    /*for(auto& reportBatch: m_scheduler.getRepackReportBatches(lc)){
      reportBatch.report(lc);
    }*/
  }
  
}

} // namespace cta