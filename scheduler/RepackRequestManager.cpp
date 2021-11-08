/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
