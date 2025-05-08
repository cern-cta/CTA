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

#pragma once

#include <XrdSsiPbOStreamBuffer.hpp>
#include <catalogue/Catalogue.hpp>
#include <xrootd/private/XrdSsi/XrdSsiStream.hh>
#include "cmdline/admin_common/DataItemMessageFill.hpp"


namespace cta::xrd {

 /*!
 * Stream object which implements "repack ls" command.
 */
  class RepackLsStream: public XrdCtaStream {
  public:
    
    RepackLsStream(const frontend::AdminCmdStream& requestMsg, cta::Scheduler& scheduler, cta::catalogue::Catalogue& catalogue, const std::optional<std::string> vid):
    XrdCtaStream(catalogue, scheduler), m_vid(vid), m_instanceName(requestMsg.getInstanceName()) {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "RepackLsStream() constructor");
      if(!vid){
        m_repackList = m_scheduler.getRepacks();
        m_repackList.sort([](cta::common::dataStructures::RepackInfo repackInfo1, cta::common::dataStructures::RepackInfo repackInfo2){
          return repackInfo1.creationLog.time < repackInfo2.creationLog.time;
        });
      } else {
	      m_repackList.push_back(m_scheduler.getRepack(vid.value()));
      }
    }

    /*!
    * Can we close the stream?
    */
    virtual bool isDone() const {
      return m_repackList.empty();
    }
    
    virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) override {
      if(m_repackList.empty()) {
	      // Nothing to send, close the stream
	      return 0;
	    }
      std::set<std::string, std::less<>> tapeVids;
    	std::transform(m_repackList.begin(), m_repackList.end(),
                 std::inserter(tapeVids, tapeVids.begin()),
                 [](cta::common::dataStructures::RepackInfo &ri) {return ri.vid;});
      
      cta::common::dataStructures::VidToTapeMap tapeVidMap = m_catalogue.Tape()->getTapesByVid(tapeVids); // throws an exception if a vid does not exist

      for(bool is_buffer_full = false; !m_repackList.empty() && !is_buffer_full; m_repackList.pop_front()){
        Data record;
        auto &repackRequest = m_repackList.front();
        auto repackRequestItem = record.mutable_rels_item();
        
        fillRepackRequestItem(repackRequest, repackRequestItem, tapeVidMap, m_instanceName);
        
        is_buffer_full = streambuf->Push(record);
      }
      return streambuf->Size();
    }
  
  private:
    const std::optional<std::string> m_vid;
    const std::string m_instanceName;
    std::list<common::dataStructures::RepackInfo> m_repackList;
    static constexpr const char * const LOG_SUFFIX = "RepackLsStream";
  };

} // namespace cta::xrd
