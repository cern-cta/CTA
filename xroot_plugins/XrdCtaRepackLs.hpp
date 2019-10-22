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

#pragma once

#include <XrdSsiPbOStreamBuffer.hpp>
#include <catalogue/Catalogue.hpp>
#include <xrootd/private/XrdSsi/XrdSsiStream.hh>



namespace cta { namespace xrd {
 /*!
 * Stream object which implements "repack ls" command.
 */
  class RepackLsStream: public XrdSsiStream {
  public:
    
    RepackLsStream(cta::Scheduler& scheduler, const cta::optional<std::string> vid):
    XrdSsiStream(XrdSsiStream::isActive),m_scheduler(scheduler), m_vid(vid){
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "RepackLsStream() constructor");
      if(!vid){
	m_repackList = m_scheduler.getRepacks();
      } else {
	m_repackList.push_back(m_scheduler.getRepack(vid.value()));
      }
    }
    
    virtual ~RepackLsStream(){
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG,LOG_SUFFIX,"~RepackLsStream() destructor");
    }
    
    virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo,int &dlen, bool &last) override {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): XrdSsi buffer fill request (", dlen, " bytes)");

      XrdSsiPb::OStreamBuffer<Data> *streambuf;

      try {
	
	if(m_repackList.empty()) {
	  // Nothing more to send, close the stream
	  last = true;
	  return nullptr;
	}
	
	streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);
	
	for(bool is_buffer_full = false; !m_repackList.empty() && !is_buffer_full; m_repackList.pop_front()){
	  Data record;
	  auto &repackRequest = m_repackList.front();
	  auto repackRequestItem = record.mutable_rels_item();
	  repackRequestItem->set_vid(repackRequest.vid);
	  repackRequestItem->set_repack_buffer_url(repackRequest.repackBufferBaseURL);
	  repackRequestItem->set_user_provided_files(repackRequest.userProvidedFiles);
	  repackRequestItem->set_total_files_to_retrieve(repackRequest.totalFilesToRetrieve);
	  repackRequestItem->set_total_bytes_to_retrieve(repackRequest.totalBytesToRetrieve);
	  repackRequestItem->set_total_files_to_archive(repackRequest.totalFilesToArchive);
	  repackRequestItem->set_total_bytes_to_archive(repackRequest.totalBytesToArchive);
	  repackRequestItem->set_retrieved_files(repackRequest.retrievedFiles);
	  repackRequestItem->set_archived_files(repackRequest.archivedFiles);
	  repackRequestItem->set_failed_to_retrieve_files(repackRequest.failedFilesToRetrieve);
	  repackRequestItem->set_failed_to_retrieve_bytes(repackRequest.failedBytesToRetrieve);
	  repackRequestItem->set_failed_to_archive_files(repackRequest.failedFilesToArchive);
	  repackRequestItem->set_failed_to_archive_bytes(repackRequest.failedBytesToArchive);
	  repackRequestItem->set_status(toString(repackRequest.status));
	  //Last expanded fSeq is in reality the next FSeq to Expand. So last one is next - 1
	  repackRequestItem->set_last_expanded_fseq(repackRequest.lastExpandedFseq != 0 ? repackRequest.lastExpandedFseq - 1 : 0);
	  repackRequestItem->mutable_destination_infos()->Clear();
	  for(auto destinationInfo: repackRequest.destinationInfos){
	    auto * destinationInfoToInsert = repackRequestItem->mutable_destination_infos()->Add();
	    destinationInfoToInsert->set_vid(destinationInfo.vid);
	    destinationInfoToInsert->set_files(destinationInfo.files);
	    destinationInfoToInsert->set_bytes(destinationInfo.bytes);
	  }
	  is_buffer_full = streambuf->Push(record);
	}
	
	dlen = streambuf->Size();
	XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): Returning buffer with ", dlen, " bytes of data.");
	
      } catch(cta::exception::Exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught CTA exception: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      delete streambuf;
      } catch(std::exception &ex) {
	std::ostringstream errMsg;
	errMsg << __FUNCTION__ << " failed: " << ex.what();
	eInfo.Set(errMsg.str().c_str(), ECANCELED);
	delete streambuf;
      } catch(...) {
	std::ostringstream errMsg;
	errMsg << __FUNCTION__ << " failed: Caught an unknown exception";
	eInfo.Set(errMsg.str().c_str(), ECANCELED);
	delete streambuf;
      }
      return streambuf;
    }
  
  private:
    cta::Scheduler &m_scheduler;
    const cta::optional<std::string> m_vid;
    std::list<common::dataStructures::RepackInfo> m_repackList;
    static constexpr const char * const LOG_SUFFIX = "RepackLsStream";
  };

}}