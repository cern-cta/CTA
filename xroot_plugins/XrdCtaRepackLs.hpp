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



namespace cta::xrd {

 /*!
 * Stream object which implements "repack ls" command.
 */
  class RepackLsStream: public XrdCtaStream {
  public:
    
    RepackLsStream(cta::Scheduler& scheduler, cta::catalogue::Catalogue& catalogue, const std::optional<std::string> vid):
    XrdCtaStream(catalogue, scheduler), m_vid(vid) {
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
      std::set<std::string> tapeVids;
    	std::transform(m_repackList.begin(), m_repackList.end(),
                 std::inserter(tapeVids, tapeVids.begin()),
                 [](cta::common::dataStructures::RepackInfo &ri) {return ri.vid;});
      
      cta::common::dataStructures::VidToTapeMap tapeVidMap = m_catalogue.Tape()->getTapesByVid(tapeVids); // throws an exception if a vid does not exist

      for(bool is_buffer_full = false; !m_repackList.empty() && !is_buffer_full; m_repackList.pop_front()){
        Data record;
        auto &repackRequest = m_repackList.front();
        
        uint64_t filesLeftToRetrieve = repackRequest.totalFilesToRetrieve - repackRequest.retrievedFiles;
        uint64_t filesLeftToArchive = repackRequest.totalFilesToArchive - repackRequest.archivedFiles;
        uint64_t totalFilesToRetrieve = repackRequest.totalFilesToRetrieve;
        uint64_t totalFilesToArchive = repackRequest.totalFilesToArchive;
        
        auto repackRequestItem = record.mutable_rels_item();
        repackRequestItem->set_vid(repackRequest.vid);
        repackRequestItem->set_tapepool(tapeVidMap[repackRequest.vid].tapePoolName);
        repackRequestItem->set_repack_buffer_url(repackRequest.repackBufferBaseURL);
        repackRequestItem->set_user_provided_files(repackRequest.userProvidedFiles);
        repackRequestItem->set_total_files_on_tape_at_start(repackRequest.totalFilesOnTapeAtStart);
        repackRequestItem->set_total_bytes_on_tape_at_start(repackRequest.totalBytesOnTapeAtStart);
        repackRequestItem->set_all_files_selected_at_start(repackRequest.allFilesSelectedAtStart);
        repackRequestItem->set_total_files_to_retrieve(totalFilesToRetrieve);
        repackRequestItem->set_total_bytes_to_retrieve(repackRequest.totalBytesToRetrieve);
        repackRequestItem->set_total_files_to_archive(totalFilesToArchive);
        repackRequestItem->set_total_bytes_to_archive(repackRequest.totalBytesToArchive);
        repackRequestItem->set_retrieved_files(repackRequest.retrievedFiles);
        repackRequestItem->set_retrieved_bytes(repackRequest.retrievedBytes);
        repackRequestItem->set_files_left_to_retrieve(filesLeftToRetrieve);
        repackRequestItem->set_files_left_to_archive(filesLeftToArchive);
        repackRequestItem->set_archived_files(repackRequest.archivedFiles);
        repackRequestItem->set_archived_bytes(repackRequest.archivedBytes);
        repackRequestItem->set_failed_to_retrieve_files(repackRequest.failedFilesToRetrieve);
        repackRequestItem->set_failed_to_retrieve_bytes(repackRequest.failedBytesToRetrieve);
        repackRequestItem->set_failed_to_archive_files(repackRequest.failedFilesToArchive);
        repackRequestItem->set_failed_to_archive_bytes(repackRequest.failedBytesToArchive);
        repackRequestItem->set_total_failed_files(repackRequest.failedFilesToRetrieve + repackRequest.failedFilesToArchive);
        repackRequestItem->set_status(toString(repackRequest.status));
        uint64_t repackTime = time(nullptr) - repackRequest.creationLog.time;
          repackRequestItem->set_repack_finished_time(repackRequest.repackFinishedTime);
        if(repackRequest.repackFinishedTime != 0){
          //repackFinishedTime != 0: repack is finished
          repackTime = repackRequest.repackFinishedTime - repackRequest.creationLog.time;
        }
        repackRequestItem->set_repack_time(repackTime);
        repackRequestItem->mutable_creation_log()->set_username(repackRequest.creationLog.username);
        repackRequestItem->mutable_creation_log()->set_host(repackRequest.creationLog.host);
        repackRequestItem->mutable_creation_log()->set_time(repackRequest.creationLog.time);
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
      return streambuf->Size();
    }
  
  private:
    const std::optional<std::string> m_vid;
    std::list<common::dataStructures::RepackInfo> m_repackList;
    static constexpr const char * const LOG_SUFFIX = "RepackLsStream";
  };

} // namespace cta::xrd
