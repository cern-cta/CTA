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
 * Stream object which implements "ta ls" command.
 */
class TapeLsStream: public XrdSsiStream{
public:
  TapeLsStream(cta::catalogue::Catalogue& catalogue, const cta::catalogue::TapeSearchCriteria& searchCriteria):
  XrdSsiStream(XrdSsiStream::isActive),m_catalogue(catalogue),m_searchCriteria(searchCriteria),m_tapeList(catalogue.getTapes(searchCriteria))
  {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapeLsStream() constructor");
  }
  
  virtual ~TapeLsStream(){
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG,LOG_SUFFIX,"~TapeLsStream() destructor");
  }
  
  virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) override {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): XrdSsi buffer fill request (", dlen, " bytes)");

    XrdSsiPb::OStreamBuffer<Data> *streambuf;
    
    try {
      if(m_tapeList.empty()) {
        // Nothing more to send, close the stream
        last = true;
        return nullptr;
      }
      
      streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);
      
      for(bool is_buffer_full = false; !m_tapeList.empty() && !is_buffer_full; m_tapeList.pop_front()){
	Data record;
	auto &tape = m_tapeList.front();
	auto tape_item = record.mutable_tals_item();
	
	tape_item->set_vid(tape.vid);
	tape_item->set_media_type(tape.mediaType);
        tape_item->set_vendor(tape.vendor);
        tape_item->set_logical_library(tape.logicalLibraryName);
        tape_item->set_tapepool(tape.tapePoolName);
        tape_item->set_vo(tape.vo);
        tape_item->set_encryption_key((bool)tape.encryptionKey ? tape.encryptionKey.value() : "-");
        tape_item->set_capacity(tape.capacityInBytes);
        tape_item->set_occupancy(tape.dataOnTapeInBytes);
        tape_item->set_last_fseq(tape.lastFSeq);
        tape_item->set_full(tape.full);
        tape_item->set_disabled(tape.disabled);
	if(tape.labelLog) {
          ::cta::common::TapeLog * labelLog = tape_item->mutable_label_log();
          labelLog->set_drive(tape.labelLog.value().drive);
          labelLog->set_time(tape.labelLog.value().time);
        }
        if(tape.lastWriteLog){
          ::cta::common::TapeLog * lastWriteLog = tape_item->mutable_last_written_log();
          lastWriteLog->set_drive(tape.lastWriteLog.value().drive);
          lastWriteLog->set_time(tape.lastWriteLog.value().time);
        }
        if(tape.lastReadLog){
          ::cta::common::TapeLog * lastReadLog = tape_item->mutable_last_read_log();
          lastReadLog->set_drive(tape.lastReadLog.value().drive);
          lastReadLog->set_time(tape.lastReadLog.value().time);
        }
        ::cta::common::EntryLog * creationLog = tape_item->mutable_creation_log();
        creationLog->set_username(tape.creationLog.username);
        creationLog->set_host(tape.creationLog.host);
        creationLog->set_time(tape.creationLog.time);
        ::cta::common::EntryLog * lastModificationLog = tape_item->mutable_last_modification_log();
        lastModificationLog->set_username(tape.lastModificationLog.username);
        lastModificationLog->set_host(tape.lastModificationLog.host);
        lastModificationLog->set_time(tape.lastModificationLog.time);
	
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
  cta::catalogue::Catalogue &m_catalogue;
  const cta::catalogue::TapeSearchCriteria m_searchCriteria;
  std::list<common::dataStructures::Tape> m_tapeList;
  static constexpr const char * const LOG_SUFFIX = "TapeLsStream";
};
  
}}

