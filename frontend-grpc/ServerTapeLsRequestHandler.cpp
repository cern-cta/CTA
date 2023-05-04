/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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
 
#include "ServerTapeLsRequestHandler.hpp"
#include "AsyncServer.hpp"
#include "RequestMessage.hpp"
#include "common/log/LogContext.hpp"

/*
 * Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
 */
constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
   return (cmd << 16) + subcmd;
}

cta::frontend::grpc::server::TapeLsRequestHandler::TapeLsRequestHandler(cta::log::Logger& log,
                                                                AsyncServer& asyncServer,
                                                                cta::frontend::rpc::CtaRpcStream::AsyncService& ctaRpcStreamSvc
                                                              ) :
                                                                m_log(log),
                                                                m_tag(this),
                                                                m_asyncServer(asyncServer),
                                                                m_ctaRpcStreamSvc(ctaRpcStreamSvc),
                                                                m_streamState(StreamState::NEW),
                                                                m_writer(&m_ctx)
                                                                {
  
}

cta::frontend::grpc::server::TapeLsRequestHandler::~TapeLsRequestHandler() {
}

bool cta::frontend::grpc::server::TapeLsRequestHandler::next(const bool bOk) {
  bool bNext = false;
  log::LogContext lc(m_log);
  
  // Check the state and report an error
  if(!bOk) {
    switch (m_streamState) {
      case StreamState::PROCESSING:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::server::TapeLsRequestHandler::next(): Server has been shut down before receiving a matching request.");
        }
        break;
      default:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::server::TapeLsRequestHandler::next(): Request processing aborted, call is cancelled or connection is dropped.");
        }
        break;
    }
    return bNext;
  }
  // else everything is OK
  bNext = true;
  
  switch (m_streamState) {
    case StreamState::NEW:
      m_ctaRpcStreamSvc.RequestTapeLs(&m_ctx, &m_request, &m_writer, &m_asyncServer.completionQueue(), &m_asyncServer.completionQueue(), m_tag);
      m_streamState = StreamState::PROCESSING;
      break;
    case StreamState::PROCESSING:
      m_asyncServer.registerHandler<cta::frontend::grpc::server::TapeLsRequestHandler>(m_ctaRpcStreamSvc).next(bOk);
      switch(cmd_pair(m_request.admincmd().cmd(), m_request.admincmd().subcmd())) {
        case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
          {
            log::ScopedParamContainer params(lc);
            params.add("tag", m_tag);
            lc.log(cta::log::INFO, "In grpc::server::TapeLsRequestHandler::next(): Processing tape ls.");
            request::RequestMessage requestMsg(m_request);
            bool bHasAny = false; // set to true if at least one optional option is set

            // Get the search criteria from the optional options

            m_searchCriteria.full            = requestMsg.getOptional(cta::admin::OptionBoolean::FULL,           &bHasAny);
            m_searchCriteria.fromCastor      = requestMsg.getOptional(cta::admin::OptionBoolean::FROM_CASTOR,    &bHasAny);
            m_searchCriteria.capacityInBytes = requestMsg.getOptional(cta::admin::OptionUInt64::CAPACITY,        &bHasAny);
            m_searchCriteria.logicalLibrary  = requestMsg.getOptional(cta::admin::OptionString::LOGICAL_LIBRARY, &bHasAny);
            m_searchCriteria.tapePool        = requestMsg.getOptional(cta::admin::OptionString::TAPE_POOL,       &bHasAny);
            m_searchCriteria.vo              = requestMsg.getOptional(cta::admin::OptionString::VO,              &bHasAny);
            m_searchCriteria.vid             = requestMsg.getOptional(cta::admin::OptionString::VID,             &bHasAny);
            m_searchCriteria.mediaType       = requestMsg.getOptional(cta::admin::OptionString::MEDIA_TYPE,      &bHasAny);
            m_searchCriteria.vendor          = requestMsg.getOptional(cta::admin::OptionString::VENDOR,          &bHasAny);
            m_searchCriteria.purchaseOrder   = requestMsg.getOptional(cta::admin::OptionString::PURCHASE_ORDER,  &bHasAny);
            m_searchCriteria.diskFileIds     = requestMsg.getOptional(cta::admin::OptionStrList::FILE_ID,        &bHasAny);
            auto stateOpt                    = requestMsg.getOptional(cta::admin::OptionString::STATE,           &bHasAny);
            if(stateOpt){
              m_searchCriteria.state = common::dataStructures::Tape::stringToState(stateOpt.value());
            }

            if(!(requestMsg.hasFlag(cta::admin::OptionBoolean::ALL) || bHasAny)) {
              lc.log(cta::log::ERR, "In grpc::server::TapeLsRequestHandler::next(): Must specify at least one search option, or --all.");
              m_response.mutable_header()->set_type(cta::xrd::Response::RSP_ERR_USER);
              m_response.mutable_header()->set_show_header(cta::admin::HeaderType::NONE);
              m_response.mutable_header()->set_message_txt("Must specify at least one search option, or --all.");
              m_streamState = StreamState::ERROR;
            } else if(requestMsg.hasFlag(cta::admin::OptionBoolean::ALL) && bHasAny) {
              lc.log(cta::log::ERR, "In grpc::server::TapeLsRequestHandler::next(): Cannot specify --all together with other search options.");
              m_response.mutable_header()->set_type(cta::xrd::Response::RSP_ERR_USER);
              m_response.mutable_header()->set_show_header(cta::admin::HeaderType::NONE);
              m_response.mutable_header()->set_message_txt("Cannot specify --all together with other search options.");
              m_streamState = StreamState::ERROR;
            } else {
              // OK
              m_response.mutable_header()->set_type(cta::xrd::Response::RSP_SUCCESS);
              m_response.mutable_header()->set_show_header(cta::admin::HeaderType::TAPE_LS);
              m_streamState = StreamState::WRITE;
              m_tapeList = m_asyncServer.catalogue().Tape()->getTapes(m_searchCriteria);
            }
          }
          break;
        default:
          {
            log::ScopedParamContainer params(lc);
            params.add("tag", m_tag);
            lc.log(cta::log::ERR, "In grpc::server::TapeLsRequestHandler::next(): Unrecognized Request message. Possible Protocol Buffer version mismatch between client and server.");
            m_response.mutable_header()->set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
            m_response.mutable_header()->set_show_header(cta::admin::HeaderType::NONE);
            m_response.mutable_header()->set_message_txt("Unrecognized Request message. Possible Protocol Buffer version mismatch between client and server.");
            m_streamState = StreamState::ERROR;
          }
          break;
      }
      m_writer.Write(m_response, m_tag);
      break;
    case StreamState::WRITE:
      for(; !m_tapeList.empty(); m_tapeList.pop_front()) {
        cta::admin::TapeLsItem* pTapeLsItem = m_response.mutable_data()->mutable_tals_item();
        if (!pTapeLsItem) {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::server::TapeLsRequestHandler::next(): Request processing error, TapeLsItem points to null.");
          m_writer.Write(m_response, m_tag); // Wrtie an empty response
          m_streamState = StreamState::ERROR;
          break;
        }

        common::dataStructures::Tape &tape = m_tapeList.front();

        pTapeLsItem->set_vid(tape.vid);
        pTapeLsItem->set_media_type(tape.mediaType);
        pTapeLsItem->set_vendor(tape.vendor);
        pTapeLsItem->set_logical_library(tape.logicalLibraryName);
        pTapeLsItem->set_tapepool(tape.tapePoolName);
        pTapeLsItem->set_vo(tape.vo);
        pTapeLsItem->set_encryption_key_name((bool)tape.encryptionKeyName ? tape.encryptionKeyName.value() : "-");
        pTapeLsItem->set_capacity(tape.capacityInBytes);
        pTapeLsItem->set_occupancy(tape.dataOnTapeInBytes);
        pTapeLsItem->set_last_fseq(tape.lastFSeq);
        pTapeLsItem->set_full(tape.full);
        pTapeLsItem->set_dirty(tape.dirty);
        pTapeLsItem->set_from_castor(tape.isFromCastor);
        pTapeLsItem->set_read_mount_count(tape.readMountCount);
        pTapeLsItem->set_write_mount_count(tape.writeMountCount);
        pTapeLsItem->set_nb_master_files(tape.nbMasterFiles);
        pTapeLsItem->set_master_data_in_bytes(tape.masterDataInBytes);
        pTapeLsItem->set_purchase_order((bool)tape.purchaseOrder ? tape.purchaseOrder.value() : "-");

        if(tape.labelLog) {
          ::cta::common::TapeLog* pLabelLog = pTapeLsItem->mutable_label_log();
          pLabelLog->set_drive(tape.labelLog.value().drive);
          pLabelLog->set_time(tape.labelLog.value().time);
        }
        if(tape.lastWriteLog) {
          ::cta::common::TapeLog* pLastWriteLog = pTapeLsItem->mutable_last_written_log();
          pLastWriteLog->set_drive(tape.lastWriteLog.value().drive);
          pLastWriteLog->set_time(tape.lastWriteLog.value().time);
        }
        if(tape.lastReadLog) {
          ::cta::common::TapeLog* pLastReadLog = pTapeLsItem->mutable_last_read_log();
          pLastReadLog->set_drive(tape.lastReadLog.value().drive);
          pLastReadLog->set_time(tape.lastReadLog.value().time);
        }
        ::cta::common::EntryLog* pCreationLog = pTapeLsItem->mutable_creation_log();
        pCreationLog->set_username(tape.creationLog.username);
        pCreationLog->set_host(tape.creationLog.host);
        pCreationLog->set_time(tape.creationLog.time);
        ::cta::common::EntryLog* pLastModificationLog = pTapeLsItem->mutable_last_modification_log();
        pLastModificationLog->set_username(tape.lastModificationLog.username);
        pLastModificationLog->set_host(tape.lastModificationLog.host);
        pLastModificationLog->set_time(tape.lastModificationLog.time);
        pTapeLsItem->set_comment(tape.comment);
        
        pTapeLsItem->set_state(tape.getStateStr());
        pTapeLsItem->set_state_reason(tape.stateReason ? tape.stateReason.value() : "");
        pTapeLsItem->set_state_update_time(tape.stateUpdateTime);
        pTapeLsItem->set_state_modified_by(tape.stateModifiedBy);
        if (tape.verificationStatus) {
          pTapeLsItem->set_verification_status(tape.verificationStatus.value());
        }
        
        m_writer.Write(m_response, ::grpc::WriteOptions().set_buffer_hint(), m_tag);
      }// end for
      
      if(m_tapeList.empty()) {
        m_streamState = StreamState::FINISH;
        m_writer.Finish(::grpc::Status::OK, m_tag);
      }
      break;
    case StreamState::ERROR:
      m_streamState = StreamState::FINISH;
      m_writer.Finish(::grpc::Status::CANCELLED, m_tag);
      break;
    case StreamState::FINISH:
      bNext = false;
      break;
    default:
      // no default
      break;
  }
  
  return bNext;
}
