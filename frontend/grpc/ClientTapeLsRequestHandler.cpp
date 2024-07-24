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
 
#include "ClientTapeLsRequestHandler.hpp"
#include "common/log/LogContext.hpp"


cta::frontend::grpc::client::TapeLsRequestHandler::TapeLsRequestHandler(cta::log::Logger& log,
                                                                cta::frontend::rpc::CtaRpcStream::Stub& stub,
                                                                ::grpc::CompletionQueue& completionQueue,
                                                                cta::admin::TextFormatter& textFormatter,
                                                                cta::frontend::rpc::AdminRequest& request
                                                              ) :
                                                                m_log(log),
                                                                m_stub(stub),
                                                                m_completionQueue(completionQueue),
                                                                m_textFormatter(textFormatter),
                                                                m_request(request),
                                                                m_tag(this),
                                                                m_streamState(StreamState::NEW)
                                                                {

}

bool cta::frontend::grpc::client::TapeLsRequestHandler::next(const bool bOk) {
  bool bNext = false;
  log::LogContext lc(m_log);
  std::string strErrorMsg;
  
  // Check the state and report an error
  if(!bOk) {
    switch (m_streamState) {
      case StreamState::REQUEST:
      case StreamState::HEADER:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::client::TapeLsRequestHandler::next(): Server has been shut down before receiving a matching request.");
        }
        break;
      case StreamState::READ:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::INFO, "In grpc::client::TapeLsRequestHandler::next(): End of stream.");
        }
        break;
      case StreamState::FINISH:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::INFO, "In grpc::client::TapeLsRequestHandler::next(): Request processing finished.");
        }
        break;
      default:
        {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::INFO, "In grpc::client::TapeLsRequestHandler::next(): Request processing aborted, call is cancelled or connection is dropped.");

        }
        break;
    }
    m_streamState = StreamState::FINISH;
    m_upReader->Finish(&m_grpcStatus, m_tag);
    // return bNext;
    return true;
  }
  // else everything is OK
  bNext = true;
  
  switch (m_streamState) {
    case StreamState::NEW:
      // CMD: prepare TapeLS stream
      m_upReader = m_stub.PrepareAsyncTapeLs(&m_ctx, m_request, &m_completionQueue);
      m_streamState = StreamState::REQUEST;
      m_upReader->StartCall(m_tag);
      break;
    case StreamState::REQUEST:
      m_streamState = StreamState::HEADER;
      m_upReader->Read(&m_response, m_tag);
      break;
    case StreamState::HEADER:
      if (m_response.has_header()) {
        // Handle responses
        switch(m_response.header().type())
        {
          case  cta::xrd::Response::RSP_SUCCESS:
            m_textFormatter.printTapeLsHeader();
            m_streamState = StreamState::READ;
            m_response.Clear();
            m_upReader->Read(&m_response, m_tag);
            break;
          // case cta::xrd::::RSP_ERR_PROTOBUF:      throw XrdSsiPb::PbException(response.message_txt());
          // case cta::xrd::Response::RSP_ERR_USER:  throw XrdSsiPb::UserException(response.message_txt());
          // case cta::xrd::Response::RSP_ERR_CTA:   throw std::runtime_error(response.message_txt());
          case cta::xrd::Response::RSP_ERR_PROTOBUF:
          case cta::xrd::Response::RSP_ERR_USER:
          case cta::xrd::Response::RSP_ERR_CTA:
          default:
            strErrorMsg = m_response.header().message_txt();
            if(strErrorMsg.empty()) {
              strErrorMsg = "Invalid response type";
            }
            {
              log::ScopedParamContainer params(lc);
              params.add("tag", m_tag);
              params.add("errorMessage", strErrorMsg);
              lc.log(cta::log::ERR, "In grpc::client::TapeLsRequestHandler::next(): Response error.");
            }
            strErrorMsg.clear();
            m_streamState = StreamState::FINISH;
            m_upReader->Finish(&m_grpcStatus, m_tag);
        }
      } else {
        log::ScopedParamContainer params(lc);
        params.add("tag", m_tag);
        lc.log(cta::log::ERR, "In grpc::client::TapeLsRequestHandler::next(): Request processing failed, header expected.");
        m_streamState = StreamState::FINISH;
        m_upReader->Finish(&m_grpcStatus, m_tag);
      }
      break;
    case StreamState::READ:
      if (m_response.has_data()) {
        if(m_response.data().data_case() != cta::xrd::Data::kTalsItem) {
          log::ScopedParamContainer params(lc);
          params.add("tag", m_tag);
          lc.log(cta::log::ERR, "In grpc::client::TapeLsRequestHandler::next(): Request processing failed, TapeLsItem expected.");
          m_streamState = StreamState::FINISH;
          m_upReader->Finish(&m_grpcStatus, m_tag);
          break;
        }
        const cta::admin::TapeLsItem& tapeLsItem = m_response.data().tals_item();
        m_textFormatter.print(tapeLsItem);
        m_streamState = StreamState::READ;
        m_response.Clear();
        m_upReader->Read(&m_response, m_tag);
      } else {
        log::ScopedParamContainer params(lc);
        params.add("tag", m_tag);
        lc.log(cta::log::ERR, "In grpc::client::TapeLsRequestHandler::next(): Request processing failed, data chunk expected.");
        m_streamState = StreamState::FINISH;
        m_upReader->Finish(&m_grpcStatus, m_tag);
      }
      break;
    case StreamState::FINISH:
      bNext = false;
      switch(m_grpcStatus.error_code()) {
        case ::grpc::OK:
            {
              log::ScopedParamContainer params(lc);
              params.add("tag", m_tag);
              lc.log(cta::log::INFO, "In grpc::client::TapeLsRequestHandler::next(): Request processing finished.");
            }
            break;
        case ::grpc::CANCELLED:
            {
              log::ScopedParamContainer params(lc);
              params.add("tag", m_tag);
              lc.log(cta::log::ERR, "In grpc::client::TapeLsRequestHandler::next(): Request processing canceled.");
            }
            break;
        default:
            {
              log::ScopedParamContainer params(lc);
              params.add("tag", m_tag);
              params.add("errorMessage", m_grpcStatus.error_message());
              lc.log(cta::log::ERR, "In grpc::client::TapeLsRequestHandler::next(): gRPC Error");
            }
            break;
      }
    default:
      // no default
      break;
  }
  
  return bNext;
}

