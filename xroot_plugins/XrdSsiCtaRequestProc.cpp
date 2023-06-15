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

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbRequestProc.hpp>

#include "common/dataStructures/ArchiveRequest.hpp"
#include "frontend/common/PbException.hpp"
#include "XrdSsiCtaServiceProvider.hpp"
#include "XrdSsiCtaRequestMessage.hpp"

namespace XrdSsiPb {

/*!
 * Convert a XrdSsiPb framework exception into a Response
 */
template<>
void ExceptionHandler<cta::xrd::Response, XrdSsiPb::PbException>::operator()(cta::xrd::Response& response,
                                                                             const XrdSsiPb::PbException& ex) {
  response.set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
  response.set_message_txt(ex.what());

  // Also log the error in the XRootD log
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, "ExceptionHandler", ex.what());
}

/*!
 * Convert a CTA Protobuf exception into a Response
 */
template<>
void ExceptionHandler<cta::xrd::Response, cta::exception::PbException>::operator()(
  cta::xrd::Response& response,
  const cta::exception::PbException& ex) {
  response.set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
  response.set_message_txt(ex.what());

  // Also log the error in the XRootD log
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, "ExceptionHandler", ex.what());
}

/*!
 * Process the Notification Request
 */
template<>
void RequestProc<cta::xrd::Request, cta::xrd::Response, cta::xrd::Alert>::ExecuteAction() {
  const std::string ErrorFunction("In RequestProc::ExecuteAction(): ");
  XrdSsiCtaServiceProvider* cta_service_ptr;

  // Perform a capability query (sanity check):
  // the object pointed to by XrdSsiProviderServer must be a XrdSsiCtaServiceProvider
  if (!(cta_service_ptr = dynamic_cast<XrdSsiCtaServiceProvider*>(XrdSsiProviderServer))) {
    // If not, this is fatal unrecoverable error
    throw std::logic_error("XRootD Service is not a CTA Service");
  }
  auto lc = cta_service_ptr->getFrontendService().getLogContext();

  try {
    // Process the message
    cta::xrd::RequestMessage request_msg(*(m_resource.client), cta_service_ptr);
    request_msg.process(m_request, m_metadata, m_response_stream_ptr);
  }
  catch (cta::exception::PbException& ex) {
    m_metadata.set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
    m_metadata.set_message_txt(ex.what());
    lc.log(cta::log::ERR, ErrorFunction + "RSP_ERR_PROTOBUF: " + ex.what());
  }
  catch (cta::exception::UserError& ex) {
    m_metadata.set_type(cta::xrd::Response::RSP_ERR_USER);
    m_metadata.set_message_txt(ex.getMessageValue());
    lc.log(cta::log::ERR, ErrorFunction + "RSP_ERR_USER: " + ex.getMessageValue());
  }
  catch (cta::exception::Exception& ex) {
    m_metadata.set_type(cta::xrd::Response::RSP_ERR_CTA);
    m_metadata.set_message_txt(ex.getMessageValue());
    lc.log(cta::log::ERR, ErrorFunction + "RSP_ERR_CTA: " + ex.what());
  }
  catch (std::runtime_error& ex) {
    m_metadata.set_type(cta::xrd::Response::RSP_ERR_CTA);
    m_metadata.set_message_txt(ex.what());
    lc.log(cta::log::ERR, ErrorFunction + "RSP_ERR_CTA: " + ex.what());
  }
}

}  // namespace XrdSsiPb
