/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "XrdSsiCtaRequestMessage.hpp"

#include "AdminCmdStream.hpp"
#include "frontend/common/AdminCmd.hpp"
#include "frontend/common/PbException.hpp"
#include "frontend/common/WorkflowEvent.hpp"
#include "tools/CtaAdminCmdParser.hpp"

namespace cta::xrd {

void RequestMessage::process(const cta::xrd::Request& request, cta::xrd::Response& response, XrdSsiStream*& stream) {
  // Branch on the Request payload type
  switch (request.request_case()) {
    using namespace cta::xrd;

    case Request::kAdmincmd:
      // Check if repack requests are blocked
      if (!m_service.getFrontendService().getRepackRequestsAllowed()
          && request.admincmd().subcmd() != admin::AdminCmd::SUBCMD_LS
          && request.admincmd().cmd() == admin::AdminCmd::CMD_REPACK) {
        response.set_message_txt("Repack requests are disabled.");
        response.set_type(xrd::Response::RSP_ERR_USER);
        break;
      }

      if (cta::admin::isStreamCmd(request.admincmd())) {
        frontend::AdminCmdStream adminCmdStream(m_service.getFrontendService(),
                                                m_cliIdentity,
                                                request.admincmd(),
                                                stream);
        response = adminCmdStream.process();
      } else {
        frontend::AdminCmd adminCmd(m_service.getFrontendService(), m_cliIdentity, request.admincmd());
        response = adminCmd.process();
      }
      break;

    case Request::kNotification: {
      // Check if WFE requests are blocked
      if (!m_service.getFrontendService().getUserRequestsAllowed()) {
        response.set_message_txt("User requests are disabled.");
        response.set_type(xrd::Response::RSP_ERR_USER);
        break;
      }

      frontend::WorkflowEvent wfe(m_service.getFrontendService(), m_cliIdentity, request.notification());
      response = wfe.process();
      break;
    }  // end case Request::kNotification

    case Request::REQUEST_NOT_SET:
      throw exception::PbException("Request message has not been set.");

    default:
      throw exception::PbException("Unrecognized Request message. "
                                   "Possible Protocol Buffer version mismatch between client and server.");
  }
}

}  // namespace cta::xrd
