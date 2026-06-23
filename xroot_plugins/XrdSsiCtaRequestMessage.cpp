/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "XrdSsiCtaRequestMessage.hpp"

#include "AdminCmdStream.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "frontend/common/AdminCmd.hpp"
#include "frontend/common/PbException.hpp"
#include "frontend/common/WorkflowEvent.hpp"
#include "tools/CtaAdminCmdParser.hpp"

namespace cta::xrd {

using cta::common::dataStructures::SecurityIdentity;

RequestMessage::RequestMessage(const XrdSsiEntity& client, const XrdSsiCtaServiceProvider* service)
    : m_service(*service) {
  std::string protoStr {client.prot};
  SecurityIdentity::Protocol proto;

  if (protoStr == "krb5") {
    proto = SecurityIdentity::Protocol::KRB5;
  } else if (protoStr == "sss") {
    proto = SecurityIdentity::Protocol::SSS;
  } else {
    throw exception::PbException("Unrecognized protocol in XrdSsiEntity: " + protoStr);
  }

  m_cliIdentity = SecurityIdentity(client.name, cta::utils::getShortHostname(), client.host, proto);
}

void RequestMessage::process(const cta::xrd::Request& request,
                             cta::xrd::Response& response,
                             XrdSsiStream*& stream) const {
  // Branch on the Request payload type
  switch (request.request_case()) {
    using namespace cta::xrd;

    case Request::kAdmincmd:
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
