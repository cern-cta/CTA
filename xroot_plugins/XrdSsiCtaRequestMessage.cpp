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

#include "frontend/common/PbException.hpp"
#include "frontend/common/WorkflowEvent.hpp"
#include "frontend/common/AdminCmd.hpp"
#include "AdminCmdStream.hpp"
#include "XrdSsiCtaRequestMessage.hpp"

namespace cta::xrd {

void RequestMessage::process(const cta::xrd::Request& request, cta::xrd::Response& response, XrdSsiStream*& stream)
{
  // Branch on the Request payload type
  switch(request.request_case()) {
    using namespace cta::xrd;

    case Request::kAdmincmd:
      if(frontend::AdminCmdStream::isStreamCmd(request.admincmd())) {
        frontend::AdminCmdStream adminCmdStream(m_service.getFrontendService(), m_cliIdentity, request.admincmd(), stream);
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
    } // end case Request::kNotification

    case Request::REQUEST_NOT_SET:
      throw exception::PbException("Request message has not been set.");

    default:
      throw exception::PbException("Unrecognized Request message. "
        "Possible Protocol Buffer version mismatch between client and server.");
  }
}

} // namespace cta::xrd
