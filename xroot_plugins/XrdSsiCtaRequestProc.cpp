/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD SSI Responder class implementation
 * @copyright      Copyright 2017 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <iostream>
#include "../frontend/test_util.h" // for Json output (for debugging)

#include "XrdSsiPbException.h"
#include "XrdSsiPbRequestProc.h"
#include "eos/messages/eos_messages.pb.h"



/*!
 * Process a Notification Request
 */

template <>
void RequestProc<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>::ExecuteAction()
{
   // Output message in Json format (for debugging)

   std::cerr << "Received message:" << std::endl;
   OutputJsonString(&m_request);

#if 0
   // Set reply

   m_response.set_result_code(0);
   m_response.mutable_response()->set_message_text("This is the reply to " + *m_request.mutable_message_text());

   // Output message in Json format (for debugging)

   std::cerr << "Preparing response:" << std::endl;
   OutputJsonString(&m_response);
#endif
}



/*!
 * Prepare a Metadata response
 *
 * A metadata-only reply is appropriate when we just need to send a short response/acknowledgement,
 * as it has less overhead than a full response.
 *
 * The maximum amount of metadata that may be sent is defined by XrdSsiResponder::MaxMetaDataSZ
 * constant member.
 */

template <>
void RequestProc<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>::ExecuteMetadata()
{
   // Set metadata

   m_metadata.set_rsp_type(eos::wfe::Response::XATTR_RSP);

   // Output message in Json format (for debugging)

   std::cerr << "Preparing metadata..." << std::endl;
   OutputJsonString(&m_metadata);
}



/*!
 * Send a message to EOS log or EOS user
 *
 * The SSI framework enforces the following rules:
 *
 * - alerts are sent in the order posted
 * - all outstanding alerts are sent before the final response is sent (i.e. the one posted using a
 *   SetResponse() method)
 * - once a final response is posted, subsequent alert messages are not sent
 * - if a request is cancelled, all pending alerts are discarded
 */

template <>
void RequestProc<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>::ExecuteAlerts()
{
   // Set alert message

   m_alert.set_audience(eos::wfe::Alert::EOSLOG);
   m_alert.set_code(1);
   m_alert.set_message("Something bad happened");

   // Send the alert message

   Alert(m_alert);

   // If we want to allow an arbitrary number of alerts, note that m_alert is not reusable (needs a container)
}



/*!
 * Handle framework errors
 *
 * Framework errors are sent back to EOS in the Metadata response
 */

template <>
void RequestProc<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>::
   HandleError(XrdSsiRequestProcErr err_num, const std::string &err_text)
{
   // Set metadata

   m_metadata.set_rsp_type(eos::wfe::Response::EXCEPTION_RSP);

   switch(err_num)
   {
      case PB_PARSE_ERR:    m_metadata.mutable_exception()->set_code(eos::wfe::Exception::PB_PARSE_ERR);
   }

   m_metadata.mutable_exception()->set_message(err_text);

   // Output message in Json format (for debugging)

   std::cerr << "Preparing error metadata..." << std::endl;
   OutputJsonString(&m_metadata);
}

