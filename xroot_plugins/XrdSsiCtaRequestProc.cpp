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

#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/exception/Exception.hpp"

#ifdef XRDSSI_DEBUG
#include "XrdSsiPbDebug.hpp"
#endif
#include "XrdSsiPbException.hpp"
#include "XrdSsiPbResource.hpp"
#include "XrdSsiPbRequestProc.hpp"
#include "xroot_plugins/messages/cta_frontend.pb.h"

#include "XrdSsiCtaServiceProvider.hpp"
#include "XrdSsiCtaEos.hpp"



namespace XrdSsiPb {

/*!
 * Convert a framework exception into a Response
 */
template<>
void ExceptionHandler<cta::xrd::Response, PbException>::operator()(cta::xrd::Response &response, const PbException &ex)
{
   response.set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
   response.set_message_txt(ex.what());
}



/*!
 * Process the Notification Request
 */
template <>
void RequestProc<cta::xrd::Request, cta::xrd::Response, cta::xrd::Alert>::ExecuteAction()
{
   try
   {
      // Perform a capability query on the XrdSsiProviderServer object: it must be a XrdSsiCtaServiceProvider

      XrdSsiCtaServiceProvider *cta_service_ptr;
     
      if(!(cta_service_ptr = dynamic_cast<XrdSsiCtaServiceProvider*>(XrdSsiProviderServer)))
      {
         throw cta::exception::Exception("XRootD Service is not a CTA Service");
      }

#ifdef XRDSSI_DEBUG
      // Output message in Json format

      OutputJsonString(std::cerr, &m_request);
#endif
      // Process EOS Request

      //const cta::eos::Notification &notification = m_request.notification();

      cta::frontend::EosNotificationRequest eos_rq(getClient(m_resource), cta_service_ptr);
      eos_rq.process(m_request.notification(), m_metadata);
   }
   catch(std::exception &ex)
   {
#ifdef XRDSSI_DEBUG
      // Serialize and send a log message

      cta::xrd::Alert alert_msg;

      alert_msg.set_audience(cta::xrd::Alert::LOG);
      alert_msg.set_message_txt("Something bad happened");

      Alert(alert_msg);
#endif
      // Set the response

      m_metadata.set_type(cta::xrd::Response::RSP_ERR_CTA);
      m_metadata.set_message_txt(ex.what());
   }
}

} // namespace XrdSsiPb
