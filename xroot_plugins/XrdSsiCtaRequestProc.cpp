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
#include "eos/messages/eos_messages.pb.h"

#include "XrdSsiCtaServiceProvider.hpp"



namespace XrdSsiPb {

/*!
 * Process the CLOSEW workflow event
 *
 * @param[in,out]    scheduler       CTA Scheduler to queue this request
 * @param[in,out]    lc              Log Context for this request
 * @param[in]        cli_username    Client username, from authentication key for the request
 * @param[in]        notification    EOS WFE Notification message
 * @param[out]       response        Response message to return to EOS
 */
static void requestProcCLOSEW(cta::Scheduler &scheduler, cta::log::LogContext &lc,
   const std::string &cli_username, const eos::wfe::Notification &notification, eos::wfe::Response &response)
{
   // Unpack message

   cta::common::dataStructures::UserIdentity originator;
   originator.name          = notification.cli().user().username();
   originator.group         = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner       = notification.file().owner().username();
   diskFileInfo.group       = notification.file().owner().groupname();
   diskFileInfo.path        = notification.file().lpath();

   cta::common::dataStructures::ArchiveRequest request;
   request.checksumType     = notification.file().cks().name();
   request.checksumValue    = notification.file().cks().value();
   request.diskFileInfo     = diskFileInfo;
   request.diskFileID       = notification.file().fid();
   request.fileSize         = notification.file().size();
   request.requester        = originator;
   request.srcURL           = notification.wf().instance().url();
   request.storageClass     = notification.file().xattr().at("CTA_StorageClass");
   request.archiveReportURL = "null:";

   // Queue the request

   uint64_t archiveFileId = scheduler.queueArchive(cli_username, request, lc);

   // Return archiveFileId (deprecated)

   std::string result_str = "<eos::wfe::path::fxattr:sys.archiveFileId>" + std::to_string(archiveFileId);
#ifdef XRDSSI_DEBUG
   std::cerr << result_str << std::endl;
#endif
   response.set_message_txt(result_str);

   // Set metadata

   response.set_type(eos::wfe::Response::RSP_SUCCESS);
}



/*!
 * Convert a framework exception into a Response
 */
template<>
void ExceptionHandler<eos::wfe::Response, PbException>::operator()(eos::wfe::Response &response, const PbException &ex)
{
   response.set_type(eos::wfe::Response::RSP_ERR_PROTOBUF);
   response.set_message_txt(ex.what());
}



/*!
 * Process the Notification Request
 */
template <>
void RequestProc<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>::ExecuteAction()
{
   try
   {
      // Perform a capability query on the XrdSsiProviderServer object: it must be a XrdSsiCtaServiceProvider

      XrdSsiCtaServiceProvider *cta_service_ptr;
     
      if(!(cta_service_ptr = dynamic_cast<XrdSsiCtaServiceProvider *>(XrdSsiProviderServer)))
      {
         throw cta::exception::Exception("XRootD Service is not a CTA Service");
      }

#ifdef XRDSSI_DEBUG
      // Output message in Json format

      OutputJsonString(std::cerr, &m_request);
#endif
      // Get client username

      std::string cli_username = getClient(m_resource);

      // Get CTA Scheduler and Log Context

      auto scheduler = cta_service_ptr->getScheduler();
      auto lc = cta_service_ptr->getLogContext();

      // Determine the message type

      switch(m_request.wf().event())
      {
         using namespace eos::wfe;

         case(Workflow::CLOSEW):
            requestProcCLOSEW(scheduler, lc, cli_username, m_request, m_metadata);
            break;
         case(Workflow::PREPARE):
         case(Workflow::DELETE):
         default:
            throw PbException("Workflow Event type " + std::to_string(m_request.wf().event()) + " is not supported.");
      }

   }
   catch(std::exception &ex)
   {
#ifdef XRDSSI_DEBUG
      // Serialize and send a log message

      eos::wfe::Alert alert_msg;

      alert_msg.set_audience(eos::wfe::Alert::EOSLOG);
      alert_msg.set_message_txt("Something bad happened");

      Alert(alert_msg);
#endif
      // Set the response

      m_metadata.set_type(eos::wfe::Response::RSP_ERR_CTA);
      m_metadata.set_message_txt(ex.what());
   }
}

} // namespace XrdSsiPb
