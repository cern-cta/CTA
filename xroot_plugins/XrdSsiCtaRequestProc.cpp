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
 * @param[in,out]    lc              CTA Log Context for this request
 * @param[in]        client          XRootD Client Entity, taken from authentication key for the request
 * @param[in]        notification    EOS WFE Notification message
 * @param[out]       response        Response message to return to EOS
 */
static void requestProcCLOSEW(cta::Scheduler &scheduler, cta::log::LogContext &lc, XrdSsiEntity &client,
   const eos::wfe::Notification &notification, eos::wfe::Response &response)
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

   uint64_t archiveFileId = scheduler.queueArchive(client.name, request, lc);

   // Set archiveFileId in response (deprecated)

   std::string result_str = "<eos::wfe::path::fxattr:sys.archiveFileId>" + std::to_string(archiveFileId);
#ifdef XRDSSI_DEBUG
   std::cerr << result_str << std::endl;
#endif
   response.set_message_txt(result_str);

   // Set response type

   response.set_type(eos::wfe::Response::RSP_SUCCESS);
}



/*!
 * Process the PREPARE workflow event.
 *
 * @param[in,out]    scheduler       CTA Scheduler to queue this request
 * @param[in,out]    lc              Log Context for this request
 * @param[in]        client          XRootD Client Entity, taken from authentication key for the request
 * @param[in]        notification    EOS WFE Notification message
 * @param[out]       response        Response message to return to EOS
 */
static void requestProcPREPARE(cta::Scheduler &scheduler, cta::log::LogContext &lc, XrdSsiEntity &client,
   const eos::wfe::Notification &notification, eos::wfe::Response &response)
{
   // Unpack message

   cta::common::dataStructures::UserIdentity originator;
   originator.name          = notification.cli().user().username();
   originator.group         = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner       = notification.file().owner().username();
   diskFileInfo.group       = notification.file().owner().groupname();
   diskFileInfo.path        = notification.file().lpath();

   cta::common::dataStructures::RetrieveRequest request;
   request.requester        = originator;
   request.dstURL           = notification.transport().url();
   request.diskFileInfo     = diskFileInfo;

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t

   std::string archiveFileIdStr = notification.file().xattr().at("CTA_ArchiveFileId");
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Queue the request

   scheduler.queueRetrieve(client.name, request, lc);

   // Set response type

   response.set_type(eos::wfe::Response::RSP_SUCCESS);
}



/*!
 * Process the DELETE workflow event.
 *
 * @param[in,out]    scheduler       CTA Scheduler to queue this request
 * @param[in,out]    lc              Log Context for this request
 * @param[in]        client          XRootD Client Entity, taken from authentication key for the request
 * @param[in]        notification    EOS WFE Notification message
 * @param[out]       response        Response message to return to EOS
 */
static void requestProcDELETE(cta::Scheduler &scheduler, cta::log::LogContext &lc, XrdSsiEntity &client,
   const eos::wfe::Notification &notification, eos::wfe::Response &response)
{
   // Unpack message

   cta::common::dataStructures::UserIdentity originator;
   originator.name          = notification.cli().user().username();
   originator.group         = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner       = notification.file().owner().username();
   diskFileInfo.group       = notification.file().owner().groupname();
   diskFileInfo.path        = notification.file().lpath();

   cta::common::dataStructures::DeleteArchiveRequest request;
   request.requester        = originator;

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t

   std::string archiveFileIdStr = notification.file().xattr().at("CTA_ArchiveFileId");
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Queue the request

   cta::utils::Timer t;
   scheduler.deleteArchive(client.name, request);

   // Create a log entry

   cta::log::ScopedParamContainer params(lc);
   params.add("fileId", request.archiveFileID).add("catalogueTime", t.secs());
   lc.log(cta::log::INFO, "In requestProcDELETE(): deleted archive file.");

   // Set response type

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
      // Get XRootD client entity

      auto client = getClient(m_resource);

      // Get CTA Scheduler and Log Context

      auto scheduler = cta_service_ptr->getScheduler();
      auto lc = cta_service_ptr->getLogContext();

      // Determine the message type

      switch(m_request.wf().event())
      {
         using namespace eos::wfe;

         case Workflow::CLOSEW:
             requestProcCLOSEW(scheduler, lc, client, m_request, m_metadata); break;

         case Workflow::PREPARE:
             requestProcPREPARE(scheduler, lc, client, m_request, m_metadata); break;

         case Workflow::DELETE:
             requestProcDELETE(scheduler, lc, client, m_request, m_metadata); break;

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
