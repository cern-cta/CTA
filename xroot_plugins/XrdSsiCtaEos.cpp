/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD EOS Notification handler
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

#include "XrdSsiPbException.hpp"
using XrdSsiPb::PbException;

#include "xroot_plugins/messages/cta_frontend.pb.h"
#include "XrdSsiCtaEos.hpp"


#if 0
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/exception/Exception.hpp"

#ifdef XRDSSI_DEBUG
#include "XrdSsiPbDebug.hpp"
#endif
#include "XrdSsiPbResource.hpp"
#include "XrdSsiPbRequestProc.hpp"
#include "xroot_plugins/messages/cta_frontend.pb.h"

#include "XrdSsiCtaServiceProvider.hpp"
#endif



namespace cta { namespace frontend {

void EosNotificationRequest::process(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   switch(notification.wf().event())
   {
      using namespace cta::eos;

      case Workflow::CLOSEW:  requestProcCLOSEW (notification, response); break;
      case Workflow::PREPARE: requestProcPREPARE(notification, response); break;
      case Workflow::DELETE:  requestProcDELETE (notification, response); break;

      default:
         throw PbException("Workflow Event type " + std::to_string(notification.wf().event()) + " is not supported.");
   }
}




void EosNotificationRequest::requestProcCLOSEW(const cta::eos::Notification &notification, cta::xrd::Response &response)
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

   uint64_t archiveFileId = m_scheduler.queueArchive(m_instance_name, request, m_lc);

   // Set archiveFileId in response (deprecated)

   std::string result_str = "<eos::wfe::path::fxattr:sys.archiveFileId>" + std::to_string(archiveFileId);
#ifdef XRDSSI_DEBUG
   std::cerr << result_str << std::endl;
#endif
   response.set_message_txt(result_str);

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void EosNotificationRequest::requestProcPREPARE(const cta::eos::Notification &notification, cta::xrd::Response &response)
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

   m_scheduler.queueRetrieve(m_instance_name, request, m_lc);

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void EosNotificationRequest::requestProcDELETE(const cta::eos::Notification &notification, cta::xrd::Response &response)
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
   m_scheduler.deleteArchive(m_instance_name, request, m_lc);

   // Create a log entry

   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID).add("catalogueTime", t.secs());
   m_lc.log(cta::log::INFO, "In requestProcDELETE(): deleted archive file.");

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

}} // namespace cta::frontend

