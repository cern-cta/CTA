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
 * Convert a framework exception into a Response
 */

template<>
void ExceptionHandler<eos::wfe::Response, PbException>::operator()(eos::wfe::Response &response, const PbException &ex)
{
   response.set_type(eos::wfe::Response::RSP_ERR_PROTOBUF);
   response.set_message_txt(ex.what());
}



/*!
 * Process a Notification Request
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

      // Unpack message

      cta::common::dataStructures::UserIdentity originator;
      originator.name          = m_request.cli().user().username();
      originator.group         = m_request.cli().user().groupname();

      cta::common::dataStructures::DiskFileInfo diskFileInfo;
      diskFileInfo.owner       = m_request.file().owner().username();
      diskFileInfo.group       = m_request.file().owner().groupname();
      diskFileInfo.path        = m_request.file().lpath();

      cta::common::dataStructures::ArchiveRequest request;
      request.checksumType     = m_request.file().cks().name();
      request.checksumValue    = m_request.file().cks().value();
      request.diskFileInfo     = diskFileInfo;
      request.diskFileID       = m_request.file().fid();
      request.fileSize         = m_request.file().size();
      request.requester        = originator;
      request.srcURL           = m_request.wf().instance().url();
      request.storageClass     = m_request.file().xattr().at("CTA_StorageClass");
      request.archiveReportURL = "null:";

      std::string client_username = getClient(m_resource);

      // Queue the request

      auto lc = cta_service_ptr->getLogContext();
      uint64_t archiveFileId = cta_service_ptr->Scheduler().queueArchive(client_username, request, lc);

      // Temporary hack to send back archiveFileId (deprecated)

      std::string result_str = "<eos::wfe::path::fxattr:sys.archiveFileId>" + std::to_string(archiveFileId);
#ifdef XRDSSI_DEBUG
      std::cerr << result_str << std::endl;
#endif
      m_metadata.set_message_txt(result_str);

      // Set metadata

      m_metadata.set_type(eos::wfe::Response::RSP_SUCCESS);
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
