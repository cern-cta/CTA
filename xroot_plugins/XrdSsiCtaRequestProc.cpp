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
#include <memory>

#include "XrdSsiPbDebug.hpp" // for Json output
#include "XrdSsiPbException.hpp"
#include "XrdSsiPbRequestProc.hpp"
#include "eos/messages/eos_messages.pb.h"

#include "common/Configuration.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/log/StdoutLogger.hpp"
#include "scheduler/Scheduler.hpp"
#include "objectstore/BackendFactory.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#include "common/make_unique.hpp"



namespace XrdSsiPb {

/*!
 * Process a Notification Request
 */

template <>
void RequestProc<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>::ExecuteAction()
{
   try
   {
      // Instantiate the scheduler

      const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile("/etc/cta/cta_catalogue_db.conf");
      const uint64_t nbConns = 10;
      const uint64_t nbArchiveFileListingConns = 2;

      std::unique_ptr<cta::catalogue::Catalogue> my_catalogue = cta::catalogue::CatalogueFactory::create(catalogueLogin, nbConns, nbArchiveFileListingConns);
      cta::common::Configuration ctaConf("/etc/cta/cta-frontend.conf");
      std::string backend_str = ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr);

      std::unique_ptr<cta::objectstore::Backend> backend(cta::objectstore::BackendFactory::createBackend(backend_str));
      cta::objectstore::BackendPopulator backendPopulator(*backend, "Frontend");
      cta::OStoreDBWithAgent scheddb(*backend, backendPopulator.getAgentReference());

      cta::Scheduler *scheduler = new cta::Scheduler(*my_catalogue, scheddb, 5, 2*1000*1000);
      cta::log::StdoutLogger log("ctafrontend");
      cta::log::LogContext lc(log);

      // Output message in Json format (for debugging)

      std::cerr << "Received message:" << std::endl;
      OutputJsonString(std::cerr, &m_request);

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

      std::string client_username = m_request.cli().user().username();

      // Queue the request

      uint64_t archiveFileId = scheduler->queueArchive(client_username, request, lc);
      std::cout << "<eos::wfe::path::fxattr:sys.archiveFileId>" << archiveFileId << std::endl;

      // Set metadata

      m_metadata.set_type(eos::wfe::Response::RSP_SUCCESS);
   }
   catch(std::exception &ex)
   {
      m_metadata.set_type(eos::wfe::Response::RSP_ERR_CTA);
      m_metadata.set_message_txt(ex.what());
   }

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
   // Output message in Json format (for debugging)

   std::cerr << "Preparing metadata..." << std::endl;
   OutputJsonString(std::cerr, &m_metadata);
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
   eos::wfe::Alert alert_msg;

   alert_msg.set_audience(eos::wfe::Alert::EOSLOG);
   alert_msg.set_message_txt("Something bad happened");

   // Serialize and send the alert message

   Alert(alert_msg);
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

   m_metadata.set_type(eos::wfe::Response::RSP_ERR_CTA);

   //switch(err_num)
   //{
      //case PB_PARSE_ERR:    m_metadata.mutable_exception()->set_code(eos::wfe::Exception::PB_PARSE_ERR);
   //}

   m_metadata.set_message_txt(err_text);

   // Output message in Json format (for debugging)

   std::cerr << "Preparing error metadata..." << std::endl;
   OutputJsonString(std::cerr, &m_metadata);
}

} // namespace XrdSsiPb

