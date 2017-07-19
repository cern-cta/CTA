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
      // Send a log message

      eos::wfe::Alert alert_msg;

      alert_msg.set_audience(eos::wfe::Alert::EOSLOG);
      alert_msg.set_message_txt("Something bad happened");

      // Serialize and send the alert message

      Alert(alert_msg);

      // Set the response

      m_metadata.set_type(eos::wfe::Response::RSP_ERR_CTA);
      m_metadata.set_message_txt(ex.what());
   }
}

} // namespace XrdSsiPb

