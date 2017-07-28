/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD Service Provider class implementation
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

#ifdef XRDSSI_DEBUG
#include <iostream>
#endif
#include "common/make_unique.hpp"

#include "rdbms/Login.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "common/Configuration.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#include "objectstore/BackendFactory.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "common/log/StdoutLogger.hpp"

#include "XrdSsiPbAlert.hpp"
#include "XrdSsiPbService.hpp"
#include "eos/messages/eos_messages.pb.h"

#include "XrdSsiCtaServiceProvider.hpp"



/*!
 * Global pointer to the Service Provider object.
 *
 * This must be defined at library load time (i.e. it is a file-level global static symbol). When the
 * shared library is loaded, XRootD initialization fails if the appropriate symbol cannot be found (or
 * it is a null pointer).
 */

XrdSsiProvider *XrdSsiProviderServer = new XrdSsiCtaServiceProvider;



/*!
 * Initialise the Service Provider
 */

bool XrdSsiCtaServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms, int argc, char **argv)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] Called Init(" << cfgFn << "," << parms << ")" << std::endl;
#endif

   const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile("/etc/cta/cta_catalogue_db.conf");
   const uint64_t nbConns = 10;
   const uint64_t nbArchiveFileListingConns = 2;

   std::unique_ptr<cta::catalogue::Catalogue> my_catalogue = cta::catalogue::CatalogueFactory::create(catalogueLogin, nbConns, nbArchiveFileListingConns);
   cta::common::Configuration ctaConf("/etc/cta/cta-frontend.conf");
   std::string backend_str = ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr);

   std::unique_ptr<cta::objectstore::Backend> backend(cta::objectstore::BackendFactory::createBackend(backend_str));
   cta::objectstore::BackendPopulator backendPopulator(*backend, "Frontend");
   cta::OStoreDBWithAgent scheddb(*backend, backendPopulator.getAgentReference());

   // Instantiate the scheduler

   m_scheduler_ptr = cta::make_unique<cta::Scheduler>(*my_catalogue, scheddb, 5, 2*1000*1000);

   // Instantiate the logger

   cta::log::StdoutLogger log("ctafrontend");
   m_log_context_ptr = cta::make_unique<cta::log::LogContext>(log);

   return true;
}



/*!
 * Instantiate a Service object
 */

XrdSsiService* XrdSsiCtaServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] Called GetService(" << contact << "," << oHold << ")" << std::endl;
#endif

   XrdSsiService *ptr = new XrdSsiPb::Service<eos::wfe::Notification, eos::wfe::Response, eos::wfe::Alert>;

   return ptr;
}



/*!
 * Query whether a resource exists on a server.
 *
 * @param[in]    rName      The resource name
 * @param[in]    contact    Used by client-initiated queries for a resource at a particular endpoint.
 *                          It is set to NULL for server-initiated queries.
 *
 * @retval    XrdSsiProvider::notPresent    The resource does not exist
 * @retval    XrdSsiProvider::isPresent     The resource exists
 * @retval    XrdSsiProvider::isPending     The resource exists but is not immediately available. (Useful
 *                                          only in clustered environments where the resource may be
 *                                          immediately available on some other node.)
 */

XrdSsiProvider::rStat XrdSsiCtaServiceProvider::QueryResource(const char *rName, const char *contact)
{
   // We only have one resource

   XrdSsiProvider::rStat resourcePresence = (strcmp(rName, "/ctafrontend") == 0) ?
                                            XrdSsiProvider::isPresent : XrdSsiProvider::notPresent;

#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] XrdSsiCtaServiceProvider::QueryResource(" << rName << "): "
             << ((resourcePresence == XrdSsiProvider::isPresent) ? "isPresent" : "notPresent")
             << std::endl;
#endif

   return resourcePresence;
}

