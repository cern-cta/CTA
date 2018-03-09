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

#include <XrdSsiPbConfig.hpp>
#include <XrdSsiPbAlert.hpp>
#include <XrdSsiPbService.hpp>
#include <cta_frontend.pb.h>

#include <version.h>
#include <common/make_unique.hpp>
#include <common/log/Logger.hpp>
#include <common/log/SyslogLogger.hpp>
#include <common/log/StdoutLogger.hpp>
#include <common/log/FileLogger.hpp>
#include <rdbms/Login.hpp>
#include <catalogue/CatalogueFactory.hpp>
#include <objectstore/BackendVFS.hpp>

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
   using namespace XrdSsiPb;
   using namespace cta;

   Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "Called Init(", cfgFn, ',', parms, ')');

   // Read CTA namespaced configuration options from XRootD config file
   Config config(cfgFn, "cta");

   // Set XRootD SSI Protobuf logging level
   auto loglevel = config.getOptionList("log");
   if(!loglevel.empty()) {
      Log::SetLogLevel(loglevel);
   } else {
      Log::SetLogLevel("info");
   }

   // Instantiate the CTA logging system
   try {
      std::string loggerURL = m_ctaConf.getConfEntString("Log", "URL", "syslog:");
      if (loggerURL == "syslog:") {
         m_log.reset(new log::SyslogLogger("cta-frontend", log::DEBUG));
      } else if (loggerURL == "stdout:") {
         m_log.reset(new log::StdoutLogger("cta-frontend"));
      } else if (loggerURL.substr(0, 5) == "file:") {
         m_log.reset(new log::FileLogger("cta-frontend", loggerURL.substr(5), log::DEBUG));
      } else {
         throw exception::Exception(std::string("Unknown log URL: ")+loggerURL);
      }
   } catch(exception::Exception &ex) {
      std::string ex_str("Failed to instantiate object representing CTA logging system: ");
      throw exception::Exception(ex_str + ex.getMessage().str());
   }

   const std::list<log::Param> params = {log::Param("version", CTA_VERSION)};
   log::Logger &log = *m_log;

   // Initialise the catalogue
   const rdbms::Login catalogueLogin = rdbms::Login::parseFile("/etc/cta/cta-catalogue.conf");
   const uint64_t nbConns = m_ctaConf.getConfEntInt<uint64_t>("Catalogue", "NumberOfConnections", nullptr);
   const uint64_t nbArchiveFileListingConns = 2;

   m_catalogue = catalogue::CatalogueFactory::create(*m_log, catalogueLogin, nbConns, nbArchiveFileListingConns);

   // Initialise the Backend
   m_backend = std::move(cta::objectstore::BackendFactory::createBackend(m_ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr), *m_log));
   m_backendPopulator = cta::make_unique<cta::objectstore::BackendPopulator>(*m_backend, "Frontend", cta::log::LogContext(*m_log));
   m_scheddb = cta::make_unique<cta::OStoreDBWithAgent>(*m_backend, m_backendPopulator->getAgentReference(), *m_catalogue, *m_log);

   // Initialise the Scheduler
   m_scheduler = cta::make_unique<cta::Scheduler>(*m_catalogue, *m_scheddb, 5, 2*1000*1000);

   try {
      // If the backend is a VFS, make sure we don't delete it on exit
      dynamic_cast<objectstore::BackendVFS &>(*m_backend).noDeleteOnExit();
   } catch (std::bad_cast &) {
      // If not, never mind
   }
  
   // Start the heartbeat thread for the agent object. The thread is guaranteed to have started before we call the unique_ptr deleter
   auto aht = new cta::objectstore::AgentHeartbeatThread(m_backendPopulator->getAgentReference(), *m_backend, *m_log);
   aht->startThread();
   m_agentHeartbeat = std::move(UniquePtrAgentHeartbeatThread(aht));

   // All done
   log(log::INFO, std::string("cta-frontend started"), params);

   return true;
}



/*!
 * Instantiate a Service object
 */

XrdSsiService* XrdSsiCtaServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
   XrdSsiPb::Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "Called GetService(", contact, ',', oHold, ')');

   XrdSsiService *ptr = new XrdSsiPb::Service<cta::xrd::Request, cta::xrd::Response, cta::xrd::Alert>;

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

   XrdSsiPb::Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "QueryResource(", rName, "): ",
                     ((resourcePresence == XrdSsiProvider::isPresent) ? "isPresent" : "notPresent"));

   return resourcePresence;
}

