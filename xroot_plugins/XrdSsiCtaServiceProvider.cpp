/*
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

#include "cta_frontend.pb.h"
#include "common/make_unique.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/utils/utils.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "objectstore/BackendVFS.hpp"
#include "rdbms/Login.hpp"
#include "version.h"
#include "XrdSsiCtaServiceProvider.hpp"
#include "XrdSsiPbAlert.hpp"
#include "XrdSsiPbConfig.hpp"
#include "XrdSsiPbService.hpp"

/*
 * Global pointer to the Service Provider object.
 *
 * This must be defined at library load time (i.e. it is a file-level global static symbol). When the
 * shared library is loaded, XRootD initialization fails if the appropriate symbol cannot be found (or
 * it is a null pointer).
 */
XrdSsiProvider *XrdSsiProviderServer = new XrdSsiCtaServiceProvider;

bool XrdSsiCtaServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn,
                                    const std::string parms, int argc, char **argv)
{
   try {
      ExceptionThrowingInit(logP, clsP, cfgFn, parms, argc, argv);
      return true;
   } catch(XrdSsiPb::XrdSsiException &ex) {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): XrdSsiPb::XrdSsiException ", ex.what());
   } catch(cta::exception::Exception &ex) {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): cta::exception::Exception ", ex.what());
   } catch(std::exception &ex) {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): std::exception ", ex.what());
   } catch(...) {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): unknown exception");
   }

   // XRootD has a bug where if we return false here, it triggers a SIGABRT. Until this is fixed, exit(1) as a workaround.
   exit(1);

   return false;
}

void XrdSsiCtaServiceProvider::ExceptionThrowingInit(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string &cfgFn,
                                                     const std::string &parms, int argc, char **argv)
{
   using namespace XrdSsiPb;
   using namespace cta;

   Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "Called Init(", cfgFn, ',', parms, ')');

   // Read CTA namespaced configuration options from XRootD config file
   Config config(cfgFn);

   // Set XRootD SSI Protobuf logging level
   auto loglevel = config.getOptionList("cta.log.ssi");
   if(!loglevel.empty()) {
      Log::SetLogLevel(loglevel);
   } else {
      Log::SetLogLevel("info");
   }

   // Instantiate the CTA logging system
   try {
      // Set the logger URL
      auto loggerURL = config.getOptionValueStr("cta.log.url");
      if(!loggerURL.first) loggerURL.second = "syslog:";
      const auto shortHostname = utils::getShortHostname();

      // Set the logger level
      int loggerLevel = log::INFO;
      auto loggerLevelStr = config.getOptionValueStr("cta.log.level");
      if(loggerLevelStr.first) loggerLevel = log::toLogLevel(loggerLevelStr.second);

      if (loggerURL.second == "syslog:") {
         m_log.reset(new log::SyslogLogger(shortHostname, "cta-frontend", loggerLevel));
      } else if (loggerURL.second == "stdout:") {
         m_log.reset(new log::StdoutLogger(shortHostname, "cta-frontend"));
      } else if (loggerURL.second.substr(0, 5) == "file:") {
         m_log.reset(new log::FileLogger(shortHostname, "cta-frontend", loggerURL.second.substr(5), loggerLevel));
      } else {
         throw exception::Exception(std::string("Unknown log URL: ") + loggerURL.second);
      }
   } catch(exception::Exception &ex) {
      std::string ex_str("Failed to instantiate object representing CTA logging system: ");
      throw exception::Exception(ex_str + ex.getMessage().str());
   }

   const std::list<log::Param> params = {log::Param("version", CTA_VERSION)};
   log::Logger &log = *m_log;

   // Initialise the catalogue
   const rdbms::Login catalogueLogin = rdbms::Login::parseFile("/etc/cta/cta-catalogue.conf");
   auto catalogue_numberofconnections = config.getOptionValueInt("cta.catalogue.numberofconnections");
   if(!catalogue_numberofconnections.first) {
      throw exception::Exception("cta.catalogue.numberofconnections is not set in configuration file " + cfgFn);
   }
   const uint64_t nbArchiveFileListingConns = 2;

   m_catalogue = catalogue::CatalogueFactory::create(*m_log, catalogueLogin, catalogue_numberofconnections.second, nbArchiveFileListingConns);

   // Initialise the Backend
   auto objectstore_backendpath = config.getOptionValueStr("cta.objectstore.backendpath");
   if(!objectstore_backendpath.first) {
      throw exception::Exception("cta.objectstore.backendpath is not set in configuration file " + cfgFn);
   }
   m_backend = std::move(cta::objectstore::BackendFactory::createBackend(objectstore_backendpath.second, *m_log));
   m_backendPopulator = cta::make_unique<cta::objectstore::BackendPopulator>(*m_backend, "Frontend", cta::log::LogContext(*m_log));
   m_scheddb = cta::make_unique<cta::OStoreDBWithAgent>(*m_backend, m_backendPopulator->getAgentReference(), *m_catalogue, *m_log);
   auto threadPoolSize = config.getOptionValueInt("cta.schedulerdb.numberofthreads");
   if (threadPoolSize.first) {
     m_scheddb->setThreadNumber(threadPoolSize.second);
   }
   m_scheddb->setBottomHalfQueueSize(25000);

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
}

XrdSsiService* XrdSsiCtaServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
   XrdSsiPb::Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "Called GetService(", contact, ',', oHold, ')');

   XrdSsiService *ptr = new XrdSsiPb::Service<cta::xrd::Request, cta::xrd::Response, cta::xrd::Alert>;

   return ptr;
}

XrdSsiProvider::rStat XrdSsiCtaServiceProvider::QueryResource(const char *rName, const char *contact)
{
   // We only have one resource

   XrdSsiProvider::rStat resourcePresence = (strcmp(rName, "/ctafrontend") == 0) ?
                                            XrdSsiProvider::isPresent : XrdSsiProvider::notPresent;

   XrdSsiPb::Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "QueryResource(", rName, "): ",
                     ((resourcePresence == XrdSsiProvider::isPresent) ? "isPresent" : "notPresent"));

   return resourcePresence;
}
