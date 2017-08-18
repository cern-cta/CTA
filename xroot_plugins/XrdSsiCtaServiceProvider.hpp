/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD Service Provider class
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

#pragma once

#ifdef XRDSSI_DEBUG
#include <iostream>
#endif

#include <XrdSsi/XrdSsiProvider.hh>

#include "common/Configuration.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "objectstore/BackendFactory.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#include "objectstore/AgentHeartbeatThread.hpp"



/*!
 * Global pointer to the Service Provider object.
 */

extern XrdSsiProvider *XrdSsiProviderServer;



/*!
 * Instantiates a Service to process client requests.
 */

class XrdSsiCtaServiceProvider : public XrdSsiProvider
{
public:
   XrdSsiCtaServiceProvider() : m_ctaConf("/etc/cta/cta-frontend.conf")
   {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] XrdSsiCtaServiceProvider() constructor" << std::endl;
#endif
   }

   virtual ~XrdSsiCtaServiceProvider() {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] ~XrdSsiCtaServiceProvider() destructor" << std::endl;
#endif
   }

   /*!
    * Initialize the object.
    *
    * This is always called before any other method.
    */
   bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn,
             const std::string parms, int argc, char **argv) override;

   /*!
    * Called exactly once after initialisation to obtain an instance of an XrdSsiService object
    */
   XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold=256) override;

   /*!
    * Determine resource availability. Can be called any time the client asks for the resource status.
    */
   XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact=0) override;

   /*!
    * Get a reference to the Scheduler for this Service
    */
   cta::Scheduler &getScheduler() const { return *m_scheduler; }

   /*!
    * Get the log context for this Service
    */
   cta::log::LogContext getLogContext() const { return cta::log::LogContext(*m_log); }

private:
   /*!
    * Deleter for instances of the AgentHeartbeatThread class.
    *
    * As the aht object is initialised by Init() rather than in the constructor, we can't simply call
    * stopAndWaitThread() in the destructor. Using a deleter guarantees that we call stopAndWaitThread()
    * after the AgentHeartbeatThread has been started by Init().
    */
   struct AgentHeartbeatThreadDeleter {
      void operator()(cta::objectstore::AgentHeartbeatThread *aht) {
         aht->stopAndWaitThread();
      }
   };

   /*!
    * Typedef for unique pointer to AgentHeartbeatThread
    */
   typedef std::unique_ptr<cta::objectstore::AgentHeartbeatThread, AgentHeartbeatThreadDeleter>
      UniquePtrAgentHeartbeatThread;

   // Member variables

   cta::common::Configuration                          m_ctaConf;             //!< CTA configuration

   std::unique_ptr<cta::objectstore::Backend>          m_backend;             //!< VFS backend for the objectstore DB
   std::unique_ptr<cta::objectstore::BackendPopulator> m_backendPopulator;    //!< Object used to populate the backend
   std::unique_ptr<cta::OStoreDBWithAgent>             m_scheddb;             //!< DB/Object Store of persistent objects
   std::unique_ptr<cta::catalogue::Catalogue>          m_catalogue;           //!< CTA catalogue of tapes and tape files
   std::unique_ptr<cta::Scheduler>                     m_scheduler;           //!< The scheduler
   std::unique_ptr<cta::log::Logger>                   m_log;                 //!< The logger
   UniquePtrAgentHeartbeatThread                       m_agentHeartbeat;      //!< Agent heartbeat thread
};

