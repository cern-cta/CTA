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

#ifndef __XRD_SSI_CTA_SERVICE_PROVIDER_H
#define __XRD_SSI_CTA_SERVICE_PROVIDER_H

#include <XrdSsi/XrdSsiProvider.hh>

#include "scheduler/Scheduler.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"



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
   XrdSsiCtaServiceProvider() {
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
    * Initialize the object for its intended use. This is always called before any other method.
    */

   bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn, const std::string parms,
             int argc, char **argv) override;

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

   cta::Scheduler &getScheduler() { return *m_scheduler_ptr; }

   /*!
    * Get a reference to the Log Context for this Service
    */

   cta::log::LogContext &getLogContext() { return *m_log_context_ptr; }

private:
   std::unique_ptr<cta::catalogue::Catalogue> m_catalogue_ptr;      //!< Catalogue for the Service
   std::unique_ptr<cta::objectstore::Backend> m_backend_ptr;        //!< Backend for the Service
   std::unique_ptr<cta::Scheduler>            m_scheduler_ptr;      //!< Scheduler for the Service
   std::unique_ptr<cta::log::LogContext>      m_log_context_ptr;    //!< Log Context for the Service
};

#endif
