/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <XrdSsi/XrdSsiProvider.hh>

#include <common/Configuration.hpp>
#include <common/utils/utils.hpp>
#include <xroot_plugins/Namespace.hpp>
#include <XrdSsiPbLog.hpp>
#include <scheduler/Scheduler.hpp>
#include <scheduler/OStoreDB/OStoreDBInit.hpp>

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
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "XrdSsiCtaServiceProvider() constructor");
   }

   virtual ~XrdSsiCtaServiceProvider() {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "~XrdSsiCtaServiceProvider() destructor");
   }

   /*!
    * Initialize the object.
    *
    * This is always called before any other method.
    */
   bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn,
             const std::string parms, int argc, char **argv) override;

   /*!
    * Instantiate a Service object.
    *
    * Called exactly once after initialisation to obtain an instance of an XrdSsiService object
    */
   XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold=256) override;

   /*!
    * Query whether a resource exists on a server.
    *
    * Determines resource availability. Can be called any time the client asks for the resource status.
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
   XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact=0) override;

   /*!
    * Get a reference to the Scheduler DB for this Service
    */
   cta::SchedulerDB_t &getSchedDb() const { return *m_scheddb; }

   /*!
    * Get a reference to the Catalogue for this Service
    */
   cta::catalogue::Catalogue &getCatalogue() const { return *m_catalogue; }

   /*!
    * Get a reference to the Scheduler for this Service
    */
   cta::Scheduler &getScheduler() const { return *m_scheduler; }

   /*!
    * Get the log context for this Service
    */
   cta::log::LogContext getLogContext() const { return cta::log::LogContext(*m_log); }

   /*!
    * Get the maximum file size for an archive request
    */
   uint64_t getArchiveFileMaxSize() const { return m_archiveFileMaxSize; }

   /*!
    * Get the repack buffer URL
    */
   cta::optional<std::string> getRepackBufferURL() const { return m_repackBufferURL; }

   /*!
    * Populate the namespace endpoint configuration from a keytab file
    */
   void setNamespaceMap(const std::string &keytab_file);

   /*!
    * Get the endpoints for namespace queries
    */
   cta::NamespaceMap_t getNamespaceMap() const { return m_namespaceMap; }
   
   const std::string getCatalogueConnectionString() const {return m_catalogue_conn_string; }

private:
   /*!
    * Version of Init() that throws exceptions in case of problems
    */
   void ExceptionThrowingInit(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string &cfgFn,
                              const std::string &parms, int argc, char **argv);

   // Member variables

   std::unique_ptr<cta::catalogue::Catalogue>          m_catalogue;               //!< Catalogue of tapes and tape files
   std::unique_ptr<cta::SchedulerDB_t>                 m_scheddb;                 //!< Scheduler DB for persistent objects (queues and requests)
   std::unique_ptr<cta::SchedulerDBInit_t>             m_scheddb_init;            //!< Wrapper to manage Scheduler DB initialisation
   std::unique_ptr<cta::Scheduler>                     m_scheduler;               //!< The scheduler
   std::unique_ptr<cta::log::Logger>                   m_log;                     //!< The logger

   uint64_t                                            m_archiveFileMaxSize;      //!< Maximum allowed file size for archive requests
   cta::optional<std::string>                          m_repackBufferURL;         //!< The repack buffer URL
   cta::NamespaceMap_t                                 m_namespaceMap;            //!< Endpoints for namespace queries
   std::string                                         m_catalogue_conn_string;   //!< The catalogue connection string (without the password)

   static constexpr const char* const LOG_SUFFIX = "XrdSsiCtaServiceProvider";    //!< Identifier for log messages
};
