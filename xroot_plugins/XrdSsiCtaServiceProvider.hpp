/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
class XrdSsiCtaServiceProvider : public XrdSsiProvider {
public:
  XrdSsiCtaServiceProvider() : m_archiveFileMaxSize(0) {
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
  // This method inherits from an external class to this project, so we cannot modify the interface
  bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn,  // cppcheck-suppress passedByValue
    const std::string parms, int argc, char **argv) override;  // cppcheck-suppress passedByValue

  /*!
   * Instantiate a Service object.
   *
   * Called exactly once after initialisation to obtain an instance of an XrdSsiService object
   */
  XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold = 256) override;

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
  XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact = 0) override;

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
  std::optional<std::string> getRepackBufferURL() const { return m_repackBufferURL; }

  /*!
   * Get the verification mount policy
   */
  std::optional<std::string> getVerificationMountPolicy() const { return m_verificationMountPolicy; }


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
  std::optional<std::string>                          m_repackBufferURL;         //!< The repack buffer URL
  std::optional<std::string>                          m_verificationMountPolicy; //!< The mount policy for verification requests
  cta::NamespaceMap_t                                 m_namespaceMap;            //!< Endpoints for namespace queries
  std::string                                         m_catalogue_conn_string;   //!< The catalogue connection string (without the password)

  static constexpr const char* const LOG_SUFFIX = "XrdSsiCtaServiceProvider";    //!< Identifier for log messages
};
