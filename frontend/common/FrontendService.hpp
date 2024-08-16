/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "scheduler/Scheduler.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif
#include "Namespace.hpp"

namespace cta::frontend {

class FrontendService {
public:
  explicit FrontendService(const std::string& configFilename);

  FrontendService(const FrontendService &) = delete;

  ~FrontendService() = default;

  /*!
   * Get the log context
   */
  cta::log::LogContext getLogContext() const { return cta::log::LogContext(*m_log); }

  /*!
   * Get the Catalogue connection string
   */
  const std::string& getCatalogueConnString() const { return m_catalogue_conn_string; }

  /*!
   * Get a reference to the Catalogue
   */
  cta::catalogue::Catalogue& getCatalogue() const { return *m_catalogue; }

  /*!
   * Get a reference to the Scheduler DB
   */
  cta::SchedulerDB_t& getSchedDb() const { return *m_scheddb; }

  /*!
   * Get a reference to the Scheduler
   */
  cta::Scheduler& getScheduler() const { return *m_scheduler; }

  /*!
   * Get a reference to the Scheduler
   */
  bool getUserRequestsAllowed() const { return m_acceptUserRequests; }

  /*!
   * Get a reference to the Scheduler
   */
  bool getRepackRequestsAllowed() const { return m_acceptRepackRequests; }

  /*!
   * Get the maximum file size for an archive request
   */
  uint64_t getArchiveFileMaxSize() const { return m_archiveFileMaxSize; }

  /*!
   * Get the repack buffer URL
   */
  std::optional<std::string> getRepackBufferURL() const { return m_repackBufferURL; }

  /*!
   * Get the repacm max files to expand
   */
  std::optional<std::uint64_t> getRepackMaxFilesToSelect() const { return m_repackMaxFilesToSelect; }

  /*!
   * Get the verification mount policy
   */
  const std::string& getVerificationMountPolicy() const { return m_verificationMountPolicy; }

  /*!
   * Get the endpoints for namespace queries
   */
  const cta::NamespaceMap_t& getNamespaceMap() const { return m_namespaceMap; }

private:
  /*!
   * Set the verification mount policy
   */
  void setVerificationMountPolicy(const std::string& verificationMountPolicy) { m_verificationMountPolicy = verificationMountPolicy; }

  /*!
   * Populate the namespace endpoint configuration from a keytab file
   */
  void setNamespaceMap(const std::string& keytab_file);

  // Member variables
  std::unique_ptr<cta::log::Logger>             m_log;                     //!< The logger
  std::unique_ptr<cta::catalogue::Catalogue>    m_catalogue;               //!< Catalogue of tapes and tape files
  std::unique_ptr<SchedulerDBInit_t>            m_scheddbInit;             //!< Persistent initialiser object for Scheduler DB
  std::unique_ptr<cta::SchedulerDB_t>           m_scheddb;                 //!< Scheduler DB for persistent objects (queues and requests)
  std::unique_ptr<cta::Scheduler>               m_scheduler;               //!< The scheduler

  bool                                          m_acceptRepackRequests;    //!< Flag to allow the processing of repack requests
  bool                                          m_acceptUserRequests;      //!< Flag to allow the processing of user requests
  std::optional<uint64_t>                       m_tapeCacheMaxAgeSecs;          //!< Option to override the tape cache timeout value in the scheduler DB
  std::optional<uint64_t>                       m_retrieveQueueCacheMaxAgeSecs; //!< Option to override the retrieve queue timeout value in the scheduler DB
  std::string                                   m_catalogue_conn_string;   //!< The catalogue connection string (without the password)
  uint64_t                                      m_archiveFileMaxSize;      //!< Maximum allowed file size for archive requests
  std::optional<std::string>                    m_repackBufferURL;         //!< The repack buffer URL
  std::optional<uint64_t>                       m_repackMaxFilesToSelect;  //!< The max number of files to expand during a repack
  std::string                                   m_verificationMountPolicy; //!< The mount policy for verification requests
  cta::NamespaceMap_t                           m_namespaceMap;            //!< Endpoints for namespace queries
};

} // namespace cta::frontend
