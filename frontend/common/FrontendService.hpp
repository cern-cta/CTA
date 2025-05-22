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

  FrontendService(const FrontendService&) = delete;

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

  /**
   * Getting the configured scheduler backend name
   *
   * @return value of cta.schedulerdb.scheduler_backend_name from the CTA frontend config file
   */
  const std::string& getSchedulerBackendName() const {
      return m_schedulerBackendName;
  }

  /*!
   * Get missing tape file copies minimum age
   */
  uint64_t getMissingFileCopiesMinAgeSecs() const { return m_missingFileCopiesMinAgeSecs; }

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
   * Check if 0-length files are disallowed
   */
  bool getDisallowZeroLengthFiles() const { return m_zeroLengthFilesForbidden; };

  /*!
   * If 0-length files are disallowed, get the tape pools that are exempt from this restriction
   */
  const std::vector<std::string>& getDisallowZeroLengthFilesExemptions() const {
    return m_zeroLengthFilesForbidden_voExceptions;
  };

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

  /*
   * Get the TLS value
   */
  bool getTls() const { return m_tls; }

  /*
   * Get the TlsKey
   */
  const std::optional<std::string> getTlsKey() const { return m_tlsKey; }

  /*
   * Get the TlsCert
   */
  const std::optional<std::string> getTlsCert() const { return m_tlsCert; }

  /*
   * Get the TlsChain
   */
  const std::optional<std::string> getTlsChain() const { return m_tlsChain; }

  /*
   * Get the gRPC server port
   */
  const std::optional<std::string> getPort() const { return m_port; }

  /*
   * Get the number of threads
   */
  const std::optional<int> getThreads() const { return m_threads; }

  /*
   * Get the instanceName from config file
   */
  const std::string& getInstanceName() const { return m_instanceName; }

  /*
   * Get the url to query for the public keys
   */
  const std::optional<std::string> getJwksUri() const { return m_jwksUri; }

private:
  /*!
   * Set the verification mount policy
   */
  void setVerificationMountPolicy(std::string_view verificationMountPolicy) {
    m_verificationMountPolicy = verificationMountPolicy;
  }

  /*!
   * Populate the namespace endpoint configuration from a keytab file
   */
  void setNamespaceMap(const std::string& keytab_file);

  // Member variables
  // clang-format off
  std::unique_ptr<cta::log::Logger>             m_log;                          //!< The logger
  std::unique_ptr<cta::catalogue::Catalogue>    m_catalogue;                    //!< Catalogue of tapes and tape files
  std::unique_ptr<SchedulerDBInit_t>            m_scheddbInit;                  //!< Persistent initialiser object for Scheduler DB
  std::unique_ptr<cta::SchedulerDB_t>           m_scheddb;                      //!< Scheduler DB for persistent objects (queues and requests)
  std::unique_ptr<cta::Scheduler>               m_scheduler;                    //!< The scheduler

  bool                                          m_acceptRepackRequests;         //!< Flag to allow the processing of repack requests
  bool                                          m_acceptUserRequests;           //!< Flag to allow the processing of user requests
  std::optional<uint64_t>                       m_tapeCacheMaxAgeSecs;          //!< Option to override the tape cache timeout value in the scheduler DB
  std::optional<uint64_t>                       m_retrieveQueueCacheMaxAgeSecs; //!< Option to override the retrieve queue timeout value in the scheduler DB
  std::string                                   m_catalogue_conn_string;        //!< The catalogue connection string (without the password)
  std::string                                   m_schedulerBackendName;         //!< value of cta.schedulerdb.scheduler_backend_name in the CTA frontend configuration file
  uint64_t                                      m_archiveFileMaxSize;           //!< Maximum allowed file size for archive requests
  bool                                          m_zeroLengthFilesForbidden;     //!< Flag to explicitly reject the 0-length files by default
  std::vector<std::string>                      m_zeroLengthFilesForbidden_voExceptions; //!< If 0-length files are rejected by default, do not apply check to these VOs
  std::optional<std::string>                    m_repackBufferURL;              //!< The repack buffer URL
  std::optional<uint64_t>                       m_repackMaxFilesToSelect;       //!< The max number of files to expand during a repack
  std::string                                   m_verificationMountPolicy;      //!< The mount policy for verification requests
  cta::NamespaceMap_t                           m_namespaceMap;                 //!< Endpoints for namespace queries  // gRPC-frontend specific variables
  std::optional<std::string>                    m_port;                         //!< The port for the gRPC server
  std::optional<int>                            m_threads;                      //!< The number of threads used by the gRPC server
  bool                                          m_tls;                          //!< Use TLS encryption for gRPC
  std::optional<std::string>                    m_tlsKey;                       //!< The TLS service key file
  std::optional<std::string>                    m_tlsCert;                      //!< The TLS service certificate file
  std::optional<std::string>                    m_tlsChain;                     //!< The TLS CA chain file
  uint64_t                                      m_missingFileCopiesMinAgeSecs;  //!< Missing tape file copies minimum age.
  std::string                                   m_instanceName;               //!< value of cta.instance_name in the CTA frontend configuration file
  std::optional<std::string>                    m_jwksUri;                      //!< The endpoint to obtain public keys from, for validating tokens
  // clang-format on
};

}  // namespace cta::frontend
