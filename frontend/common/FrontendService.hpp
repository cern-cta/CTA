/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AuthMethod.hpp"
#include "OperationModes.hpp"
#include "common/config/Config.hpp"
#include "scheduler/Scheduler.hpp"

#include <stdexcept>
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

namespace cta::frontend {

using MapOfSets = std::map<std::string, std::set<std::string, std::less<>>, std::less<>>;

/**
 * @brief Convert an AuthMethod to its string representation
 * @param method The enum value
 * @return A string representing the method
 */
std::string toString(AuthMethod method);

/**
 * @brief Wrapper for JWT configuration options
 */
struct JWTConfig {
  // clang-format off
  std::string m_jwksUri;       //!< The endpoint to obtain public keys from, for validating tokens
  int m_cacheRefreshInterval;  //!< The number of seconds after which to update the cache of public keys used to sign JWT tokens
  int m_pubkeyTimeout;         //!< The number of seconds after which to update the cache entry for a cached key
  int m_jwksTotalTimeout;      //!< The total timeout in seconds for JWKS endpoint (default 60)
  // clang-format on
};

class FrontendService {
public:
  explicit FrontendService(const std::string& configFilename,
                           const bool inGrpcMode,
                           const std::optional<std::string>& mtlsMappingFilename);

  FrontendService(const FrontendService&) = delete;

  ~FrontendService() = default;

  /**
   * @brief Configure the authentication methods for the Admin API and related settings
   * @param configFileName The name of the configuration file (for error messages)
   * @param config The configuration object
   * @param log A logger
   */
  void
  loadAdminAuthConfigParams(const std::string& configFileName, const cta::common::Config& config, log::Logger& log);

  /**
   * @brief Configure the authentication method for WFE mode and related settings
   * @param configFileName The name of the configuration file (for logging/errors)
   * @param mtlsMappingFilename The optional filename of the MTLS mapping table (required if MTLS authentication is enabled)
   * @param config The configuration object
   * @param log A logger
   */
  void loadWFEAuthConfigParams(const std::string& configFileName,
                               const std::optional<std::string>& mtlsMappingFilename,
                               const cta::common::Config& config,
                               log::Logger& log);

  /**
   * @brief Load the instance -> certificate identity map from its TOML file into memory
   * @param filePath the path to the TOML file
   */
  void loadMtlsMappingTable(const std::string& filePath);

  /**
   * @brief Load the GRPC config parameters
   * @param configFileName The name of the configuration file (for logging/errors)
   * @param config The configuration object
   * @param log A logger
   */
  void loadGrpcConfigParams(const std::string& configFileName, const cta::common::Config& config, log::Logger& log);

  /**
   * @brief Load the JWT config parameters
   * @param configFileName The name of the configuration file (for logging/errors)
   * @param config The configuration object
   * @param log A logger
   */
  void loadJWTConfigParams(const std::string& configFileName, const cta::common::Config& config, log::Logger& log);

  /**
    * @brief Look up and identity in the instance -> certificate identity map
    * @param instance the name of the instance to look for in the map
    * @return the set of found certificate identities
    */
  std::set<std::string, std::less<>> getMtlsCertIdentitiesForInstance(const std::string& instance) const;

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
  const std::string& getSchedulerBackendName() const { return m_schedulerBackendName; }

  /*!
   * Get missing tape file copies minimum age
   */
  uint64_t getMissingFileCopiesMinAgeSecs() const { return m_missingFileCopiesMinAgeSecs; }

  /*!
   * Get the frontend's operation mode (wfe / admin_*)
   */
  OperationMode getOperationMode() const { return m_operationMode; }

  /*!
   * Get a reference to the Scheduler
   */
  //bool getRepackRequestsAllowed() const { return m_acceptRepackCommands; }

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

  /*
   * Get the TLS value
   */
  bool getTls() const { return m_tls; }

  /*
   * Get the TlsKey
   */
  std::optional<std::string> getTlsKey() const { return m_tlsKey; }

  /*
   * Get the TlsCert
   */
  std::optional<std::string> getTlsCert() const { return m_tlsCert; }

  /*
   * Get the TlsChain
   */
  std::optional<std::string> getTlsChain() const { return m_tlsChain; }

  /*
   * Get the gRPC server port
   */
  std::optional<std::string> getPort() const { return m_port; }

  /*
   * Get the gRPC server keytab
   */
  std::optional<std::string> getKeytab() const { return m_keytab; }

  /*
   * Get the gRPC server service principal
   */
  std::optional<std::string> getServicePrincipal() const { return m_servicePrincipal; }

  /*
   * Get the number of threads
   */
  std::optional<int> getThreads() const { return m_threads; }

  /*
   * Get the JWT Config
   */
  std::optional<JWTConfig> getJwtConfig() const { return m_jwtConfig; }

  /*
   * Get the instanceName from config file
   */
  const std::string& getInstanceName() const { return m_instanceName; }

  /*
   * Get the authentication method which is configured for the WFE
   */
  AuthMethod getWfeAuthMethod() const { return m_wfeAuthMethod; }

  /*
   * Get the authentication method which is configured for the Admin API
   */
  const std::set<AuthMethod, std::less<>>& getAdminAuthMethods() const { return m_adminAuthMethods; }

  /*
   * Check whether a particular auth method is used by the Admin API
   */
  bool usesAdminAuthMethod(const AuthMethod method) const {
    return std::ranges::find(m_adminAuthMethods, method) != std::end(m_adminAuthMethods);
  }

private:
  /*!
   * Set the verification mount policy
   */
  void setVerificationMountPolicy(std::string_view verificationMountPolicy) {
    m_verificationMountPolicy = verificationMountPolicy;
  }

  // Member variables
  // clang-format off
  std::unique_ptr<cta::log::Logger>             m_log;                          //!< The logger
  std::unique_ptr<cta::catalogue::Catalogue>    m_catalogue;                    //!< Catalogue of tapes and tape files
  std::unique_ptr<SchedulerDBInit_t>            m_scheddbInit;                  //!< Persistent initialiser object for Scheduler DB
  std::unique_ptr<cta::SchedulerDB_t>           m_scheddb;                      //!< Scheduler DB for persistent objects (queues and requests)
  std::unique_ptr<cta::Scheduler>               m_scheduler;                    //!< The scheduler
  OperationMode                                 m_operationMode;                //!< Which operation mode (wfe / admin_*) is being used
  std::optional<uint64_t>                       m_tapeCacheMaxAgeSecs;          //!< Option to override the tape cache timeout value in the scheduler DB
  std::optional<uint64_t>                       m_retrieveQueueCacheMaxAgeSecs; //!< Option to override the retrieve queue timeout value in the scheduler DB
  std::string                                   m_catalogue_conn_string;        //!< The catalogue connection string (without the password)
  std::string                                   m_schedulerBackendName;         //!< value of cta.schedulerdb.scheduler_backend_name in the CTA frontend configuration file
  uint64_t                                      m_archiveFileMaxSize = 0;       //!< Maximum allowed file size for archive requests
  bool                                          m_zeroLengthFilesForbidden;     //!< Flag to explicitly reject the 0-length files by default
  std::vector<std::string>                      m_zeroLengthFilesForbidden_voExceptions; //!< If 0-length files are rejected by default, do not apply check to these VOs
  std::optional<std::string>                    m_repackBufferURL;              //!< The repack buffer URL
  std::optional<uint64_t>                       m_repackMaxFilesToSelect;       //!< The max number of files to expand during a repack
  std::string                                   m_verificationMountPolicy;      //!< The mount policy for verification requests

  // GRPC Options
  std::optional<std::string>                    m_port;                         //!< The port for the gRPC server
  std::optional<std::string>                    m_keytab;                       //!< The keytab file to be used for Kerberos authentication by the gRPC server
  std::optional<std::string>                    m_servicePrincipal;             //!< The service principal to be used for Kerberos authentication by the gRPC server
  std::optional<int>                            m_threads;                      //!< The number of threads used by the gRPC server
  bool                                          m_tls;                          //!< Use TLS encryption for gRPC
  std::optional<std::string>                    m_certMap;                      //!< The file that maps certificates to clients for authorization purposes
  std::optional<std::string>                    m_tlsKey;                       //!< The TLS service key file
  std::optional<std::string>                    m_tlsCert;                      //!< The TLS service certificate file
  std::optional<std::string>                    m_tlsChain;                     //!< The TLS CA chain file
  std::optional<JWTConfig>                      m_jwtConfig;                     //!< The JWT configuration parameters

  uint64_t                                      m_missingFileCopiesMinAgeSecs;  //!< Missing tape file copies minimum age.
  std::string                                   m_instanceName;                 //!< value of cta.instance_name in the CTA frontend configuration file
  AuthMethod                                    m_wfeAuthMethod;                //!< The authentication method which is currently set for the WFE
  std::set<AuthMethod, std::less<>>             m_adminAuthMethods;             //!< The authentication methods which are currently set for the Admin API
  MapOfSets                                     m_mtlsMappingTable;             //!< Table which maps instance name -> certificate identities
  // clang-format on
};

}  // namespace cta::frontend
