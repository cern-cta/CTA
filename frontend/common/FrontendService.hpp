/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AuthMethod.hpp"
#include "OperationModes.hpp"
#include "common/config/Config.hpp"
#include "frontend/grpc/common/GrpcConfig.hpp"
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

class FrontendService {
public:
  explicit FrontendService(const std::string& configFilename, const std::optional<std::string>& grpcConfigFilePath);

  FrontendService(const FrontendService&) = delete;

  ~FrontendService() = default;

  /**
   * @brief Configure the authentication methods for the Admin API and related settings
   * @param configFileName The name of the configuration file (for error messages)
   * @param authConfigFileName The name of the (TOML) auth configuration file
   * @param config The configuration object
   * @param log A logger
   */
  void loadAdminAuthConfigParams(const std::string& configFileName,
                                 const std::string& authConfigFileName,
                                 const cta::common::Config& config,
                                 log::Logger& log);

  /**
   * @brief Configure the authentication method for WFE mode and related settings
   * @param configFileName The name of the configuration file (for logging/errors)
   * @param authConfigFileName The name of the (TOML) auth configuration file
   * @param config The configuration object
   * @param log A logger
   */
  void loadWFEAuthConfigParams(const std::string& configFileName,
                               const std::string& authConfigFileName,
                               const cta::common::Config& config,
                               log::Logger& log);

  /**
   * @brief Load the GRPC config parameters
   * @param configFileName The name of the configuration file (for logging/errors)
   * @param log A logger
   */
  void loadGrpcConfigParams(const std::string& configFilePath, log::Logger& log);

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
 * Get the minimum quarantine period, in seconds, before a tape can be reclaimed.
 */
  uint64_t getRecycleLogQuarantineSecs() const { return m_recycleLogQuarantineSecs; }

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
   * Get the TlsKey
   */
  std::optional<grpc::common::GrpcConfig> getGrpcConfig() const { return m_grpcConfig; }

  /*
   * Get the instanceName from config file
   */
  const std::string& getInstanceName() const { return m_instanceName; }

  /*
   * Check whether a particular auth method is used by this frontend
   */
  bool usesAuthMethod(const AuthMethod method) const {
    return std::ranges::find(m_authMethods, method) != std::end(m_authMethods);
  }

  /*
   * Get the auth methods used by this frontend
   */
  const std::set<AuthMethod, std::less<>>& getAuthMethods() const { return m_authMethods; }

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

  std::optional<grpc::common::GrpcConfig>       m_grpcConfig;                   //!< gRPC configuration information

  uint64_t                                      m_missingFileCopiesMinAgeSecs;  //!< Missing tape file copies minimum age.
  uint64_t                                      m_recycleLogQuarantineSecs;     //!< Minimum quarantine period before tape reclaim
  std::string                                   m_instanceName;                 //!< value of cta.instance_name in the CTA frontend configuration file
  std::set<AuthMethod, std::less<>>             m_authMethods;                  //!< The authentication methods which are currently set for this frontend
  // clang-format on
};

}  // namespace cta::frontend
