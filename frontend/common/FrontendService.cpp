/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FrontendService.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/runtime/config/parsing/TomlParser.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/telemetry/config/TelemetryConfig.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "version.hpp"

#include <fstream>
#include <opentelemetry/sdk/common/global_log_handler.h>

namespace cta::frontend {

// TODO: Rewrite this once we have std::format
std::optional<std::string> authMethodAsString(const AuthMethod& method) {
  using enum AuthMethod;

  if (method == JWT) {
    return "JWT";
  } else if (method == KERBEROS) {
    return "Kerberos";
  } else if (method == MTLS) {
    return "mTLS";
  }
  return std::nullopt;
}

FrontendService::FrontendService(const std::string& configFilename,
                                 const std::optional<std::string>& mtlsMappingFilename) {
  int logToStdout = 0;
  int logtoFile = 0;
  std::string logFilePath = "";

  // Read CTA namespaced configuration options from XRootD config file
  cta::common::Config config(configFilename);

  if (mtlsMappingFilename.has_value()) {
    // Read MTLS mapping table
    loadMtlsMappingTable(mtlsMappingFilename.value());
  }

  // Instantiate the CTA logging system
  try {
    // Set the logger URL
    auto loggerURL = config.getOptionValueStr("cta.log.url");
    if (!loggerURL.has_value()) {
      loggerURL = "stdout:";
    }
    const auto shortHostname = utils::getShortHostname();

    // Set the logger level
    auto loggerLevelStr = config.getOptionValueStr("cta.log.level");
    auto loggerLevel = loggerLevelStr.has_value() ? log::toLogLevel(loggerLevelStr.value()) : log::INFO;

    // log header, or not
    auto log_header = config.getOptionValueBool("cta.log.log_header");
    bool shortHeader = false;
    if (log_header.has_value()) {
      shortHeader = !log_header.value();
    }

    // Set the log context
    if (loggerURL.value() == "stdout:") {
      m_log = std::make_unique<log::StdoutLogger>(shortHostname, "cta-frontend", shortHeader);
      logToStdout = 1;
    } else if (loggerURL.value().substr(0, 5) == "file:") {
      logtoFile = 1;
      logFilePath = loggerURL.value().substr(5);
      m_log = std::make_unique<log::FileLogger>(shortHostname, "cta-frontend", logFilePath, loggerLevel);
    } else {
      throw exception::UserError(std::string("Unknown log URL: ") + loggerURL.value());
    }

    // Set the logger output format
    auto loggerFormat = config.getOptionValueStr("cta.log.format");
    if (loggerFormat.has_value()) {
      m_log->setLogFormat(loggerFormat.value());
    }
  } catch (exception::Exception& ex) {
    std::string ex_str("Failed to instantiate object representing CTA logging system: ");
    throw exception::Exception(ex_str + ex.getMessage().str());
  }
  log::Logger& log = *m_log;

  // Set static parameters: Instance and backend names
  {
    std::map<std::string, std::string> staticParamMap;
    const auto instanceName = config.getOptionValueStr("cta.instance_name");
    const auto backendName = config.getOptionValueStr("cta.schedulerdb.scheduler_backend_name");

    if (!instanceName.has_value()) {
      throw exception::UserError("cta.instance_name is not set in configuration file " + configFilename);
    }
    m_instanceName = instanceName.value();
    if (!backendName.has_value()) {
      throw exception::UserError("cta.schedulerdb.scheduler_backend_name is not set in configuration file "
                                 + configFilename);
    } else {
      m_schedulerBackendName = backendName.value();
    }

    staticParamMap["instance"] = m_instanceName;
    staticParamMap["sched_backend"] = m_schedulerBackendName;
    log.setStaticParams(staticParamMap);
  }

  {
    // Log starting message
    std::vector<log::Param> params;
    params.emplace_back("version", CTA_VERSION);
    params.emplace_back("configFilename", configFilename);
    params.emplace_back("logToStdout", std::to_string(logToStdout));
    params.emplace_back("logtoFile", std::to_string(logtoFile));
    params.emplace_back("logFilePath", logFilePath);
    log(log::INFO, std::string("Starting cta-frontend"), params);
  }

  // Instantiate telemetry
  // Must be instantiated before the catalogue and scheduler are initialised
  if (auto experimentalTelemetryEnabled = config.getOptionValueBool("cta.experimental.telemetry.enabled");
      experimentalTelemetryEnabled.has_value() && experimentalTelemetryEnabled.value()) {
    try {
      auto retainInstanceIdOnRestart =
        config.getOptionValueBool("cta.telemetry.retain_instance_id_on_restart").value_or(false);
      auto metricsBackend = config.getOptionValueStr("cta.telemetry.metrics.backend").value_or("NOOP");
      auto metricsExportInterval = config.getOptionValueUInt("cta.telemetry.metrics.export.interval").value_or(15000);
      auto metricsExportTimeout = config.getOptionValueUInt("cta.telemetry.metrics.export.timeout").value_or(3000);
      auto metricsOtlpEndpoint = config.getOptionValueStr("cta.telemetry.metrics.otlp.endpoint").value_or("");
      auto metricsOtlpBasicAuthPasswordFile =
        config.getOptionValueStr("cta.telemetry.metrics.otlp.auth.basic.password_file");
      std::string metricsOtlpBasicAuthPassword = "";
      if (metricsOtlpBasicAuthPasswordFile.has_value()) {
        metricsOtlpBasicAuthPassword = cta::telemetry::stringFromFile(metricsOtlpBasicAuthPasswordFile.value());
      }
      auto metricsOtlpBasicAuthUsername =
        config.getOptionValueStr("cta.telemetry.metrics.otlp.auth.basic.username").value_or("");
      auto metricsFileEndpoint = config.getOptionValueStr("cta.telemetry.metrics.file.endpoint")
                                   .value_or("/var/log/cta/cta-frontend-metrics.txt");

      cta::telemetry::TelemetryConfig telemetryConfig =
        cta::telemetry::TelemetryConfigBuilder()
          .serviceName(cta::semconv::attr::ServiceNameValues::kCtaFrontend)
          .serviceNamespace(m_instanceName)
          .serviceVersion(CTA_VERSION)
          .retainInstanceIdOnRestart(retainInstanceIdOnRestart)
          .resourceAttribute(cta::semconv::attr::kSchedulerNamespace, m_schedulerBackendName)
          .metricsBackend(metricsBackend)
          .metricsExportInterval(std::chrono::milliseconds(metricsExportInterval))
          .metricsExportTimeout(std::chrono::milliseconds(metricsExportTimeout))
          .metricsOtlpEndpoint(metricsOtlpEndpoint)
          .metricsOtlpBasicAuth(metricsOtlpBasicAuthUsername, metricsOtlpBasicAuthPassword)
          .metricsFileEndpoint(metricsFileEndpoint)
          .build();
      cta::log::LogContext lc(log);  // temporary log context
      cta::telemetry::initTelemetry(telemetryConfig, lc);
    } catch (exception::Exception& ex) {
      std::vector<cta::log::Param> params = {cta::log::Param(semconv::log::exceptionMessage, ex.getMessage().str())};
      log(log::ERR, "Failed to instantiate OpenTelemetry", params);
      cta::log::LogContext lc(log);
      cta::telemetry::shutdownTelemetry(lc);
    }
  }

  auto missingFileCopiesMinAgeSecs = config.getOptionValueUInt("cta.catalogue.missing_file_copies_min_age_secs");
  m_missingFileCopiesMinAgeSecs = missingFileCopiesMinAgeSecs.value_or(0);

  {
    // Log cta.catalogue.missing_file_copies_min_age_secs
    std::vector<log::Param> params;
    params.emplace_back("source", missingFileCopiesMinAgeSecs.has_value() ? configFilename : "Compile time default");
    params.emplace_back("category", "cta.catalogue");
    params.emplace_back("key", "missingFileCopiesMinAgeSecs");
    params.emplace_back("value", std::to_string(missingFileCopiesMinAgeSecs.value_or(0)));
    log(log::INFO, "Configuration entry", params);
  }

  // Initialise the Catalogue
  std::string catalogueConfigFile = "/etc/cta/cta-catalogue.conf";
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(catalogueConfigFile);
  auto catalogue_numberofconnections = config.getOptionValueInt("cta.catalogue.numberofconnections");
  if (!catalogue_numberofconnections.has_value()) {
    throw exception::UserError("cta.catalogue.numberofconnections is not set in configuration file " + configFilename);
  }
  const uint64_t nbArchiveFileListingConns = 2;

  {
    // Log catalogue.numberofconnections
    std::vector<log::Param> params;
    params.emplace_back("source", configFilename);
    params.emplace_back("category", "cta.catalogue");
    params.emplace_back("key", "numberofconnections");
    params.emplace_back("value", std::to_string(catalogue_numberofconnections.value()));
    log(log::INFO, "Configuration entry", params);
  }
  {
    // Log catalogue number of archive file listing connections
    std::vector<log::Param> params;
    params.emplace_back("source", "Compile time default");
    params.emplace_back("category", "cta.catalogue");
    params.emplace_back("key", "nbArchiveFileListingConns");
    params.emplace_back("value", std::to_string(nbArchiveFileListingConns));
    log(log::INFO, "Configuration entry", params);
  }

  {
    auto catalogueFactory = catalogue::CatalogueFactoryFactory::create(*m_log,
                                                                       catalogueLogin,
                                                                       catalogue_numberofconnections.value(),
                                                                       nbArchiveFileListingConns);
    m_catalogue = catalogueFactory->create();
    try {
      m_catalogue->Schema()->ping();
    } catch (cta::exception::Exception& ex) {
      auto lc = getLogContext();
      lc.log(cta::log::CRIT, ex.getMessageValue());
      throw ex;
    }
  }

  m_catalogue_conn_string = catalogueLogin.connectionString;

  // Initialise the Scheduler DB
  const std::string DB_CONN_PARAM = "cta.objectstore.backendpath";
  auto db_conn = config.getOptionValueStr(DB_CONN_PARAM);
  if (!db_conn.has_value()) {
    throw exception::UserError(DB_CONN_PARAM + " is not set in configuration file " + configFilename);
  }

  {
    // Log cta.objectstore.backendpath
    std::vector<log::Param> params;
    params.emplace_back("source", configFilename);
    params.emplace_back("category", "cta.objectstore");
    params.emplace_back("key", "backendpath");
    params.emplace_back("value", db_conn.value());
    log(log::INFO, "Configuration entry", params);
  }
#ifdef CTA_PGSCHED
  auto db_nb_conns = config.getOptionValueUInt("cta.schedulerdb.numberofconnections");
  m_scheddbInit = std::make_unique<SchedulerDBInit_t>("Frontend", db_conn.value(), db_nb_conns.value_or(2), *m_log);
#else
  m_scheddbInit = std::make_unique<SchedulerDBInit_t>("Frontend", db_conn.value(), *m_log);
#endif

  m_scheddb = m_scheddbInit->getSchedDB(*m_catalogue, *m_log);

  // Set Scheduler DB cache timeouts
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs = m_tapeCacheMaxAgeSecs;
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = m_retrieveQueueCacheMaxAgeSecs;
  m_scheddb->setStatisticsCacheConfig(statisticsCacheConfig);

  /** [[OStoreDB specific]]
   * The osThreadStackSize and osThreadPoolSize variables
   * shall be removed once we decommission OStoreDB
   */
  auto osThreadStackSize = config.getOptionValueInt("cta.schedulerdb.threadstacksize_mb");
  auto osThreadPoolSize = config.getOptionValueInt("cta.schedulerdb.numberofthreads");

  // Log cta.schedulerdb.numberofthreads
  if (osThreadPoolSize.has_value()) {
    std::vector<log::Param> params;
    params.emplace_back("source", configFilename);
    params.emplace_back("category", "cta.schedulerdb");
    params.emplace_back("key", "numberofthreads");
    params.emplace_back("value", std::to_string(osThreadPoolSize.value()));
    log(log::INFO, "Configuration entry", params);
  }

  // Log cta.schedulerdb.threadstacksize_mb
  if (osThreadStackSize.has_value()) {
    std::vector<log::Param> params;
    params.emplace_back("source", configFilename);
    params.emplace_back("category", "cta.schedulerdb");
    params.emplace_back("key", "threadstacksize_mb");
    params.emplace_back("value", std::to_string(osThreadStackSize.value()));
    log(log::INFO, "Configuration entry", params);
  }

  m_scheddb->initConfig(osThreadPoolSize, osThreadStackSize);
  // Initialise the Scheduler
  m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_scheddb, m_schedulerBackendName);

  // Initialise the Frontend
  auto archiveFileMaxSize = config.getOptionValueUInt("cta.archivefile.max_size_gb");
  // Convert archiveFileMaxSize from GB to bytes
  m_archiveFileMaxSize =
    archiveFileMaxSize.has_value() ? static_cast<uint64_t>(archiveFileMaxSize.value()) * 1000 * 1000 * 1000 : 0;

  {
    // Log cta.archivefile.max_size_gb
    std::vector<log::Param> params;
    params.emplace_back("source", archiveFileMaxSize.has_value() ? configFilename : "Compile time default");
    params.emplace_back("category", "cta.archivefile");
    params.emplace_back("key", "max_size_gb");
    params.push_back(log::Param("value", std::to_string(archiveFileMaxSize.value_or(0))));
    log(log::INFO, "Configuration entry", params);
  }

  std::optional<bool> zeroLengthFilesForbidden =
    config.getOptionValueBool("cta.archivefile.zero_length_files_forbidden");
  m_zeroLengthFilesForbidden = zeroLengthFilesForbidden.value_or(true);  // disallow 0-length files by default
  {
    // Log cta.archivefile.zero_length_files_forbidden
    std::vector<log::Param> params;
    params.emplace_back("source", zeroLengthFilesForbidden.has_value() ? configFilename : "Compile time default");
    params.emplace_back("category", "cta.archivefile");
    params.emplace_back("key", "zero_length_files_forbidden");
    params.push_back(
      log::Param("value", config.getOptionValueStr("cta.archivefile.zero_length_files_forbidden").value_or("true")));
    log(log::INFO, "Configuration entry", params);
  }

  m_zeroLengthFilesForbidden_voExceptions =
    config.getOptionValueStrVector("cta.archivefile.zero_length_files_forbidden_vo_exception_list");
  {
    // Log cta.archivefile.zero_length_files_forbidden_vo_exception_list
    std::vector<log::Param> params;
    params.push_back(
      log::Param("source", m_zeroLengthFilesForbidden_voExceptions.empty() ? "Compile time default" : configFilename));
    params.emplace_back("category", "cta.archivefile");
    params.emplace_back("key", "zero_length_files_forbidden_vo_exception_list");
    std::ostringstream oss;
    bool is_first = true;
    for (auto& val : m_zeroLengthFilesForbidden_voExceptions) {
      if (!is_first) {
        oss << ",";
      }
      oss << val;
      is_first = false;
    }
    params.emplace_back("value", oss.str());
    log(log::INFO, "Configuration entry", params);
  }

  // Get the repack buffer URL
  auto repackBufferURLConf = config.getOptionValueStr("cta.repack.repack_buffer_url");
  if (repackBufferURLConf.has_value()) {
    m_repackBufferURL = repackBufferURLConf.value();
  }

  if (auto repackMaxFilesToSelectConf = config.getOptionValueUInt("cta.repack.repack_max_files_to_select");
      repackMaxFilesToSelectConf.has_value()) {
    m_repackMaxFilesToSelect = repackMaxFilesToSelectConf.value();
  }

  // Get the verification mount policy
  if (const auto verificationMountPolicy = config.getOptionValueStr("cta.verification.mount_policy");
      verificationMountPolicy.has_value()) {
    m_verificationMountPolicy = verificationMountPolicy.value();
  }

  {
    // Log cta.repack.repack_buffer_url
    if (repackBufferURLConf.has_value()) {
      std::vector<log::Param> params;
      params.emplace_back("source", configFilename);
      params.emplace_back("category", "cta.repack");
      params.emplace_back("key", "repack_buffer_url");
      params.emplace_back("value", repackBufferURLConf.value());
      log(log::INFO, "Configuration entry", params);
    }
  }

  // Configure admin commands allowed
  {
    std::optional<std::string> adminCmdMode = config.getOptionValueStr("cta.admin_commands.mode");
    m_adminCommandMode = common::toAdminCmdMode(adminCmdMode.value_or("all"));

    std::vector<log::Param> params;
    params.emplace_back("source", adminCmdMode.has_value() ? configFilename : "Compile time default");
    params.emplace_back("category", "cta.admin_commands");
    params.emplace_back("key", "mode");
    params.emplace_back("value", adminCmdMode.value_or("all"));
    log(log::INFO, "Configuration entry", params);
  }

  // Configure workflow events enabled
  {
    std::optional<bool> workflowEventsEnabled = config.getOptionValueBool("cta.workflow_events.enabled");
    m_workflowEventsEnabled = workflowEventsEnabled.value_or(true);

    std::vector<log::Param> params;
    params.emplace_back("source", workflowEventsEnabled.has_value() ? configFilename : "Compile time default");
    params.emplace_back("category", "cta.workflow_events");
    params.emplace_back("key", "enabled");
    params.emplace_back("value", m_workflowEventsEnabled ? "true" : "false");
    log(log::INFO, "Configuration entry", params);
  }

  {
    auto tapeCacheMaxAgeSecsConf = config.getOptionValueUInt("cta.schedulerdb.tape_cache_max_age_secs");
    if (tapeCacheMaxAgeSecsConf.has_value()) {
      m_tapeCacheMaxAgeSecs = tapeCacheMaxAgeSecsConf.value();
      std::vector<log::Param> params;
      params.emplace_back("source", configFilename);
      params.emplace_back("category", "cta.schedulerdb");
      params.emplace_back("key", "tape_cache_max_age_secs");
      params.emplace_back("value", tapeCacheMaxAgeSecsConf.value());
      log(log::INFO, "Configuration entry", params);
    }
  }

  {
    auto retrieveQueueCacheMaxAgeSecsConf =
      config.getOptionValueUInt("cta.schedulerdb.retrieve_queue_cache_max_age_secs");
    if (retrieveQueueCacheMaxAgeSecsConf.has_value()) {
      m_retrieveQueueCacheMaxAgeSecs = retrieveQueueCacheMaxAgeSecsConf.value();
      std::vector<log::Param> params;
      params.emplace_back("source", configFilename);
      params.emplace_back("category", "cta.schedulerdb");
      params.emplace_back("key", "retrieve_queue_cache_max_age_secs");
      params.emplace_back("value", retrieveQueueCacheMaxAgeSecsConf.value());
      log(log::INFO, "Configuration entry", params);
    }
  }

  // Get the mount policy name for verification requests

  // Get the gRPC-specific values, if they are set (getOptionValue returns an std::optional)
  std::optional<bool> tls = config.getOptionValueBool("grpc.tls.enabled");
  m_tls = tls.value_or(false);  // default value is false

  if (auto TlsKey = config.getOptionValueStr("grpc.tls.server_key_path"); TlsKey.has_value()) {
    m_tlsKey = TlsKey.value();
  }
  if (auto TlsCert = config.getOptionValueStr("grpc.tls.server_cert_path"); TlsCert.has_value()) {
    m_tlsCert = TlsCert.value();
  }
  if (auto TlsChain = config.getOptionValueStr("grpc.tls.chain_cert_path"); TlsChain.has_value()) {
    m_tlsChain = TlsChain.value();
  }
  if (auto keytab = config.getOptionValueStr("grpc.keytab"); keytab.has_value()) {
    m_keytab = keytab.value();
  }
  if (auto servicePrincipal = config.getOptionValueStr("grpc.service_principal"); servicePrincipal.has_value()) {
    m_servicePrincipal = servicePrincipal.value();
  }
  if (auto port = config.getOptionValueStr("grpc.port"); port.has_value()) {
    m_port = port.value();
  }
  if (auto threads = config.getOptionValueInt("grpc.numberofthreads"); threads.has_value()) {
    if (threads.value() < 1) {
      throw exception::UserError("value of grpc.numberofthreads must be at least 1");
    }
    m_threads = threads.value();
  }

  if (auto jwksUri = config.getOptionValueStr("grpc.jwks.uri"); jwksUri.has_value()) {
    m_jwksUri = jwksUri.value();
  }

  auto authMethods = config.getOptionValueStrVector("grpc.admin.auth_methods");

  if (authMethods.empty()) {
    log(log::WARNING, "Admin API authentication methods not explicitly set. Defaulting to JWT.");
    authMethods.emplace_back("jwt");
  }

  for (auto& method : authMethods) {
    cta::utils::toLower(method);
    m_adminAuthMethods = {};
    if (method == "jwt") {
      m_adminAuthMethods.emplace(AuthMethod::JWT);
    } else if (method == "kerberos") {
      m_adminAuthMethods.emplace(AuthMethod::KERBEROS);
    } else {
      throw exception::UserError("'" + method + "' is not a valid authorization method (" + configFilename + ")");
    }
  }

  auto optAuthMethod = config.getOptionValueStr("grpc.wfe.auth_method");

  if (!optAuthMethod.has_value()) {
    log(log::WARNING, "WFE authentication method not explicitly set. Defaulting to JWT.");
  }

  auto authMethod = optAuthMethod.value_or("jwt");
  cta::utils::toLower(authMethod);

  if (authMethod == "jwt") {
    m_wfeAuthMethod = AuthMethod::JWT;
  } else if (authMethod == "mtls") {
    m_wfeAuthMethod = AuthMethod::MTLS;
  } else {
    throw exception::UserError("'" + authMethod + "' is not a valid authorization method (" + configFilename + ")");
  }

  auto admin_methods = cta::utils::joinWithMap(m_adminAuthMethods, std::string {", "}, [](const auto& method) {
    return authMethodAsString(method).value();
  });

  log(log::INFO,
      "Using auth methods: " + authMethodAsString(m_wfeAuthMethod).value() + " (WFE) and {" + admin_methods
        + "} (Admin API)");

  const bool usingJWT = usesAdminAuthMethod(AuthMethod::JWT) || m_wfeAuthMethod == AuthMethod::JWT;

  if (!m_tls && usingJWT) {
    throw exception::UserError("JWT is being set up when grpc.tls is set to false in configuration file "
                               + configFilename + ". Cannot use tokens over unencrypted channel, tls must be enabled.");
  }

  if (!m_jwksUri.has_value() && usingJWT) {
    throw exception::UserError("JWT is being setup but no endpoint is provided in grpc.jwks.uri in configuration file "
                               + configFilename);
  }

  auto cacheRefreshInterval = config.getOptionValueInt("grpc.jwks.cache.refresh_interval_secs");
  if (cacheRefreshInterval.has_value() && cacheRefreshInterval.value() < 0) {
    throw exception::UserError("grpc.jwks.cache.refresh_interval_secs is set to a negative value in configuration file "
                               + configFilename);
  }
  m_cacheRefreshInterval = cacheRefreshInterval;

  auto pubkeyTimeout = config.getOptionValueInt("grpc.jwks.cache.timeout_secs");
  if (pubkeyTimeout.has_value() && pubkeyTimeout.value() < 0) {
    throw exception::UserError("grpc.jwks.cache.timeout_secs is set to a negative value in configuration file "
                               + configFilename);
  }
  m_pubkeyTimeout = pubkeyTimeout;

  auto jwksTotalTimeout = config.getOptionValueInt("grpc.jwks.total_timeout");
  if (jwksTotalTimeout.has_value() && jwksTotalTimeout.value() < 0) {
    throw exception::UserError("grpc.jwks.total_timeout is set to a negative value in configuration file "
                               + configFilename);
  }
  m_jwksTotalTimeout = jwksTotalTimeout;

  if (usingJWT) {
    if (!m_cacheRefreshInterval.has_value()) {
      log(log::WARNING, "No value set for grpc.jwks.cache.refresh_interval_secs, using default value");
      m_cacheRefreshInterval = 600;
    }
    if (!m_pubkeyTimeout.has_value()) {
      log(log::WARNING, "No value set for grpc.jwks.cache.timeout_secs, cached public keys will not expire");
      m_pubkeyTimeout = 0;
    }
    if (m_pubkeyTimeout.value() != 0 && m_pubkeyTimeout.value() < m_cacheRefreshInterval.value()) {
      log(log::WARNING,
          "Cannot use a value for grpc.jwks.cache.timeout_secs that is less than grpc.jwks.cache.refresh_interval_secs."
          "Setting timeout_secs equal to cache_refresh_interval_secs.");
      m_pubkeyTimeout = std::optional<int>(m_cacheRefreshInterval.value());
    }
    if (!m_jwksTotalTimeout.has_value()) {
      log(log::INFO, "No value set for grpc.jwks.total_timeout, using default value of 60 seconds");
      m_jwksTotalTimeout = 60;
    }
  }

  // All done
  log(log::INFO, std::string("cta-frontend started"), {log::Param("version", CTA_VERSION)});
}

/**
 * @brief Load the instance -> certificate identity map from its TOML file into memory
 * @param filePath the path to the TOML file
 */
void FrontendService::loadMtlsMappingTable(const std::string& filePath) {
  toml::table tbl;
  try {
    tbl = toml::parse_file(filePath);
  } catch (const toml::parse_error& e) {
    std::ostringstream oss;
    oss << e;
    throw cta::exception::UserError("Failed to parse toml file '" + filePath + "': " + oss.str(), false);
  }

  // get `[aliases]`
  auto aliases = tbl.get("aliases");

  if (!aliases) {
    throw cta::exception::UserError("Invalid config in '" + filePath + "': missing [aliases] section", false);
  }

  // go over each (key, string|array<string>) pair and add them to the table
  aliases->as_table()->for_each([&](const toml::key& key, const auto& val_node) {
    std::set<std::string, std::less<>> elems {};

    if constexpr (toml::is_string<decltype(val_node)>) {
      elems.emplace(val_node);
    } else if constexpr (toml::is_array<decltype(val_node)>) {
      val_node.as_array()->for_each([&elems, filePath](auto&& elem) {
        if constexpr (toml::is_string<decltype(elem)>) {
          elems.emplace(*elem.as_string());
        } else {
          throw cta::exception::UserError("Invalid config in '" + filePath + "': alias identities should be strings");
        }
      });
      m_mtlsMappingTable.try_emplace(std::string {key.str()}, elems);
    } else {
      throw cta::exception::UserError("Invalid config in '" + filePath
                                      + "': alias value should be either a string or an array");
    }
  });
}

/**
  * @brief Look up and identity in the instance -> certificate identity map
  * @param instance the name of the instance to look for in the map
  * @return the set of found certificate identities
**/
std::set<std::string, std::less<>> FrontendService::getMtlsCertIdentitiesForInstance(const std::string& instance) {
  std::set<std::string, std::less<>> cert_identities;
  if (const auto& search = m_mtlsMappingTable.find(instance); search != m_mtlsMappingTable.end()) {
    for (const auto& ident : search->second) {
      cert_identities.insert(ident);
    }
  }

  return cert_identities;
}

}  // namespace cta::frontend
