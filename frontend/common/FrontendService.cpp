/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FrontendService.hpp"

#include "OperationModes.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/runtime/config/ConfigLoader.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/telemetry/config/TelemetryConfig.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "version.hpp"

#include <fstream>
#include <opentelemetry/sdk/common/global_log_handler.h>

namespace cta::frontend {

std::string toString(AuthMethod method) {
  using enum AuthMethod;
  switch (method) {
    case JWT:
      return "jwt";
    case KERBEROS:
      return "kerberos";
    case MTLS:
      return "mtls";
  }
  throw std::invalid_argument("Invalid AuthMethod value");
}

void FrontendService::loadGrpcConfigParams(const std::string& configFilePath, log::Logger& log) {
  m_grpcConfig = runtime::loadFromToml<grpc::common::GrpcConfig>(configFilePath, false);

  auto grpcConfig = m_grpcConfig.value();

  // first, check that the configuration parameters are consistent between them
  grpcConfig.validate(m_operationMode, log);

  // now, set the auth methods accordingly
  m_authMethods = grpcConfig.auth.getEnabledMethods();
}

std::set<std::string, std::less<>>
FrontendService::getMtlsCertIdentitiesForInstance(const std::string& instance) const {
  std::set<std::string, std::less<>> cert_identities;
  auto mappingTable = m_grpcConfig->auth.mtls->aliases;

  if (const auto& search = mappingTable.find(instance); search != mappingTable.end()) {
    for (const auto& ident : search->second) {
      cert_identities.insert(ident);
    }
  }

  return cert_identities;
}

FrontendService::FrontendService(const std::string& configFilename,
                                 const std::optional<std::string>& grpcConfigFilePath) {
  int logToStdout = 0;
  int logtoFile = 0;
  std::string logFilePath = "";

  // Read CTA namespaced configuration options from XRootD config file
  cta::common::Config config(configFilename);

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
    params.emplace_back(semconv::log::eventName, semconv::log::EventNameValues::kProgramStarting);
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
          .serviceName(cta::semconv::attr::ServiceNameValuesOld::kCtaFrontend)
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

  auto recycleLogQuarantineSecs = config.getOptionValueUInt("cta.catalogue.recycle_log_quarantine_secs");
  m_recycleLogQuarantineSecs = recycleLogQuarantineSecs.value_or(31 * 24 * 60 * 60);

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
    params.emplace_back("value", std::to_string(archiveFileMaxSize.value_or(0)));
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
    params.emplace_back("value",
                        config.getOptionValueStr("cta.archivefile.zero_length_files_forbidden").value_or("true"));
    log(log::INFO, "Configuration entry", params);
  }

  m_zeroLengthFilesForbidden_voExceptions =
    config.getOptionValueStrVector("cta.archivefile.zero_length_files_forbidden_vo_exception_list");
  {
    // Log cta.archivefile.zero_length_files_forbidden_vo_exception_list
    std::vector<log::Param> params;
    params.emplace_back("source",
                        m_zeroLengthFilesForbidden_voExceptions.empty() ? "Compile time default" : configFilename);
    params.emplace_back("category", "cta.archivefile");
    params.emplace_back("key", "zero_length_files_forbidden_vo_exception_list");
    std::ostringstream oss;
    bool is_first = true;
    for (const auto& val : m_zeroLengthFilesForbidden_voExceptions) {
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

  auto operationMode = config.getOptionValue<OperationMode>("cta.operation_mode");

  if (!operationMode.has_value()) {
    throw exception::UserError("cta.operation_mode is not set in configuration file " + configFilename);
  }
  m_operationMode = operationMode.value();

  std::vector<log::Param> params;
  params.emplace_back("category", "cta.operation_mode");
  params.emplace_back("key", "mode");
  params.emplace_back("value", toString(m_operationMode));
  log(log::INFO, "Configuration entry", params);

  log(log::INFO, "Working in " + toString(m_operationMode) + " mode.");

  if (grpcConfigFilePath.has_value()) {
    loadGrpcConfigParams(grpcConfigFilePath.value(), log);
  }

  if (auto tapeCacheMaxAgeSecsConf = config.getOptionValueUInt("cta.schedulerdb.tape_cache_max_age_secs");
      tapeCacheMaxAgeSecsConf.has_value()) {
    m_tapeCacheMaxAgeSecs = tapeCacheMaxAgeSecsConf.value();
    std::vector<log::Param> params;
    params.emplace_back("source", configFilename);
    params.emplace_back("category", "cta.schedulerdb");
    params.emplace_back("key", "tape_cache_max_age_secs");
    params.emplace_back("value", tapeCacheMaxAgeSecsConf.value());
    log(log::INFO, "Configuration entry", params);
  }

  if (auto retrieveQueueCacheMaxAgeSecsConf =
        config.getOptionValueUInt("cta.schedulerdb.retrieve_queue_cache_max_age_secs");
      retrieveQueueCacheMaxAgeSecsConf.has_value()) {
    m_retrieveQueueCacheMaxAgeSecs = retrieveQueueCacheMaxAgeSecsConf.value();
    std::vector<log::Param> params;
    params.emplace_back("source", configFilename);
    params.emplace_back("category", "cta.schedulerdb");
    params.emplace_back("key", "retrieve_queue_cache_max_age_secs");
    params.emplace_back("value", retrieveQueueCacheMaxAgeSecsConf.value());
    log(log::INFO, "Configuration entry", params);
  }

  // All done
  log(log::INFO, std::string("cta-frontend started"), {log::Param("version", CTA_VERSION)});
}
}  // namespace cta::frontend
