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

#include "version.h"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/config/Config.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/config/TelemetryConfig.hpp"
#include "common/semconv/Attributes.hpp"
#include <opentelemetry/sdk/common/global_log_handler.h>

#include "rdbms/Login.hpp"

#include "FrontendService.hpp"

#include <fstream>

namespace cta::frontend {

FrontendService::FrontendService(const std::string& configFilename) : m_archiveFileMaxSize(0) {
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
      throw exception::UserError("cta.schedulerdb.scheduler_backend_name is not set in configuration file " +
                                 configFilename);
    } else {
      m_schedulerBackendName = backendName.value();
    }

    staticParamMap["instance"] = m_instanceName;
    staticParamMap["sched_backend"] = m_schedulerBackendName;
    log.setStaticParams(staticParamMap);
  }

  {
    // Log starting message
    std::list<log::Param> params;
    params.push_back(log::Param("version", CTA_VERSION));
    params.push_back(log::Param("configFilename", configFilename));
    params.push_back(log::Param("logToStdout", std::to_string(logToStdout)));
    params.push_back(log::Param("logtoFile", std::to_string(logtoFile)));
    params.push_back(log::Param("logFilePath", logFilePath));
    log(log::INFO, std::string("Starting cta-frontend"), params);
  }

  // Instantiate telemetry
  // Must be instantiated before the catalogue and scheduler are initialised
  auto experimentalTelemetryEnabled = config.getOptionValueBool("cta.experimental.telemetry.enabled");
  if (experimentalTelemetryEnabled.has_value() && experimentalTelemetryEnabled.value()) {
    try {
      // If we stick with this config parsing, would be nice to have the option to provide a default/fallback value

      auto retainInstanceIdOnRestart = config.getOptionValueBool("cta.telemetry.retain_instance_id_on_restart");
      if (!retainInstanceIdOnRestart.has_value()) {
        retainInstanceIdOnRestart = false;
      }
      auto metricsBackend = config.getOptionValueStr("cta.telemetry.metrics.backend");
      if (!metricsBackend.has_value()) {
        metricsBackend = "NOOP";
      }
      auto metricsExportInterval = config.getOptionValueUInt("cta.telemetry.metrics.export.interval");
      if (!metricsExportInterval.has_value()) {
        metricsExportInterval = 15000;
      }
      auto metricsExportTimeout = config.getOptionValueUInt("cta.telemetry.metrics.export.timeout");
      if (!metricsExportTimeout.has_value()) {
        metricsExportTimeout = 3000;
      }
      auto metricsOtlpEndpoint = config.getOptionValueStr("cta.telemetry.metrics.export.otlp.endpoint");
      if (!metricsOtlpEndpoint.has_value()) {
        metricsOtlpEndpoint = "";
      }
      auto metricsExportOtlpBasicAuthFile =
        config.getOptionValueStr("cta.telemetry.metrics.export.otlp.basic_auth_file");
      std::string metricsExportOtlpBasicAuthString = "";
      if (metricsExportOtlpBasicAuthFile.has_value()) {
        metricsExportOtlpBasicAuthString =
          cta::telemetry::authStringFromFile(metricsExportOtlpBasicAuthFile.value());
      }
      auto metricsFileEndpoint = config.getOptionValueStr("cta.telemetry.metrics.export.file.endpoint");
      if (!metricsFileEndpoint.has_value()) {
        metricsFileEndpoint = "/var/log/cta/cta-frontend-metrics.txt";
      }

      cta::telemetry::TelemetryConfig telemetryConfig =
        cta::telemetry::TelemetryConfigBuilder()
          .serviceName(cta::semconv::attr::ServiceNameValues::kCtaFrontend)
          .serviceNamespace(m_instanceName)
          .serviceVersion(CTA_VERSION)
          .retainInstanceIdOnRestart(retainInstanceIdOnRestart.value())
          .resourceAttribute(cta::semconv::attr::kSchedulerNamespace, m_schedulerBackendName)
          .metricsBackend(metricsBackend.value())
          .metricsExportInterval(std::chrono::milliseconds(metricsExportInterval.value()))
          .metricsExportTimeout(std::chrono::milliseconds(metricsExportTimeout.value()))
          .metricsOtlpEndpoint(metricsOtlpEndpoint.value())
          .metricsOtlpBasicAuthString(metricsExportOtlpBasicAuthString)
          .metricsFileEndpoint(metricsFileEndpoint.value())
          .build();
      cta::log::LogContext lc(log);  // temporary log context
      cta::telemetry::initTelemetry(telemetryConfig, lc);
    } catch (exception::Exception& ex) {
      std::string ex_str("Failed to instantiate OpenTelemetry: ");
      throw exception::Exception(ex_str + ex.getMessage().str());
    }
  }


  auto missingFileCopiesMinAgeSecs = config.getOptionValueUInt("cta.catalogue.missing_file_copies_min_age_secs");
  m_missingFileCopiesMinAgeSecs = missingFileCopiesMinAgeSecs.value_or(0);

  {
    // Log cta.catalogue.missing_file_copies_min_age_secs
    std::list<log::Param> params;
    params.push_back(log::Param("source", missingFileCopiesMinAgeSecs.has_value() ? configFilename : "Compile time default"));
    params.push_back(log::Param("category", "cta.catalogue"));
    params.push_back(log::Param("key", "missingFileCopiesMinAgeSecs"));
    params.push_back(
      log::Param("value", std::to_string(missingFileCopiesMinAgeSecs.value_or(0))));
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
    std::list<log::Param> params;
    params.push_back(log::Param("source", configFilename));
    params.push_back(log::Param("category", "cta.catalogue"));
    params.push_back(log::Param("key", "numberofconnections"));
    params.push_back(log::Param("value", std::to_string(catalogue_numberofconnections.value())));
    log(log::INFO, "Configuration entry", params);
  }
  {
    // Log catalogue number of archive file listing connections
    std::list<log::Param> params;
    params.push_back(log::Param("source", "Compile time default"));
    params.push_back(log::Param("category", "cta.catalogue"));
    params.push_back(log::Param("key", "nbArchiveFileListingConns"));
    params.push_back(log::Param("value", std::to_string(nbArchiveFileListingConns)));
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
    std::list<log::Param> params;
    params.push_back(log::Param("source", configFilename));
    params.push_back(log::Param("category", "cta.objectstore"));
    params.push_back(log::Param("key", "backendpath"));
    params.push_back(log::Param("value", db_conn.value()));
    log(log::INFO, "Configuration entry", params);
  }

  m_scheddbInit = std::make_unique<SchedulerDBInit_t>("Frontend", db_conn.value(), *m_log);
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
    std::list<log::Param> params;
    params.push_back(log::Param("source", configFilename));
    params.push_back(log::Param("category", "cta.schedulerdb"));
    params.push_back(log::Param("key", "numberofthreads"));
    params.push_back(log::Param("value", std::to_string(osThreadPoolSize.value())));
    log(log::INFO, "Configuration entry", params);
  }

  // Log cta.schedulerdb.threadstacksize_mb
  if (osThreadStackSize.has_value()) {
    std::list<log::Param> params;
    params.push_back(log::Param("source", configFilename));
    params.push_back(log::Param("category", "cta.schedulerdb"));
    params.push_back(log::Param("key", "threadstacksize_mb"));
    params.push_back(log::Param("value", std::to_string(osThreadStackSize.value())));
    log(log::INFO, "Configuration entry", params);
  }

  m_scheddb->initConfig(osThreadPoolSize, osThreadStackSize);
  // Initialise the Scheduler
  m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_scheddb, m_schedulerBackendName, 5, 2 * 1000 * 1000);

  // Initialise the Frontend
  auto archiveFileMaxSize = config.getOptionValueUInt("cta.archivefile.max_size_gb");
  // Convert archiveFileMaxSize from GB to bytes
  m_archiveFileMaxSize =
    archiveFileMaxSize.has_value() ? static_cast<uint64_t>(archiveFileMaxSize.value()) * 1000 * 1000 * 1000 : 0;

  {
    // Log cta.archivefile.max_size_gb
    std::list<log::Param> params;
    params.push_back(log::Param("source", archiveFileMaxSize.has_value() ? configFilename : "Compile time default"));
    params.push_back(log::Param("category", "cta.archivefile"));
    params.push_back(log::Param("key", "max_size_gb"));
    params.push_back(
      log::Param("value", std::to_string(archiveFileMaxSize.has_value() ? archiveFileMaxSize.value() : 0)));
    log(log::INFO, "Configuration entry", params);
  }

  std::optional<bool> zeroLengthFilesForbidden = config.getOptionValueBool("cta.archivefile.zero_length_files_forbidden");
  m_zeroLengthFilesForbidden = zeroLengthFilesForbidden.value_or(true); // disallow 0-length files by default
  {
    // Log cta.archivefile.zero_length_files_forbidden
    std::list<log::Param> params;
    params.push_back(log::Param("source", zeroLengthFilesForbidden.has_value() ? configFilename : "Compile time default"));
    params.push_back(log::Param("category", "cta.archivefile"));
    params.push_back(log::Param("key", "zero_length_files_forbidden"));
    params.push_back(
      log::Param("value", config.getOptionValueStr("cta.archivefile.zero_length_files_forbidden").value_or("true")));
    log(log::INFO, "Configuration entry", params);
  }

  m_zeroLengthFilesForbidden_voExceptions =
    config.getOptionValueStrVector("cta.archivefile.zero_length_files_forbidden_vo_exception_list");
  {
    // Log cta.archivefile.zero_length_files_forbidden_vo_exception_list
    std::list<log::Param> params;
    params.push_back(
      log::Param("source", m_zeroLengthFilesForbidden_voExceptions.empty() ? "Compile time default" : configFilename));
    params.push_back(log::Param("category", "cta.archivefile"));
    params.push_back(log::Param("key", "zero_length_files_forbidden_vo_exception_list"));
    std::ostringstream oss;
    bool is_first = true;
    for (auto& val : m_zeroLengthFilesForbidden_voExceptions) {
      if (!is_first) {
        oss << ",";
      }
      oss << val;
      is_first = false;
    }
    params.push_back(log::Param("value", oss.str()));
    log(log::INFO, "Configuration entry", params);
  }

  // Get the repack buffer URL
  auto repackBufferURLConf = config.getOptionValueStr("cta.repack.repack_buffer_url");
  if (repackBufferURLConf.has_value()) {
    m_repackBufferURL = repackBufferURLConf.value();
  }

  auto repackMaxFilesToSelectConf = config.getOptionValueUInt("cta.repack.repack_max_files_to_select");
  if (repackMaxFilesToSelectConf.has_value()) {
    m_repackMaxFilesToSelect = repackMaxFilesToSelectConf.value();
  }

  // Get the verification mount policy
  const auto verificationMountPolicy = config.getOptionValueStr("cta.verification.mount_policy");
  if (verificationMountPolicy.has_value()) {
    m_verificationMountPolicy = verificationMountPolicy.value();
  }

  {
    // Log cta.repack.repack_buffer_url
    if (repackBufferURLConf.has_value()) {
      std::list<log::Param> params;
      params.push_back(log::Param("source", configFilename));
      params.push_back(log::Param("category", "cta.repack"));
      params.push_back(log::Param("key", "repack_buffer_url"));
      params.push_back(log::Param("value", repackBufferURLConf.value()));
      log(log::INFO, "Configuration entry", params);
    }
  }

  // Configure allowed requests
  {
    // default value for both repack and user requests is "on"
    std::optional<bool> disableRepackRequests = config.getOptionValueBool("cta.schedulerdb.disable_repack_requests");
    m_acceptRepackRequests = disableRepackRequests.has_value() ? (!disableRepackRequests.value()) : true;
    if (!disableRepackRequests.has_value()) {
      log(log::WARNING, "cta.schedulerdb.disable_repack_requests is not set in configuration file, using default value false");
    }

    std::list<log::Param> params;
    params.push_back(log::Param("source", disableRepackRequests.has_value() ? configFilename : "Compile time default"));
    params.push_back(log::Param("category", "cta.schedulerdb"));
    params.push_back(log::Param("key", "disable_repack_requests"));
    params.push_back(log::Param("value", disableRepackRequests.has_value() ? config.getOptionValueStr("cta.schedulerdb.disable_repack_requests").value() : "false"));
    log(log::INFO, "Configuration entry", params);
  }

  {
    auto disableUserRequests = config.getOptionValueBool("cta.schedulerdb.disable_user_requests");
    m_acceptUserRequests = disableUserRequests.has_value() ? (!disableUserRequests.value()) : true;
    if (!disableUserRequests.has_value()) {
      log(log::WARNING, "cta.schedulerdb.disable_user_requests is not set in configuration file, using default value false");
    }

    std::list<log::Param> params;
    params.push_back(log::Param("source", disableUserRequests.has_value() ? configFilename : "Compile time default"));
    params.push_back(log::Param("category", "cta.schedulerdb"));
    params.push_back(log::Param("key", "disable_user_requests"));
    params.push_back(log::Param("value", disableUserRequests.has_value() ? config.getOptionValueStr("cta.schedulerdb.disable_user_requests").value() : "false"));
    log(log::INFO, "Configuration entry", params);
  }

  {
    auto tapeCacheMaxAgeSecsConf = config.getOptionValueUInt("cta.schedulerdb.tape_cache_max_age_secs");
    if (tapeCacheMaxAgeSecsConf.has_value()) {
      m_tapeCacheMaxAgeSecs = tapeCacheMaxAgeSecsConf.value();
      std::list<log::Param> params;
      params.push_back(log::Param("source", configFilename));
      params.push_back(log::Param("category", "cta.schedulerdb"));
      params.push_back(log::Param("key", "tape_cache_max_age_secs"));
      params.push_back(log::Param("value", tapeCacheMaxAgeSecsConf.value()));
      log(log::INFO, "Configuration entry", params);
    }
  }

  {
    auto retrieveQueueCacheMaxAgeSecsConf =
      config.getOptionValueUInt("cta.schedulerdb.retrieve_queue_cache_max_age_secs");
    if (retrieveQueueCacheMaxAgeSecsConf.has_value()) {
      m_retrieveQueueCacheMaxAgeSecs = retrieveQueueCacheMaxAgeSecsConf.value();
      std::list<log::Param> params;
      params.push_back(log::Param("source", configFilename));
      params.push_back(log::Param("category", "cta.schedulerdb"));
      params.push_back(log::Param("key", "retrieve_queue_cache_max_age_secs"));
      params.push_back(log::Param("value", retrieveQueueCacheMaxAgeSecsConf.value()));
      log(log::INFO, "Configuration entry", params);
    }
  }

  // Get the mount policy name for verification requests

  // Get the gRPC-specific values, if they are set (getOptionValue returns an std::optional)
  std::optional<bool> tls = config.getOptionValueBool("grpc.tls.enabled");
  m_tls = tls.value_or(false);  // default value is false
  auto TlsKey = config.getOptionValueStr("grpc.tls.server_key_path");
  if (TlsKey.has_value()) {
    m_tlsKey = TlsKey.value();
  }
  auto TlsCert = config.getOptionValueStr("grpc.tls.server_cert_path");
  if (TlsCert.has_value()) {
    m_tlsCert = TlsCert.value();
  }
  auto TlsChain = config.getOptionValueStr("grpc.tls.chain_cert_path");
  if (TlsChain.has_value()) {
    m_tlsChain = TlsChain.value();
  }
  auto port = config.getOptionValueStr("grpc.port");
  if (port.has_value()) {
    m_port = port.value();
  }
  auto threads = config.getOptionValueInt("grpc.numberofthreads");
  if (threads.has_value()) {
    if (threads.value() < 1) {
      throw exception::UserError("value of grpc.numberofthreads must be at least 1");
    }
    m_threads = threads.value();
  }

  if (auto jwksUri = config.getOptionValueStr("grpc.jwks.uri"); jwksUri.has_value()) {
    m_jwksUri = jwksUri.value();
  }

  std::optional<bool> jwtAuth = config.getOptionValueBool("grpc.jwt.enabled");
  m_jwtAuth = jwtAuth.value_or(false);  // default value is false
  if (!m_tls && m_jwtAuth) {
    throw exception::UserError("grpc.jwt.auth is set to true when grpc.tls is set to false in configuration file " +
                               configFilename + ". Cannot use tokens over unencrypted channel, tls must be enabled.");
  }

  if (m_jwtAuth && !m_jwksUri.has_value()) {
    throw exception::UserError(
      "grpc.jwt.auth is set to true but no endpoint is provided in grpc.jwks.uri in configuration file " +
      configFilename);
  }

  auto cacheRefreshInterval = config.getOptionValueInt("grpc.jwks.cache.refresh_interval_secs");
  if (cacheRefreshInterval.has_value() && cacheRefreshInterval.value() < 0) {
    throw exception::UserError(
      "grpc.jwks.cache.refresh_interval_secs is set to a negative value in configuration file " + configFilename);
  }
  m_cacheRefreshInterval = cacheRefreshInterval;

  auto pubkeyTimeout = config.getOptionValueInt("grpc.jwks.cache.timeout_secs");
  if (pubkeyTimeout.has_value() && pubkeyTimeout.value() < 0) {
    throw exception::UserError("grpc.jwks.cache.timeout_secs is set to a negative value in configuration file " +
                               configFilename);
  }
  m_pubkeyTimeout = pubkeyTimeout;

  if (m_jwtAuth) {
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
  }

  // All done
  log(log::INFO, std::string("cta-frontend started"), {log::Param("version", CTA_VERSION)});
}

}  // namespace cta::frontend
