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

#include <signal.h>
#include <sys/prctl.h>
#include <chrono>
#include <thread>

#include "Maintenance.hpp"
#include "diskbufferrunners/DiskReportRunner.hpp"
#include "repackrequestrunner/RepackRequestManager.hpp"
#include "osrunners/QueueCleanupRunner.hpp"
#include "osrunners/GarbageCollector.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "common/telemetry/TelemetryInit.hpp"
#include "common/telemetry/config/TelemetryConfig.hpp"
#include "common/semconv/Attributes.hpp"
#include <opentelemetry/sdk/common/global_log_handler.h>
#include "rdbms/Login.hpp"

namespace cta::maintenance {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Maintenance::Maintenance(cta::log::LogContext& lc, const cta::common::Config& config) : m_lc(lc) {
  // Instantiate telemetry
  if (config.getOptionValueBool("cta.experimental.telemetry.enabled").value_or(false)) {
    try {
      std::string metricsBackend = config.getOptionValueStr("cta.telemetry.metrics.backend").value_or("NOOP");

      std::string otlpBasicAuthFile =
        config.getOptionValueStr("cta.telemetry.metrics.export.otlp.basic_auth_file").value();
      std::string otlpBasicAuthString =
        otlpBasicAuthFile.empty() ? "" : cta::telemetry::authStringFromFile(otlpBasicAuthFile);
      cta::telemetry::TelemetryConfig telemetryConfig =
        cta::telemetry::TelemetryConfigBuilder()
          .serviceName(cta::semconv::attr::ServiceNameValues::kCtaMaintenance)
          .serviceNamespace(config.getOptionValueStr("cta.instance_name").value())
          .serviceVersion(CTA_VERSION)
          .retainInstanceIdOnRestart(
            config.getOptionValueBool("cta.telemetry.retain_instance_id_on_restart").value_or(false))
          .resourceAttribute(cta::semconv::attr::kSchedulerNamespace,
                             config.getOptionValueStr("cta.scheduler_backend_name").value())
          .metricsBackend(metricsBackend)
          .metricsExportInterval(std::chrono::milliseconds(
            config.getOptionValueInt("cta.telemetry.metrics.export.interval").value_or(15000)))
          .metricsExportTimeout(
            std::chrono::milliseconds(config.getOptionValueInt("cta.telemetry.metrics.export.timeout").value_or(3000)))
          .metricsOtlpEndpoint(config.getOptionValueStr("cta.telemetry.metrics.export.otlp.endpoint").value())
          .metricsOtlpBasicAuthString(otlpBasicAuthString)
          .metricsFileEndpoint(config.getOptionValueStr("cta.telemetry.metrics.export.file.endpoint")
                                 .value_or("/var/log/cta/cta-maintenance-metrics.txt"))
          .build();
      // taped is a special case where we only do initTelemetry after the process name has been set
      cta::telemetry::initTelemetryConfig(telemetryConfig);
    } catch (exception::Exception& ex) {
      throw InvalidConfiguration("Failed to instantiate OpenTelemetry. Exception message: " + ex.getMessage().str());
    }
  }

  m_sleepInterval = config.getOptionValueInt("cta.maintenance.sleep_interval").value_or(1000);

  if (!config.getOptionValueStr("cta.objectstore.backendpath").has_value()) {
    throw InvalidConfiguration("Could not find config entry 'cta.objectstore.backendpath' in");
  }
  m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Maintenance",
                                                      config.getOptionValueStr("cta.objectstore.backendpath").value(),
                                                      m_lc.logger());

  if (!config.getOptionValueStr("cta.catalogue.config_file").has_value()) {
    throw InvalidConfiguration("Could not find config entry 'cta.catalogue.config_file'");
  }
  const rdbms::Login catalogueLogin =
    rdbms::Login::parseFile(config.getOptionValueStr("cta.catalogue.config_file").value());
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    cta::catalogue::CatalogueFactoryFactory::create(m_lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);

  m_catalogue = catalogueFactory->create();
  m_scheddb = m_schedDbInit->getSchedDB(*m_catalogue, m_lc.logger());

  // Set Scheduler DB cache timeouts
  SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
  statisticsCacheConfig.tapeCacheMaxAgeSecs =
    config.getOptionValueInt("cta.schedulerdb.tape_cache_max_age_secs").value_or(600);
  statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs =
    config.getOptionValueInt("cta.schedulerdb.retrieve_queue_cache_max_age_secs").value_or(10);
  m_scheddb->setStatisticsCacheConfig(statisticsCacheConfig);

  if (!config.getOptionValueStr("cta.scheduler_backend_name").has_value()) {
    throw InvalidConfiguration("Could not find config entry 'cta.scheduler_backend_name'");
  }
  m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue,
                                                 *m_scheddb,
                                                 config.getOptionValueStr("cta.scheduler_backend_name").value());

  // Add Disk Reporter
  if (config.getOptionValueBool("cta.disk_reporter.enabled").value_or(true)) {
    m_maintenanceRunners.emplace_back(
      std::make_unique<DiskReportRunner>(*m_scheduler,
                                         config.getOptionValueInt("cta.disk_reporter.batch_size").value_or(500),
                                         config.getOptionValueInt("cta.disk_reporter.soft_timeout").value_or(30)));
    log::ScopedParamContainer params(m_lc);
    params.add("softTimeout", config.getOptionValueInt("cta.disk_reporter.soft_timeout").value_or(30));
    params.add("batchSize", config.getOptionValueInt("cta.disk_reporter.batch_size").value_or(500));
    m_lc.log(cta::log::INFO, "Created Disk Reporter Runner");
  }

  // Add Garbage Collector
  if (config.getOptionValueBool("cta.garbage_collector.enabled").value_or(true)) {
    m_maintenanceRunners.push_back(m_schedDbInit->getGarbageCollector(*m_catalogue));
    m_lc.log(cta::log::INFO, "Created Garbage Collector Runner");
  }

  // Add Queue Cleanup
  if (config.getOptionValueBool("cta.queue_cleanup.enabled").value_or(true)) {
    m_maintenanceRunners.push_back(
      m_schedDbInit->getQueueCleanupRunner(*m_catalogue,
                                           *m_scheddb,
                                          config.getOptionValueInt("cta.queue_cleanup.batch_size").value_or(500)));
    log::ScopedParamContainer params(m_lc);
    params.add("batchSize", config.getOptionValueInt("cta.queue_cleanup.batch_size").value_or(500));
    m_lc.log(cta::log::INFO, "Created Queue Cleanup Runner");
  }

  // // Add Repack request manager
  if (config.getOptionValueBool("cta.repack.enabled").value_or(true)) {
    m_maintenanceRunners.emplace_back(std::make_unique<RepackRequestManager>(
      *m_scheduler,
      config.getOptionValueInt("cta.repack.request_manager.max_to_toexpand").value_or(2),
      config.getOptionValueInt("cta.repack.reporter.soft_timeout").value_or(30)));
    log::ScopedParamContainer params(m_lc);
    params.add("maxRequestsToToExpand",
               config.getOptionValueInt("cta.reoack.request_manager.max_to_toexpand").value_or(2));
    params.add("softTimeout", config.getOptionValueInt("cta.repack.reporter.soft_timeout").value_or(30));
    m_lc.log(cta::log::INFO, "Created Repack Request Runner");
  }

  // At least one runner should be enabled.
  if (m_maintenanceRunners.empty()) {
    throw InvalidConfiguration("ERROR: No runner is enabled.");
  }
}

uint32_t Maintenance::run() {
  // Before anything, we will check for access to the scheduler's central storage.
  
  try {
    do {
      std::set<uint32_t> sigSet;
      m_lc.log(log::INFO, "In Maintenance::run(): About to do a maintenance pass.");
      for (const auto& runner : m_maintenanceRunners) {
        runner->executeRunner(m_lc);

        sigSet = m_signalHandler->processAndGetSignals(m_lc);
        if (!sigSet.empty()) {
          break;
        }
      }

      // We need to terminate the process
      if (sigSet.contains(SIGTERM)) {
        m_lc.log(log::INFO, "In Maintenance::run(): received signal to shutdown, exiting the process.");
        return SIGTERM;
      }

      // We neeed to refresh the logs
      if (sigSet.contains(SIGUSR1)) {
        m_lc.log(log::INFO, "In Maintenance::run(): recevied signal to refresh the log file descriptor");
        m_lc.logger().refresh();
        m_lc.log(log::INFO, "In Maintenance::run(): refreshed log file descriptor");
      }
      
      // We need to reload the config
      if (sigSet.contains(SIGHUP)) {
        m_lc.log(log::INFO, "In Maintenance::run(): received signal to refresh the config file");
        return SIGHUP;
      }
       std::this_thread::sleep_for(std::chrono::milliseconds(std::chrono::milliseconds(m_sleepInterval)));
    } while (true);
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In Maintenance::run(): received an exception. Backtrace follows.");
    m_lc.logBacktrace(log::INFO, ex.backtrace());
    throw ex;
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.what());
    m_lc.log(log::ERR, "In Maintenance::run(): received a std::exception.");
    throw ex;
  } catch (...) {
    m_lc.log(log::ERR, "In Maintenance::run(): received an unknown exception.");
    throw;
  }
}

}  // namespace cta::maintenance
