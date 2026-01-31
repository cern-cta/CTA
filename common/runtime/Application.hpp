/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CommonConfig.hpp"
#include "ConfigLoader.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/Logger.hpp"
#include "signals/SignalReactor.hpp"

#include <concepts>
#include <string>
#include <utility>

namespace cta::runtime {

template<class App>
concept HasReadinessFunction = requires(const App& app) {
  { app.isReady() } -> std::same_as<bool>;
};

template<class App>
concept HasLivenessFunction = requires(const App& app) {
  { app.isLive() } -> std::same_as<bool>;
};

template<class App>
concept HasStopFunction = requires(App& app) {
  { app.stop() } -> std::same_as<void>;
};

template<class App, class Config, class Args, class Logger>
concept HasRunFunction = requires(App& app, const Config& cfg, const Args& args, Logger& logger) {
  { app.run(cfg, args, logger) } -> std::same_as<int>;
};

template<class Config>
concept HasHealthServerConfig =
  requires(Config cfg) { requires std::same_as<std::remove_cvref_t<decltype(cfg.health_server)>, HealthServerConfig>; };

template<class Config>
concept HasTelemetryConfig = requires(const Config& cfg) {
  requires std::same_as<std::remove_cvref_t<decltype(cfg.telemetry)>, TelemetryConfig>;

  requires std::same_as<std::remove_cvref_t<decltype(cfg.experimental.telemetry_enabled)>, bool>;
};

template<class Config>
concept HasXRootDConfig =
  requires(Config cfg) { requires std::same_as<std::remove_cvref_t<decltype(cfg.xrootd)>, XRootDConfig>; };

template<class Config>
concept HasLoggingConfig =
  requires(Config cfg) { requires std::same_as<std::remove_cvref_t<decltype(cfg.logging)>, LoggingConfig>; };

template<class Config>
concept HasSchedulerConfig =
  requires(const Config& cfg) { requires std::same_as<std::remove_cvref_t<decltype(cfg.scheduler)>, SchedulerConfig>; };

template<class Config>
concept HasCatalogueConfig =
  requires(const Config& cfg) { requires std::same_as<std::remove_cvref_t<decltype(cfg.scheduler)>, CatalogueConfig>; };

template<class T, class U, class V>
class Application {
public:
  Application(const std::string& appName, U cmdLineArgs)
    requires HasLoggingConfig<V> && HasStopFunction<T>
      : m_appName(appName),
        m_cmdlineArgs(cmdLineArgs),
        m_appConfig(runtime::loadFromToml<V>(m_cmdlineArgs.configPath, m_cmdlineArgs.configStrict)) {
    initLogging();
    m_signalReactorBuilder = std::make_unique<SignalReactorBuilder>(*m_logPtr);
    m_signalReactorBuilder->addSignalFunction(SIGTERM, [this]() { m_app.stop(); });
    m_signalReactorBuilder->addSignalFunction(SIGUSR1, [this]() { m_logPtr->refresh(); });
  }

  Application& addSignalFunction(int signal, const std::function<void()>& func);

  // TODO: use SFINAE so that if the app doesn't need the commandline arguments it can still run. I.e. don't enforce an interface that is not necessary
  int run()
    requires HasRunFunction<T, V, U, log::Logger>
  {
    auto log = *m_logPtr;

    const auto signalReactor = m_signalReactorBuilder->build();
    signalReactor.start();

    if constexpr (HasHealthServerConfig<V>) {
      static_assert(HasReadinessFunction<T> && HasLivenessFunction<T>,
                    "Config has health_server, but app type lacks isReady()/isLive() methods");
      initHealthServer();
    }

    if constexpr (HasTelemetryConfig<V>) {
      initTelemetry();
    }

    if constexpr (HasXRootDConfig<V>) {
      initXRootD();
    }

    // Start
    int returnCode = EXIT_FAILURE;
    try {
      log(log::INFO, "Starting " + m_appName);
      returnCode = m_app.run(m_appConfig, m_cmdlineArgs, *m_logPtr);
    } catch (exception::Exception& ex) {
      log(log::CRIT,
          "FATAL: Caught an unexpected CTA exception",
          {log::Param("exceptionMessage", ex.getMessage().str())});
      sleep(1);
      return EXIT_FAILURE;
    } catch (std::exception& se) {
      log(log::CRIT, "FATAL: Caught an unexpected standard exception", {log::Param("error", se.what())});
      sleep(1);
      return EXIT_FAILURE;
    } catch (...) {
      log(log::CRIT, "FATAL: Caught an unexpected and unknown exception", {});
      sleep(1);
      return EXIT_FAILURE;
    }
    log(log::INFO, "Exiting " + m_appName);
    return returnCode;
  }

private:
  void initLogging(
    std::string shortHostName;
    try { shortHostName = utils::getShortHostname(); } catch (const exception::Errnum& ex) {
      throw cta::exception::Exception("Failed to get short host name: " << ex.getMessage().str());
    }

    try {
      if (m_cmdlineArgs.logToFile) {
        m_logPtr = std::make_unique<log::FileLogger>(shortHostName, m_appName, m_cmdlineArgs.logFilePath, log::INFO);
      } else {
        // Default to stdout logger
        m_logPtr = std::make_unique<log::StdoutLogger>(shortHostName, m_appName);
      }
      m_logPtr->setLogFormat(m_appConfig.logging.format);
    } catch (exception::Exception & ex) {
      throw cta::exception::Exception("Failed to instantiate CTA logging system: " << ex.getMessage().str());
    }

    m_logPtr->setLogMask(m_appConfig.logging.level);
    std::map<std::string, std::string> logAttributes;
    // This should eventually be handled by auto-discovery
    if constexpr (HasSchedulerConfig<V>) {
      logAttributes["sched_backend"] = m_appConfig.scheduler.backend_name;
    } for (const auto& [key, value] : m_appConfig.logging.attributes) {
      logAttributes[key] = value;
    } m_logPtr->setStaticParams(logAttributes););

  void initSignalReactor();

  void initHealthServer();

  void initTelemetry(
    if (!m_appConfig.experimental.telemetry_enabled || m_appConfig.telemetry.config.empty()) {
      return;
    } log::LogContext lc(*m_logPtr);
    try {
      std::map<std::string, std::string> ctaResourceAttributes = {
        {cta::semconv::attr::kServiceName,       cta::semconv::attr::ServiceNameValues::kCtaMaintd},
        {cta::semconv::attr::kServiceVersion,    std::string(CTA_VERSION)                         },
        {cta::semconv::attr::kServiceInstanceId, cta::utils::generateUuid()                       },
        {cta::semconv::attr::kHostName,          cta::utils::getShortHostname()                   }
      };

      if constexpr (HasSchedulerConfig<V>) {
        ctaResourceAttributes[cta::semconv::attr::kSchedulerNamespace] = m_appConfig.scheduler.backend_name;
      }
      cta::telemetry::initOpenTelemetry(config.telemetry.config, ctaResourceAttributes, lc);
    } catch (exception::Exception & ex) {
      cta::log::ScopedParamContainer params(lc);
      params.add("exceptionMessage", ex.getMessage().str());
      lc.log(log::ERR, "Failed to instantiate OpenTelemetry");
      cta::telemetry::cleanupOpenTelemetry(lc);
    });

  void initXRootD();

  const std::string m_appName;

  // The actual application class
  T m_app;
  // A struct representing the commandline arguments
  const U m_cmdlineArgs;
  // A struct containing all configuration
  const V m_appConfig;

  std::unique_ptr<SignalReactorBuilder> m_signalReactorBuilder;
  std::unique_ptr<HealthServer> m_healthServer;

  std::unique_ptr<log::Logger> m_logPtr;
};

}  // namespace cta::runtime
