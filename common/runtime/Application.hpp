/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CommonConfig.hpp"
#include "ConfigLoader.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/Logger.hpp"

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
    requires HasLoggingConfig<V>
      : m_appName(appName),
        m_cmdLineArgs(cmdLineArgs),
        m_appConfig(runtime::loadFromToml<V>(m_cmdlineArgs.configPath, m_cmdlineArgs.configStrict)) {
    initLogging();
  }

  // TODO: always add a signal reactor
  // TODO: always handle SIGUSR1 for log rotation (make sure to handle stdout logging)
  // TODO: always enforce SIGTERM handling
  // TODO: add option for the app to define other signals?

  // TODO: use SFINAE so that if the app doesn't need the commandline arguments it can still run. I.e. don't enforce an interface that is not necessary
  int run()
    requires HasRunFunction<T, V, U, log::Logger>
  {
    auto log = *m_logPtr;

    // Init signal handler here

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
  void initLogging();

  void initHealthServer();

  void initTelemetry();

  void initXRootD();

  const std::string m_appName;

  // The actual application class
  T m_app;
  // A struct representing the commandline arguments
  const U m_cmdlineArgs;
  // A struct containing all configuration
  const V m_appConfig;

  std::unique_ptr<log::Logger> m_logPtr;
};

}  // namespace cta::runtime
