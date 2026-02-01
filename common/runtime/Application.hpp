/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CommonCliOptions.hpp"
#include "CommonConfig.hpp"
#include "ConfigLoader.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/OtelInit.hpp"
#include "common/utils/utils.hpp"
#include "health/HealthServer.hpp"
#include "signals/SignalReactor.hpp"
#include "signals/SignalReactorBuilder.hpp"
#include "version.h"

#include <concepts>
#include <signal.h>
#include <string>
#include <utility>

namespace cta::runtime {

//------------------------------------------------------------------------------
// Compile time constraints
//------------------------------------------------------------------------------
template<class TApp>
concept HasReadinessFunction = requires(const TApp& app) {
  { app.isReady() } -> std::same_as<bool>;
};

template<class TApp>
concept HasLivenessFunction = requires(const TApp& app) {
  { app.isLive() } -> std::same_as<bool>;
};

template<class TApp>
concept HasStopFunction = requires(TApp& app) {
  { app.stop() } -> std::same_as<void>;
};

template<class TApp, class TConfig, class Logger>
concept HasRunFunction = requires(TApp& app, const TConfig& cfg, Logger& logger) {
  { app.run(cfg, logger) } -> std::same_as<int>;
};

template<class TApp, class TConfig, class TOpts, class Logger>
concept HasRunFunctionWithOpts = requires(TApp& app, const TConfig& cfg, const TOpts& opts, Logger& logger) {
  { app.run(cfg, opts, logger) } -> std::same_as<int>;
};

template<class TConfig>
concept HasHealthServerConfig = requires(const TConfig& cfg) {
  requires std::same_as<std::remove_cvref_t<decltype(cfg.health_server)>, HealthServerConfig>;
};

template<class TConfig>
concept HasTelemetryConfig = requires(const TConfig& cfg) {
  requires std::same_as<std::remove_cvref_t<decltype(cfg.telemetry)>, TelemetryConfig>;

  requires std::same_as<std::remove_cvref_t<decltype(cfg.experimental.telemetry_enabled)>, bool>;
};

template<class TConfig>
concept HasXRootDConfig =
  requires(const TConfig& cfg) { requires std::same_as<std::remove_cvref_t<decltype(cfg.xrootd)>, XRootDConfig>; };

template<class TConfig>
concept HasLoggingConfig =
  requires(const TConfig& cfg) { requires std::same_as<std::remove_cvref_t<decltype(cfg.logging)>, LoggingConfig>; };

template<class TConfig>
concept HasSchedulerConfig = requires(const TConfig& cfg) {
  requires std::same_as<std::remove_cvref_t<decltype(cfg.scheduler)>, SchedulerConfig>;
};

template<class TConfig>
concept HasCatalogueConfig = requires(const TConfig& cfg) {
  requires std::same_as<std::remove_cvref_t<decltype(cfg.catalogue)>, CatalogueConfig>;
};

//------------------------------------------------------------------------------
// Utility Functions
//------------------------------------------------------------------------------

int safeRun(const std::function<int()>& func) noexcept {
  int returnCode = EXIT_FAILURE;
  try {
    returnCode = func();
  } catch (exception::UserError& ex) {
    std::cerr << "FATAL: User Error:\n\t" << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  } catch (exception::Exception& ex) {
    std::cerr << "FATAL: Caught an unexpected CTA exception:\n\t" << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  } catch (std::exception& se) {
    std::cerr << "FATAL: Caught an unexpected exception:\n\t" << se.what() << std::endl;
    return EXIT_FAILURE;
  } catch (...) {
    std::cerr << "FATAL: Caught an unexpected and unknown exception." << std::endl;
    return EXIT_FAILURE;
  }
  return returnCode;
}

//------------------------------------------------------------------------------
// Application
//------------------------------------------------------------------------------

/**
 * @brief
 *
 * @tparam TApp The type of application. Several constraints are enforced on this application depending on what is in the config.
 * @tparam TConfig The type of config to use for the application.
 * @tparam TOpts The commandline options used to construct the config and the app.
 */
template<class TApp, class TConfig, class TOpts>
class Application {
public:
  Application(const std::string& appName, const TOpts& cliOptions)
    requires HasLoggingConfig<TConfig> && HasStopFunction<TApp> && HasRequiredCliOptions<TOpts>
      : m_appName(appName),
        m_cliOptions(cliOptions),
        m_appConfig() {
    // If we got here and help was supposed to show, then there is a bug in how the program was set up
    assert(!m_cliOptions.showHelp);

    if (m_cliOptions.configFilePath.empty()) {
      m_cliOptions.configFilePath = "/etc/cta/" + m_appName + ".toml";
    }
    m_appConfig = runtime::loadFromToml<TConfig>(m_cliOptions.configFilePath, m_cliOptions.configStrict);

    initLogging();
    m_signalReactorBuilder = std::make_unique<SignalReactorBuilder>(*m_logPtr);
    m_signalReactorBuilder->addSignalFunction(SIGTERM, [this]() { m_app.stop(); });
    m_signalReactorBuilder->addSignalFunction(SIGUSR1, [this]() { m_logPtr->refresh(); });
  }

  Application& addSignalFunction(int signal, const std::function<void()>& func) {
    m_signalReactorBuilder->addSignalFunction(signal, func);
    return *this;
  }

  int run() noexcept {
    return safeRunWithLog(*m_logPtr, [this]() {
      auto signalReactor = m_signalReactorBuilder->build();
      signalReactor.start();

      if constexpr (HasHealthServerConfig<TConfig>) {
        static_assert(HasReadinessFunction<TApp> && HasLivenessFunction<TApp>,
                      "Config has health_server, but app type lacks isReady()/isLive() methods");
        initHealthServer();
      }

      if constexpr (HasTelemetryConfig<TConfig>) {
        initTelemetry();
      }

      if constexpr (HasXRootDConfig<TConfig>) {
        initXRootD();
      }

      // Start
      cta::log::Logger& log = *m_logPtr;
      log(log::INFO, "Starting " + m_appName);
      return runApp(log);
    });
  }

private:
  // SFINAE: allow an app to ignore the cliOptions if it does not use them
  int runApp(cta::log::Logger& log)
    requires HasRunFunctionWithOpts<TApp, TConfig, TOpts, log::Logger>
  {
    return m_app.run(m_appConfig, m_cliOptions, log);
  }

  int runApp(cta::log::Logger& log)
    requires HasRunFunction<TApp, TConfig, log::Logger>
  {
    return m_app.run(m_appConfig, log);
  }

  void initLogging() {
    using namespace cta;
    std::string shortHostName;
    try {
      shortHostName = utils::getShortHostname();
    } catch (const exception::Errnum& ex) {
      std::cerr << "Failed to get short host name: " << ex.getMessage().str() << std::endl;
      std::exit(EXIT_FAILURE);
    }

    try {
      if (!m_cliOptions.logFilePath.empty()) {
        m_logPtr = std::make_unique<log::FileLogger>(shortHostName, m_appName, m_cliOptions.logFilePath, log::INFO);
      } else {
        // Default to stdout logger
        m_logPtr = std::make_unique<log::StdoutLogger>(shortHostName, m_appName);
      }
      m_logPtr->setLogFormat(m_appConfig.logging.format);
    } catch (exception::Exception& ex) {
      std::cerr << "Failed to instantiate CTA logging system: " << ex.getMessage().str() << std::endl;
      std::exit(EXIT_FAILURE);
    }

    m_logPtr->setLogMask(m_appConfig.logging.level);
    std::map<std::string, std::string> logAttributes;
    // This should eventually be handled by auto-discovery
    if constexpr (HasSchedulerConfig<TConfig>) {
      logAttributes["sched_backend"] = m_appConfig.scheduler.backend_name;
    }
    for (const auto& [key, value] : m_appConfig.logging.attributes) {
      logAttributes[key] = value;
    }
    m_logPtr->setStaticParams(logAttributes);
  }

  void initHealthServer() {
    if (m_appConfig.health_server.enabled) {
      m_healthServer = std::make_unique<HealthServer>(
        *m_logPtr,
        m_appConfig.health_server.host,
        m_appConfig.health_server.port,
        [this]() { return m_app.isReady(); },
        [this]() { return m_app.isLive(); });

      // We only start it if it's enabled. We explicitly construct the object outside the scope of this if statement
      // Otherwise, healthServer will go out of scope and be destructed immediately
      m_healthServer->start();
    }
  }

  void initTelemetry() {
    if (!m_appConfig.experimental.telemetry_enabled || m_appConfig.telemetry.config.empty()) {
      return;
    }
    log::LogContext lc(*m_logPtr);
    try {
      std::map<std::string, std::string> ctaResourceAttributes = {
        {cta::semconv::attr::kServiceName,       cta::semconv::attr::ServiceNameValues::kCtaMaintd},
        {cta::semconv::attr::kServiceVersion,    std::string(CTA_VERSION)                         },
        {cta::semconv::attr::kServiceInstanceId, cta::utils::generateUuid()                       },
        {cta::semconv::attr::kHostName,          cta::utils::getShortHostname()                   }
      };

      if constexpr (HasSchedulerConfig<TConfig>) {
        ctaResourceAttributes[cta::semconv::attr::kSchedulerNamespace] = m_appConfig.scheduler.backend_name;
      }
      cta::telemetry::initOpenTelemetry(m_appConfig.telemetry.config, ctaResourceAttributes, lc);
    } catch (exception::Exception& ex) {
      cta::log::ScopedParamContainer params(lc);
      params.add("exceptionMessage", ex.getMessage().str());
      lc.log(log::ERR, "Failed to instantiate OpenTelemetry");
      cta::telemetry::cleanupOpenTelemetry(lc);
    }
  }

  void initXRootD() {
    // Overwrite XRootD env variables
    ::setenv("XrdSecPROTOCOL", m_appConfig.xrootd.security_protocol.c_str(), 1);
    ::setenv("XrdSecSSSKT", m_appConfig.xrootd.sss_keytab_path.c_str(), 1);
    // TODO: check the keytab here
  }

  // TODO: For internal use
  static int safeRunWithLog(log::Logger& log, const std::function<int()>& func) noexcept {
    int returnCode = EXIT_FAILURE;
    try {
      returnCode = func();
    } catch (exception::UserError& ex) {
      log(log::CRIT,
          "FATAL: User Error",
          {
            {"exceptionMessage", ex.getMessage().str()}
      });
      sleep(1);
      return EXIT_FAILURE;
    } catch (exception::Exception& ex) {
      log(log::CRIT,
          "FATAL: Caught an unexpected CTA exception",
          {
            {"exceptionMessage", ex.getMessage().str()}
      });
      sleep(1);
      return EXIT_FAILURE;
    } catch (std::exception& se) {
      log(log::CRIT,
          "FATAL: Caught an unexpected exception",
          {
            {"error", se.what()}
      });
      sleep(1);
      return EXIT_FAILURE;
    } catch (...) {
      log(log::CRIT, "FATAL: Caught an unexpected and unknown exception", {});
      sleep(1);
      return EXIT_FAILURE;
    }
    return returnCode;
  }

  const std::string m_appName;

  // The actual application class
  TApp m_app;
  // A struct representing the commandline arguments
  TOpts m_cliOptions;
  // A struct containing all configuration
  TConfig m_appConfig;

  std::unique_ptr<SignalReactorBuilder> m_signalReactorBuilder;
  std::unique_ptr<HealthServer> m_healthServer;

  std::unique_ptr<log::Logger> m_logPtr;
};

}  // namespace cta::runtime
