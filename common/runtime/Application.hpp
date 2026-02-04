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

// Unused for now, maybe something interesting for the future...
template<class TConfig>
concept HasCatalogueConfig = requires(const TConfig& cfg) {
  requires std::same_as<std::remove_cvref_t<decltype(cfg.catalogue)>, CatalogueConfig>;
};

//------------------------------------------------------------------------------
// Utility Functions
//------------------------------------------------------------------------------

/**
 * @brief Wraps the provided function into a try catch and logs any thrown exceptions to stderr.
 *
 * @param func The function to wrap. Expected to return a returncode.
 * @return int EXIT_FAILURE if an exception was thrown, otherwise the return code of the func.
 */
int safeRun(const std::function<int()>& func) noexcept {
  int returnCode = EXIT_FAILURE;
  try {
    returnCode = func();
  } catch (exception::UserError& ex) {
    // This will be mostly used to print issues with users e.g. making a mistake in the config file.
    // So we only print FATAL to keep it reasonably user friendly.
    std::cerr << "FATAL:\n" << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  } catch (exception::Exception& ex) {
    std::cerr << "FATAL: Caught an unexpected CTA exception:\n" << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  } catch (std::exception& se) {
    std::cerr << "FATAL: Caught an unexpected exception:\n" << se.what() << std::endl;
    return EXIT_FAILURE;
  } catch (...) {
    std::cerr << "FATAL: Caught an unexpected and unknown exception." << std::endl;
    return EXIT_FAILURE;
  }
  return returnCode;
}

/**
 * @brief Wraps the provided function into a try catch and logs any thrown exceptions
 * as CRIT errors to the logging system.
 *
 * @param func The function to wrap. Expected to return a returncode.
 * @return int EXIT_FAILURE if an exception was thrown, otherwise the return code of the func.
 * @return int
 */
int safeRunWithLog(log::Logger& log, const std::function<int()>& func) noexcept {
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

//------------------------------------------------------------------------------
// Application
//------------------------------------------------------------------------------

/**
 * @brief A wrapper class to instantiate custom applications.
 * The basic idea is that you tell this class a few things and it will construct your application object for you:
 * - The Class of the application. Note that you pass in the class (compile time), not the object.
 * - The Config struct the application is expected to consume. Note that you pass in the struct (compile time), not the object.
 * - The CliOptions object that will be used to determine how to populate application.
 *   For example, where to load the configuration from.
 * - The name of the application.
 *
 * Using this information, this class will instantiate the application, performing several compile time checks along the way.
 *
 * @tparam TApp The type of application. Several constraints are enforced on this application depending on what is in the config.
 * @tparam TConfig The type of config to use for the application.
 * @tparam TOpts The commandline options used to construct the config and the app.
 */
template<class TApp, class TConfig, class TOpts>
class Application {
public:
  /**
   * @brief Construct a new Application object.
   *
   * @param appName Name of the application. For example, cta-maintd, cta-taped, cta-admins
   * @param cliOptions Struct containing the (populated) commandline options. Note that, while that commandline options
   * are populated from the config file, they are NOT verified yet. So string values may be empty, integers may be zero
   * where they shouldn't be etc. This constraints must still be checked later.
   */
  Application(const std::string& appName, const TOpts& cliOptions)
    requires HasLoggingConfig<TConfig> && HasStopFunction<TApp> && HasRequiredCliOptions<TOpts>
      : m_appName(appName),
        m_cliOptions(cliOptions),
        m_appConfig() {
    // These two checks should never be reached; if they are, it means the developer made a mistake in how they set up the application.
    if (m_cliOptions.showHelp) {
      throw std::logic_error("Help was set to true and should have been shown before. If this part was reached, it "
                             "means the program did not exit correctly after showing help.");
    }
    if (m_appName.empty()) {
      throw std::logic_error("Application name cannot be empty.");
    }

    if (m_cliOptions.configFilePath.empty()) {
      // Construct a default path based on the app name
      m_cliOptions.configFilePath = "/etc/cta/" + m_appName + ".toml";
    }
    m_appConfig = runtime::loadFromToml<TConfig>(m_cliOptions.configFilePath, m_cliOptions.configStrict);
    // If the user requested to only check the config, we can exit now.
    if (cliOptions.configCheck) {
      std::cout << "Config check passed." << std::endl;
      std::exit(EXIT_SUCCESS);
    }

    initLogging();

    cta::log::Logger& log = *m_logPtr;
    log(log::INFO, "Initialising application: " + m_appName);
    m_signalReactorBuilder = std::make_unique<SignalReactorBuilder>(*m_logPtr);
    m_signalReactorBuilder->addSignalFunction(SIGTERM, [this]() { m_app.stop(); });
    m_signalReactorBuilder->addSignalFunction(SIGUSR1, [this]() { m_logPtr->refresh(); });
    // Don't build the signal reactor yet, because we may want to register more signal functions.
  }

  /**
   * @brief Add support for additional signals to the application.
   * As part of the design, the function/method that will be called on the registered signal MUST be part of the underlying application (m_app).
   * Example:
   *   app.addSignalFunction(SIGUSR2, &TestCustomSignalApp::myCustomSignalFunc);
   * Which will require TestCustomSignalApp to have a method:
   *   void myCustomSignalFunc();
   *
   * @param signal The signal to react to.
   * @param method The member function of m_app to call when this signal is received.
   * @return Application& This application object. Can be used to easily chain multiple of these calls together.
   */
  Application& addSignalFunction(int signal, void (TApp::*method)()) {
    auto& app = m_app;
    m_signalReactorBuilder->addSignalFunction(signal, [&app, method] { (app.*method)(); });
    return *this;
  }

  /**
   * @brief Runs the application. Will do some initialisation and call the run() method of the underlying app.
   *
   * @return int Return code.
   */
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
      log(log::INFO,
          "Starting " + m_appName,
          {
            {"version", std::string(CTA_VERSION)}
      });
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
      std::cerr << "Failed to get host name: " << ex.getMessage().str() << std::endl;
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

    // Add the attributes present in every single log message
    std::map<std::string, std::string> logAttributes;
    if constexpr (HasSchedulerConfig<TConfig>) {
      if (m_appConfig.scheduler.backend_name.empty()) {
        throw exception::UserError("Scheduler backend name cannot be empty");
      }
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
      m_healthServer->start();
    }
  }

  void initTelemetry() {
    if (!m_appConfig.experimental.telemetry_enabled || m_appConfig.telemetry.config_file.empty()) {
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
      cta::telemetry::initOpenTelemetry(m_appConfig.telemetry.config_file, ctaResourceAttributes, lc);
    } catch (exception::Exception& ex) {
      cta::log::ScopedParamContainer params(lc);
      params.add("exceptionMessage", ex.getMessage().str());
      lc.log(log::ERR, "Failed to instantiate OpenTelemetry");
      cta::telemetry::cleanupOpenTelemetry(lc);
    }
  }

  void initXRootD() {
    if (m_appConfig.xrootd.security_protocol.empty()) {
      throw exception::UserError("XRootD security protocol cannot be empty");
    }

    if (m_appConfig.xrootd.security_protocol == "sss" && m_appConfig.xrootd.sss_keytab_path.empty()) {
      throw exception::UserError("XRootD SSS Keytab cannot be empty when security protocol is set to SSS");
    }
    // Overwrite XRootD env variables
    ::setenv("XrdSecPROTOCOL", m_appConfig.xrootd.security_protocol.c_str(), 1);
    ::setenv("XrdSecSSSKT", m_appConfig.xrootd.sss_keytab_path.c_str(), 1);
    // TODO: check the keytab here
  }

  const std::string m_appName;

  // The actual application class
  TApp m_app;
  // A struct representing the commandline arguments
  TOpts m_cliOptions;
  // A struct containing all configuration
  TConfig m_appConfig;

  std::unique_ptr<SignalReactorBuilder> m_signalReactorBuilder;
  // The health server must exist at this level as it needs to be in-scope for as long as the main app runs.
  // If not, it would immediately be destroyed after initHealthServer finished.
  std::unique_ptr<HealthServer> m_healthServer;

  std::unique_ptr<log::Logger> m_logPtr;
};

}  // namespace cta::runtime
