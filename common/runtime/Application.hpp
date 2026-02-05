/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ArgParser.hpp"
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
template<typename F>
  requires std::invocable<F> && std::convertible_to<std::invoke_result_t<F>, int>
int safeRun(F&& func) {
  int returnCode = EXIT_FAILURE;
  try {
    returnCode = func();
  } catch (const exception::UserError& ex) {
    // This will be mostly used to print issues with users e.g. making a mistake in the config file.
    // So we only print FATAL to keep it reasonably user friendly.
    std::cerr << "FATAL:\n" << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  } catch (const exception::Exception& ex) {
    std::cerr << "FATAL: Caught an unexpected CTA exception:\n" << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  } catch (const std::exception& se) {
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
template<typename F>
  requires std::invocable<F> && std::convertible_to<std::invoke_result_t<F>, int>
int safeRunWithLog(log::Logger& log, F&& func) {
  int returnCode = EXIT_FAILURE;
  try {
    returnCode = func();
  } catch (const exception::UserError& ex) {
    log(log::CRIT,
        "FATAL: User Error",
        {
          {"exceptionMessage", ex.getMessage().str()}
    });
    sleep(1);
    return EXIT_FAILURE;
  } catch (const exception::Exception& ex) {
    log(log::CRIT,
        "FATAL: Caught an unexpected CTA exception",
        {
          {"exceptionMessage", ex.getMessage().str()}
    });
    sleep(1);
    return EXIT_FAILURE;
  } catch (const std::exception& se) {
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
 * - The CliOptions struct the commandline arguments should populate. Note that you pass in the struct (compile time), not the object.
 * - The name of the application.
 * - The description of the application (may be empty)
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
   * @param appName Name of the application. For example, cta-maintd, cta-taped, cta-admin.
   * @param description. Description of the application. Used for the help message. May be empty.
   */
  Application(const std::string& appName, const std::string& description)
    requires HasLoggingConfig<TConfig> && HasStopFunction<TApp> && HasRequiredCliOptions<TOpts>
      : m_appName(appName),
        m_argParser(appName) {
    if (m_appName.empty()) {
      throw exception::Exception("Application name cannot be empty.");
    }
    m_argParser.withDescription(description);
    // We need to start the reactor builder here as we may want to register custom signals.
    // It is also for that reason that we don't build the actual SignalReactor just yet.
    m_signalReactorBuilder = std::make_unique<SignalReactorBuilder>();
    m_signalReactorBuilder->addSignalFunction(SIGTERM, [this]() { m_app.stop(); });
    m_signalReactorBuilder->addSignalFunction(SIGUSR1, [this]() {
      if (m_logPtr) {
        m_logPtr->refresh();
      }
    });
  }

  ~Application() {
    if (m_logPtr) {
      cta::log::Logger& log = *m_logPtr;
      log(log::INFO, "Exiting application: " + m_appName);
    }
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
   * @param overwrite Whether to overwrite the function associated with the given signal if a function was already registered to it.
   * @return Application& This application object. Can be used to easily chain multiple of these calls together.
   */
  Application& addSignalFunction(int signal, void (TApp::*method)(), bool overwrite = false) {
    auto& app = m_app;
    m_signalReactorBuilder->addSignalFunction(signal, [&app, method] { (app.*method)(); }, overwrite);
    return *this;
  }

  /**
   * @brief Returns the parser object. Allows for adding additional commandline arguments.
   * Example:
   *   app.parser().withStringArg(&CustomCliOptions::iAmExtra, "extra", 'e', "STUFF", "Some extra argument.");
   * Which will require CustomCliOptions (TConfig) to have a field:
   *   std::string iAmExtra;
   *
   * @return ArgParser<TOpts>& Reference to the parser object.
   */
  ArgParser<TOpts>& parser() { return m_argParser; }

  /**
   * @brief Runs the application. Will do some initialisation and call the run() method of the underlying app.
   *
   * @return int Return code.
   */
  int run(const int argc, char** const argv) {
    // Parse commandline opts
    auto cliOptions = m_argParser.parse(argc, argv);
    if (cliOptions.showHelp) {
      std::cout << m_argParser.usageString() << std::endl;
      return EXIT_SUCCESS;
    }

    if (cliOptions.showVersion) {
      std::cout << m_argParser.versionString() << std::endl;
      return EXIT_SUCCESS;
    }

    const auto config = runtime::loadFromToml<TConfig>(cliOptions.configFilePath, cliOptions.configStrict);
    if (cliOptions.configCheck) {
      std::cout << "Config check passed." << std::endl;
      return EXIT_SUCCESS;
    }

    initLogging(config, cliOptions);
    auto signalReactor = m_signalReactorBuilder->build(*m_logPtr);
    signalReactor.start();
    // The health server must exist at this level as it needs to be in-scope for as long as the main app runs.
    // If not, it would immediately be destroyed after initHealthServer finished.
    // Note that healthServer lives outside of safeRunWithLog so that it is not destructed before we output a (potential) FATAL message.
    std::unique_ptr<HealthServer> healthServer;

    return safeRunWithLog(*m_logPtr, [this, cliOptions, config, &healthServer]() {
      cta::log::Logger& log = *m_logPtr;
      log(log::INFO, "Initialising application: " + m_appName);

      // We dynamically add/init the relevant parts depending on what is in the config type.
      if constexpr (HasHealthServerConfig<TConfig>) {
        static_assert(HasReadinessFunction<TApp> && HasLivenessFunction<TApp>,
                      "Config has health_server, but app type lacks isReady()/isLive() methods");
        healthServer = initHealthServer(config);
      }

      if constexpr (HasTelemetryConfig<TConfig>) {
        initTelemetry(config);
      }

      if constexpr (HasXRootDConfig<TConfig>) {
        initXRootD(config);
      }

      log(log::INFO,
          "Starting application: " + m_appName,
          {
            {"version", std::string(CTA_VERSION)}
      });
      // Not all apps need to consume the CliOptions.
      // So here we allow either the full run() signature, or one that omits the CliOptions
      if constexpr (HasRunFunctionWithOpts<TApp, TConfig, TOpts, log::Logger>) {
        return m_app.run(config, cliOptions, log);
      } else if constexpr (HasRunFunction<TApp, TConfig, log::Logger>) {
        return m_app.run(config, log);
      } else {
        static_assert([] { return false; }(), "TApp has no suitable run(...) overload");
      }
    });
  }

private:
  void initLogging(const TConfig& config, const TOpts& cliOptions) {
    using namespace cta;
    std::string shortHostName;
    try {
      shortHostName = utils::getShortHostname();
    } catch (const exception::Errnum& ex) {
      throw exception::Exception("Failed to get host name: " + ex.getMessage().str());
    }

    try {
      if (!cliOptions.logFilePath.empty()) {
        m_logPtr = std::make_unique<log::FileLogger>(shortHostName, m_appName, cliOptions.logFilePath, log::INFO);
      } else {
        // Default to stdout logger
        m_logPtr = std::make_unique<log::StdoutLogger>(shortHostName, m_appName);
      }
      m_logPtr->setLogFormat(config.logging.format);
    } catch (exception::Exception& ex) {
      throw exception::Exception("Failed to instantiate CTA logging system: " + ex.getMessage().str());
    }

    m_logPtr->setLogMask(config.logging.level);

    // Add the attributes present in every single log message
    std::map<std::string, std::string> logAttributes;
    if constexpr (HasSchedulerConfig<TConfig>) {
      if (config.scheduler.backend_name.empty()) {
        throw exception::UserError("Scheduler backend name cannot be empty");
      }
      logAttributes["sched_backend"] = config.scheduler.backend_name;
    }
    for (const auto& [key, value] : config.logging.attributes) {
      logAttributes[key] = value;
    }
    m_logPtr->setStaticParams(logAttributes);
  }

  std::unique_ptr<HealthServer> initHealthServer(const TConfig& config) {
    if (config.health_server.enabled) {
      auto healthServer = std::make_unique<HealthServer>(
        *m_logPtr,
        config.health_server.host,
        config.health_server.port,
        [this]() { return m_app.isReady(); },
        [this]() { return m_app.isLive(); });
      healthServer->start();
      return healthServer;
    }
    return nullptr;
  }

  void initTelemetry(const TConfig& config) {
    if (!config.experimental.telemetry_enabled || config.telemetry.config_file.empty()) {
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
        ctaResourceAttributes[cta::semconv::attr::kSchedulerNamespace] = config.scheduler.backend_name;
      }
      cta::telemetry::initOpenTelemetry(config.telemetry.config_file, ctaResourceAttributes, lc);
    } catch (exception::Exception& ex) {
      cta::log::ScopedParamContainer params(lc);
      params.add("exceptionMessage", ex.getMessage().str());
      lc.log(log::ERR, "Failed to instantiate OpenTelemetry");
      cta::telemetry::cleanupOpenTelemetry(lc);
    }
  }

  void initXRootD(const TConfig& config) const {
    if (config.xrootd.security_protocol.empty()) {
      throw exception::UserError("XRootD security protocol cannot be empty");
    }

    if (config.xrootd.security_protocol == "sss" && config.xrootd.sss_keytab_path.empty()) {
      throw exception::UserError("XRootD SSS Keytab cannot be empty when security protocol is set to SSS");
    }
    // Overwrite XRootD env variables
    ::setenv("XrdSecPROTOCOL", config.xrootd.security_protocol.c_str(), 1);
    ::setenv("XrdSecSSSKT", config.xrootd.sss_keytab_path.c_str(), 1);
    if (!config.xrootd.sss_keytab_path.empty() && ::access(config.xrootd.sss_keytab_path.c_str(), R_OK) != 0) {
      // Can we replace this check by something the XRootD libs provide?
      throw cta::exception::Exception("Failed to read or open XRootD SSS keytab file: "
                                      + config.xrootd.sss_keytab_path);
    }
  }

  const std::string m_appName;
  std::unique_ptr<log::Logger> m_logPtr;

  ArgParser<TOpts> m_argParser;
  // The actual application class
  TApp m_app;

  std::unique_ptr<SignalReactorBuilder> m_signalReactorBuilder;
};

}  // namespace cta::runtime
