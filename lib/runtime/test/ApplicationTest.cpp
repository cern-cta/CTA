/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/Application.hpp"

#include "tests/Argv.hpp"
#include "tests/TempFile.hpp"

#include <chrono>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <httplib.h>
#include <iterator>
#include <stdexcept>
#include <thread>

namespace unitTests {

TEST(Application, LegacyTelemetryServiceNameUsesDots) {
  EXPECT_EQ("cta.maintd", cta::runtime::legacyTelemetryServiceName("cta-maintd"));
  EXPECT_EQ("cta.maintd", cta::runtime::legacyTelemetryServiceName("cta.maintd"));
}

class SignalMaskGuard {
public:
  explicit SignalMaskGuard(int signal) {
    sigset_t signals;
    if (::sigemptyset(&signals) != 0 || ::sigaddset(&signals, signal) != 0
        || ::pthread_sigmask(SIG_BLOCK, &signals, &m_previousMask) != 0) {
      throw std::runtime_error("Failed to block signal for test");
    }
  }

  ~SignalMaskGuard() { ::pthread_sigmask(SIG_SETMASK, &m_previousMask, nullptr); }

private:
  sigset_t m_previousMask;
};

struct MinimalTestConfig {
  cta::runtime::LoggingConfig logging;

  static constexpr std::size_t memberCount() { return 1; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("logging", logging.validate());
    return result;
  }
};

class TestApp {
public:
  TestApp() = default;
  ~TestApp() = default;

  void stop() {}

  int run(const MinimalTestConfig& config, cta::runtime::CommonCliOptions opts, cta::log::Logger& log) {
    return EXIT_SUCCESS;
  }
};

TEST(Application, SimpleApp) {
  using namespace cta;

  TempFile f(R"toml(
[logging]
level = "WARNING"
format = "json"
)toml",
             ".toml");

  int rc = runtime::safeRun([f]() {
    const std::string appName = "cta-test";
    Argv args({appName, "--config", f.path()});
    runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions> app(appName, "");
    return app.run(args.count, args.data());
  });
  ASSERT_EQ(rc, EXIT_SUCCESS);
}

TEST(Application, RootConfigValidationIsExercisedDuringConfigCheck) {
  using namespace cta;

  TempFile f(R"toml(
[logging]
level = "NOT_A_LEVEL"
format = "json"
)toml",
             ".toml");

  const std::string appName = "cta-test";
  Argv args({appName, "--config", f.path(), "--config-check"});
  runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions> app(appName, "");

  ASSERT_THROW(app.run(args.count, args.data()), exception::UserError);
}

TEST(Application, AppWithNonExistingConfigFile) {
  using namespace cta;
  const std::string appName = "cta-test";
  Argv args({appName, "--config", "IDontExistWow"});
  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;
  App app(appName, "");
  ASSERT_THROW({ app.run(args.count, args.data()); }, exception::UserError);
}

TEST(Application, SimpleAppWithCustomCliOpts) {
  using namespace cta;

  struct ExtendsFromCliOptions : public runtime::CommonCliOptions {
    std::string iAmExtra;
  };

  class TestAppCustomCliOpts {
  public:
    TestAppCustomCliOpts() = default;
    ~TestAppCustomCliOpts() = default;

    void stop() {}

    int run(const MinimalTestConfig& config, ExtendsFromCliOptions opts, cta::log::Logger& log) {
      return (opts.iAmExtra == "test") ? EXIT_SUCCESS : EXIT_FAILURE;
    }
  };

  TempFile f(R"toml(
[logging]
level = "WARNING"
format = "json"
)toml",
             ".toml");

  int rc = runtime::safeRun([f]() {
    const std::string appName = "cta-test";
    Argv args({appName, "--config", f.path(), "--extra", "test"});
    runtime::Application<TestAppCustomCliOpts, MinimalTestConfig, ExtendsFromCliOptions> app(appName, "");
    app.parser().withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "STUFF", "Some extra argument");
    return app.run(args.count, args.data());
  });
  ASSERT_EQ(rc, EXIT_SUCCESS);
}

TEST(Application, AppCompilesIfRunDoesNotConsumeCliOpts) {
  using namespace cta;

  class TestAppWithoutCliOpts {
  public:
    TestAppWithoutCliOpts() = default;
    ~TestAppWithoutCliOpts() = default;

    void stop() {}

    int run(const MinimalTestConfig& config, cta::log::Logger& log) { return EXIT_SUCCESS; }
  };

  TempFile f(R"toml(
[logging]
level = "WARNING"
format = "json"
)toml",
             ".toml");

  int rc = runtime::safeRun([f]() {
    const std::string appName = "cta-test";
    Argv args({appName, "--config", f.path()});
    runtime::Application<TestAppWithoutCliOpts, MinimalTestConfig, runtime::CommonCliOptions> app(appName, "");
    return app.run(args.count, args.data());
  });
  ASSERT_EQ(rc, EXIT_SUCCESS);
}

TEST(Application, AppCompilesWithCustomConfig) {
  using namespace cta;

  struct CustomTestConfig {
    cta::runtime::LoggingConfig logging;
    std::string extraConfigField;

    static constexpr std::size_t memberCount() { return 2; }

    cta::runtime::ValidationResult validate() const {
      cta::runtime::ValidationResult result;
      result.merge("logging", logging.validate());
      return result;
    }
  };

  class TestAppWithCustomConfig {
  public:
    TestAppWithCustomConfig() = default;
    ~TestAppWithCustomConfig() = default;

    void stop() {}

    int run(const CustomTestConfig& config, cta::log::Logger& log) {
      return config.extraConfigField == "extra" ? EXIT_SUCCESS : EXIT_FAILURE;
    }
  };

  TempFile f(R"toml(
extraConfigField = "extra"
[logging]
level = "WARNING"
format = "json"
)toml",
             ".toml");

  int rc = runtime::safeRun([f]() {
    const std::string appName = "cta-test";
    Argv args({appName, "--config", f.path()});
    runtime::Application<TestAppWithCustomConfig, CustomTestConfig, runtime::CommonCliOptions> app(appName, "");
    return app.run(args.count, args.data());
  });
  ASSERT_EQ(rc, EXIT_SUCCESS);
}

TEST(Application, AppCheckConfigLenientWrong) {
  using namespace cta;

  TempFile f(R"toml(
[logging]
level = 3 # Should be a string
)toml",
             ".toml");

  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;

  int rc = runtime::safeRun([f]() {
    const std::string appName = "cta-test";
    Argv args({appName, "--config", f.path(), "--config-check"});
    App app(appName, "");
    return app.run(args.count, args.data());
  });
  ASSERT_EQ(rc, EXIT_FAILURE);
}

TEST(Application, AppCheckConfigStrictWrong) {
  using namespace cta;

  TempFile f(R"toml(
[logging]
level = "WARNING"
# Missing format
)toml",
             ".toml");

  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;

  int rc = runtime::safeRun([f]() {
    const std::string appName = "cta-test";
    Argv args({appName, "--config", f.path(), "--config-check", "--config-strict"});
    App app(appName, "");
    return app.run(args.count, args.data());
  });
  ASSERT_EQ(rc, EXIT_FAILURE);
}

TEST(Application, AppHandlesSigTerm) {
  using namespace cta;

  // Application must start before any unblocked threads exist. Block SIGTERM before creating the sender so it inherits
  // the correct mask, leaving the SignalReactor as the only thread that consumes the process-directed signal.
  SignalMaskGuard signalMaskGuard(SIGTERM);

  // Global so that we can wait for this later on
  static std::atomic<bool> stoppableTestApprunning = false;
  stoppableTestApprunning = false;

  class TestStoppableApp {
  public:
    TestStoppableApp() = default;
    ~TestStoppableApp() = default;

    void stop() { stoppableTestApprunning = false; }

    int run(const MinimalTestConfig& config, cta::log::Logger& log) {
      stoppableTestApprunning = true;
      while (stoppableTestApprunning) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      return EXIT_SUCCESS;
    }
  };

  TempFile f(R"toml(
[logging]
level = "WARNING"
format = "json"
)toml",
             ".toml");

  const std::string appName = "cta-test";
  Argv args({appName, "--config", f.path()});
  runtime::Application<TestStoppableApp, MinimalTestConfig, runtime::CommonCliOptions> app(appName, "");
  std::atomic<int> signalResult = -1;
  std::jthread signalSender([&]() {
    try {
      cta::utils::waitForCondition([&]() { return stoppableTestApprunning == true; }, 2000, 10);
      signalResult = ::kill(::getpid(), SIGTERM);
    } catch (...) {
      signalResult = -1;
    }
  });

  const int rc = app.run(args.count, args.data());
  signalSender.join();

  EXPECT_EQ(0, signalResult);
  EXPECT_EQ(EXIT_SUCCESS, rc);
}

// These applications retain configuration and logging references until destruction.
struct LifecycleConfig {
  cta::runtime::LoggingConfig logging;
  cta::runtime::ExperimentalConfig experimental;
  cta::runtime::TelemetryConfig telemetry;
  cta::runtime::HealthServerConfig health_server;

  static constexpr std::size_t memberCount() { return 4; }

  cta::runtime::ValidationResult validate() const {
    cta::runtime::ValidationResult result;
    result.merge("logging", logging.validate());
    result.merge("health_server", health_server.validate());
    return result;
  }
};

struct LifecycleState {
  unsigned destructions = 0;
  bool throwFromRun = false;
  bool throwFromLogAttributes = false;
  bool exerciseCallbacks = false;
  std::atomic<bool> signalEntered = false;
  std::atomic<bool> signalFinished = false;
  std::atomic<unsigned> healthCalls = 0;
};

class LifecycleApp {
public:
  static inline LifecycleState* state = nullptr;

  ~LifecycleApp() {
    ++state->destructions;
    if (m_config) {
      EXPECT_EQ("json", m_config->logging.format);
    }
    if (state->exerciseCallbacks) {
      EXPECT_TRUE(state->signalFinished.load());
      EXPECT_GT(state->healthCalls.load(), 0);
      // A live endpoint here would still be able to call this object during destruction.
      httplib::Client client(*m_config->health_server.host, *m_config->health_server.port);
      client.set_address_family(AF_UNIX);
      client.set_connection_timeout(1);
      client.set_read_timeout(2);
      EXPECT_FALSE(client.Get("/health/ready"));
    }
    if (m_log) {
      (*m_log)(cta::log::INFO, "Lifecycle app destroyed");
    }
  }

  std::map<std::string, std::string> getStaticLogAttributes(const LifecycleConfig& config) const {
    m_config = &config;
    if (state->throwFromLogAttributes) {
      throw std::runtime_error("Static log attributes failed");
    }
    return {};
  }

  void stop() {}

  void customSignal() {
    state->signalEntered = true;
    // Keep the callback active briefly after run() is allowed to return.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    state->signalFinished = true;
  }

  bool isReady() const {
    ++state->healthCalls;
    return true;
  }

  bool isLive() const { return true; }

  int run(const LifecycleConfig& config, cta::log::Logger& log) {
    m_log = &log;
    if (state->exerciseCallbacks) {
      httplib::Client client(*config.health_server.host, *config.health_server.port);
      client.set_address_family(AF_UNIX);
      client.set_connection_timeout(1);
      client.set_read_timeout(2);
      const auto response = client.Get("/health/ready");
      EXPECT_TRUE(response);
      if (response) {
        EXPECT_EQ(200, response->status);
      }
      EXPECT_EQ(0, ::kill(::getpid(), SIGUSR2));
      cta::utils::waitForCondition([] { return state->signalEntered.load(); }, 2000, 1);
    }
    if (state->throwFromRun) {
      throw std::runtime_error("Lifecycle run failure");
    }
    return EXIT_SUCCESS;
  }

private:
  mutable const LifecycleConfig* m_config = nullptr;
  cta::log::Logger* m_log = nullptr;
};

TEST(Application, DestroysAppBeforeTelemetryAndAfterCallbacks) {
  using namespace cta;
  for (const bool throwFromRun : {false, true}) {
    SCOPED_TRACE(throwFromRun);
    LifecycleState state;
    state.throwFromRun = throwFromRun;
    state.exerciseCallbacks = true;
    LifecycleApp::state = &state;
    TempFile telemetry("file_format: \"1.0-rc.1\"\ndisabled: true\n", ".yaml");
    TempFile socket("", ".sock");
    ::unlink(socket.path().c_str());
    TempFile logs;
    TempFile config("[logging]\nlevel = \"INFO\"\nformat = \"json\"\n"
                    "[experimental]\ntelemetry_enabled = true\n"
                    "[telemetry]\nconfig_file = \""
                      + telemetry.path()
                      + "\"\n"
                        "[health_server]\nenabled = true\nhost = \""
                      + socket.path() + "\"\nport = 80\n",
                    ".toml");
    {
      runtime::Application<LifecycleApp, LifecycleConfig, runtime::CommonCliOptions> app("cta-test", "");
      app.addSignalFunction(SIGUSR2, &LifecycleApp::customSignal);
      Argv args({"cta-test", "--config", config.path(), "--log-file", logs.path()});
      EXPECT_EQ(throwFromRun ? EXIT_FAILURE : EXIT_SUCCESS, app.run(args.count, args.data()));
      EXPECT_EQ(1, state.destructions);
      EXPECT_THROW(app.run(args.count, args.data()), exception::Exception);
    }
    EXPECT_EQ(1, state.destructions);
    std::ifstream input(logs.path());
    const std::string output((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    const auto destroyed = output.find("Lifecycle app destroyed");
    const auto telemetryStopped = output.find("OpenTelemetry shut down successfully");
    EXPECT_NE(std::string::npos, destroyed);
    EXPECT_NE(std::string::npos, telemetryStopped);
    EXPECT_LT(destroyed, telemetryStopped);
    if (throwFromRun) {
      EXPECT_LT(output.find("Fatal unexpected exception"), destroyed);
    }
    LifecycleApp::state = nullptr;
  }
}

TEST(Application, LoggerInitializationFailureStillDestroysAppBeforeConfig) {
  using namespace cta;
  LifecycleState state;
  state.throwFromLogAttributes = true;
  LifecycleApp::state = &state;
  TempFile config("[logging]\nlevel = \"WARNING\"\nformat = \"json\"\n", ".toml");
  {
    runtime::Application<LifecycleApp, LifecycleConfig, runtime::CommonCliOptions> app("cta-test", "");
    Argv args({"cta-test", "--config", config.path()});
    EXPECT_THROW(app.run(args.count, args.data()), std::runtime_error);
    EXPECT_EQ(1, state.destructions);
  }
  EXPECT_EQ(1, state.destructions);
  LifecycleApp::state = nullptr;
}

TEST(Application, InitializationFailureDestroysAppBeforeConfigLeavesScope) {
  using namespace cta;
  LifecycleState state;
  LifecycleApp::state = &state;
  TempFile invalidTelemetry("not valid SDK configuration: [", ".yaml");
  TempFile config("[logging]\nlevel = \"WARNING\"\nformat = \"json\"\n"
                  "[experimental]\ntelemetry_enabled = true\n"
                  "[telemetry]\non_init_failure = \"fatal\"\nconfig_file = \""
                    + invalidTelemetry.path() + "\"\n",
                  ".toml");
  {
    runtime::Application<LifecycleApp, LifecycleConfig, runtime::CommonCliOptions> app("cta-test", "");
    Argv args({"cta-test", "--config", config.path()});
    EXPECT_EQ(EXIT_FAILURE, app.run(args.count, args.data()));
    EXPECT_EQ(1, state.destructions);
  }
  EXPECT_EQ(1, state.destructions);
  LifecycleApp::state = nullptr;
}

}  // namespace unitTests
