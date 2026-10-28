/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/Application.hpp"

#include "tests/Argv.hpp"
#include "tests/TempDirectory.hpp"
#include "tests/TempFile.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
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

struct RuntimeDirectoryConfig {
  cta::runtime::LoggingConfig logging;
  std::string value;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const { return logging.validate(); }
};

class RuntimeDirectoryApp {
public:
  static inline bool wasRun = false;

  void stop() {}

  int run(const RuntimeDirectoryConfig& config, cta::log::Logger&) {
    wasRun = true;
    return EXIT_SUCCESS;
  }
};

class ApplicationRuntimeDirectory : public ::testing::Test {
protected:
  void SetUp() override { RuntimeDirectoryApp::wasRun = false; }

  int checkConfig(bool withRuntimeDirectory = true) {
    Argv args = withRuntimeDirectory ?
                  Argv {"cta-test", "--config", config.path(), "--runtime-dir", directory.path(), "--config-check"} :
                  Argv {"cta-test", "--config", config.path(), "--config-check"};
    cta::runtime::Application<RuntimeDirectoryApp, RuntimeDirectoryConfig, cta::runtime::CommonCliOptions> app(
      "cta-test",
      "");
    return app.run(args.count, args.data());
  }

  std::string readSnapshot(const std::string& name) {
    std::ifstream input(directory.path() + "/" + name, std::ios::binary);
    EXPECT_TRUE(input.is_open()) << name;
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
  }

  const std::string original = "value = '$${1INVALID}'\r\n[logging]\r\nlevel = 'WARNING'\r\nformat = 'json'\r\n";
  TempDirectory directory;
  TempFile config {original, ".toml"};
};

TEST_F(ApplicationRuntimeDirectory, ConfigCheckRejectsRuntimeDirectoryBeforeReadingConfig) {
  ASSERT_TRUE(std::filesystem::remove(config.path()));
  try {
    checkConfig();
    FAIL() << "Expected incompatible options to be rejected";
  } catch (const cta::exception::UserError& error) {
    EXPECT_EQ(error.getMessageValue(), "--config-check cannot be combined with --runtime-dir.");
  }
  EXPECT_TRUE(std::filesystem::is_empty(directory.path()));
  EXPECT_FALSE(RuntimeDirectoryApp::wasRun);
}

TEST_F(ApplicationRuntimeDirectory, ConfigCheckLeavesExistingSnapshotsUntouched) {
  for (const auto* name : {"config.toml", "config.resolved.toml", "version.json"}) {
    cta::utils::createFileWithContents("existing snapshot", directory.path() + "/" + name);
  }
  EXPECT_THROW(checkConfig(), cta::exception::UserError);
  for (const auto* name : {"config.toml", "config.resolved.toml", "version.json"}) {
    EXPECT_EQ(readSnapshot(name), "existing snapshot");
  }
  EXPECT_FALSE(RuntimeDirectoryApp::wasRun);
}

TEST_F(ApplicationRuntimeDirectory, ConfigCheckWithoutRuntimeDirectoryValidatesWithoutStartingApp) {
  ASSERT_EQ(checkConfig(false), EXIT_SUCCESS);
  EXPECT_FALSE(RuntimeDirectoryApp::wasRun);
  EXPECT_TRUE(std::filesystem::is_empty(directory.path()));

  config.stringFill("[logging]\nlevel = 'INVALID'\n");
  EXPECT_THROW(checkConfig(false), cta::exception::UserError);
  EXPECT_FALSE(RuntimeDirectoryApp::wasRun);
}

TEST(Application, ExpandsConfigWithoutRuntimeDirectory) {
  struct Config {
    cta::runtime::LoggingConfig logging;
    std::string value;

    static constexpr std::size_t memberCount() { return 2; }

    cta::runtime::ValidationResult validate() const { return logging.validate(); }
  };

  class App {
  public:
    void stop() {}

    int run(const Config& config, cta::log::Logger&) {
      return config.value == "${1INVALID}" ? EXIT_SUCCESS : EXIT_FAILURE;
    }
  };

  TempFile file("value = '$${1INVALID}'\n[logging]\nlevel = 'WARNING'\nformat = 'json'\n", ".toml");
  Argv args({"cta-test", "--config", file.path()});
  cta::runtime::Application<App, Config, cta::runtime::CommonCliOptions> app("cta-test", "");
  EXPECT_EQ(app.run(args.count, args.data()), EXIT_SUCCESS);
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

}  // namespace unitTests
