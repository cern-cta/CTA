/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Application.hpp"

#include "CommonCliOptions.hpp"
#include "RuntimeTestHelpers.hpp"

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <thread>

namespace unitTests {

struct MinimalTestConfig {
  cta::runtime::LoggingConfig logging;
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

TEST(Application, AppWithNonExistingConfigFile) {
  using namespace cta;
  const std::string appName = "cta-test";
  Argv args({appName, "--config", "IdontExistwow"});
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

  class TestStoppableApp {
  public:
    TestStoppableApp() = default;
    ~TestStoppableApp() = default;

    void stop() { m_running = false; }

    int run(const MinimalTestConfig& config, cta::log::Logger& log) {
      m_running = true;
      while (m_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      return EXIT_SUCCESS;
    }

    // Global so that we can wait for this later on
    static inline std::atomic<bool> m_running = false;
  };

  TempFile f(R"toml(
[logging]
level = "WARNING"
format = "json"
)toml",
             ".toml");

  std::atomic<int> rc = EXIT_FAILURE;
  const std::string appName = "cta-test";
  Argv args({appName, "--config", f.path()});
  runtime::Application<TestStoppableApp, MinimalTestConfig, runtime::CommonCliOptions> app(appName, "");
  std::jthread thread([&]() { rc = app.run(args.count, args.data()); });
  // Give it some time to start
  cta::utils::waitForCondition([&]() { return TestStoppableApp::m_running; }, 2000, 10);
  ASSERT_EQ(0, ::kill(::getpid(), SIGTERM));
  cta::utils::waitForCondition([&]() { return rc == EXIT_SUCCESS; }, 2000, 100);
  ASSERT_EQ(rc, EXIT_SUCCESS);
  ASSERT_TRUE(thread.joinable());
}

}  // namespace unitTests
