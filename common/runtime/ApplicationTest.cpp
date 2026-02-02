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
    runtime::CommonCliOptions opts;
    opts.configFilePath = f.path();
    runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions> app("test-app", opts);
    return app.run();
  });
  ASSERT_EQ(rc, EXIT_SUCCESS);
}

TEST(Application, AppWithNonExistingConfigFile) {
  using namespace cta;
  runtime::CommonCliOptions opts;
  opts.configFilePath = "IdontExistwow";
  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;
  ASSERT_THROW({ App app("test-app", opts); }, exception::UserError);
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
    ExtendsFromCliOptions opts;
    opts.configFilePath = f.path();
    opts.iAmExtra = "test";
    runtime::Application<TestAppCustomCliOpts, MinimalTestConfig, ExtendsFromCliOptions> app("test-app", opts);
    return app.run();
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
    runtime::CommonCliOptions opts;
    opts.configFilePath = f.path();
    runtime::Application<TestAppWithoutCliOpts, MinimalTestConfig, runtime::CommonCliOptions> app("test-app", opts);
    return app.run();
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
    runtime::CommonCliOptions opts;
    opts.configFilePath = f.path();
    runtime::Application<TestAppWithCustomConfig, CustomTestConfig, runtime::CommonCliOptions> app("test-app", opts);
    return app.run();
  });
  ASSERT_EQ(rc, EXIT_SUCCESS);
}

TEST(ApplicationDeathTest, AppCheckConfigLenientCorrect) {
  using namespace cta;

  TempFile f(R"toml(
    # In lenient mode, we can rely on defaults
)toml",
             ".toml");
  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;

  EXPECT_EXIT(
    {
      runtime::CommonCliOptions opts;
      opts.configFilePath = f.path();
      opts.configCheck = true;
      App app("test-app", opts);
      app.run();
    },
    testing::ExitedWithCode(EXIT_SUCCESS),
    "");
}

TEST(Application, AppCheckConfigLenientWrong) {
  using namespace cta;

  TempFile f(R"toml(
[logging]
level = 3 # Should be a string
)toml",
             ".toml");

  runtime::CommonCliOptions opts;
  opts.configFilePath = f.path();
  opts.configCheck = true;
  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;

  ASSERT_THROW({ App app("test-app", opts); }, exception::UserError);

  int rc = runtime::safeRun([opts]() {
    App app("test-app", opts);
    return app.run();
  });
  ASSERT_EQ(rc, EXIT_FAILURE);
}

TEST(ApplicationDeathTest, AppCheckConfigStrictCorrect) {
  using namespace cta;

  TempFile f(R"toml(
[logging]
level = "WARNING"
format = "json"
  [logging.attributes]
)toml",
             ".toml");
  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;

  EXPECT_EXIT(
    {
      runtime::CommonCliOptions opts;
      opts.configFilePath = f.path();
      opts.configCheck = true;
      opts.configStrict = true;
      App app("test-app", opts);
      app.run();
    },
    testing::ExitedWithCode(EXIT_SUCCESS),
    "");
}

TEST(Application, AppCheckConfigStrictWrong) {
  using namespace cta;

  TempFile f(R"toml(
[logging]
level = "WARNING"
# Missing format
)toml",
             ".toml");

  runtime::CommonCliOptions opts;
  opts.configFilePath = f.path();
  opts.configCheck = true;
  opts.configStrict = true;
  using App = runtime::Application<TestApp, MinimalTestConfig, runtime::CommonCliOptions>;

  ASSERT_THROW({ App app("test-app", opts); }, exception::UserError);

  int rc = runtime::safeRun([opts]() {
    App app("test-app", opts);
    return app.run();
  });
  ASSERT_EQ(rc, EXIT_FAILURE);
}

// These tests should ideally be implemented at some point, but they are a bit lower priority

TEST(Application, AppHandlesSigTerm) {
  using namespace cta;

  class TestStoppableApp {
  public:
    TestStoppableApp() = default;
    ~TestStoppableApp() = default;

    void stop() { m_stop = true; }

    int run(const MinimalTestConfig& config, cta::log::Logger& log) {
      while (!m_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
      return EXIT_SUCCESS;
    }

  private:
    std::atomic<bool> m_stop = false;
  };

  TempFile f(R"toml(
[logging]
level = "WARNING"
format = "json"
)toml",
             ".toml");

  std::atomic<int> rc = EXIT_FAILURE;
  runtime::CommonCliOptions opts;
  opts.configFilePath = f.path();
  runtime::Application<TestStoppableApp, MinimalTestConfig, runtime::CommonCliOptions> app("test-app", opts);
  std::jthread thread([&]() { rc = app.run(); });
  // Give it some time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_EQ(0, ::kill(::getpid(), SIGTERM));
  cta::utils::waitForCondition([&]() { return rc == EXIT_SUCCESS; }, 1000, 200);
  ASSERT_EQ(rc, EXIT_SUCCESS);
  ASSERT_TRUE(thread.joinable());
}

TEST(Application, AppCorrectXRootDKeyTab) {}

TEST(Application, AppWrongXRootDKeyTab) {}

TEST(Application, AppEmptyXRootDKeyTab) {}

}  // namespace unitTests
