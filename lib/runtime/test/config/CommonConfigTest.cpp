/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/config/CommonConfig.hpp"

#include "runtime/config/ConfigLoader.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(CommonConfig, DefaultValuesAreValid) {
  EXPECT_TRUE(cta::runtime::CatalogueConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::LoggingConfig {}.validate().ok());
  EXPECT_FALSE(cta::runtime::TelemetryConfig {}.validate().ok());
  EXPECT_FALSE(cta::runtime::HealthServerConfig {}.validate().ok());
  EXPECT_TRUE(cta::runtime::XRootDConfig {}.validate().ok());
}

TEST(CommonConfig, RejectsInvalidStringChoice) {
  cta::runtime::LoggingConfig config;
  config.level = "verbose";

  EXPECT_FALSE(config.validate().ok());
}

TEST(CommonConfig, RejectsEmptyRequiredString) {
  cta::runtime::CatalogueConfig config;
  config.config_file.clear();

  EXPECT_FALSE(config.validate().ok());
}

TEST(CommonConfig, SchedulerConfigRequiresBackendName) {
  cta::runtime::SchedulerConfig config;
  EXPECT_FALSE(config.validate().ok());

  config.backend_name = "vfsTest";
  EXPECT_TRUE(config.validate().ok());
}

TEST(CommonConfig, RejectsNonPositiveValue) {
  cta::runtime::SchedulerConfig config;
  config.backend_name = "vfsTest";
  config.tape_cache_max_age_secs = 0;

  EXPECT_FALSE(config.validate().ok());
}

TEST(CommonConfig, ReportsAllInvalidFields) {
  cta::runtime::LoggingConfig config;
  config.level = "verbose";
  config.format = "xml";

  EXPECT_EQ(config.validate().what(),
            "1) Field 'format' has unsupported value 'xml'.\n"
            "2) Field 'level' has unsupported value 'verbose'.\n");
}

}  // namespace unitTests

namespace unitTests {
namespace {
struct FeatureTestConfig {
  std::vector<std::string> enabled_features;
  std::vector<std::string> enabled_experimental_features;
  std::optional<cta::runtime::TelemetryConfig> telemetry;
  std::optional<cta::runtime::HealthServerConfig> health_server;

  static constexpr std::size_t memberCount() { return 4; }

  cta::runtime::ValidationResult validate() const {
    auto result = cta::runtime::validateFeatureLists(enabled_features,
                                                     enabled_experimental_features,
                                                     {"health_server"},
                                                     {"telemetry"});
    cta::runtime::validateFeature(result, enabled_experimental_features, "telemetry", telemetry);
    cta::runtime::validateFeature(result, enabled_features, "health_server", health_server);
    return result;
  }
};
}  // namespace

TEST(CommonConfig, FeatureSelectionIsValidatedInBothModes) {
  using cta::runtime::loadFromTomlString;
  const std::string telemetry = "\n[telemetry]\nconfig_file = '/otel.yaml'\non_init_failure = 'fatal'\n";
  const std::string health = "\n[health_server]\ntransport = 'unix'\n";
  for (const bool strict : {false, true}) {
    for (const auto& document :
         {"enabled_features = []\nenabled_experimental_features = []",
          "enabled_features = ['health_server']\nenabled_experimental_features = []",
          "enabled_features = []\nenabled_experimental_features = ['telemetry']",
          "enabled_features = ['health_server']\nenabled_experimental_features = ['telemetry']"}) {
      std::string text = document;
      if (text.find("'health_server'") != std::string::npos) {
        text += health;
      }
      if (text.find("'telemetry'") != std::string::npos) {
        text += telemetry;
      }
      EXPECT_NO_THROW(loadFromTomlString<FeatureTestConfig>(text, "test.toml", strict)) << text;
    }
    // Both selection lists enforce names, duplicates, maturity and matching sections.
    for (const auto& text :
         {std::string("enabled_features = ['unknown']\nenabled_experimental_features = []"),
          std::string("enabled_features = []\nenabled_experimental_features = ['unknown']"),
          std::string("enabled_features = []\nenabled_experimental_features = ['telemetry']"),
          "enabled_features = []\nenabled_experimental_features = []" + telemetry,
          "enabled_features = ['health_server', 'health_server']\nenabled_experimental_features = []" + health,
          "enabled_features = []\nenabled_experimental_features = ['telemetry', 'telemetry']" + telemetry,
          "enabled_features = ['telemetry']\nenabled_experimental_features = []" + telemetry,
          "enabled_features = []\nenabled_experimental_features = ['health_server']" + health,
          "enabled_features = ['telemetry']\nenabled_experimental_features = ['telemetry']" + telemetry}) {
      EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(text, "test.toml", strict), cta::exception::UserError) << text;
    }
  }
}

TEST(CommonConfig, WrongFeatureListExplainsHowToMigrate) {
  const auto experimental = cta::runtime::validateFeatureLists({"telemetry"}, {}, {"health_server"}, {"telemetry"});
  EXPECT_NE(experimental.what().find("is experimental; move it to enabled_experimental_features"), std::string::npos);
  const auto stable = cta::runtime::validateFeatureLists({}, {"health_server"}, {"health_server"}, {"telemetry"});
  EXPECT_NE(stable.what().find("is stable; move it to enabled_features"), std::string::npos);
}

TEST(CommonConfig, StrictFeatureSettingsRemainExplicit) {
  using cta::runtime::loadFromTomlString;
  for (const auto& text : {"", "enabled_features = []", "enabled_experimental_features = []"}) {
    EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(text, "test.toml", true), cta::exception::UserError);
    EXPECT_NO_THROW(loadFromTomlString<FeatureTestConfig>(text, "test.toml", false));
  }
  const std::string partial = "enabled_features = []\nenabled_experimental_features = ['telemetry']\n"
                              "[telemetry]\nconfig_file = '/otel.yaml'\n";
  EXPECT_NO_THROW(loadFromTomlString<FeatureTestConfig>(partial, "test.toml", false));
  EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(partial, "test.toml", true), cta::exception::UserError);
  EXPECT_THROW(
    loadFromTomlString<FeatureTestConfig>(partial + "on_init_failure = 'fatal'\nunknown = 1", "test.toml", true),
    cta::exception::UserError);
  EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(
                 "enabled_features = []\nenabled_experimental_features = ['telemetry']\ntelemetry = 1",
                 "test.toml",
                 true),
               cta::exception::UserError);
}

TEST(CommonConfig, HealthTransportRequiresOnlyItsOwnSettings) {
  using cta::runtime::HealthServerConfig;
  using cta::runtime::loadFromTomlString;
  for (const bool strict : {false, true}) {
    EXPECT_NO_THROW(loadFromTomlString<HealthServerConfig>("transport = 'unix'", "test.toml", strict));
    EXPECT_NO_THROW(loadFromTomlString<HealthServerConfig>("transport = 'tcp'\n[tcp]\nhost = '127.0.0.1'\nport = 8080",
                                                           "test.toml",
                                                           strict));
    EXPECT_THROW(loadFromTomlString<HealthServerConfig>("transport = 'tcp'", "test.toml", strict),
                 cta::exception::UserError);
    EXPECT_THROW(loadFromTomlString<HealthServerConfig>("transport = 'unix'\n[tcp]\nhost = '127.0.0.1'\nport = 8080",
                                                        "test.toml",
                                                        strict),
                 cta::exception::UserError);
    EXPECT_THROW(loadFromTomlString<HealthServerConfig>("transport = 'invalid'", "test.toml", strict),
                 cta::exception::UserError);
  }
  EXPECT_THROW(
    loadFromTomlString<HealthServerConfig>("transport = 'tcp'\n[tcp]\nhost = '127.0.0.1'", "test.toml", true),
    cta::exception::UserError);
}
}  // namespace unitTests

namespace unitTests {
namespace {
struct SelectedSectionConfig {
  // Declare the section before its selector to exercise order-independent loading.
  std::optional<cta::runtime::TelemetryConfig> telemetry = cta::runtime::TelemetryConfig {"/default.yaml", "warn"};
  bool telemetry_enabled = true;

  static constexpr std::size_t memberCount() { return 2; }

  bool isSectionEnabled(std::string_view) const { return telemetry_enabled; }
};
}  // namespace

TEST(CommonConfig, SectionSelectionPrecedesOptionalLoading) {
  const auto disabled =
    cta::runtime::loadFromTomlString<SelectedSectionConfig>("telemetry_enabled = false", "test.toml", true);
  EXPECT_FALSE(disabled.telemetry);
  const auto defaults = cta::runtime::loadFromTomlString<SelectedSectionConfig>("", "test.toml", false);
  ASSERT_TRUE(defaults.telemetry);
  EXPECT_EQ(defaults.telemetry->config_file, "/default.yaml");
  for (bool strict : {false, true}) {
    EXPECT_THROW(cta::runtime::loadFromTomlString<SelectedSectionConfig>(
                   "telemetry_enabled = false\n[telemetry]\nconfig_file = '/explicit.yaml'\non_init_failure = 'warn'",
                   "test.toml",
                   strict),
                 cta::exception::UserError);
  }
}
}  // namespace unitTests
