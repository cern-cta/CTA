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
  std::optional<cta::runtime::TelemetryConfig> telemetry = std::nullopt;

  static constexpr std::size_t memberCount() { return 2; }

  cta::runtime::ValidationResult validate() const {
    auto result = cta::runtime::validateFeatureNames(enabled_features, {"telemetry"});
    cta::runtime::validateFeature(result, enabled_features, "telemetry", telemetry);
    return result;
  }
};
}  // namespace

TEST(CommonConfig, FeatureSelectionIsValidatedInBothModes) {
  using cta::runtime::loadFromTomlString;
  const std::string section = "\n[telemetry]\nconfig_file = '/otel.yaml'\non_init_failure = 'fatal'\n";
  for (const bool strict : {false, true}) {
    EXPECT_NO_THROW(loadFromTomlString<FeatureTestConfig>("enabled_features = []", "test.toml", strict));
    EXPECT_NO_THROW(
      loadFromTomlString<FeatureTestConfig>("enabled_features = ['telemetry']" + section, "test.toml", strict));
    for (const auto& text : {std::string("enabled_features = ['unknown']"),
                             std::string("enabled_features = ['telemetry']"),
                             "enabled_features = []" + section,
                             "enabled_features = ['telemetry', 'telemetry']" + section}) {
      EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(text, "test.toml", strict), cta::exception::UserError) << text;
    }
  }
}

TEST(CommonConfig, StrictFeatureSettingsRemainExplicit) {
  using cta::runtime::loadFromTomlString;
  EXPECT_THROW(loadFromTomlString<FeatureTestConfig>("", "test.toml", true), cta::exception::UserError);
  EXPECT_NO_THROW(loadFromTomlString<FeatureTestConfig>("", "test.toml", false));
  const std::string partial = "enabled_features = ['telemetry']\n[telemetry]\nconfig_file = '/otel.yaml'\n";
  EXPECT_NO_THROW(loadFromTomlString<FeatureTestConfig>(partial, "test.toml", false));
  EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(partial, "test.toml", true), cta::exception::UserError);
  EXPECT_THROW(
    loadFromTomlString<FeatureTestConfig>(partial + "on_init_failure = 'fatal'\nunknown = 1", "test.toml", true),
    cta::exception::UserError);
  EXPECT_THROW(
    loadFromTomlString<FeatureTestConfig>("enabled_features = ['telemetry']\ntelemetry = 1", "test.toml", true),
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
