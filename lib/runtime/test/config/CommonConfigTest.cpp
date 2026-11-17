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

namespace {
struct FeatureTestConfig {
  enum class Feature { Telemetry, HealthServer };

  std::vector<std::string> enabled_features;
  std::vector<std::string> enabled_experimental_features;
  std::optional<cta::runtime::TelemetryConfig> telemetry;
  std::optional<cta::runtime::HealthServerConfig> health_server;

  static constexpr std::size_t memberCount() { return 4; }

  static constexpr auto features =
    std::tuple {cta::runtime::experimentalFeature(Feature::Telemetry, "telemetry", &FeatureTestConfig::telemetry),
                cta::runtime::stableFeature(Feature::HealthServer, "health_server", &FeatureTestConfig::health_server)};

  cta::runtime::ValidationResult validate() const { return cta::runtime::validateFeatures(*this); }
};
}  // namespace

TEST(CommonConfig, FeatureDescriptorsSupportSectionsAndStandaloneFeatures) {
  struct Config {
    std::optional<cta::runtime::TelemetryConfig> reporting = cta::runtime::TelemetryConfig {"/otel.yaml", "warn"};
  } config;

  constexpr auto cleanup = cta::runtime::stableFeature("cleanup");
  const auto features = std::tuple {cta::runtime::stableFeature("reporting", &Config::reporting), cleanup};
  const auto enabled = cta::runtime::featureNames(features);
  EXPECT_EQ(enabled, (std::vector<std::string> {"reporting", "cleanup"}));
  EXPECT_TRUE(cta::runtime::featureEnabled(enabled, cleanup));
  EXPECT_TRUE(cta::runtime::validateFeatureSelection(config, enabled, features).ok());
  EXPECT_TRUE(cta::runtime::validateFeatureSelection(config, {"cleanup"}, features).ok());
  config.reporting.reset();
  EXPECT_TRUE(cta::runtime::validateFeatureSelection(config, {"cleanup"}, features).ok());
  EXPECT_FALSE(cta::runtime::validateFeatureSelection(config, {"cleanup", "cleanup"}, features).ok());
  const auto invalid = cta::runtime::validateFeatureSelection(config, {"unknown"}, features, "enabled_items");
  EXPECT_NE(invalid.what().find("Field 'enabled_items' unsupported feature 'unknown'"), std::string::npos);
  EXPECT_FALSE(cta::runtime::validateFeatureSelection(config, enabled, features).ok());
}

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
          "enabled_features = ['health_server', 'health_server']\nenabled_experimental_features = []" + health,
          "enabled_features = []\nenabled_experimental_features = ['telemetry', 'telemetry']" + telemetry,
          "enabled_features = ['telemetry']\nenabled_experimental_features = []" + telemetry,
          "enabled_features = []\nenabled_experimental_features = ['health_server']" + health,
          "enabled_features = ['telemetry']\nenabled_experimental_features = ['telemetry']" + telemetry}) {
      EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(text, "test.toml", strict), cta::exception::UserError) << text;
    }
  }
}

TEST(CommonConfig, FeatureDescriptorsUseTheirDeclaredSelectionList) {
  FeatureTestConfig config;
  const auto& telemetry = std::get<0>(FeatureTestConfig::features);
  const auto& health = std::get<1>(FeatureTestConfig::features);
  config.enabled_features = {"telemetry"};
  config.enabled_experimental_features = {"health_server"};
  EXPECT_FALSE(cta::runtime::featureEnabled(config, telemetry));
  EXPECT_FALSE(cta::runtime::featureEnabled(config, health));

  config.enabled_features = {"health_server"};
  config.enabled_experimental_features = {"telemetry"};
  EXPECT_TRUE(cta::runtime::featureEnabled(config, telemetry));
  EXPECT_TRUE(cta::runtime::featureEnabled(config, health));
}

TEST(CommonConfig, EnumFeatureLookupUsesIdentityAndDeclaredSelectionList) {
  using Feature = FeatureTestConfig::Feature;
  FeatureTestConfig config;
  EXPECT_FALSE(cta::runtime::featureEnabled(config, Feature::Telemetry));
  EXPECT_FALSE(cta::runtime::featureEnabled(config, Feature::HealthServer));

  config.enabled_features = {"telemetry"};
  config.enabled_experimental_features = {"health_server"};
  EXPECT_FALSE(cta::runtime::featureEnabled(config, Feature::Telemetry));
  EXPECT_FALSE(cta::runtime::featureEnabled(config, Feature::HealthServer));

  config.enabled_experimental_features = {"telemetry"};
  EXPECT_TRUE(cta::runtime::featureEnabled(config, Feature::Telemetry));
  EXPECT_FALSE(cta::runtime::featureEnabled(config, Feature::HealthServer));

  config.enabled_features = {"health_server"};
  config.enabled_experimental_features.clear();
  EXPECT_FALSE(cta::runtime::featureEnabled(config, Feature::Telemetry));
  EXPECT_TRUE(cta::runtime::featureEnabled(config, Feature::HealthServer));
  EXPECT_FALSE(cta::runtime::featureEnabled(config, static_cast<Feature>(99)));
}

TEST(CommonConfig, WrongFeatureListExplainsHowToMigrate) {
  FeatureTestConfig config;
  config.enabled_features = {"telemetry"};
  const auto experimental = cta::runtime::validateFeatures(config);
  EXPECT_NE(experimental.what().find("feature 'telemetry' must be listed in enabled_experimental_features"),
            std::string::npos);

  config.enabled_features.clear();
  config.enabled_experimental_features = {"health_server"};
  const auto stable = cta::runtime::validateFeatures(config);
  EXPECT_NE(stable.what().find("feature 'health_server' must be listed in enabled_features"), std::string::npos);
}

TEST(CommonConfig, InactiveSectionsAreParsedAndRetained) {
  using cta::runtime::loadFromTomlString;
  const std::string disabled = "enabled_features = []\nenabled_experimental_features = []\n";
  for (bool strict : {false, true}) {
    for (const auto& settings : {"[telemetry]\nconfig_file = 42", "telemetry = 42\nhealth_server = 'unused'"}) {
      EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(disabled + settings, "test.toml", strict),
                   cta::exception::UserError);
    }
    const auto config = loadFromTomlString<FeatureTestConfig>(
      disabled + "[telemetry]\nconfig_file = '/unused.yaml'\non_init_failure = 'invalid'\n",
      "test.toml",
      strict);
    ASSERT_TRUE(config.telemetry);
    EXPECT_EQ(config.telemetry->on_init_failure, "invalid");
    EXPECT_FALSE(cta::runtime::featureEnabled(config, FeatureTestConfig::Feature::Telemetry));
  }
  const std::string partial = disabled + "[telemetry]\nconfig_file = '/unused.yaml'\n";
  EXPECT_NO_THROW(loadFromTomlString<FeatureTestConfig>(partial, "test.toml", false));
  EXPECT_THROW(loadFromTomlString<FeatureTestConfig>(partial, "test.toml", true), cta::exception::UserError);
}

TEST(CommonConfig, ValidationUsesSelectionRatherThanSectionPresence) {
  FeatureTestConfig config;
  config.telemetry.emplace().on_init_failure = "invalid";
  EXPECT_TRUE(cta::runtime::validateFeatures(config).ok());
  config.enabled_experimental_features = {"telemetry"};
  EXPECT_FALSE(cta::runtime::validateFeatures(config).ok());
}

TEST(CommonConfig, UnixTransportRetainsTypedInactiveTcpSettings) {
  for (bool strict : {false, true}) {
    const auto config = cta::runtime::loadFromTomlString<cta::runtime::HealthServerConfig>(
      "transport = 'unix'\n[tcp]\nhost = ''\nport = 0",
      "test.toml",
      strict);
    ASSERT_TRUE(config.tcp);
    EXPECT_EQ(config.tcp->port, 0);
    EXPECT_THROW(cta::runtime::loadFromTomlString<cta::runtime::HealthServerConfig>(
                   "transport = 'unix'\n[tcp]\nhost = ''\nport = 'invalid'",
                   "test.toml",
                   strict),
                 cta::exception::UserError);
  }
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
    EXPECT_THROW(loadFromTomlString<HealthServerConfig>("transport = 'invalid'", "test.toml", strict),
                 cta::exception::UserError);
  }
  EXPECT_THROW(
    loadFromTomlString<HealthServerConfig>("transport = 'tcp'\n[tcp]\nhost = '127.0.0.1'", "test.toml", true),
    cta::exception::UserError);
}

namespace {
struct OptionalSectionConfig {
  std::optional<cta::runtime::TelemetryConfig> telemetry = cta::runtime::TelemetryConfig {"/default.yaml", "warn"};
  bool telemetry_enabled = true;

  static constexpr std::size_t memberCount() { return 2; }
};
}  // namespace

TEST(CommonConfig, OptionalSectionPresenceIsIndependentOfSelectors) {
  for (bool strict : {false, true}) {
    const auto missing =
      cta::runtime::loadFromTomlString<OptionalSectionConfig>("telemetry_enabled = true", "test.toml", strict);
    ASSERT_EQ(missing.telemetry.has_value(), !strict);
    if (!strict) {
      EXPECT_EQ(missing.telemetry->config_file, "/default.yaml");
    }
    const auto present = cta::runtime::loadFromTomlString<OptionalSectionConfig>(
      "telemetry_enabled = false\n[telemetry]\nconfig_file = '/explicit.yaml'\non_init_failure = 'warn'",
      "test.toml",
      strict);
    ASSERT_TRUE(present.telemetry);
    EXPECT_EQ(present.telemetry->config_file, "/explicit.yaml");
  }
}
}  // namespace unitTests
