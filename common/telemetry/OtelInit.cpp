/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "OtelInit.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NullPtrException.hpp"
#include "common/telemetry/CtaOtelLogHandler.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"

#include <opentelemetry/exporters/ostream/console_push_metric_builder.h>
#include <opentelemetry/exporters/otlp/otlp_file_push_metric_builder.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_push_metric_builder.h>
#include <opentelemetry/exporters/otlp/otlp_http_push_metric_builder.h>
#include <opentelemetry/sdk/common/attribute_utils.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/sdk/configuration/configuration.h>
#include <opentelemetry/sdk/configuration/configured_sdk.h>
#include <opentelemetry/sdk/configuration/registry.h>
#include <opentelemetry/sdk/configuration/yaml_configuration_parser.h>

namespace cta::telemetry {

static std::unique_ptr<opentelemetry::sdk::configuration::ConfiguredSdk> sdk;

// See https://github.com/open-telemetry/opentelemetry-cpp/blob/main/examples/configuration/main.cc
void initOpenTelemetry(const std::string& configFile,
                       const std::map<std::string, std::string>& ctaResourceAttributes,
                       cta::log::LogContext& lc) {
  // Before we get started, populate the CTA_OTEL_RESOURCE_ATTRIBUTES environment variable
  // This allows operators to reference these in the declarative config

  std::string otelResourceAttributes;
  for (const auto& [key, value] : ctaResourceAttributes) {
    if (!otelResourceAttributes.empty()) {
      otelResourceAttributes += ",";
    }
    otelResourceAttributes += key + "=" + value;
  }
  ::setenv("CTA_OTEL_RESOURCE_ATTRIBUTES", otelResourceAttributes.c_str(), 1);

  // Ensure any logged messages go through the CTA logging system
  opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogHandler(
    std::make_unique<CtaOtelLogHandler>(lc.logger()));

  // Populate the registry with the core components supported
  std::shared_ptr<opentelemetry::sdk::configuration::Registry> registry(
    new opentelemetry::sdk::configuration::Registry);

  // If we ever support logRecords/Spans, make sure to register them here

  // Console exporters
  opentelemetry::exporter::metrics::ConsolePushMetricBuilder::Register(registry.get());

  // Http exporters
  opentelemetry::exporter::otlp::OtlpHttpPushMetricBuilder::Register(registry.get());

  // Grpc exporters
  opentelemetry::exporter::otlp::OtlpGrpcPushMetricBuilder::Register(registry.get());

  // File exporters
  opentelemetry::exporter::otlp::OtlpFilePushMetricBuilder::Register(registry.get());

  // Parse the provided config.yaml
  // See e.g. https://github.com/open-telemetry/opentelemetry-configuration/blob/main/examples/kitchen-sink.yaml
  auto model = opentelemetry::sdk::configuration::YamlConfigurationParser::ParseFile(configFile);

  // Build the SDK from the parsed config.yaml
  sdk = opentelemetry::sdk::configuration::ConfiguredSdk::Create(registry, model);

  // Deploy the SDK
  if (sdk == nullptr) {
    throw cta::exception::NullPtrException("in initOpenTelemetry(): failed to create SDK");
  }
  sdk->Install();
  // Ensure all of our instruments are re-initialised to use the newly configured SDK
  cta::telemetry::metrics::initAllInstruments();
  lc.log(log::INFO, "In initOpenTelemetry(): OpenTelemetry was instantiated successfully");
}

void cleanupOpenTelemetry(cta::log::LogContext& lc) {
  if (sdk != nullptr) {
    sdk->UnInstall();
  }
  sdk.reset(nullptr);
  // Ensure all of our instruments are NOOP again
  cta::telemetry::metrics::initAllInstruments();
  lc.log(log::INFO, "In cleanupOpenTelemetry(): OpenTelemetry SDK was cleaned up");
}

}  // namespace cta::telemetry
