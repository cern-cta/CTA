#include "TelemetryInit.hpp"

#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h>
#include <opentelemetry/exporters/ostream/metric_exporter_factory.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h"
#include <opentelemetry/sdk/metrics/meter_context.h>
#include <opentelemetry/sdk/metrics/meter_context_factory.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/sdk/common/attribute_utils.h>

#include "config/TelemetryConfigSingleton.hpp"

namespace cta::telemetry {

namespace metric_sdk = opentelemetry::sdk::metrics;
namespace metrics_api = opentelemetry::metrics;
namespace otlp = opentelemetry::exporter::otlp;

void initMetrics(const TelemetryConfig& config) {
  // printTelemetryConfig(config, std::cerr);
  if (config.metrics.backend == MetricsBackend::NOOP) {
    metrics_api::Provider::SetMeterProvider(
      std::shared_ptr<metrics_api::MeterProvider>(new metrics_api::NoopMeterProvider()));
    return;
  }

  std::unique_ptr<metric_sdk::PushMetricExporter> exporter;

  switch (config.metrics.backend) {
    case MetricsBackend::STDOUT:
      exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create();
      break;

    case MetricsBackend::OTLP: {
      otlp::OtlpHttpMetricExporterOptions opts;
      opts.url = config.metrics.otlpEndpoint;

      for (const auto& kv : config.metrics.headers) {
        opts.http_headers.insert({kv.first, kv.second});
      }

      exporter = otlp::OtlpHttpMetricExporterFactory::Create(opts);
      break;
    }

    default:
      throw std::runtime_error("initMetrics: Unsupported metrics backend");
  }
  if (!exporter) {
    throw std::runtime_error("initMetrics: failed to initialise exporter");
  }

  metric_sdk::PeriodicExportingMetricReaderOptions readerOptions {config.metrics.exportInterval,
                                                                  config.metrics.exportTimeout};

  auto reader = metric_sdk::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), readerOptions);
  opentelemetry::sdk::common::AttributeMap attributes = {
    {opentelemetry::sdk::resource::SemanticConventions::kServiceName, config.serviceName}
  };
  for (const auto& kv : config.resourceAttributes) {
    attributes.SetAttribute(kv.first, kv.second);
  }
  auto resource = opentelemetry::sdk::resource::Resource::Create(attributes);
  auto view_registry = std::make_unique<opentelemetry::sdk::metrics::ViewRegistry>();
  auto context = metric_sdk::MeterContextFactory::Create(std::move(view_registry), resource);
  context->AddMetricReader(std::move(reader));
  auto providerFactory = metric_sdk::MeterProviderFactory::Create(std::move(context));
  std::shared_ptr<metrics_api::MeterProvider> provider(std::move(providerFactory));

  metrics_api::Provider::SetMeterProvider(provider);
}

void initTelemetry(const TelemetryConfig& config) {
  // Eventually we can init e.g. traces here as well
  initMetrics(config);
  // Ensure we can reuse the config when re-initialise the metrics after e.g. a fork
  cta::telemetry::TelemetryConfigSingleton::initialize(config);
}

void reinitTelemetry() {
  initTelemetry(cta::telemetry::TelemetryConfigSingleton::get());
}

void resetTelemetry() {
  metrics_api::Provider::SetMeterProvider(
      std::shared_ptr<metrics_api::MeterProvider>(new metrics_api::NoopMeterProvider()));
}

}  // namespace cta::telemetry
