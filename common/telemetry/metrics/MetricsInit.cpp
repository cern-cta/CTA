#include "MetricsInit.hpp"

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

#include "MetricsConfig.hpp"

namespace cta::telemetry::metrics {

namespace metric_sdk = opentelemetry::sdk::metrics;
namespace metrics_api = opentelemetry::metrics;
namespace otlp = opentelemetry::exporter::otlp;

void initMetrics(const MetricsConfig& config) {
  if (config.backend == MetricsBackend::NOOP) {
    metrics_api::Provider::SetMeterProvider(
      opentelemetry::nostd::shared_ptr<metrics_api::MeterProvider>(new metrics_api::NoopMeterProvider()));
    return;
  }

  std::unique_ptr<metric_sdk::PushMetricExporter> exporter;

  switch (config.backend) {
    case MetricsBackend::STDOUT:
      exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create();
      break;

    case MetricsBackend::OTLP: {
      otlp::OtlpHttpMetricExporterOptions opts;
      opts.url = config.otlpEndpoint;

      for (const auto& kv : config.headers) {
        opts.http_headers.insert({kv.first, kv.second});
      }

      exporter = otlp::OtlpHttpMetricExporterFactory::Create(opts);
      break;
    }

    default:
      throw std::runtime_error("Unsupported metrics backend");
  }

  metric_sdk::PeriodicExportingMetricReaderOptions readerOptions {config.exportInterval, config.exportTimeout};

  auto reader = metric_sdk::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), readerOptions);
  opentelemetry::sdk::common::AttributeMap attributes = {
    {opentelemetry::sdk::resource::SemanticConventions::kServiceName, config.serviceName}
  };
  for (const auto& kv : config.resourceAttributes) {
    attributes.SetAttribute(kv.first, kv.second);
  }
  auto resource = opentelemetry::sdk::resource::Resource::Create(attributes);
  auto context = metric_sdk::MeterContextFactory::Create({}, resource);
  context->AddMetricReader(std::move(reader));
  auto providerFactory = metric_sdk::MeterProviderFactory::Create(std::move(context));

  std::shared_ptr<metrics_api::MeterProvider> provider(std::move(providerFactory));
  metrics_api::Provider::SetMeterProvider(provider);
}

}  // namespace cta::telemetry::metrics
