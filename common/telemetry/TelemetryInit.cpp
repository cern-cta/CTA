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
#include "metrics/InstrumentProvider.hpp"
#include "common/utils/utils.hpp"
#include "version.h"

namespace cta::telemetry {

namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace metrics_api = opentelemetry::metrics;
namespace otlp = opentelemetry::exporter::otlp;

void initMetrics(const TelemetryConfig& config) {
  if (config.metrics.backend == MetricsBackend::NOOP) {
    metrics_api::Provider::SetMeterProvider(
      std::shared_ptr<metrics_api::MeterProvider>(new metrics_api::NoopMeterProvider()));
    return;
  }

  std::unique_ptr<metrics_sdk::PushMetricExporter> exporter;

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

  metrics_sdk::PeriodicExportingMetricReaderOptions readerOptions {config.metrics.exportInterval,
                                                                   config.metrics.exportTimeout};

  auto reader = metrics_sdk::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), readerOptions);

  // These metrics should make sure that each and every process is uniquely identifiable
  // Otherwise, metrics will not be aggregated correctly.
  opentelemetry::sdk::common::AttributeMap attributes = {
    {opentelemetry::sdk::resource::SemanticConventions::kServiceName,    config.serviceName      },
    {opentelemetry::sdk::resource::SemanticConventions::kServiceVersion, CTA_VERSION             },
    {opentelemetry::sdk::resource::SemanticConventions::kProcessPid,     std::to_string(getpid())}
  };
  for (const auto& kv : config.resourceAttributes) {
    attributes.SetAttribute(kv.first, kv.second);
  }
  auto resource = opentelemetry::sdk::resource::Resource::Create(attributes);
  auto viewRegistry = std::make_unique<opentelemetry::sdk::metrics::ViewRegistry>();
  auto context = metrics_sdk::MeterContextFactory::Create(std::move(viewRegistry), resource);
  context->AddMetricReader(std::move(reader));
  auto meterProvider = metrics_sdk::MeterProviderFactory::Create(std::move(context));
  std::shared_ptr<metrics_api::MeterProvider> apiProvider(std::move(meterProvider));

  metrics_api::Provider::SetMeterProvider(apiProvider);
}

void initTelemetry(const TelemetryConfig& config) {
  // Eventually we can init e.g. traces here as well
  initMetrics(config);
  // Ensure we can reuse the config when re-initialise the metrics after e.g. a fork
  cta::telemetry::TelemetryConfigSingleton::initialize(config);
}

void reinitTelemetry() {
  initTelemetry(cta::telemetry::TelemetryConfigSingleton::get());
  cta::telemetry::metrics::InstrumentProvider::instance().reset();
}

void resetTelemetry() {
  auto provider = metrics_api::Provider::GetMeterProvider();
  if (provider) {
    auto sdkProvider = dynamic_cast<metrics_sdk::MeterProvider*>(provider.get());
    if (sdkProvider != nullptr) {
      sdkProvider->ForceFlush();
    }
  }

  // This will invoke shutdown and clean up the background threads as needed before a fork
  std::shared_ptr<metrics_api::MeterProvider> none;
  metrics_api::Provider::SetMeterProvider(none);
}

}  // namespace cta::telemetry
