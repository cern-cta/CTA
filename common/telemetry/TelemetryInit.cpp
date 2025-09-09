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
#include <opentelemetry/sdk/common/attribute_utils.h>

#include "common/utils/utils.hpp"
#include "common/telemetry/config/TelemetryConfigSingleton.hpp"
#include "common/telemetry/metrics/InstrumentRegistry.hpp"
#include "common/semconv/SemConv.hpp"

namespace cta::telemetry {

namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace metrics_api = opentelemetry::metrics;
namespace otlp = opentelemetry::exporter::otlp;

// This is a hack for the taped parent process to maintain the same service.instance.id
// after forking, since forking requires a shutdown of telemetry prior to it
// Once we remove forking entirely from taped, this can go away
static std::string previousServiceInstanceId;

void initMetrics(const TelemetryConfig& config, cta::log::LogContext& lc) {
  if (config.metrics.backend == MetricsBackend::NOOP) {
    std::shared_ptr<metrics_api::MeterProvider> noopProvider = std::make_shared<metrics_api::NoopMeterProvider>();
    metrics_api::Provider::SetMeterProvider(noopProvider);
    lc.log(log::INFO, "In initMetrics(): Telemetry is disabled.");
    return;
  }

  std::string processName = cta::utils::getProcessName();
  std::string hostName = cta::utils::getShortHostname();
  std::string serviceInstanceId;
  if (!previousServiceInstanceId.empty()) {
    serviceInstanceId = previousServiceInstanceId;
  } else {
    serviceInstanceId = cta::utils::generateUuid();
    previousServiceInstanceId = serviceInstanceId;
  }

  log::ScopedParamContainer params(lc);
  params.add("serviceName", config.serviceName)
    .add("serviceNamespace", config.serviceNamespace)
    .add("serviceInstanceId", serviceInstanceId)
    .add("serviceVersion", config.serviceVersion)
    .add("metricsBackend", metricsBackendToString(config.metrics.backend))
    .add("exportInterval", std::chrono::duration_cast<std::chrono::milliseconds>(config.metrics.exportInterval).count())
    .add("exportTimeout", std::chrono::duration_cast<std::chrono::milliseconds>(config.metrics.exportTimeout).count())
    .add("otlpEndpoint", config.metrics.otlpEndpoint);

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
      lc.log(log::ERR, "In initMetrics: Unsupported metrics backend provided");
      throw std::runtime_error("initMetrics: Unsupported metrics backend provided.");
  }
  if (!exporter) {
    lc.log(log::ERR, "In initMetrics: failed to initialise exporter.");
    throw std::runtime_error("initMetrics: failed to initialise exporter.");
  }

  metrics_sdk::PeriodicExportingMetricReaderOptions readerOptions {config.metrics.exportInterval,
                                                                   config.metrics.exportTimeout};

  auto reader = metrics_sdk::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), readerOptions);

  // These metrics should make sure that each and every process is uniquely identifiable
  // Otherwise, metrics will not be aggregated correctly.
  // The processName could go once we get rid of the forking in taped
  opentelemetry::sdk::common::AttributeMap attributes = {
    {cta::semconv::kServiceName,       config.serviceName     },
    {cta::semconv::kServiceNamespace,  config.serviceNamespace},
    {cta::semconv::kServiceVersion,    config.serviceVersion  },
    {cta::semconv::kServiceInstanceId, serviceInstanceId      },
    {cta::semconv::kProcessTitle,      processName            },
    {cta::semconv::kHostName,          hostName               }
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
  cta::telemetry::metrics::initAllInstruments();
  lc.log(log::INFO, "In initMetrics(): Telemetry metrics initialised.");
}

void initTelemetry(const TelemetryConfig& config, cta::log::LogContext& lc) {
  // Eventually we can init e.g. traces here as well
  initMetrics(config, lc);
  // Ensure we can reuse the config when re-initialise the metrics after e.g. a fork
  cta::telemetry::TelemetryConfigSingleton::initialize(config);
}

void initTelemetryConfig(const TelemetryConfig& config) {
  // Ensure we can reuse the config when re-initialise the metrics after e.g. a fork
  cta::telemetry::TelemetryConfigSingleton::initialize(config);
}

void reinitTelemetry(cta::log::LogContext& lc, bool persistServiceInstanceId) {
  if (!persistServiceInstanceId) {
    previousServiceInstanceId = "";
  }
  initTelemetry(cta::telemetry::TelemetryConfigSingleton::get(), lc);
}

void shutdownTelemetry(cta::log::LogContext& lc) {
  const auto config = cta::telemetry::TelemetryConfigSingleton::get();
  if (config.metrics.backend == MetricsBackend::NOOP) {
    // Nothing to reset
    return;
  }

  lc.log(log::INFO, "In shutdownTelemetry(): Clearing telemetry state.");
  auto provider = metrics_api::Provider::GetMeterProvider();
  if (provider) {
    auto sdkProvider = dynamic_cast<metrics_sdk::MeterProvider*>(provider.get());
    if (sdkProvider != nullptr) {
      sdkProvider->ForceFlush();
    }
  }

  // This will invoke shutdown and clean up the background threads as needed before a fork
  auto noop = std::make_shared<metrics_api::NoopMeterProvider>();
  metrics_api::Provider::SetMeterProvider(noop);
  // Ensure all of our instruments are NOOP again
  cta::telemetry::metrics::initAllInstruments();
  lc.log(log::INFO, "In shutdownTelemetry(): Telemetry state cleared.");
}

}  // namespace cta::telemetry
