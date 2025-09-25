#include "TelemetryInit.hpp"

#include <fstream>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h>
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

std::unique_ptr<metrics_sdk::PushMetricExporter> createExporter(const TelemetryConfig& config,
                                                                cta::log::LogContext& lc) {
  std::unique_ptr<metrics_sdk::PushMetricExporter> exporter;
  switch (config.metrics.backend) {
    case MetricsBackend::STDOUT:
      lc.log(log::WARNING,
             "In createExporter(): OpenTelemetry backend STDOUT is meant for testing/debugging only and should not be "
             "used in production.");
      exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create();
      break;
    case MetricsBackend::FILE: {
      log::ScopedParamContainer params(lc);
      params.add("exportFileEndpoint", config.metrics.fileEndpoint);
      lc.log(log::WARNING,
             "In createExporter(): OpenTelemetry backend FILE is meant for testing/debugging only and should not be "
             "used in production.");
      static std::ofstream fileStream(config.metrics.fileEndpoint, std::ios::app);
      exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create(fileStream);
      break;
    }
    case MetricsBackend::OTLP_HTTP: {
      log::ScopedParamContainer params(lc);
      params.add("exportOtlpHttpEndpoint", config.metrics.otlpHttpEndpoint);
      opentelemetry::exporter::otlp::OtlpHttpMetricExporterOptions opts;
      opts.url = config.metrics.otlpHttpEndpoint;

      for (const auto& kv : config.metrics.otlpHttpHeaders) {
        opts.http_headers.insert({kv.first, kv.second});
      }
      exporter = opentelemetry::exporter::otlp::OtlpHttpMetricExporterFactory::Create(opts);
      break;
    }
    case MetricsBackend::OTLP_GRPC: {
      // All configuration goes via environment options here
      exporter = opentelemetry::exporter::otlp::OtlpGrpcMetricExporterFactory::Create();
      break;
    }

    default:
      lc.log(log::ERR, "In createExporter: Unsupported metrics backend provided");
      throw std::runtime_error("createExporter: Unsupported metrics backend provided.");
  }
  if (!exporter) {
    lc.log(log::ERR, "In createExporter: failed to initialise exporter.");
    throw std::runtime_error("createExporter: failed to initialise exporter.");
  }
  lc.log(log::DEBUG,
         "In createExporter(): OpenTelemetry exporter initialised"
         "used in production.");
  return exporter;
}

void initMetrics(const TelemetryConfig& config, cta::log::LogContext& lc) {
  if (config.metrics.backend == MetricsBackend::NOOP) {
    std::shared_ptr<metrics_api::MeterProvider> noopProvider = std::make_shared<metrics_api::NoopMeterProvider>();
    metrics_api::Provider::SetMeterProvider(noopProvider);
    lc.log(log::INFO, "In initMetrics(): NOOP backend provided. Telemetry will be disabled.");
    return;
  }

  std::string processName = cta::utils::getProcessName();
  std::string hostName = cta::utils::getShortHostname();
  std::string serviceInstanceId;
  if (config.retainInstanceIdOnRestart) {
    // Note that this requires the process name to be unique on a given host
    // However, it controls cardinality across restarts (in particular important for drive sessions)
    serviceInstanceId = hostName + ":" + processName;
  } else {
    serviceInstanceId = cta::utils::generateUuid();
  }

  log::ScopedParamContainer params(lc);
  params.add("serviceName", config.serviceName)
    .add("serviceNamespace", config.serviceNamespace)
    .add("serviceInstanceId", serviceInstanceId)
    .add("serviceVersion", config.serviceVersion)
    .add("metricsBackend", metricsBackendToString(config.metrics.backend))
    .add("exportInterval", std::chrono::duration_cast<std::chrono::milliseconds>(config.metrics.exportInterval).count())
    .add("exportTimeout", std::chrono::duration_cast<std::chrono::milliseconds>(config.metrics.exportTimeout).count());

  std::unique_ptr<metrics_sdk::PushMetricExporter> exporter = createExporter(config, lc);
  metrics_sdk::PeriodicExportingMetricReaderOptions readerOptions {config.metrics.exportInterval,
                                                                   config.metrics.exportTimeout};

  auto reader = metrics_sdk::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), readerOptions);

  // These metrics should make sure that each and every process is uniquely identifiable
  // Otherwise, metrics will not be aggregated correctly.
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

void reinitTelemetry(cta::log::LogContext& lc) {
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
