#include "MetricsUtils.hpp"
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include "version.h"

namespace cta::telemetry::metrics {

namespace metrics_sdk = opentelemetry::sdk::metrics;

std::shared_ptr<opentelemetry::metrics::Meter> getMeter(std::string_view componentName) {
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  return provider->GetMeter(componentName, CTA_VERSION);
}

}  // namespace cta::telemetry::metrics
