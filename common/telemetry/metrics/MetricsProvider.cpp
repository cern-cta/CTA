#include "MetricsProvider.hpp"

#include "version.h"

namespace cta::telemetry::metrics {

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> getMeter(const std::string& componentName) {
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  return provider->GetMeter(componentName, CTA_VERSION);
}

}  // namespace cta::telemetry::metrics
