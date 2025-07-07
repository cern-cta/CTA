#include "MeterProvider.hpp"
#include <opentelemetry/metrics/provider.h>
#include "version.h"

namespace cta::telemetry::metrics {

std::shared_ptr<opentelemetry::metrics::Meter> getMeter(std::string_view componentName) {
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  return provider->GetMeter(componentName, CTA_VERSION);
}

}  // namespace cta::telemetry::metrics
