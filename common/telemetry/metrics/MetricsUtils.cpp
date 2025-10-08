/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "MetricsUtils.hpp"
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include "version.h"

namespace cta::telemetry::metrics {

namespace metrics_sdk = opentelemetry::sdk::metrics;

std::shared_ptr<opentelemetry::metrics::Meter> getMeter(std::string_view libraryName, std::string_view libraryVersion) {
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  return provider->GetMeter(libraryName, libraryVersion);
}

}  // namespace cta::telemetry::metrics
