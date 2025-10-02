/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
