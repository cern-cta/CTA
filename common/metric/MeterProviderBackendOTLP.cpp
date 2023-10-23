/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022-2023 CERN
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

#include "common/metric/MeterProviderBackendOTLP.hpp"
#include "common/metric/MeterCounterOTLP.hpp"
#include "common/metric/MeterHistogramOTLP.hpp"

#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/exporters/ostream/metric_exporter_factory.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h"
#include "opentelemetry/sdk/metrics/meter_context_factory.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include "opentelemetry/nostd/shared_ptr.h"

namespace cta::metric {

MeterProviderBackendOTLP::MeterProviderBackendOTLP() {
  m_ofs = std::ofstream(METER_LOG_FILE, std::ofstream::out | std::ofstream::app);
  auto exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create(m_ofs);
  //auto exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create();

  // Initialize and set the global MeterProvider
  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions options;
  options.export_interval_millis = std::chrono::milliseconds(1000);
  options.export_timeout_millis = std::chrono::milliseconds(500);

  auto reader = opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), options);

  auto context = opentelemetry::sdk::metrics::MeterContextFactory::Create();
  context->AddMetricReader(std::move(reader));
  auto u_provider = opentelemetry::sdk::metrics::MeterProviderFactory::Create(std::move(context));
  std::shared_ptr<opentelemetry::metrics::MeterProvider> provider(std::move(u_provider));

  opentelemetry::metrics::Provider::SetMeterProvider(provider);
}

std::unique_ptr<MeterCounter> MeterProviderBackendOTLP::getMeterCounter(const std::string & topic, const std::string & counterName) {
  auto meter = std::make_unique<MeterCounterOTLP>();
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  meter->_meter = provider->GetMeter(topic);
  meter->_counter = meter->_meter->CreateUInt64Counter(counterName);
  return meter;
}
std::unique_ptr<MeterHistogram> MeterProviderBackendOTLP::getMeterHistogram(const std::string & topic, const std::string & histogramName) {
  auto meter = std::make_unique<MeterHistogramOTLP>();
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  meter->_meter = provider->GetMeter(topic);
  meter->_histogram = meter->_meter->CreateDoubleHistogram(histogramName);
  return meter;
}

}
