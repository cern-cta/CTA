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

#include <atomic>
#include <fstream>

#include "common/metric/Meter.hpp"

#define METER_LOG_FILE "/tmp/cta-meter.log"

namespace cta::metric {

MeterProvider::MeterProvider() {
  static std::mutex setup_lock;
  static std::atomic<bool> setup_done = false;

  if (!setup_done) {
    std::unique_lock lock(setup_lock);
    if (setup_done == false) {
      initMeterProvider();
      setup_done = true;
    }
  }
}

void MeterProvider::initMeterProvider() {
  std::ofstream ofs;
  //ofs.open(METER_LOG_FILE, std::ofstream::out | std::ofstream::app);
  //auto exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create(ofs);
  auto exporter = opentelemetry::exporter::metrics::OStreamMetricExporterFactory::Create();

  // Initialize and set the global MeterProvider
  opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions options;
  options.export_interval_millis = std::chrono::milliseconds(1000);
  options.export_timeout_millis = std::chrono::milliseconds(500);

  auto reader = opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), options);

  auto u_provider = opentelemetry::sdk::metrics::MeterProviderFactory::Create();
  auto *p = static_cast<opentelemetry::sdk::metrics::MeterProvider *>(u_provider.get());
  p->AddMetricReader(std::move(reader));
}

MeterCounter MeterProvider::getMeterCounter(const std::string & meterName, const std::string & counterName) {
  auto meter = MeterCounter();
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  meter._meter = provider->GetMeter(meterName);
  meter._counter = meter._meter->CreateUInt64Counter(counterName);
  return meter;
}

MeterHistogram MeterProvider::getMeterHistogram(const std::string & meterName, const std::string & histogramName) {
  auto meter = MeterHistogram();
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  meter._meter = provider->GetMeter(meterName);
  meter._histogram = meter._meter->CreateDoubleHistogram(histogramName);
  return meter;
}

void MeterCounter::add(uint64_t value, std::map<std::string, std::string> attributes) {
  auto labelKv = opentelemetry::common::KeyValueIterableView<decltype(attributes)>{attributes};
  _counter->Add(value, labelKv);
}

void MeterHistogram::record(double value, std::map<std::string, std::string> attributes) {
  auto labelKv = opentelemetry::common::KeyValueIterableView<decltype(attributes)>{attributes};
  auto context = opentelemetry::context::Context{};
  _histogram->Record(value, labelKv, context);
}

} // namespace cta::metric
