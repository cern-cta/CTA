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

#pragma once

#include "opentelemetry/context/context.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/exporters/ostream/metric_exporter_factory.h"
#include "opentelemetry/exporters/ostream/metric_exporter.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h"
#include "opentelemetry/sdk/metrics/meter.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include "opentelemetry/nostd/shared_ptr.h"

namespace cta::metric {

class MeterCounter;
class MeterHistogram;

class MeterProvider {
public:
  MeterProvider();
  MeterCounter getMeterCounter(const std::string & meterName, const std::string & counterName);
  MeterHistogram getMeterHistogram(const std::string & meterName, const std::string & histogramName);
private:
  void initMeterProvider();
};

class MeterCounter {
  friend class MeterProvider;
public:
  void add(uint64_t value, std::map<std::string, std::string> attributes);
private:
  MeterCounter();
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> _meter;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> _counter;
};

class MeterHistogram {
  friend class MeterProvider;
public:
  void record(double value, std::map<std::string, std::string> attributes);
private:
  MeterHistogram();
  opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> _meter;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>> _histogram;
};

} // namespace cta::metric
