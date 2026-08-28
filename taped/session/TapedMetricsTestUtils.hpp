/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/semconv/Attributes.hpp"
#include "common/semconv/Metrics.hpp"
#include "lib/telemetry/src/telemetry/metrics/InstrumentRegistry.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <map>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/metric_reader.h>
#include <stdexcept>
#include <string>

namespace cta::telemetry::testing {

// Collect real SDK observations synchronously, without an exporter or background reader.
class ManualMetricReader : public opentelemetry::sdk::metrics::MetricReader {
public:
  opentelemetry::sdk::metrics::AggregationTemporality
  GetAggregationTemporality(opentelemetry::sdk::metrics::InstrumentType) const noexcept override {
    return opentelemetry::sdk::metrics::AggregationTemporality::kCumulative;
  }

private:
  bool OnForceFlush(std::chrono::microseconds) noexcept override { return true; }

  bool OnShutDown(std::chrono::microseconds) noexcept override { return true; }
};

// Install isolated instruments and restore the previous provider once all test workers have joined.
class ScopedTapedMetrics {
public:
  ScopedTapedMetrics() : m_previous(opentelemetry::metrics::Provider::GetMeterProvider()) {
    m_provider->AddMetricReader(m_reader);
    opentelemetry::metrics::Provider::SetMeterProvider(m_provider);
    metrics::initAllInstruments();
    metrics::setDriveStatus(common::dataStructures::DriveStatus::Unknown);
    metrics::setMountType(common::dataStructures::MountType::NoMount);
  }

  ~ScopedTapedMetrics() {
    metrics::setDriveStatus(common::dataStructures::DriveStatus::Unknown);
    metrics::setMountType(common::dataStructures::MountType::NoMount);
    opentelemetry::metrics::Provider::SetMeterProvider(m_previous);
    metrics::initAllInstruments();
  }

  ScopedTapedMetrics(const ScopedTapedMetrics&) = delete;
  ScopedTapedMetrics& operator=(const ScopedTapedMetrics&) = delete;

  std::map<std::string, int64_t> collect(const std::string& metricName, const std::string& attribute) {
    std::map<std::string, int64_t> values;
    const bool collected = m_reader->Collect([&](opentelemetry::sdk::metrics::ResourceMetrics& resource) {
      for (const auto& scope : resource.scope_metric_data_) {
        for (const auto& metric : scope.metric_data_) {
          if (metric.instrument_descriptor.name_ != metricName) {
            continue;
          }
          for (const auto& point : metric.point_data_attr_) {
            const auto& sum = std::get<opentelemetry::sdk::metrics::SumPointData>(point.point_data);
            values.emplace(std::get<std::string>(point.attributes.at(attribute)), std::get<int64_t>(sum.value_));
          }
        }
      }
      return true;
    });
    if (!collected) {
      throw std::runtime_error("Metric collection failed");
    }
    return values;
  }

  int64_t driveStatus(common::dataStructures::DriveStatus status) {
    return collect(semconv::metrics::kMetricCtaTapedDriveStatus, semconv::attr::kCtaTapedDriveState)
      .at(common::dataStructures::toString(status));
  }

  int64_t mountType(common::dataStructures::MountType type) {
    return collect(semconv::metrics::kMetricCtaTapedMountType, semconv::attr::kCtaTapedMountType)
      .at(common::dataStructures::toCamelCaseString(type));
  }

private:
  std::shared_ptr<opentelemetry::metrics::MeterProvider> m_previous;
  std::shared_ptr<ManualMetricReader> m_reader = std::make_shared<ManualMetricReader>();
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> m_provider =
    std::make_shared<opentelemetry::sdk::metrics::MeterProvider>();
};

}  // namespace cta::telemetry::testing
