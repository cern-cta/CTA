#pragma once

#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/metrics/meter.h>

#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <memory>
#include <functional>

#include "version.h"

namespace cta::telemetry::metrics {

namespace metrics_api = opentelemetry::metrics;

class InstrumentProvider {
public:
  static InstrumentProvider& instance();

  void reset();

  std::shared_ptr<metrics_api::Counter<uint64_t>> getUInt64Counter(const std::string& meterName,
                                                                    const std::string& componentName,
                                                                    const std::string& description = "",
                                                                    const std::string& unit = "");

  std::shared_ptr<metrics_api::Counter<double>> getDoubleCounter(const std::string& meterName,
                                                                  const std::string& componentName,
                                                                  const std::string& description = "",
                                                                  const std::string& unit = "");

  std::shared_ptr<metrics_api::Histogram<uint64_t>> getUInt64Histogram(const std::string& meterName,
                                                                        const std::string& componentName,
                                                                        const std::string& description = "",
                                                                        const std::string& unit = "");

  std::shared_ptr<metrics_api::Histogram<double>> getDoubleHistogram(const std::string& meterName,
                                                                      const std::string& componentName,
                                                                      const std::string& description = "",
                                                                      const std::string& unit = "");

  std::shared_ptr<metrics_api::UpDownCounter<int64_t>> getInt64UpDownCounter(const std::string& meterName,
                                                                             const std::string& componentName,
                                                                             const std::string& description = "",
                                                                             const std::string& unit = "");

  std::shared_ptr<metrics_api::UpDownCounter<double>> getDoubleUpDownCounter(const std::string& meterName,
                                                                             const std::string& componentName,
                                                                             const std::string& description = "",
                                                                             const std::string& unit = "");

private:
  InstrumentProvider();
  static std::string makeKey(const std::string& meterName, const std::string& componentName);
  static opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> getMeter(const std::string& componentName);

  template<typename T>
  T getOrCreateInstrument(const std::string& key,
                          std::unordered_map<std::string, T>& map,
                          std::shared_mutex& mutex,
                          const std::function<T()>& createInstrument);

  std::unordered_map<std::string, std::shared_ptr<metrics_api::Counter<uint64_t>>> m_uint64Counters;
  std::shared_mutex m_uint64CounterMutex;

  std::unordered_map<std::string, std::shared_ptr<metrics_api::Counter<double>>> m_doubleCounters;
  std::shared_mutex m_doubleCounterMutex;

  std::unordered_map<std::string, std::shared_ptr<metrics_api::Histogram<uint64_t>>> m_uint64Histograms;
  std::shared_mutex m_uint64HistogramMutex;

  std::unordered_map<std::string, std::shared_ptr<metrics_api::Histogram<double>>> m_doubleHistograms;
  std::shared_mutex m_doubleHistogramMutex;

  std::unordered_map<std::string, std::shared_ptr<metrics_api::UpDownCounter<int64_t>>> m_int64UpDownCounters;
  std::shared_mutex m_int64UpDownCounterMutex;

  std::unordered_map<std::string, std::shared_ptr<metrics_api::UpDownCounter<double>>> m_doubleUpDownCounters;
  std::shared_mutex m_doubleUpDownCounterMutex;
};

}  // namespace cta::telemetry::metrics
