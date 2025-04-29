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
  static InstrumentProvider& instance() {
    static InstrumentProvider instance;
    return instance;
  }

  void reset() {
    {
      std::unique_lock lock(m_uint64CounterMutex);
      m_uint64Counters.clear();
    }
    {
      std::unique_lock lock(m_doubleCounterMutex);
      m_doubleCounters.clear();
    }
    {
      std::unique_lock lock(m_uint64HistogramMutex);
      m_uint64Histograms.clear();
    }
    {
      std::unique_lock lock(m_doubleHistogramMutex);
      m_doubleHistograms.clear();
    }
    {
      std::unique_lock lock(m_int64UpDownCounterMutex);
      m_int64UpDownCounters.clear();
    }
    {
      std::unique_lock lock(m_doubleUpDownCounterMutex);
      m_doubleUpDownCounters.clear();
    }
  }

  std::shared_ptr<metrics_api::Counter<uint64_t>> getUInt64Counter(const std::string& meterName,
                                                                   const std::string& componentName,
                                                                   const std::string& description = "",
                                                                   const std::string& unit = "") {
    auto key = makeKey(meterName, componentName);
    return getOrCreateInstrument<std::shared_ptr<metrics_api::Counter<uint64_t>>>(
      key,
      m_uint64Counters,
      m_uint64CounterMutex,
      [=]() {
        auto meter = getMeter(meterName);
        return std::shared_ptr<metrics_api::Counter<uint64_t>>(
          meter->CreateUInt64Counter(componentName, description, unit).release());
      });
  }

  std::shared_ptr<metrics_api::Counter<double>> getDoubleCounter(const std::string& meterName,
                                                                 const std::string& componentName,
                                                                 const std::string& description = "",
                                                                 const std::string& unit = "") {
    auto key = makeKey(meterName, componentName);
    return getOrCreateInstrument<std::shared_ptr<metrics_api::Counter<double>>>(
      key,
      m_doubleCounters,
      m_doubleCounterMutex,
      [=]() {
        auto meter = getMeter(meterName);
        return std::shared_ptr<metrics_api::Counter<double>>(
          meter->CreateDoubleCounter(componentName, description, unit).release());
      });
  }

  std::shared_ptr<metrics_api::Histogram<uint64_t>> getUInt64Histogram(const std::string& meterName,
                                                                       const std::string& componentName,
                                                                       const std::string& description = "",
                                                                       const std::string& unit = "") {
    auto key = makeKey(meterName, componentName);
    return getOrCreateInstrument<std::shared_ptr<metrics_api::Histogram<uint64_t>>>(
      key,
      m_uint64Histograms,
      m_uint64HistogramMutex,
      [=]() {
        auto meter = getMeter(meterName);
        return std::shared_ptr<metrics_api::Histogram<uint64_t>>(
          meter->CreateUInt64Histogram(componentName, description, unit).release());
      });
  }

  std::shared_ptr<metrics_api::Histogram<double>> getDoubleHistogram(const std::string& meterName,
                                                                     const std::string& componentName,
                                                                     const std::string& description = "",
                                                                     const std::string& unit = "") {
    auto key = makeKey(meterName, componentName);
    return getOrCreateInstrument<std::shared_ptr<metrics_api::Histogram<double>>>(
      key,
      m_doubleHistograms,
      m_doubleHistogramMutex,
      [=]() {
        auto meter = getMeter(meterName);
        return std::shared_ptr<metrics_api::Histogram<double>>(
          meter->CreateDoubleHistogram(componentName, description, unit).release());
      });
  }

  std::shared_ptr<metrics_api::UpDownCounter<int64_t>> getInt64UpDownCounter(const std::string& meterName,
                                                                             const std::string& componentName,
                                                                             const std::string& description = "",
                                                                             const std::string& unit = "") {
    auto key = makeKey(meterName, componentName);
    return getOrCreateInstrument<std::shared_ptr<metrics_api::UpDownCounter<int64_t>>>(
      key,
      m_int64UpDownCounters,
      m_int64UpDownCounterMutex,
      [=]() {
        auto meter = getMeter(meterName);
        return std::shared_ptr<metrics_api::UpDownCounter<int64_t>>(
          meter->CreateInt64UpDownCounter(componentName, description, unit).release());
      });
  }

  std::shared_ptr<metrics_api::UpDownCounter<double>> getDoubleUpDownCounter(const std::string& meterName,
                                                                             const std::string& componentName,
                                                                             const std::string& description = "",
                                                                             const std::string& unit = "") {
    auto key = makeKey(meterName, componentName);
    return getOrCreateInstrument<std::shared_ptr<metrics_api::UpDownCounter<double>>>(
      key,
      m_doubleUpDownCounters,
      m_doubleUpDownCounterMutex,
      [=]() {
        auto meter = getMeter(meterName);
        return std::shared_ptr<metrics_api::UpDownCounter<double>>(
          meter->CreateDoubleUpDownCounter(componentName, description, unit).release());
      });
  }

private:
  InstrumentProvider() = default;

  static std::string makeKey(const std::string& meterName, const std::string& componentName) {
    return meterName + "." + componentName;
  }

  static opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> getMeter(const std::string& componentName) {
    auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
    return provider->GetMeter(componentName, CTA_VERSION);
  }

  template<typename T>
  T getOrCreateInstrument(const std::string& key,
                          std::unordered_map<std::string, T>& map,
                          std::shared_mutex& mutex,
                          const std::function<T()>& createInstrument) {
    {
      std::shared_lock lock(mutex);
      auto it = map.find(key);
      if (it != map.end()) {
        return it->second;
      }
    }

    std::unique_lock lock(mutex);
    auto it = map.find(key);
    if (it != map.end()) {
      return it->second;
    }

    auto instrument = createInstrument();
    map[key] = instrument;
    return instrument;
  }

  // Storage + mutex per type
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
