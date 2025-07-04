#include "InstrumentProvider.hpp"

namespace cta::telemetry::metrics {

InstrumentProvider& InstrumentProvider::instance() {
  static InstrumentProvider instance;
  return instance;
}

// TODO: add #HELP with description and #TYPE with the instrument type

InstrumentProvider::InstrumentProvider() = default;

void InstrumentProvider::reset() {
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

std::shared_ptr<metrics_api::Counter<uint64_t>> InstrumentProvider::getUInt64Counter(
    const std::string& meterName, const std::string& componentName,
    const std::string& description, const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::Counter<uint64_t>>>(
    key, m_uint64Counters, m_uint64CounterMutex, [=]() {
      auto meter = getMeter(meterName);
      return std::shared_ptr<metrics_api::Counter<uint64_t>>(
        meter->CreateUInt64Counter(componentName, description, unit).release());
    });
}

std::shared_ptr<metrics_api::Counter<double>> InstrumentProvider::getDoubleCounter(
    const std::string& meterName, const std::string& componentName,
    const std::string& description, const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::Counter<double>>>(
    key, m_doubleCounters, m_doubleCounterMutex, [=]() {
      auto meter = getMeter(meterName);
      return std::shared_ptr<metrics_api::Counter<double>>(
        meter->CreateDoubleCounter(componentName, description, unit).release());
    });
}

std::shared_ptr<metrics_api::Histogram<uint64_t>> InstrumentProvider::getUInt64Histogram(
    const std::string& meterName, const std::string& componentName,
    const std::string& description, const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::Histogram<uint64_t>>>(
    key, m_uint64Histograms, m_uint64HistogramMutex, [=]() {
      auto meter = getMeter(meterName);
      return std::shared_ptr<metrics_api::Histogram<uint64_t>>(
        meter->CreateUInt64Histogram(componentName, description, unit).release());
    });
}

std::shared_ptr<metrics_api::Histogram<double>> InstrumentProvider::getDoubleHistogram(
    const std::string& meterName, const std::string& componentName,
    const std::string& description, const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::Histogram<double>>>(
    key, m_doubleHistograms, m_doubleHistogramMutex, [=]() {
      auto meter = getMeter(meterName);
      return std::shared_ptr<metrics_api::Histogram<double>>(
        meter->CreateDoubleHistogram(componentName, description, unit).release());
    });
}

std::shared_ptr<metrics_api::UpDownCounter<int64_t>> InstrumentProvider::getInt64UpDownCounter(
    const std::string& meterName, const std::string& componentName,
    const std::string& description, const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::UpDownCounter<int64_t>>>(
    key, m_int64UpDownCounters, m_int64UpDownCounterMutex, [=]() {
      auto meter = getMeter(meterName);
      return std::shared_ptr<metrics_api::UpDownCounter<int64_t>>(
        meter->CreateInt64UpDownCounter(componentName, description, unit).release());
    });
}

std::shared_ptr<metrics_api::UpDownCounter<double>> InstrumentProvider::getDoubleUpDownCounter(
    const std::string& meterName, const std::string& componentName,
    const std::string& description, const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::UpDownCounter<double>>>(
    key, m_doubleUpDownCounters, m_doubleUpDownCounterMutex, [=]() {
      auto meter = getMeter(meterName);
      return std::shared_ptr<metrics_api::UpDownCounter<double>>(
        meter->CreateDoubleUpDownCounter(componentName, description, unit).release());
    });
}

std::shared_ptr<metrics_api::ObservableInstrument> InstrumentProvider::getInt64ObservableGauge(const std::string& meterName,
                                                                             const std::string& componentName,
                                                                             const std::string& description,
                                                                             const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::ObservableInstrument>>(
    key, m_observableInstruments, m_observableInstrumentMutex, [=]() {
      auto meter = getMeter(meterName);
      return meter->CreateInt64ObservableGauge(componentName, description, unit);
    });
}

std::shared_ptr<metrics_api::ObservableInstrument> InstrumentProvider::getDoubleObservableGauge(const std::string& meterName,
                                                                             const std::string& componentName,
                                                                             const std::string& description,
                                                                             const std::string& unit) {
  auto key = makeKey(meterName, componentName);
  return getOrCreateInstrument<std::shared_ptr<metrics_api::ObservableInstrument>>(
    key, m_observableInstruments, m_observableInstrumentMutex, [=]() {
      auto meter = getMeter(meterName);
      return meter->CreateDoubleObservableGauge(componentName, description, unit);
    });
}


std::string InstrumentProvider::makeKey(const std::string& meterName, const std::string& componentName) {
  return meterName + "." + componentName;
}

std::shared_ptr<opentelemetry::metrics::Meter> InstrumentProvider::getMeter(const std::string& componentName) {
  auto provider = opentelemetry::metrics::Provider::GetMeterProvider();
  return provider->GetMeter(componentName, CTA_VERSION);
}

template<typename T>
T InstrumentProvider::getOrCreateInstrument(const std::string& key,
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

// Explicit template instantiations
template std::shared_ptr<metrics_api::Counter<uint64_t>> InstrumentProvider::getOrCreateInstrument(
  const std::string&, std::unordered_map<std::string, std::shared_ptr<metrics_api::Counter<uint64_t>>>&,
  std::shared_mutex&, const std::function<std::shared_ptr<metrics_api::Counter<uint64_t>>()>&);

template std::shared_ptr<metrics_api::Counter<double>> InstrumentProvider::getOrCreateInstrument(
  const std::string&, std::unordered_map<std::string, std::shared_ptr<metrics_api::Counter<double>>>&,
  std::shared_mutex&, const std::function<std::shared_ptr<metrics_api::Counter<double>>()>&);

template std::shared_ptr<metrics_api::Histogram<uint64_t>> InstrumentProvider::getOrCreateInstrument(
  const std::string&, std::unordered_map<std::string, std::shared_ptr<metrics_api::Histogram<uint64_t>>>&,
  std::shared_mutex&, const std::function<std::shared_ptr<metrics_api::Histogram<uint64_t>>()>&);

template std::shared_ptr<metrics_api::Histogram<double>> InstrumentProvider::getOrCreateInstrument(
  const std::string&, std::unordered_map<std::string, std::shared_ptr<metrics_api::Histogram<double>>>&,
  std::shared_mutex&, const std::function<std::shared_ptr<metrics_api::Histogram<double>>()>&);

template std::shared_ptr<metrics_api::UpDownCounter<int64_t>> InstrumentProvider::getOrCreateInstrument(
  const std::string&, std::unordered_map<std::string, std::shared_ptr<metrics_api::UpDownCounter<int64_t>>>&,
  std::shared_mutex&, const std::function<std::shared_ptr<metrics_api::UpDownCounter<int64_t>>()>&);

template std::shared_ptr<metrics_api::UpDownCounter<double>> InstrumentProvider::getOrCreateInstrument(
  const std::string&, std::unordered_map<std::string, std::shared_ptr<metrics_api::UpDownCounter<double>>>&,
  std::shared_mutex&, const std::function<std::shared_ptr<metrics_api::UpDownCounter<double>>()>&);

template std::shared_ptr<metrics_api::ObservableInstrument> InstrumentProvider::getOrCreateInstrument(
  const std::string&, std::unordered_map<std::string, std::shared_ptr<metrics_api::ObservableInstrument>>&,
  std::shared_mutex&, const std::function<std::shared_ptr<metrics_api::ObservableInstrument>()>&);

} // namespace cta::telemetry::metrics
