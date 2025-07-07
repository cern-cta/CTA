#include "InstrumentRegistry.hpp"

namespace cta::telemetry::metrics {

std::vector<std::function<void()>>& getInstrumentRegistry() {
  static std::vector<std::function<void()>> registry;
  return registry;
}

void registerInstrumentInit(std::function<void()> initFunction) {
  getInstrumentRegistry().emplace_back(std::move(initFunction));
}

void initAllInstruments() {
  for (const auto& initFunction : getInstrumentRegistry()) {
    initFunction();
  }
}

}  // namespace cta::telemetry::metrics
