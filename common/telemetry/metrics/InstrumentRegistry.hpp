/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <functional>
#include <vector>

namespace cta::telemetry::metrics {

void registerInstrumentInit(std::function<void()> initFunction);
void initAllInstruments();

/**
 *
 * namespace {
 *     const auto _ = cta::telemetry::metrics::InstrumentRegistrar(myInstrumentInitFunction);
 * }
 *
 * Note that the anonymous namespace is important here and this must be defined in the source file, not the header file.
 */
struct InstrumentRegistrar {
  /**
   * Runs the init function and registers it for later use to ensure that initAllInstruments()
   * calls the correct set of init functions.
   */
  explicit InstrumentRegistrar(std::function<void()> initFunction) {
    // Call the init function once to ensure the instruments are instantiated as NOOP instruments
    initFunction();
    registerInstrumentInit(std::move(initFunction));
  }
};

}  // namespace cta::telemetry::metrics
