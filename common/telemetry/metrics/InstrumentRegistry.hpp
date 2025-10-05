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
