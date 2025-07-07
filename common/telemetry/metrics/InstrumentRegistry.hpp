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
 * The purpose of this struct is to be able to run and register an instrument init function at start time.
 * This solves two problems:
 * - It ensures all instruments are initialised to a NOOP when the program starts.
 *   This is important to ensure we are not accessing a nullptr when using an instrument
 *   For applications that don't require telemetry, it should not be mandatory to explicitly initialise this to a NOOP.
 *   Take e.g. the CLI tools: they require the catalogue library which has telemetry. It should work out of the box.
 *   Additionally, it also bypasses the need for thread-safety in the initialisation of the instruments
 *   because the init functions are only called once at any given time (at startup, after initialisation, at reset)
 * - It makes the instrument files self-contained and ensures there is a clear dependency hierarchy. This allows these
 *   files to be put into there respective library directories if so desired.
 *
 * Usage:
 *
 * namespace {
 *     const auto _ = cta::telemetry::metrics::InstrumentRegistrar(myInstrumentInitFunction);
 * }
 *
 * Note that the anonymous namespace is important here and this must be defined in the source file, not the header file
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
