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
