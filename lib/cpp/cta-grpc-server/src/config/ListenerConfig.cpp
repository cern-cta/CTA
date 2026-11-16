/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/config/ListenerConfig.hpp"

#include <algorithm>
#include <limits>

namespace cta::grpc_server {

int ListenerConfig::resolveThreadCount(unsigned detectedCpuCount) const {
  const auto cpus = std::clamp(detectedCpuCount, 1u, static_cast<unsigned>(std::numeric_limits<int>::max() / 8));
  return number_of_threads == 0 ? static_cast<int>(8 * cpus) : number_of_threads;
}

cta::runtime::ValidationResult ListenerConfig::validate() const {
  cta::runtime::ValidationResult result;
  if (port == 0) {
    result.addError("port", "must be greater than zero");
  }
  // check that our number of threads is OK
  if (number_of_threads < 0) {
    result.addError("number_of_threads", "must be zero (automatic) or positive");
  }
  result.merge("tls", tls.validate());
  return result;
}

}  // namespace cta::grpc_server
