/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/config/ListenerConfig.hpp"

namespace cta::grpc_server {

cta::runtime::ValidationResult ListenerConfig::validate() const {
  cta::runtime::ValidationResult result;
  if (port == 0) {
    result.addError("port", "must be greater than zero");
  }
  // check that our number of threads is OK
  if (number_of_threads.has_value() && *number_of_threads < 1) {
    result.addError("number_of_threads", "must be at least 1");
  }
  result.merge("tls", tls.validate());
  return result;
}

}  // namespace cta::grpc_server
