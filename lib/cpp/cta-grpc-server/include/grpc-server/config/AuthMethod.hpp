/**
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/config/Config.hpp"

#include <optional>
#include <string>

namespace cta::grpc_server {

using cta::common::FromString;

/**
 * @brief Authentication methods supported by the CTA frontend
 */
enum class AuthMethod { JWT, KERBEROS, MTLS };

}  // namespace cta::grpc_server

template<>
struct cta::common::FromString<cta::grpc_server::AuthMethod> {
  static std::optional<cta::grpc_server::AuthMethod> tryFrom(std::string_view text) {
    using enum cta::grpc_server::AuthMethod;
    if (text == "jwt") {
      return JWT;
    } else if (text == "kerberos") {
      return KERBEROS;
    } else if (text == "mtls") {
      return MTLS;
    } else {
      return std::nullopt;
    }
  }
};
