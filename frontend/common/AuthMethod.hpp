/**
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/config/Config.hpp"

#include <optional>
#include <string>

namespace cta::frontend {

using cta::common::FromString;

/**
 * @brief Authentication methods supported by the CTA frontend
 */
enum class AuthMethod { JWT, KERBEROS, MTLS };

}  // namespace cta::frontend

template<>
struct cta::common::FromString<cta::frontend::AuthMethod> {
  static std::optional<cta::frontend::AuthMethod> tryFrom(std::string_view text) {
    using enum cta::frontend::AuthMethod;
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
