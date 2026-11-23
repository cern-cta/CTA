/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "utils.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/StdRegex.hpp"

#include <fstream>
#include <sstream>
#include <string>

namespace cta::frontend::grpc::utils {

bool isJwtFormatValid(const std::string& token) {
  static const std::regex jwtPattern(R"(^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$)");
  return std::regex_match(token, jwtPattern);
}

}  // namespace cta::frontend::grpc::utils
