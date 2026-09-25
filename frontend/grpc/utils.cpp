/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "utils.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"

#include <fstream>
#include <regex>
#include <sstream>
#include <string>

namespace cta::frontend::grpc::utils {

void read(const std::string& strPath, std::string& strValue) {
  if (strPath.empty()) {
    throw cta::exception::Exception("Path is an empty string");
  }

  std::ifstream ifs(strPath);

  if (!ifs.is_open()) {
    std::ostringstream osExMsg;
    osExMsg << "Could not open the file: " << strPath;
    throw cta::exception::Exception(osExMsg.str());
  }

  strValue.assign(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
}

bool isJwtFormatValid(const std::string& token) {
  static const std::regex jwtPattern(R"(^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$)");
  return std::regex_match(token, jwtPattern);
}

}  // namespace cta::frontend::grpc::utils
