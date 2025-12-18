/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "utils.hpp"

#include "common/exception/Exception.hpp"

#include <fstream>
#include <sstream>
#include <string>

namespace cta::frontend::grpc::utils {

/**
 * Load the content of the file into a string
 */
void read(const std::string& strPath, std::string& strValu) {
  if (strPath.empty()) {
    throw cta::exception::Exception("Path is an empty string");
  }

  std::ifstream ifs(strPath);

  if (!ifs.is_open()) {
    std::ostringstream osExMsg;
    osExMsg << "Could not open the file: " << strPath;
    throw cta::exception::Exception(osExMsg.str());
  }

  strValu.assign(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
}

}  // namespace cta::frontend::grpc::utils
