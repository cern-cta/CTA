/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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
