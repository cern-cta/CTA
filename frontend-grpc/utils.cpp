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

#include <cryptopp/base64.h>
#include <string>
#include <sstream>
#include <fstream>

namespace cta {
namespace frontend {
namespace grpc {
namespace utils {
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

/**
 * https://cryptopp.com/wiki/Base64Decoder
 */
void decode(const std::string& strEncoded, std::string& strDecoded) {
  CryptoPP::StringSource ss(strEncoded, true,
                            new CryptoPP::Base64Decoder(new CryptoPP::StringSink(strDecoded))  // Base64Decoder
  );                                                                                           // StringSourc
}

/**
 * https://cryptopp.com/wiki/Base64Encoder
 */
void encode(const std::string& strDecoded, std::string& strEncoded) {
  CryptoPP::StringSource ss(strDecoded, true,
                            new CryptoPP::Base64Encoder(new CryptoPP::StringSink(strEncoded), false  // no new line
                                                        )                                            // Base64Encoder
  );                                                                                                 // StringSource
}

}  // namespace utils
}  // namespace grpc
}  // namespace frontend
}  // namespace cta