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

#include <cryptopp/base64.h>
#include <sstream>
#include <string>

namespace cta::utils {

/**
 * https://cryptopp.com/wiki/Base64Decoder
 */
std::string base64decode(const std::string& input) {
  std::string output;
  CryptoPP::StringSource ss(input,
                            true,
                            new CryptoPP::Base64Decoder(new CryptoPP::StringSink(output))  // Base64Decoder
  );                                                                                       // StringSource
  return output;
}

/**
 * https://cryptopp.com/wiki/Base64Encoder
 */
std::string base64encode(const std::string& input) {
  std::string output;
  CryptoPP::StringSource ss(input,
                            true,
                            new CryptoPP::Base64Encoder(new CryptoPP::StringSink(output), false  // no new line
                                                        )                                        // Base64Encoder
  );                                                                                             // StringSource
  return output;
}

}  // namespace cta::utils
