/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
