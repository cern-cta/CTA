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

#pragma once

#include <string>

namespace cta {
namespace frontend {
namespace grpc {
namespace utils {
/**
 * Load the content of the file into a string
 */
void read(const std::string& strPath, std::string& strValu);
/**
 * https://cryptopp.com/wiki/Base64Decoder
 */
void decode(const std::string& strEncoded, std::string& strDecoded);
/**
 * https://cryptopp.com/wiki/Base64Encoder
 */
void encode(const std::string& strDecoded, std::string& strEncoded);

}  // namespace utils
}  // namespace grpc
}  // namespace frontend
}  // namespace cta