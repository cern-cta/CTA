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

#include "mediachanger/MessageHeader.hpp"
#include "common/exception/Exception.hpp"

#include <errno.h>
#include <stdint.h>
#include <string>
#include <time.h>


namespace cta    {
namespace mediachanger {

/**
 * Marshals the specified source message header structure into the
 * specified destination buffer.
 *
 * @param dst    The destination buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the header.
 */
size_t marshal(char *const dst, const size_t dstLen,
  const MessageHeader &src);

/**
 * Marshals the specified source message header structure into the
 * specified destination buffer.
 *
 * @param dst The destination buffer.
 * @param src The source structure.
 * @return    The total length of the header.
 */
template<int n> size_t marshal(char (&dst)[n],
  const MessageHeader &src)  {
  return marshal(dst, n, src);
}

/**
 * Unmarshals a message header from the specified source buffer into the
 * specified destination message header structure.
 *
 * @param src    In/out parameter, before invocation points to the source
 *               buffer where the message header should be unmarshalled from
 *               and on return points to the byte in the source buffer
 *               immediately after the unmarshalled message header.
 * @param srcLen In/out parameter, before invocation is the length of the
 *               source buffer from where the message header should be
 *               unmarshalled and on return is the number of bytes remaining in
 *               the source buffer.
 * @param dst    The destination structure.
 */
void unmarshal(const char * &src, size_t &srcLen, MessageHeader &dst);

} // namespace mediachanger
} // namespace cta


