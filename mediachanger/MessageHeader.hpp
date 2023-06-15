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

#include <stdint.h>

namespace cta {
namespace mediachanger {

/**
 * A message header
 */
struct MessageHeader {
  /**
   * The magic number of the message.
   */
  uint32_t magic;

  /**
   * The request type of the message.
   */
  uint32_t reqType;

  /**
   * The length of the message body in bytes if this is the header of any
   * message other than an acknowledge message.  If this is the header of
   * an acknowledge message then there is no message body and this field is
   * used to pass the status of the acknowledge.
   */
  uint32_t lenOrStatus;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0.
   */
  MessageHeader();
};  // struct MessageHeader

}  // namespace mediachanger
}  // namespace cta
