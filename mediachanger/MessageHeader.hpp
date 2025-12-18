/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>

namespace cta::mediachanger {

/**
 * A message header
 */
struct MessageHeader {
  /**
   * The magic number of the message.
   */
  uint32_t magic = 0;

  /**
   * The request type of the message.
   */
  uint32_t reqType = 0;

  /**
   * The length of the message body in bytes if this is the header of any
   * message other than an acknowledge message.  If this is the header of
   * an acknowledge message then there is no message body and this field is
   * used to pass the status of the acknowledge.
   */
  uint32_t lenOrStatus = 0;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0.
   */
  MessageHeader();
};  // struct MessageHeader

}  // namespace cta::mediachanger
