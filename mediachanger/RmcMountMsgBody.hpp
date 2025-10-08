/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/Constants.hpp"
#include "Constants.hpp"

namespace cta::mediachanger {

/**
 * The body of an RMC_SCSI_MOUNT message
 */
struct RmcMountMsgBody {
  uint32_t uid;
  uint32_t gid;
  char unusedLoader[1]; // Should always be set to the empty string
  char vid[CA_MAXVIDLEN+1];
  uint16_t side;
  uint16_t drvOrd;

  /**
   * Constructor
   *
   * Sets all integer member-variables to 0 and all string member-variables to the empty string
   */
  RmcMountMsgBody();

  /**
   * Returns message body length
   */
  uint32_t bodyLen() const;

  static const RequestType requestType = RMC_MOUNT;
}; // struct RmcMountMsgBody

} // namespace cta::mediachanger

