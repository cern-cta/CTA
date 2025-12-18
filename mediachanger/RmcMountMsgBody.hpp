/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Constants.hpp"
#include "common/Constants.hpp"
#include "common/exception/Exception.hpp"

namespace cta::mediachanger {

/**
 * The body of an RMC_SCSI_MOUNT message
 */
struct RmcMountMsgBody {
  uint32_t uid = 0;
  uint32_t gid = 0;
  char unusedLoader[1];  // Should always be set to the empty string
  char vid[CA_MAXVIDLEN + 1];
  uint16_t side = 0;
  uint16_t drvOrd = 0;

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
};  // struct RmcMountMsgBody

}  // namespace cta::mediachanger
