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

#include "common/Constants.hpp"

#include <stdint.h>

namespace cta {
namespace mediachanger {

/**
 * The body of an RMC_SCSI_UNMOUNT message.
 */
struct RmcUnmountMsgBody {
  uint32_t uid;
  uint32_t gid;
  char unusedLoader[1];  // Should always be set to the emtpy string
  char vid[CA_MAXVIDLEN + 1];
  uint16_t drvOrd;
  uint16_t force;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0 and all string member-variables to
   * the empty string.
   */
  RmcUnmountMsgBody();
};  // struct RmcUnmountMsgBody

}  // namespace mediachanger
}  // namespace cta
