/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/Constants.hpp"

#include <stdint.h>

namespace cta {
namespace mediachanger {

/**
 * The body of an RMC_SCSI_MOUNT message.
 */
struct RmcMountMsgBody {
  uint32_t uid;
  uint32_t gid;
  char unusedLoader[1]; // Should always be set to the emtpy string
  char vid[CA_MAXVIDLEN + 1];
  uint16_t side;
  uint16_t drvOrd;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0 and all string member-variables to
   * the empty string.
   */
  RmcMountMsgBody();
}; // struct RmcMountMsgBody

} // namespace mediachanger
} // namespace cta

