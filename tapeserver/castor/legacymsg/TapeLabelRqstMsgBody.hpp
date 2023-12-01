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

#include "h/Castor_limits.h"
#include "common/Constants.hpp"
#include <stdint.h>

namespace castor::legacymsg {

/**
 * The body of a TPLABEL message.
 */
struct TapeLabelRqstMsgBody {
  uint16_t lbp;   // set to 1 if lbp==true, 0 otherwise
  uint16_t force; // set to 1 if force==true, 0 otherwise
  uint32_t uid;
  uint32_t gid;
  char vid[cta::CA_MAXVIDLEN + 1];
  char drive[CA_MAXUNMLEN + 1];
  char logicalLibrary[CA_MAXDGNLEN + 1];

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0 and all string member-variables to
   * the empty string.
   */
  TapeLabelRqstMsgBody() noexcept;
}; // struct TapeLabelRqstMsgBody

} // namespace castor::legacymsg

