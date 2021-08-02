/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "h/Castor_limits.h"
#include "common/Constants.hpp"
#include <stdint.h>

namespace castor {
namespace legacymsg {

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
  TapeLabelRqstMsgBody() throw();
}; // struct TapeLabelRqstMsgBody

} // namespace legacymsg
} // namespace castor

