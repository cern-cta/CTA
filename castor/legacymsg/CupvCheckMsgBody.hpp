/******************************************************************************
 *         castor/legacymsg/CupvCheckMsgBody.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * 
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "h/Castor_limits.h"

#include <stdint.h>

namespace castor {
namespace legacymsg {

/**
 * The body of a CUPV_CHECK message.
 */
struct CupvCheckMsgBody {
  uint32_t uid; // User ID of the requester
  uint32_t gid; // Group ID of the requester
  uint32_t privUid; // User ID of the priviledge
  uint32_t privGid; // Group ID of the priviledge
  char srcHost[CA_MAXREGEXPLEN + 1];
  char tgtHost[CA_MAXREGEXPLEN + 1];
  uint32_t priv;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0 and all string member-variables to
   * the empty string.
   */
  CupvCheckMsgBody() throw();
}; // struct CupvCheckMsgBody

} // namespace legacymsg
} // namespace castor

