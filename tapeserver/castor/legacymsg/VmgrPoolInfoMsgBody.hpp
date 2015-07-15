/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "Castor_limits.h"

#include <stdint.h>

namespace castor {
namespace legacymsg {

/**
 * Information about a tape pool from the VMGR.
 */
struct VmgrPoolInfoMsgBody {
  uint32_t poolUid;
  uint32_t poolGid;
  uint64_t capacityBytes;
  uint64_t freeSpaceBytes;

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0 and all string member-variables to
   * the empty string.
   */
  VmgrPoolInfoMsgBody() throw();
}; // struct VmgrPoolInfoMsgBody

} // namespace legacymsg
} // namespace castor

