/******************************************************************************
 *         castor/legacymsg/SetVidRequestMsgBody.hpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "h/Castor_limits.h"

#include <stdint.h>

namespace castor {
namespace legacymsg {

/**
 * An update-VID message, used to update the drive catalogue with the contents of a drive.
 */
struct SetVidRequestMsgBody {
  
  /**
   * The VID of the tape inside the drive ("" if empty)
   */
  char vid[CA_MAXVIDLEN+1];
  
  /**
   * The drive name (a.k.a. unit name)
   */
  char drive[CA_MAXUNMLEN+1];

  /**
   * Constructor: zeroes the two strings.
   */
  SetVidRequestMsgBody() throw();

}; // struct SetVidRequestMsgBody

} // namespace legacymsg
} // namespace castor

