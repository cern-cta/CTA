/******************************************************************************
 *                      castor/tape/legacymsg/MessageHeader.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_LEGACYMSG_MESSAGEHEADER
#define CASTOR_TAPE_LEGACYMSG_MESSAGEHEADER

#include "h/Castor_limits.h"

#include <stdint.h>
#include <string>


namespace castor    {
namespace tape      {
namespace legacymsg {

  /**
   * A message header
   */
  struct MessageHeader {
    /**
     * The magic number of the message.
     */
    uint32_t magic;

    /**
     * The requets type of the message.
     */
    uint32_t reqType;

    /**
     * The length of the message body in bytes if this is the header of any
     * message other than an acknowledge message.  If this is the header of
     * an acknowledge message then there is no message body and this field is
     * used to pass the status of the acknowledge.
     */
    uint32_t lenOrStatus;
  };

} // namespace legacymsg
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_LEGACYMSG_MESSAGEHEADER
