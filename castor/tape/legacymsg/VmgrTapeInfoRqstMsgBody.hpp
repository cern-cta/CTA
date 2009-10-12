/******************************************************************************
 *                      castor/tape/legacymsg/VmgrTapeInfoRqstMsgBody.hpp
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

#ifndef CASTOR_TAPE_LEGACYMSG_VMGRTAPEINFORQSTMSGBODY
#define CASTOR_TAPE_LEGACYMSG_VMGRTAPEINFORQSTMSGBODY

#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <stdint.h>


namespace castor    {
namespace tape      {
namespace legacymsg {

  /**
   * A request for tape information from the VMGR
   */
  struct VmgrTapeInfoRqstMsgBody {
    uint32_t uid;
    uint32_t gid;
    char     vid[CA_MAXVIDLEN+1];
    uint16_t side;
  }; // struct VmgrTapeInfoRqstMsgBody

} // namespace legacymsg
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_LEGACYMSG_VMGRTAPEINFORQSTMSGBODY
