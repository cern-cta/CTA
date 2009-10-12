/******************************************************************************
 *                      castor/tape/legacymsg/VmgrTapeInfoMsgBody.hpp
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

#ifndef CASTOR_TAPE_LEGACYMSG_VMGRTAPEINFOMSGBODY
#define CASTOR_TAPE_LEGACYMSG_VMGRTAPEINFOMSGBODY

#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <stdint.h>


namespace castor    {
namespace tape      {
namespace legacymsg {

  /**
   * Information about a tape from the VMGR.
   */
  struct VmgrTapeInfoMsgBody {
    char     vsn[CA_MAXVSNLEN+1];
    char     library[CA_MAXTAPELIBLEN+1];
    char     dgn[CA_MAXDGNLEN+1];
    char     density[CA_MAXDENLEN+1];
    char     labelType[CA_MAXLBLTYPLEN+1];
    char     model[CA_MAXMODELLEN+1];
    char     mediaLetter[CA_MAXMLLEN+1];
    char     manufacturer[CA_MAXMANUFLEN+1];
    char     serialNumber[CA_MAXSNLEN+1];
    uint16_t nbSides;
    time_t   eTime;
    uint16_t side;
    char     poolName[CA_MAXPOOLNAMELEN+1];
    uint32_t estimatedFreeSpace; // in kbytes
    uint32_t nbFiles;
    uint32_t rCount;
    uint32_t wCount;
    char     rHost[CA_MAXSHORTHOSTLEN+1];
    char     wHost[CA_MAXSHORTHOSTLEN+1];
    uint32_t rJid;
    uint32_t wJid;
    time_t   rTime; // Last access to tape in read mode
    time_t   wTime; // Last access to tape in write mode
    uint32_t status; // TAPE_FULL, DISABLED, EXPORTED
  }; // struct VmgrTapeInfoMsgBody

} // namespace legacymsg
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_LEGACYMSG_VMGRTAPEINFOMSGBODY
