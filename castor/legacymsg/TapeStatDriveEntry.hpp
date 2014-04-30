/******************************************************************************
 *         castor/legacymsg/TapeStatDriveEntry.hpp
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

namespace castor    {
namespace legacymsg {

/**
 * Structure defining a drive entry within the body of a TPSTAT message.
 */
struct TapeStatDriveEntry {
  uint32_t uid;
  uint32_t jid;                        // process group id or session id
  char     dgn[CA_MAXDGNLEN+1];        // device group name
  uint16_t up;                         // drive status: down = 0, up = 1
  uint16_t asn;                        // assign flag: assigned = 1
  uint32_t asn_time;                   // timestamp of drive assignment
  char     drive[CA_MAXUNMLEN+1];      // drive name
  uint16_t mode;                       // WRITE_DISABLE or WRITE_ENABLE
  char     lblcode[CA_MAXLBLTYPLEN+1]; // label code: AUL or DMP
  uint16_t tobemounted;                // 1 means tape to be mounted
  char     vid[CA_MAXVIDLEN+1];
  char     vsn[CA_MAXVSNLEN+1];
  uint32_t cfseq;                      // current file sequence number

  /**
   * Constructor.
   *
   * Sets all integer member-variables to 0 and all string member-variables to
   * the empty string.
   */
  TapeStatDriveEntry() throw();
};  

} // namespace legacymsg
} // namespace castor

