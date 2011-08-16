/******************************************************************************
 *                 tapebridge/tapebridge_tapeFlushModeToStr.c
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

#include "h/tapebridge_constants.h"
#include "h/tapebridge_tapeFlushModeToStr.h"


/******************************************************************************
 * tapebridge_tapeFlushModeToStr
 *****************************************************************************/
const char *tapebridge_tapeFlushModeToStr(const uint32_t tapeFlushMode) {
  switch(tapeFlushMode) {
  case TAPEBRIDGE_N_FLUSHES_PER_FILE:
    return "TAPEBRIDGE_N_FLUSHES_PER_FILE";
  case TAPEBRIDGE_ONE_FLUSH_PER_N_FILES:
    return "TAPEBRIDGE_ONE_FLUSH_PER_N_FILES";
  default:
    return "UNKNOWN";
  }
}
