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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "h/rmc_get_loader_type.h"

#include <string.h>

rmc_loader_type rmc_get_loader_type(const char *const drive) {
  if(strncmp("acs@", drive, 3) == 0) {
    return RMC_LOADER_TYPE_ACS;
  } else if(strcmp("manual", drive) == 0) {
    return RMC_LOADER_TYPE_MANUAL;
  } else if(strncmp("smc@", drive, 3) == 0) {
    return RMC_LOADER_TYPE_SMC;
  } else {
    return RMC_LOADER_TYPE_UNKNOWN;
  }
}
