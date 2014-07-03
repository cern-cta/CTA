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

/******************************************************************************
 * Please note that this file is part of the internal API of the rmc daemon
 * and its client software and should therefore not be distributed to end users
 *****************************************************************************/

#pragma once

typedef enum {
  RMC_LOADER_TYPE_ACS,
  RMC_LOADER_TYPE_MANUAL,
  RMC_LOADER_TYPE_SMC,
  RMC_LOADER_TYPE_UNKNOWN} rmc_loader_type;

/**
 * Returns the loader type of the specified drive string.
 *
 * There are three RMC loader types, namely acs, manual and smc.  The drive
 * string is expected to be in one of the following forms respectively:
 *
 *   acs@rmc_host[:rmc_port],ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER
 *   manual
 *   smc@rmc_host[:rmc_port],drive_ordinal
 *
 * @param drive The drive string.
 * @return The loader type.
 */
rmc_loader_type rmc_get_loader_type(const char *const drive);

