/******************************************************************************
 *                h/rmc_get_acs_drive_id.h
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

/**
 * Structure representing the identifier of a drive withn an ACS compatible
 * tape library.
 */
struct rmc_acs_drive_id {
        int acs;
        int lsm;
        int panel;
        int transport;
};

/**
 * Returns the drive id of the specified drive string of the form:
 *
 *    acs@rmc_host,acs,lsm,panel,transport
 *
 * There are three RMC loader types, namely acs, manual and smc.  The drive
 * string is expected to be in one of the following forms respectively:
 *
 *   acs@rmc_host[:rmc_port],ACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER
 *   manual
 *   smc@rmc_host[:rmc_port],drive_ordinal
 *
 * @return The loader type.
 */
int rmc_get_acs_drive_id(const char *const drive,
	struct rmc_acs_drive_id *const drive_id);

