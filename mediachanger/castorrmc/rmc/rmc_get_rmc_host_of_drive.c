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

#include "h/rmc_find_char.h"
#include "h/rmc_get_rmc_host_of_drive.h"

#include <string.h>

int rmc_get_rmc_host_of_drive(const char *const drive,
	char *const rmc_host_buf, const int rmc_host_buflen) {
	int indexOfComma = 0;
	int rmc_host_len = 0;

	/* Fail if there is nothing to work on */
	if(NULL == drive || NULL == rmc_host_buf || 0 == rmc_host_buflen) {
		return -1;
	}

	/* Fail if the drive string does not start with either "acs@" or */
	/* "smc@"                                                        */
	if(strncmp("acs@", drive, 4) && strncmp("smc@", drive, 4)) {
		return -1;
	}

	/* The rmc_host should follow the at sign ('@') and be terminated by */
	/* a comma (',') for example "acs@rmc_host,"                         */

	indexOfComma = rmc_find_char(drive, ',');
	rmc_host_len = indexOfComma - 3 /* index of the at sign */ - 1;
	if(	0 >= rmc_host_len || /* rmc_host contains no characters */
		rmc_host_buflen <= rmc_host_len) { /* rmc_host is too long */
		return -1;
	}

	memcpy(rmc_host_buf, drive + 4, rmc_host_len);
	rmc_host_buf[rmc_host_len] = '\0';
	return 0;
}
