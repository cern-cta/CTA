/******************************************************************************
 *                 rmc/rmc_get_acs_drive_id.c
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

#include "h/rmc_get_acs_drive_id.h"

#include <string.h>

/**
 * A simple function that converts the string representation of the unsigned
 * integer in the specified character buffer to an integer.
 *
 * @return A value of 0 or greater than 0 if the conversion was a success or -1
 * if the conversion was not a success.
 */
static int rmc_buf2uint(const char *const buf, int buflen) {
	int i = 0;
	int value = 0;
	int multipler = 1;

	/* Fail conversion if there is nothing to convert */
	if(NULL == buf || 0 == buflen) {
		return -1;
	}

	for(i = 0; i < buflen; i++) {
		const char c = buf[i];

		/* Fail conversion if the character is not a decimal digit */
		if(c < '0' || c > '9') {
			return -1;
		}
		value *= multipler;
		value += (c - '0');

		multipler *= 10;
	}
	return value;
}

int rmc_get_acs_drive_id(const char *const drive,
	struct rmc_acs_drive_id *const drive_id) {
	int str_len = 0;
	int i = 0;
	int nbCommas = 0;
	int commaIndex[4];

	/* Fail if there is nothing to work on */
	if(NULL == drive) {
		return -1;
	}

	str_len = strlen(drive);

	/* An acs drive string starts with "acs@"                             */
	/*                                                                    */
	/* Fail if the first 4 characters do not exist or are not as expected */
	if(4 > str_len || strncmp("acs@", drive, 4)) {
		return -1;
	}

	/* The drive string should be of the form   */
	/* "acs@rmc_host,acs,lsm,panel,transport"   */
	/* therefore there should be 4 commas (',') */

	/* Find the expected 4 commas */
	for(i=0; i < str_len; i++) {
		if(',' == drive[i]) {
			/* Fail if there are too many commas */
			if(4 < nbCommas) {
				return -1;
			}
			commaIndex[nbCommas] = i;
			nbCommas++;
		}
	}

	/* Fail if there are too few commas */
	if(4 != nbCommas) {
		return -1;
	}

	/* Fail if the rmc_host has zero length */
	if(0 == commaIndex[0] - 3 /* index of at sign ('@') */) {
		return -1;
	}

	/* Try to convert the 4 drive id numbers to integers */
	drive_id->acs = rmc_buf2uint(drive + commaIndex[0] + 1,
		commaIndex[1] - commaIndex[0]);
	drive_id->lsm = rmc_buf2uint(drive + commaIndex[1] + 1,
		commaIndex[2] - commaIndex[1]);
	drive_id->panel = rmc_buf2uint(drive + commaIndex[2] + 1,
		commaIndex[3] - commaIndex[2]);
	drive_id->transport = rmc_buf2uint(drive + commaIndex[3] + 1,
		str_len - 1 - commaIndex[3]);

	/* Fail if one of the drive id numbers could not be converted */
	if(	-1 == drive_id->acs   ||
		-1 == drive_id->lsm   ||
		-1 == drive_id->panel ||
		-1 == drive_id->transport) {
		return -1;
	}

	return 0;
}
