/******************************************************************************
 *                 rmc/rmc_mnt.c
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

/*      rmc_mnt - mount a cartridge into a drive that maybe in either */
/*                a SCSI compatible or an ACS compatible tape library */

#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "h/marshall.h"
#include "h/rmc_api.h"
#include "h/rmc_constants.h"
#include "h/rmc_get_acs_drive_id.h"
#include "h/rmc_get_rmc_host_of_drive.h"
#include "h/rmc_get_loader_type.h"
#include "h/serrno.h"

#include <errno.h>
#include <string.h>

static int rmc_acs_mnt(const char *const vid, const char *const drive);
static int rmc_manual_mnt(const char *const vid, const char *const drive);
static int rmc_smc_mnt(const char *const vid, const char *const drive);

int rmc_mnt(const char *const vid, const char *const drive) {
	/* If there is nothing to work on then return EINVAL */
	if(NULL == vid || NULL == drive) {
		errno = EINVAL;
		serrno = errno;
		return -1;
	}

	if(CA_MAXVIDLEN < strlen(vid)) {
		errno = ERMCVIDTOOLONG; /* VID is too long */
		serrno = errno;
		return -1;
	}

	switch(rmc_get_loader_type(drive)) {
	case RMC_LOADER_TYPE_ACS:
		return rmc_acs_mnt(vid, drive);
	case RMC_LOADER_TYPE_MANUAL:
		return rmc_manual_mnt(vid, drive);
	case RMC_LOADER_TYPE_SMC:
		return rmc_smc_mnt(vid, drive);
	default:
		errno = ERMCUKNLDRTYPE; /* Unknown loader type */
		serrno = errno;
		return -1;
	}
}

static int rmc_acs_mnt(const char *const vid, const char *const drive) {
	const gid_t gid = getgid();
	const uid_t uid = getuid();

	/* The total length of the fixed size members of the request message */
	/* is Magic (4 bytes) + request ID (4 bytes) + msglen (4 bytes) +    */
	/* uid (4 bytes) + gid (4 bytes) + ACS number (4 bytes) + LSM number */
	/* (4 bytes) + panel number (4 bytes) + transport number (4 bytes) = */
	/* 40 bytes                                                          */
	const int msglen = 40 + strlen(vid);

	char repbuf[1];
	char *sbp = NULL;
	char sendbuf[RMC_REQBUFSZ];
	char rmc_host[CA_MAXHOSTNAMELEN+1];
	struct rmc_acs_drive_id drive_id = {0, 0, 0, 0};

	if(rmc_get_rmc_host_of_drive(drive, rmc_host, sizeof(rmc_host))) {
		errno = ERMCPARSERMCHOST; /* Failed to parse RMC host */
		serrno = errno;
		return -1;
	}

	if(rmc_get_acs_drive_id(drive, &drive_id)) {
		errno = ERMCPARSEACSDRV; /* Failed to parse ACS drive id */
		serrno = errno;
		return -1;
	}

	/* It is an internal error if the total size of the request message */
	/* would be greater than RMC_REQBUFSZ                               */
	if(msglen > RMC_REQBUFSZ) {
		errno = SEINTERNAL;
		serrno = errno;
		return -1;
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, RMC_MAGIC);
	marshall_LONG (sbp, RMC_ACS_MOUNT);
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, drive_id.acs);
	marshall_LONG (sbp, drive_id.lsm);
	marshall_LONG (sbp, drive_id.panel);
	marshall_LONG (sbp, drive_id.transport);
	marshall_STRING (sbp, vid);

	/* Being paranoid; checking the calculated message length against */
	/* the number of bytes marshalled                                 */
	if(sbp - sendbuf != msglen) {
		errno = SEINTERNAL;
		serrno = errno;
		return -1;
	}

        return send2rmc (rmc_host, sendbuf, msglen, repbuf, sizeof(repbuf));
}

static int rmc_manual_mnt(const char *const vid, const char *const drive) {
	/* Return 1 indicating the mount is manual */
	return 1;
}

static int rmc_smc_mnt(const char *const vid, const char *const drive) {

	return 0;
}
