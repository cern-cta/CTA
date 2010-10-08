/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mtio.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

int chkdriveready(int tapefd)
{
	char func[16];
	struct mtget mt_info;

	strncpy (func, "chkdriveready", 16);
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (GMT_ONLINE(mt_info.mt_gstat))
		return (1);	/* drive ready */
	else
		return (0);	/* drive not ready */
}

int chkwriteprot(int tapefd)
{
	char func[16];
	struct mtget mt_info;

	strncpy (func, "chkwriteprot", 16);
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (GMT_WR_PROT(mt_info.mt_gstat))
		return (WRITE_DISABLE);
	else
		return (WRITE_ENABLE);
}
