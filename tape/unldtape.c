/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
int unldtape(const int tapefd,
             const char *const path)
{
	char func[16];
	struct mtop mtop;

	ENTRY (unldtape);
	if (tapefd < 0) RETURN (0);
	mtop.mt_op = MTOFFL;	/* unload tape */
	mtop.mt_count = 1;
	if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
	RETURN (0);
}
