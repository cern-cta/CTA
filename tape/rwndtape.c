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
int rwndtape(tapefd, path)
int tapefd;
char *path;
{
	char func[16];
	struct mtop mtop;

	ENTRY (rwndtape);
	mtop.mt_op = MTREW;	/*rewind tape */
	mtop.mt_count = 1;
	if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
		serrno = rpttperror (func, tapefd, path, "ioctl"); 
		RETURN (-1);
	}
	RETURN (0);
}
