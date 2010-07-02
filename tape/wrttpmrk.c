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
int wrttpmrk(int tapefd,
             char *path,
             int n)
{
	char func[16];
	struct mtop mtop;

	ENTRY (wrttpmrk);
	mtop.mt_op = MTWEOF;	/* write tape mark */
	mtop.mt_count = n;
	if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
	RETURN (0);
}
