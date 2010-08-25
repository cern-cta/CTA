/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

#include <linux/mtio.h>
#ifndef MTWEOFI
#include "mtio_add.h"
#endif

int wrttpmrk(int tapefd,
             char *path,
             int n,
             int immed)
{
	char func[16];
	struct mtop mtop;

	ENTRY (wrttpmrk);
	if ( 1 == immed ) {
		mtop.mt_op = MTWEOFI;	/* write immediate (buffered) tape mark */
	} else {
		mtop.mt_op = MTWEOF;	/* write standard (synchronizing) tape mark  */
	}
	
	mtop.mt_count = n;
	if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
	RETURN (0);
}
