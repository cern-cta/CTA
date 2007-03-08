/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: unldtape.c,v $ $Revision: 1.5 $ $Date: 2007/03/08 09:42:06 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#if defined(_AIX) && defined(_IBMR2)
#include <sys/tape.h>
#define mtop stop
#define mt_op st_op
#define mt_count st_count
#define MTIOCTOP STIOCTOP
#define MTOFFL STOFFL
#else
#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <sys/mtio.h>
#endif
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
int unldtape(tapefd, path)
#if defined(_WIN32)
HANDLE tapefd;
#else
int tapefd;
#endif
char *path;
{
	char func[16];
#if !defined(_WIN32)
	struct mtop mtop;
#endif

	ENTRY (unldtape);
	if (tapefd < 0) RETURN (0);
#if !defined(_WIN32)
#if sgi
	mtop.mt_op = MTUNLOAD;
#else
	mtop.mt_op = MTOFFL;	/* unload tape */
#endif
	mtop.mt_count = 1;
	if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
#else
	if (PrepareTape (tapefd, TAPE_UNLOAD, (BOOL)1)) {
#endif
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
	RETURN (0);
}
