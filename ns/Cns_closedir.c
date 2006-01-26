/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_closedir.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:17 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_closedir - free the Cns_DIR structure */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "Cns_api.h"
#include "Cns.h"
#include "marshall.h"
#include "serrno.h"

int DLL_DECL
Cns_closedir(Cns_DIR *dirp)
{
	int i;
	struct Cns_rep_info *ir;
	int msglen;
	char *sbp;
	char sendbuf[REQBUFSZ];

	if (! dirp) {
		serrno = EFAULT;
		return (-1);
	}

	/* tell nsdaemon to free the thread */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_CLOSEDIR);
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
	while (send2nsd (&dirp->dd_fd, NULL, sendbuf, msglen, NULL, 0) &&
	    serrno == ENSNACT)
		sleep (RETRYI);

	if (dirp->replicas) {	/* free previous replica information */
		ir = (struct Cns_rep_info *) dirp->replicas;
		for (i = 0; i < dirp->nbreplicas; i++) {
			free (ir->host);
			free (ir->sfn);
			ir++;
		}
		free (dirp->replicas);
		dirp->nbreplicas = 0;
		dirp->replicas = NULL;
	}
	free (dirp);
	return (0);
}
