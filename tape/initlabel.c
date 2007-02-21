/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: initlabel.c,v $ $Revision: 1.9 $ $Date: 2007/02/21 16:31:31 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

/*	initlabelroutines - allocate the structures to store the information
	needed by the label processing routines */

static int nb_rsvd_resources;
static struct devlblinfo *devlblinfop;
int initlabelroutines (nrr)
int nrr;
{
	nb_rsvd_resources = nrr;
	devlblinfop = calloc (nrr, sizeof(struct devlblinfo));
	if (devlblinfop == NULL) {
		Ctape_errmsg ("initlabelroutines", TP005);
		serrno = ENOMEM;
		return (-1);
	}
	return (0);
}

int getlabelinfo (path, dlipp)
char *path;
struct devlblinfo  **dlipp;
{
	struct devlblinfo  *dlip;
	int i;

        for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
                if (strcmp (dlip->path, path) == 0) break;
	if (i == nb_rsvd_resources) {
		serrno = ENOENT;
		return (-1);
	}
	*dlipp = dlip;
	return (0);
}

/*	rmlabelinfo - remove label information from the internal table */

int rmlabelinfo (path, flags)
char *path;
int flags;
{
	struct devlblinfo  *dlip;
	int i;

	if (devlblinfop == NULL)
		return (0);
	if (path) {
		for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
			if (strcmp (dlip->path, path) == 0) break;
		if (flags & TPRLS_KEEP_RSV) {
			memset (dlip, '\0', sizeof(struct devlblinfo));
		} else {
			nb_rsvd_resources--;
			if (i != nb_rsvd_resources) {	/* not last entry */
				memcpy (dlip, dlip + 1, nb_rsvd_resources - i);
			}
		}
	}
	if ((flags & TPRLS_ALL) || nb_rsvd_resources == 0)
		free (devlblinfop);
	return (0);
}

/*	setdevinfo - set flags for optimization of label processing routines
 *	according to device type and density
 */
int setdevinfo (path, devtype, den, lblcode)
char *path;
char *devtype;
int den;
int lblcode;
{
	struct devinfo *devinfo;
	struct devlblinfo  *dlip;
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (dlip->path[0] == '\0') break;

	strcpy (dlip->path, path);
	strcpy (dlip->devtype, devtype);

	devinfo = Ctape_devinfo (devtype);
	dlip->dev1tm = (devinfo->eoitpmrks == 1) ? 1 : 0;

	if (strcmp (devtype, "8200") == 0 || den == D8200 || den == D8200C)
		dlip->rewritetm = 1;	/* An Exabyte 8200 must be positionned
					   on the BOT side of a long filemark
					   before starting to write */
	else
		dlip->rewritetm = 0;

	dlip->lblcode = lblcode;
	return (0);
}

/*	setlabelinfo - set label information for label processing routines */

int setlabelinfo (path, flags, fseq, vol1, hdr1, hdr2, uhl1)
char *path;
int flags;
int fseq;
char *vol1;
char *hdr1;
char *hdr2;
char *uhl1;
{
	struct devlblinfo  *dlip;
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (strcmp (dlip->path, path) == 0) break;

	dlip->flags = flags;
	dlip->fseq = fseq;
	if (dlip->lblcode == AL || dlip->lblcode == AUL || dlip->lblcode == SL) {
		strcpy (dlip->vol1, vol1);
		strcpy (dlip->hdr1, hdr1);
		strcpy (dlip->hdr2, hdr2);
		strcpy (dlip->uhl1, uhl1);
	}
        return (0);
}
