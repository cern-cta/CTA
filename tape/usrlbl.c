/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: usrlbl.c,v $ $Revision: 1.2 $ $Date: 1999/09/17 09:41:16 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	usrlbl - user callable routines to read/write header and trailer labels */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#if defined(hpux) || defined(sgi) || defined(linux)
#include <sys/mtio.h>
#endif
#if defined(__alpha) && defined(__osf__)
#include <sys/ioctl.h>
#include <sys/mtio.h>
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

/*	initlabelroutines - allocate the structures to store the information
	needed by the label processing routines */

static int nb_rsvd_resources;
static struct devlblinfo *devlblinfop;
initlabelroutines (nrr)
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

/*	rmlabelinfo - remove label information from the internal table */

rmlabelinfo (path, flags)
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
		if (flags == TPRLS_KEEP_RSV) {
			memset (dlip, '\0', sizeof(struct devlblinfo));
		} else {
			nb_rsvd_resources--;
			if (i != nb_rsvd_resources) {	/* not last entry */
				memcpy (dlip, dlip + 1, nb_rsvd_resources - i);
			}
		}
	}
	if (flags == TPRLS_ALL || nb_rsvd_resources == 0)
		free (devlblinfop);
}

/*	setdevinfo - set flags for optimization of label processing routines
 *	according to device type and density
 */
setdevinfo (path, devtype, den, lblcode)
char *path;
char *devtype;
int den;
int lblcode;
{
	struct devlblinfo  *dlip;
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (dlip->path[0] == '\0') break;

	strcpy (dlip->path, path);

	if (strncmp (devtype, "DLT", 3) == 0 || strcmp (devtype, "3590") == 0 ||
	    strcmp (devtype, "SD3") == 0 || strcmp (devtype, "9840") == 0)
		dlip->dev1tm = 1;	/* one TM at EOI is enough */
	else
		dlip->dev1tm = 0;

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

setlabelinfo (path, flags, fseq, vol1, hdr1, hdr2)
char *path;
int flags;
int fseq;
char *vol1;
char *hdr1;
char *hdr2;
{
	struct devlblinfo  *dlip;
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (strcmp (dlip->path, path) == 0) break;

	dlip->flags = flags;
	dlip->fseq = fseq;
	if (dlip->lblcode == AL || dlip->lblcode == SL) {
		strcpy (dlip->vol1, vol1);
		strcpy (dlip->hdr1, hdr1);
		strcpy (dlip->hdr2, hdr2);
	}
}

/*	checkeofeov - check for EOF or EOV */
/*	return	-errno	in case of error
 *		-ETLBL	for bad label structure
 *		0	for EOF
 *		ETEOV	for EOV
 */
checkeofeov (tapefd, path)
int	tapefd;
char	*path;
{
	int c;
	struct devlblinfo  *dlip;
	char hdr1[LBLBUFSZ];
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (strcmp (dlip->path, path) == 0) break;

	if ((c = readlbl (tapefd, path, hdr1)) < 0)
		if (dlip->flags & NOTRLCHK)
			return (0);
		else
			return (c);
	if (dlip->lblcode == NL || dlip->lblcode == 'b') {	/* tape is unlabelled */
		if (c > 1) {	/* last file on this tape */
			if ((c = skiptpfb (tapefd, path, 1)) < 0) return (c);
			return (ETEOV);
		}
	} else {	/* tape is labelled */
		if (c) {
			if (dlip->flags & NOTRLCHK) return (0);
			errno = ETLBL;
			return (-ETLBL);
		}
		if (dlip->lblcode == SL) ebc2asc (hdr1, 80);
		if (strncmp (hdr1, "EOV1", 4) == 0) return (ETEOV);
		if (strncmp (hdr1, "EOF1", 4)) {
			errno = ETLBL;
			return (-ETLBL);
		}
	}
	if ((c = skiptpfb (tapefd, path, 1)) < 0) return (c);
	if ((c = skiptpff (tapefd, path, 1)) < 0) return (c);
	return (0);
}

/*	wrthdrlbl - write header labels */
wrthdrlbl (tapefd, path)
int	tapefd;
char	*path;
{
	int c;
	struct devlblinfo  *dlip;
	int fsec;
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (strcmp (dlip->path, path) == 0) break;

	if (dlip->lblcode != NL)	/* tape is labelled */
		sscanf (dlip->hdr1 + 27, "%4d", &fsec);
	else
		fsec = 1;
	if (fsec == 1 && dlip->fseq != 1 && dlip->rewritetm)
		if ((c = wrttpmrk (tapefd, path, 1)) < 0) return (c);
	if (dlip->lblcode == NL) return (0);	/* tape is unlabelled */
	if (dlip->fseq == 1) {
		if (dlip->lblcode == SL) asc2ebc (dlip->vol1, 80);
		if ((c = writelbl (tapefd, path, dlip->vol1)) < 0) return (c);
	}
	if (dlip->lblcode == SL) asc2ebc (dlip->hdr1, 80);
	if ((c = writelbl (tapefd, path, dlip->hdr1)) < 0) return (c);
	if (dlip->lblcode == SL) asc2ebc (dlip->hdr2, 80);
	if ((c = writelbl (tapefd, path, dlip->hdr2)) < 0) return (c);
	return (wrttpmrk (tapefd, path, 1));
}

/*	wrttrllbl - write trailer labels */
wrttrllbl (tapefd, path, labelid, nblocks)
int	tapefd;
char	*path;
char	*labelid;
int	nblocks;
{
	char buf[7];
	int c;
	struct devlblinfo  *dlip;
	int fseq;
	char hdr1[81], hdr2[81];
	int i;
#if defined(hpux) || defined(linux)
	struct mtget mt_info;
#endif
#if defined(__alpha) && defined(__osf__)
	struct mtop mtop;
#endif

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (strcmp (dlip->path, path) == 0) break;

	if (dlip->lblcode == NL) return (wrteotmrk (tapefd, path));	/* tape is unlabelled */
	if ((c = wrttpmrk (tapefd, path, 1)) < 0) return (c);

	memcpy (hdr1, labelid, 3);
	sprintf (buf, "%.6d", nblocks);
	memcpy (hdr1 + 54, buf, 6);
	memcpy (hdr2, labelid, 3);

	if (dlip->lblcode == SL) asc2ebc (hdr1, 80);
#if defined(hpux) || defined(linux)
	/* Please note that under Linux, this call is not necessary here
	because the previous writing of filemark has already cleared the
	EOT condition */
	if (labelid[2] == 'V')	/* must clear EOT condition */
		c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
#if defined(__alpha) && defined(__osf__)
	if (labelid[2] == 'V') {
		mtop.mt_op = MTCSE;     /* Clears serious exception */
		mtop.mt_count = 1;
		c = ioctl (tapefd, MTIOCTOP, &mtop);
	}
#endif
#if sgi
	if (labelid[2] == 'V')
		c = ioctl (tapefd, MTANSI, 1);
#endif
	if ((c = writelbl (tapefd, path, hdr1)) < 0) return (c);
	if (dlip->lblcode == SL) asc2ebc (hdr2, 80);
#if defined(hpux) || defined(linux)
	if (labelid[2] == 'V')	/* must clear EOT condition */
		c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
#if defined(__alpha) && defined(__osf__)
	if (labelid[2] == 'V') {
		mtop.mt_op = MTCSE;     /* Clears serious exception */
		mtop.mt_count = 1;
		c = ioctl (tapefd, MTIOCTOP, &mtop);
	}
#endif
	if ((c = writelbl (tapefd, path, hdr2)) < 0) return (c);
#if sun
	if (c == 0)	/* 1st try for EOV2 on Desktop SPARC & SPARC System 600 */
		if ((c = writelbl (tapefd, path, hdr2)) < 0) return (c);
#endif
	return (wrteotmrk (tapefd, path));
}

/*	deltpfil - delete current tape file */
deltpfil (tapefd, path)
int	tapefd;
char	*path;
{
	int c;
	struct devlblinfo  *dlip;
	char hdr1[81], hdr2[81];
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (strcmp (dlip->path, path) == 0) break;

	if (dlip->fseq == 1) {
		if ((c = rwndtape (tapefd, path)) < 0) return (c);
		if (dlip->lblcode != NL) {

			/* set expiration date = creation date */

			memcpy (hdr1 + 47, hdr1 + 41, 6);

			if (dlip->lblcode == SL) asc2ebc (dlip->vol1, 80);
			if ((c = writelbl (tapefd, path, dlip->vol1)) < 0) return (c);
			if (dlip->lblcode == SL) asc2ebc (hdr1, 80);
			if ((c = writelbl (tapefd, path, hdr1)) < 0) return (c);
			if (dlip->lblcode == SL) asc2ebc (hdr2, 80);
			if ((c = writelbl (tapefd, path, hdr2)) < 0) return (c);
		}
	} else {
		if ((c = skiptpfb (tapefd, path, dlip->lblcode == NL ? 1:2)) < 0) return (c);
	}
	return (wrteotmrk (tapefd, path));
}

wrteotmrk(tapefd, path)
int tapefd;
char *path;
{
	int c;
	struct devlblinfo  *dlip;
	int i;

	for (i = 0, dlip = devlblinfop; i < nb_rsvd_resources; i++, dlip++)
		if (strcmp (dlip->path, path) == 0) break;

	if (! dlip->dev1tm) {
		/* write 2 tape marks */
		if ((c = wrttpmrk (tapefd, path, 1)) < 0) return (c);
		if ((c = wrttpmrk (tapefd, path, 1)) < 0) return (c);
		/* for Exabytes in 8200 mode, position in front of the 2 tapemarks,
		   otherwise position between the 2 tapemarks */
		if ((c = skiptpfb (tapefd, path, dlip->rewritetm+1)) < 0) return (c);
	} else {
		/* write 1 tape mark */
		if ((c = wrttpmrk (tapefd, path, 1)) < 0) return (c);
		/* flush the buffer */
		if ((c = wrttpmrk (tapefd, path, 0)) < 0) return (c);
	}
	return (0);
}
