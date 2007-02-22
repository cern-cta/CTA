/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: usrlbl.c,v $ $Revision: 1.12 $ $Date: 2007/02/22 17:26:25 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	usrlbl - user callable routines to read/write header and trailer labels */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

/*	checkeofeov - check for EOF or EOV */
/*	return	-1 and serrno set in case of error
 *		-1 and serrno set to ETLBL for bad label structure
 *		0	for EOF
 *		ETEOV	for EOV
 */
int checkeofeov (tapefd, path)
int	tapefd;
char	*path;
{
	int c;
	struct devlblinfo  *dlip;
	char hdr1[LBLBUFSZ];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if ((c = readlbl (tapefd, path, hdr1)) < 0) {
		if (dlip->flags & NOTRLCHK) {
			return (0);
		} else {
			return (c);
                }
        }
	if (dlip->lblcode == NL || dlip->lblcode == BLP) {	/* tape is unlabelled */
		if (c > 1) {	/* last file on this tape */
			if ((c = skiptpfb (tapefd, path, 1)) < 0) return (c);
			return (ETEOV);
		}
	} else {	/* tape is labelled */
		if (c) {
			if (dlip->flags & NOTRLCHK) return (0);
			serrno = ETLBL;
			return (-1);
		}
		if (dlip->lblcode == SL) ebc2asc (hdr1, 80);
		if (strncmp (hdr1, "EOV1", 4) == 0) return (ETEOV);
		if (strncmp (hdr1, "EOF1", 4)) {
			serrno = ETLBL;
			return (-1);
		}
	}
	if ((c = skiptpfb (tapefd, path, 1)) < 0) return (c);
	if ((c = skiptpff (tapefd, path, 1)) < 0) return (c);
	return (0);
}

/*	wrthdrlbl - write header labels */
int wrthdrlbl (tapefd, path)
int	tapefd;
char	*path;
{
	int c;
	struct devlblinfo  *dlip;
	int fsec;
	char hdr1[80], hdr2[80];
	char uhl1[80], vol1[80];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if (dlip->lblcode != NL)	/* tape is labelled */
		sscanf (dlip->hdr1 + 27, "%4d", &fsec);
	else
		fsec = 1;
	if (fsec == 1 && dlip->fseq != 1 && dlip->rewritetm)
		if ((c = wrttpmrk (tapefd, path, 1)) < 0) return (c);
	if (dlip->lblcode == NL) return (0);	/* tape is unlabelled */
	if (dlip->fseq == 1 || fsec > 1) {
		memcpy (vol1, dlip->vol1, 80);
		if (dlip->lblcode == SL) asc2ebc (vol1, 80);
		if ((c = writelbl (tapefd, path, vol1)) < 0) return (c);
	}
	memcpy (hdr1, dlip->hdr1, 80);
	if (dlip->lblcode == SL) asc2ebc (hdr1, 80);
	if ((c = writelbl (tapefd, path, hdr1)) < 0) return (c);
	memcpy (hdr2, dlip->hdr2, 80);
	if (dlip->lblcode == SL) asc2ebc (hdr2, 80);
	if ((c = writelbl (tapefd, path, hdr2)) < 0) return (c);
	if (dlip->lblcode == AUL) {
		memcpy (uhl1, dlip->uhl1, 80);
		if ((c = writelbl (tapefd, path, uhl1)) < 0) return (c);
	}
	return (wrttpmrk (tapefd, path, 1));
}

int wrteotmrk(tapefd, path)
int tapefd;
char *path;
{
	int c;
	struct devlblinfo  *dlip;

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
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

/*	wrttrllbl - write trailer labels */
int wrttrllbl (tapefd, path, labelid, nblocks)
int	tapefd;
char	*path;
char	*labelid;
int	nblocks;
{
	char buf[7];
	int c;
	struct devlblinfo  *dlip;
	char hdr1[80], hdr2[80];
#if defined(hpux) || defined(linux)
	struct mtget mt_info;
#endif
#if defined(__alpha) && defined(__osf__)
	struct mtop mtop;
#endif
	char uhl1[80];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if (dlip->lblcode == NL) return (wrteotmrk (tapefd, path));	/* tape is unlabelled */
	if ((c = wrttpmrk (tapefd, path, 1)) < 0) return (c);

	memcpy (hdr1, dlip->hdr1, 80);
	memcpy (hdr1, labelid, 3);
	sprintf (buf, "%.6d", nblocks % 1000000);
	memcpy (hdr1 + 54, buf, 6);
	memcpy (hdr2, dlip->hdr2, 80);
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
#if defined(hpux)
	if (labelid[2] == 'V')	/* must clear EOT condition */
		c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
#if defined(linux)
	/* must clear EOT condition */
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
	if (dlip->lblcode == AUL) {
		memcpy (uhl1, dlip->uhl1, 80);
		uhl1[1] = 'T';
#if defined(hpux)
		if (labelid[2] == 'V')	/* must clear EOT condition */
			c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
#if defined(linux)
		/* must clear EOT condition */
		c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
#if defined(__alpha) && defined(__osf__)
		if (labelid[2] == 'V') {
			mtop.mt_op = MTCSE;     /* Clears serious exception */
			mtop.mt_count = 1;
			c = ioctl (tapefd, MTIOCTOP, &mtop);
		}
#endif
		if ((c = writelbl (tapefd, path, uhl1)) < 0) return (c);
#if sun
		if (c == 0)	/* 1st try for UHL1 on Desktop SPARC & SPARC System 600 */
			if ((c = writelbl (tapefd, path, uhl1)) < 0) return (c);
#endif
	}
	return (wrteotmrk (tapefd, path));
}

/*	deltpfil - delete current tape file */
int deltpfil (tapefd, path)
int	tapefd;
char	*path;
{
	int c;
	struct devlblinfo  *dlip;
	char hdr1[80], hdr2[80];
	char vol1[80];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if (dlip->fseq == 1) {
		if ((c = rwndtape (tapefd, path)) < 0) return (c);
		if (dlip->lblcode != NL) {
			memcpy (vol1, dlip->vol1, 80);
			memcpy (hdr1, dlip->hdr1, 80);
			memcpy (hdr2, dlip->hdr2, 80);

			/* set expiration date = creation date */

			memcpy (hdr1 + 47, hdr1 + 41, 6);

			if (dlip->lblcode == SL) asc2ebc (vol1, 80);
			if ((c = writelbl (tapefd, path, vol1)) < 0) return (c);
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

