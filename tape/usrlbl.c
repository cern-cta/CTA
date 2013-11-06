/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	usrlbl - user callable routines to read/write header and trailer labels */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#if defined(linux)
#include <sys/mtio.h>
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
#include "rtcp_lbl.h"
#include "getconfent.h"

#include "h/tapebridge_constants.h"

/*	checkeofeov - check for EOF or EOV */
/*	return	-1 and serrno set in case of error
 *		-1 and serrno set to ETLBL for bad label structure
 *		0	for EOF
 *		ETEOV	for EOV
 */
int checkeofeov (int	tapefd,
                 char	*path)
{
	int c;
	struct devlblinfo  *dlip;
	char hdr1[LBLBUFSZ];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if ((c = readlbl (tapefd, path, hdr1)) < 0) {
		return (c);
        }
	/* tape is labelled */
	if (c) {
		serrno = ETLBL;
		return (-1);
	}
	if (strncmp (hdr1, "EOV1", 4) == 0) return (ETEOV);
	if (strncmp (hdr1, "EOF1", 4)) {
		serrno = ETLBL;
		return (-1);
	}
	if ((c = skiptpfb (tapefd, path, 1)) < 0) return (c);
	if ((c = skiptpff (tapefd, path, 1)) < 0) return (c);
	return (0);
}

/*	wrthdrlbl - write header labels */
int wrthdrlbl(
	const int      tapefd,
	char *const    path,
	const uint32_t tapeFlushMode)
{
	int c;
	struct devlblinfo  *dlip;
	char hdr1[80], hdr2[80];
	char uhl1[80], vol1[80];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if (dlip->lblcode != AUL) {
		serrno = EINVAL;
		return(-1);
	}

	{
		int fsec = 0;
		sscanf (dlip->hdr1 + 27, "%4d", &fsec);

		if (dlip->fseq == 1 || fsec > 1) {
			memcpy (vol1, dlip->vol1, 80);
			if ((c = writelbl (tapefd, path, vol1)) < 0) return (c);
		}
	}
	memcpy (hdr1, dlip->hdr1, 80);
	if ((c = writelbl (tapefd, path, hdr1)) < 0) return (c);
	memcpy (hdr2, dlip->hdr2, 80);
	if ((c = writelbl (tapefd, path, hdr2)) < 0) return (c);
	if (dlip->lblcode == AUL) {
		memcpy (uhl1, dlip->uhl1, 80);
		if ((c = writelbl (tapefd, path, uhl1)) < 0) return (c);
	}

	/* Write buffered tape-mark */
	if ((c = wrttpmrk (tapefd, path, 1, 1)) < 0) return (c);

	/* Flush to tape if necessary */
        {
		int flushToTape = 0;

		switch(tapeFlushMode) {
		case TAPEBRIDGE_N_FLUSHES_PER_FILE:
			flushToTape = 1;
			break;
		case TAPEBRIDGE_ONE_FLUSH_PER_N_FILES:
			flushToTape = 0;
			break;
		default:
			/* Should never reach here */
			serrno = EINVAL;
			return(-1);
		}
		if(flushToTape) {
			if((c = wrttpmrk (tapefd, path, 0, 0)) < 0) return (c);
		}
	}

	return(0);
}

int wrteotmrk(
	const int      tapefd,
	char *const    path,
	const uint32_t tapeFlushMode)
{
	int c;
	struct devlblinfo  *dlip;

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	/* write 1 buffered tape-mark */
	if ((c = wrttpmrk (tapefd, path, 1, 1)) < 0) return (c);

	/* Flush to tape if necessary */
        {
		int flushToTape = 0;
		switch(tapeFlushMode) {
		case TAPEBRIDGE_N_FLUSHES_PER_FILE:
			flushToTape = 1;
			break;
		case TAPEBRIDGE_ONE_FLUSH_PER_N_FILES:
			flushToTape = 0;
			break;
		default:
			/* Should never reach here */
			serrno = EINVAL;
			return(-1);
		}
		if(flushToTape) {
			if((c = wrttpmrk (tapefd, path, 0, 0)) < 0) return (c);
		}
	}

	return (0);
}

/*	wrttrllbl - write trailer labels */
int wrttrllbl (
	const int      tapefd,
	char *const    path,
	char *const    labelid,
	const int      nblocks,
	const uint32_t tapeFlushMode)
{
	char buf[7];
	int c;
	struct devlblinfo  *dlip;
	char hdr1[80], hdr2[80];
#if defined(linux)
	struct mtget mt_info;
#endif
	char uhl1[80];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if (dlip->lblcode != AUL) return (-1);	/* unsupported tape label */

	/* Write a buffered tape-mark */
	if ((c = wrttpmrk (tapefd, path, 1, 1)) < 0) return (c);

	/* Flush to tape if necessary */
        {
		int flushToTape = 0;
		switch(tapeFlushMode) {
		case TAPEBRIDGE_N_FLUSHES_PER_FILE:
			flushToTape = 1;
			break;
		case TAPEBRIDGE_ONE_FLUSH_PER_N_FILES:
			flushToTape = 0;
			break;
		default:
			/* Should never reach here */
			serrno = EINVAL;
			return(-1);
		}
		if(flushToTape) {
			if((c = wrttpmrk (tapefd, path, 0, 0)) < 0) return (c);
		}
	}

	memcpy (hdr1, dlip->hdr1, 80);
	memcpy (hdr1, labelid, 3);
	sprintf (buf, "%.6d", nblocks % 1000000);
	memcpy (hdr1 + 54, buf, 6);
	memcpy (hdr2, dlip->hdr2, 80);
	memcpy (hdr2, labelid, 3);

#if defined(linux)
	/* Please note that under Linux, this call is not necessary here
	because the previous writing of filemark has already cleared the
	EOT condition */
	if (labelid[2] == 'V')	/* must clear EOT condition */
		c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
	if ((c = writelbl (tapefd, path, hdr1)) < 0) return (c);
#if defined(linux)
	/* must clear EOT condition */
	c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
	if ((c = writelbl (tapefd, path, hdr2)) < 0) return (c);
	if (dlip->lblcode == AUL) {
		memcpy (uhl1, dlip->uhl1, 80);
		uhl1[1] = 'T';
#if defined(linux)
		/* must clear EOT condition */
		c = ioctl (tapefd, MTIOCGET, &mt_info);
#endif
		if ((c = writelbl (tapefd, path, uhl1)) < 0) return (c);
	}
	return (wrteotmrk (tapefd, path, tapeFlushMode));
}

/*	deltpfil - delete current tape file */
int deltpfil (int	tapefd,
              char	*path)
{
	int c;
	struct devlblinfo  *dlip;
	char hdr1[80], hdr2[80];
	char vol1[80];

	if (getlabelinfo (path, &dlip) < 0)
		return (-1);
	if (dlip->fseq == 1) {
		if ((c = rwndtape (tapefd, path)) < 0) return (c);
		if (dlip->lblcode == AUL) {
			memcpy (vol1, dlip->vol1, 80);
			memcpy (hdr1, dlip->hdr1, 80);
			memcpy (hdr2, dlip->hdr2, 80);

			/* set expiration date = creation date */

			memcpy (hdr1 + 47, hdr1 + 41, 6);

			if ((c = writelbl (tapefd, path, vol1)) < 0) return (c);
			if ((c = writelbl (tapefd, path, hdr1)) < 0) return (c);
			if ((c = writelbl (tapefd, path, hdr2)) < 0) return (c);
		}
	} else {
		if ((c = skiptpfb (tapefd, path, 2)) < 0) return (c);
	}
	{
		/* Force synchronised tape-marks */
		const uint32_t tapeFlushMode = TAPEBRIDGE_N_FLUSHES_PER_FILE;
		return (wrteotmrk (tapefd, path, tapeFlushMode));
	}
}

