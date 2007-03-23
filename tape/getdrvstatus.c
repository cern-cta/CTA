/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: getdrvstatus.c,v $ $Revision: 1.6 $ $Date: 2007/03/23 13:08:33 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <string.h>
#if defined(__osf__) && defined(__alpha)
#include <sys/types.h>
#include <sys/devio.h>
#include <sys/ioctl.h>
#include "Ctape.h"
#include "serrno.h"
chkdriveready(tapefd)
int tapefd;
{
	char func[16];
	struct devget mt_info;

	strcpy (func, "chkdriveready");
	if (ioctl (tapefd, DEVIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (mt_info.stat & DEV_OFFLINE)
		return (0);	/* drive not ready */
	else
		return (1);	/* drive ready */
}

chkwriteprot(tapefd)
int tapefd;
{
	char func[16];
	struct devget mt_info;

	strcpy (func, "chkwriteprot");
	if (ioctl (tapefd, DEVIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (mt_info.stat & DEV_WRTLCK)
		return (WRITE_DISABLE);
	else
		return (WRITE_ENABLE);
}
#else
#if sgi
#include <sys/types.h>
#include <sys/mtio.h>
#include "Ctape.h"
#include "serrno.h"
chkdriveready(tapefd)
int tapefd;
{
	char func[16];
	struct mtget mt_info;

	strcpy (func, "chkdriveready");
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (mt_info.mt_dposn & MT_ONL)
		return (1);	/* drive ready */
	else
		return (0);	/* drive not ready */
}

chkwriteprot(tapefd)
int tapefd;
{
	char func[16];
	struct mtget mt_info;

	strcpy (func, "chkwriteprot");
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (mt_info.mt_dposn & MT_WPROT)
		return (WRITE_DISABLE);
	else
		return (WRITE_ENABLE);
}
#else
#if sun
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include "Ctape.h"
#include "serrno.h"
chkdriveready(tapefd)
int tapefd;
{
	char func[16];
	struct mtget mt_info;

	strcpy (func, "chkdriveready");
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	return (1);	/* if open is successful, drive is ready */
}

chkwriteprot(tapefd)
int tapefd;
{
	char func[16];
	struct mtget mt_info;

	strcpy (func, "chkwriteprot");
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (mt_info.mt_erreg == 7)
		return (WRITE_DISABLE);
	else
		return (WRITE_ENABLE);
}
#else
#if hpux || linux
#include <sys/types.h>
#include <sys/mtio.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
int chkdriveready(tapefd)
int tapefd;
{
	char func[16];
	struct mtget mt_info;

	strcpy (func, "chkdriveready");
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (GMT_ONLINE(mt_info.mt_gstat))
		return (1);	/* drive ready */
	else
		return (0);	/* drive not ready */
}

int chkwriteprot(tapefd)
int tapefd;
{
	char func[16];
	struct mtget mt_info;

	strcpy (func, "chkwriteprot");
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
	}
	if (GMT_WR_PROT(mt_info.mt_gstat))
		return (WRITE_DISABLE);
	else
		return (WRITE_ENABLE);
}
#else
#if _AIX
#include <errno.h>
#include <sys/types.h>
#if defined(RS6000PCTA)
#include <sys/ioctl.h>
#include <sys/mtio.h>
#endif
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#include "Ctape.h"
#include "serrno.h"
chkdriveready(tapefd)
int tapefd;
{
	char func[16];
#if defined(RS6000PCTA)
        struct mtop mtop;
#endif
#if defined(ADSTAR)
        struct stop stop;
#endif

#if defined(_IBMR2)
	extern char *dvrname;
	if (strcmp (dvrname, "tape") == 0)
		return (1);	/* if open is successful, drive is ready */
#endif
	strcpy (func, "chkdriveready");
#if defined(RS6000PCTA)
	if (strncmp (dvrname, "mtdd", 4) == 0) {
		mtop.mt_op = MTNOP;	/* no-op */
		mtop.mt_count = 1;
		if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
			if (errno == EIO || errno == ENOTREADY) {
				return (0);	/* drive not ready */
			} else {
				tplogit (func, TP002, "ioctl", strerror(errno));
				serrno = errno;
				return (-1);	/* error */
			}
		}
		return (1);	/* drive ready */
#endif
#if defined(ADSTAR)
	if (strcmp (dvrname, "Atape") == 0) {
		stop.st_op = STTUR;
		if (ioctl (tapefd, STIOCTOP, &stop) < 0) {
			if (errno == ENOTREADY) {
				return (0);	/* drive not ready */
			} else {
				tplogit (func, TP002, "ioctl", strerror(errno));
				serrno = errno;
				return (-1);	/* error */
			}
		}
		return (1);	/* drive ready */
	}
#endif
}

#if defined(RS6000PCTA)
chkwriteprot(tapefd)
int tapefd;
{
	char func[16];
	struct mtsrbl mtsrbl;
        struct mtop mtop;

	strcpy (func, "chkwriteprot");
	mtop.mt_op = MTNOP;	/* no-op */
	mtop.mt_count = 1;
	if (ioctl (tapefd, MTIOCTOP, &mtop) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
		return (-1);	/* error */
        }
	mtsrbl.resvd = 0;
	mtsrbl.versn = 0;
	mtsrbl.mtsrblop = MTSNSOP;
	if (ioctl (tapefd, MTSRBLOP, &mtsrbl) < 0) {
		tplogit (func, TP002, "ioctl", strerror(errno));
		serrno = errno;
                return (-1);	/* error */
        }
	if (mtsrbl.mtsrblinfo[1] & 0x2)
		return (WRITE_DISABLE);
	else
		return (WRITE_ENABLE);
}
#endif
#endif
#endif
#endif
#endif
#endif
