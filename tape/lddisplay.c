/*
 * Copyright (C) 1992-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: lddisplay.c,v $ $Revision: 1.6 $ $Date: 2005/01/20 16:30:58 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <string.h>

/*	lddisplay - load display on 3480 compatible drives */
#if defined(RS6000PCTA) || defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
#include <sys/types.h>
#include <fcntl.h>
#if defined(_IBMESA) || defined(RS6000PCTA)
#include <sys/mtio.h>
#endif
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#include "Ctape.h"
#include "scsictl.h"
#include "serrno.h"
#define DISP_TIMEOUT 10000	/* timeout for load display in milliseconds */
#if defined(_IBMR2)
extern char *dvrname;
#endif
#endif
lddisplay(fd, path, fcb, msg1, msg2, devtype)
int fd;
char *path;
int fcb;	/* format control byte */
char *msg1;
char *msg2;
int devtype;	/* 0 -> 3480, 1 -> 3590, 2 -> Vision Box */
{
#if defined(RS6000PCTA) || defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	int c = 0;
	char func[16];
#if defined(_AIX)
#if defined(RS6000PCTA)
	struct ldcmd ldcmd;
#endif
#if defined(ADSTAR)
	struct stdm_s stdm;
#endif
#else
	unsigned char cdb[6];
	struct {
		uint fcb:8;
		char msg1[8];
		char msg2[8];
	} ldcmd;
	struct {
		uint dt:8;
		uint fcb:8;
		char filler[4];
		uint msglen1:8;
		uint msglen2:8;
		char msg1[8];
		char msg2[8];
	} ntp_ldcmd;
	struct {
		uint fcb:8;
		char msg1[16];
		char msg2[16];
	} vb_ldcmd;
	char *msgaddr;
	int nb_sense_ret;
	char sense[MAXSENSE];
#endif
	int tapefd = -1;

#if defined(_IBMR2)
	if (strcmp (dvrname, "tape") == 0)
		return (0);
#endif
	ENTRY (lddisplay);
#if defined(_AIX) || defined(SOLARIS25) || defined(hpux)
	if (fd >= 0) {
		tapefd = fd;
	} else {
		if ((tapefd = open (path, O_RDONLY|O_NDELAY)) < 0) {
			serrno = errno;
			usrmsg (func, TP042, path, "open", strerror(errno));
			RETURN (-1);
		}
	}
#endif
#if defined(_AIX)
#if defined(RS6000PCTA)
	if (strncmp (dvrname, "mtdd", 4) == 0) {
		ldcmd.ld_func = fcb;
		memset (ldcmd.ld_msg1, ' ', 8);
		memcpy (ldcmd.ld_msg1, msg1, strlen (msg1));
		memset (ldcmd.ld_msg2, ' ', 8);
		memcpy (ldcmd.ld_msg2, msg2, strlen (msg2));

		if (ioctl (tapefd, MTIOCLD, &ldcmd) < 0) {
			serrno = errno;
			rpttperror (func, tapefd, path, "ioctl");
			if (fd < 0) close (tapefd);
			RETURN (-1);
		}
	}
#endif
#if defined(ADSTAR)
	if (strcmp (dvrname, "Atape") == 0) {
		stdm.dm_func = fcb;
		memset (stdm.dm_msg0, ' ', 8);
		memcpy (stdm.dm_msg0, msg1, strlen (msg1));
		memset (stdm.dm_msg1, ' ', 8);
		memcpy (stdm.dm_msg1, msg2, strlen (msg2));

		if (ioctl (tapefd, STIOCDM, &stdm) < 0) {
			serrno = errno;
			rpttperror (func, tapefd, path, "ioctl");
			if (fd < 0) close (tapefd);
			RETURN (-1);
		}
	}
#endif
#else
	memset (cdb, 0, sizeof(cdb));
	switch (devtype) {
	case 0:		/* 3480 compatible */
		cdb[0] = 0x6;
		cdb[4] = 17;
		ldcmd.fcb = fcb;
		memset (ldcmd.msg1, ' ', 8);
		memcpy (ldcmd.msg1, msg1, strlen(msg1));
		memset (ldcmd.msg2, ' ', 8);
		memcpy (ldcmd.msg2, msg2, strlen(msg2));
		if (send_scsi_cmd (tapefd, path, 0, cdb, 6, &ldcmd, 17,
		    sense, 38, DISP_TIMEOUT, SCSI_OUT, &nb_sense_ret, &msgaddr) < 0) {
			usrmsg (func, "%s", msgaddr);
			c = -1;
		}
		break;
	case 1:		/* 3590 */
		cdb[0] = 0xC0;
		cdb[4] = 24;
		ntp_ldcmd.dt = 0x80;
		ntp_ldcmd.fcb = fcb;
		memset (ntp_ldcmd.filler, 0, 4);
		ntp_ldcmd.msglen1 = 0x00;
		ntp_ldcmd.msglen2 = 0x10;
		memset (ntp_ldcmd.msg1, ' ', 8);
		memcpy (ntp_ldcmd.msg1, msg1, strlen(msg1));
		memset (ntp_ldcmd.msg2, ' ', 8);
		memcpy (ntp_ldcmd.msg2, msg2, strlen(msg2));
		if (send_scsi_cmd (tapefd, path, 0, cdb, 6, &ntp_ldcmd, 24,
		    sense, 38, DISP_TIMEOUT, SCSI_OUT, &nb_sense_ret, &msgaddr) < 0) {
			usrmsg (func, "%s", msgaddr);
			c = -1;
		}
		break;
	case 2:		/* Vision Box */
		cdb[0] = 0x6;
		cdb[4] = 33;
		vb_ldcmd.fcb = fcb;
		memset (vb_ldcmd.msg1, ' ', 16);
		memcpy (vb_ldcmd.msg1, msg1, strlen(msg1));
		memset (vb_ldcmd.msg2, ' ', 16);
		memcpy (vb_ldcmd.msg2, msg2, strlen(msg2));
		if (send_scsi_cmd (tapefd, path, 0, cdb, 6, &vb_ldcmd, 33,
		    sense, 38, DISP_TIMEOUT, SCSI_OUT, &nb_sense_ret, &msgaddr) < 0) {
			usrmsg (func, "%s", msgaddr);
			c = -1;
		}
	}
#endif  
#if defined(_AIX) || defined(SOLARIS25) || defined(hpux)
	if (fd < 0) close (tapefd);
#endif
	RETURN (c);
#else
	return (0);
#endif
}
