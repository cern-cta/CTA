/*
 * Copyright (C) 2000-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: inquiry.c,v $ $Revision: 1.3 $ $Date: 2001/10/12 12:24:31 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*      inquiry - get information about the type of a drive */
#if defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#if defined(ADSTAR)
#include <sys/Atape.h>
#else
#include "scsictl.h"
#endif
#include "Ctape.h"
#include "serrno.h"
#if defined(_IBMR2)
extern char *dvrname;
#endif
extern char *sys_errlist[];
#endif
inquiry(fd, path, inq_data)
int fd;
char *path;
unsigned char *inq_data;
{
#if defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	char func[16];
#if defined(ADSTAR)
	struct inq_data_s inqd;
#else
	unsigned char buf[36];
	unsigned char cdb[6];
	char *msgaddr;
	int nb_sense_ret;
	unsigned char sense[MAXSENSE];
#endif
	int tapefd = -1;

#if defined(_IBMR2)
	if (strcmp (dvrname, "tape") == 0) {
		*inq_data = '\0';
		return (0);
	}
#endif
	ENTRY (inquiry);
#if defined(_AIX) || defined(SOLARIS25) || defined(hpux)
	if (fd >= 0) {
		tapefd = fd;
	} else {
		if ((tapefd = open (path, O_RDONLY|O_NDELAY)) < 0) {
			serrno = errno;
			usrmsg (func, TP042, path, "open", sys_errlist[errno]);
			RETURN (-1);
		}
	}
#endif
#if defined(ADSTAR)
        if (strcmp (dvrname, "Atape") == 0) {
                if (ioctl (tapefd, STIOCQRYINQUIRY, &inqd) < 0) {
			serrno = errno;
                        rpttperror (func, tapefd, path, "ioctl");
                        if (fd < 0) close (tapefd);
			RETURN (-1);
                }
		memcpy (inq_data, inqd.vendor_identification, 8);
		memcpy (inq_data + 8, inqd.product_identification, 16);
        }
#else
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x12;		/* inquiry */
	cdb[4] = 36;
	if (send_scsi_cmd (tapefd, path, 0, cdb, 6, buf, 36,
	    sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
#if defined(SOLARIS25) || defined(hpux)
                if (fd < 0) close (tapefd);
#endif
		RETURN (-1);
	}
	memcpy (inq_data, buf+8, 24);
	inq_data[24] = '\0';
#endif
#if defined(_AIX) || defined(SOLARIS25) || defined(hpux)
        if (fd < 0) close (tapefd);
#endif
        RETURN (0);
#else
        return (0);
#endif
}
