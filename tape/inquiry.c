/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: inquiry.c,v $ $Revision: 1.6 $ $Date: 2005/01/20 16:30:46 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <string.h>

/*      inquiry - get information about the type of a drive */
#if defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
#include <sys/types.h>
#include <fcntl.h>
#if defined(ADSTAR)
#include <sys/Atape.h>
#include <sys/scsi.h>
#else
#include "scsictl.h"
#endif
#include "Ctape.h"
#include "serrno.h"
#if defined(_IBMR2)
extern char *dvrname;
#endif
#endif
inquiry(fd, path, inq_data)
int fd;
char *path;
unsigned char *inq_data;
{
#if defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	char func[16];
#if defined(ADSTAR)
	struct st_inquiry inqd;
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
			usrmsg (func, TP042, path, "open", strerror(errno));
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
		memcpy (inq_data, inqd.standard.vendor_identification, 8);
		memcpy (inq_data + 8, inqd.standard.product_identification, 16);
		memcpy (inq_data + 24, inqd.standard.product_revision_level, 4);
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
	memcpy (inq_data, buf+8, 28);
#endif
	inq_data[28] = '\0';
#if defined(_AIX) || defined(SOLARIS25) || defined(hpux)
        if (fd < 0) close (tapefd);
#endif
        RETURN (0);
#else
	*inq_data = '\0';
        return (0);
#endif
}

/*      inquiry80 - get drive serial number */
inquiry80(fd, path, inq_data)
int fd;
char *path;
unsigned char *inq_data;
{
#if defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	unsigned char buf[16];
	char func[16];
#if defined(ADSTAR)
	struct sc_iocmd sc_iocmd;
#else
	unsigned char cdb[6];
	char *msgaddr;
	int nb_sense_ret;
	unsigned char sense[MAXSENSE];
#endif
	int rc;
	int tapefd = -1;

#if defined(_IBMR2)
	if (strcmp (dvrname, "tape") == 0) {
		*inq_data = '\0';
		return (0);
	}
#endif
	ENTRY (inquiry80);
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
#if defined(ADSTAR)
        if (strcmp (dvrname, "Atape") == 0) {
		memset (&sc_iocmd, 0, sizeof(sc_iocmd));
		sc_iocmd.scsi_cdb[0] = 0x12;	/* inquiry */
		sc_iocmd.scsi_cdb[1] = 0x01;
		sc_iocmd.scsi_cdb[2] = 0x80;
		sc_iocmd.scsi_cdb[4] = 16;
		sc_iocmd.timeout_value = 10;	/* seconds */
		sc_iocmd.command_length = 6;
		sc_iocmd.buffer = buf;
		sc_iocmd.data_length = 16;
		sc_iocmd.flags = B_READ;
		if (ioctl (tapefd, STIOCMD, &sc_iocmd) < 0) {
			serrno = errno;
			if (fd < 0) close (tapefd);
			return (-1);
		}
        }
#else
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x12;		/* inquiry */
	cdb[1] = 0x01;
	cdb[2] = 0x80;
	cdb[4] = 16;
	if ((rc = send_scsi_cmd (tapefd, path, 0, cdb, 6, buf, 16,
	    sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr)) < 0) {
#ifndef NOTRACE
		if (rc != -4 || nb_sense_ret < 14 ||
		    (sense[2] & 0xF) != 5 || sense[12] != 0x24 || sense[13] != 0)
#endif
			usrmsg (func, "%s", msgaddr);
#if defined(SOLARIS25) || defined(hpux)
                if (fd < 0) close (tapefd);
#endif
		RETURN (-1);
	}
#endif
	memcpy (inq_data, buf+4, buf[3]);
	inq_data[buf[3]] = '\0';
#if defined(_AIX) || defined(SOLARIS25) || defined(hpux)
        if (fd < 0) close (tapefd);
#endif
        RETURN (0);
#else
	*inq_data = '\0';
        return (0);
#endif
}
