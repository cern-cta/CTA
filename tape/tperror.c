/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: tperror.c,v $ $Revision: 1.12 $ $Date: 2009/08/06 15:27:44 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*      gettperror - get drive status after I/O error and
			build error msg from sense key or sense bytes */
/*	return	ETBLANK		blank tape
 *		ETCOMPA		compatibility problem
 *		ETHWERR		device malfunction
 *		ETPARIT		parity error
 *		ETUNREC		unrecoverable media error
 *		ETNOSNS		no sense
 */
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

#include <linux/mtio.h> 
#ifndef MTIOCSENSE
#include "mtio_add.h"
#endif

static char bbot[] = "BOT hit";
struct sk_info {
	char *text;
	int errcat;
};
struct sk_info sk_codmsg[] = {
	{"No sense", ETNOSNS},
	{"Recovered error", 0},
	{"Not ready", 0},
	{"Medium error", ETPARIT},
	{"Hardware error", ETHWERR},
	{"Illegal request", ETHWERR},
	{"Unit attention", ETHWERR},
	{"Data protect", 0},
	{"Blank check", ETBLANK},
	{"Vendor unique", 0},
	{"Copy aborted", 0},
	{"Aborted command", 0},
	{"Equal", 0},
	{"Volume overflow", 0},
	{"Miscompare", 0},
	{"Reserved", 0},
	{"SCSI handshake failure", ETHWERR},
	{"Timeout", ETHWERR},
	{"EOF hit", 0},
	{"EOT hit", ETBLANK},
	{"Length error", ETCOMPA},
	{"BOT hit", ETUNREC},
	{"Wrong tape media", ETCOMPA}
};
int mt_rescnt;
#define TPERRMSGBUSZ 512
static char tp_err_msgbuf[TPERRMSGBUSZ];

int get_sk_msg(char *, int, int, int, char **);

int gettperror(tapefd, path, msgaddr)
int tapefd;
char *path;
char **msgaddr;
{
#ifndef NOTRACE
	extern char *devtype;
#else
	char *devtype;
	struct devlblinfo  *dlip;
#endif
	int rc;
	int save_errno;
        char func[] = "getperror";

	save_errno = errno;
#ifdef NOTRACE
	if (getlabelinfo (path, &dlip) < 0) {
		devtype = NULL;
		errno = save_errno;
	} else
		devtype = dlip->devtype;
#else
  (void)path;
#endif
        {
                /* Get the sense bytes */
                struct mtsense mt_sense;
                int ioctl_rc;
                
                ioctl_rc = ioctl(tapefd, MTIOCSENSE, &mt_sense);

                if ((ioctl_rc < 0) && (22 == errno)) {

                        /* MTIOCSENSE is not supported, probably the patched st driver is not loaded ... */
                        usrmsg(func, "%s", "MTIOCSENSE not supported\n");
                        
                        /* ... fall back to the old code. */
                        {
                                struct mtget mt_info;                
                                static char nosensekey[] = "no sense key available";
                                if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
                                        *msgaddr = nosensekey;
                                        rc = -1;
                                } else {
                                        if (((mt_info.mt_erreg >> 16) & 0xF) == 0 &&
                                            ((mt_info.mt_erreg >> 16) & 0x40)) {
                                                *msgaddr = bbot;
                                                rc = ETUNREC;
                                        } else rc = get_sk_msg (devtype, (mt_info.mt_erreg >> 16) & 0xF,
                                                                (mt_info.mt_erreg >> 8) & 0xFF,
                                                                (mt_info.mt_erreg) & 0xFF, msgaddr);
                                        mt_rescnt = mt_info.mt_resid;
                                }
                        }

                } else if (ioctl_rc < 0) {

                        /* MTIOCSENSE is supported, but failed */
                        snprintf(tp_err_msgbuf, TPERRMSGBUSZ, "no sense key available: MTIOCSENSE failed with %s", 
                                 strerror(errno));
                        *msgaddr = tp_err_msgbuf;
                        rc = -1;

                } else {

                        /* MTIOCSENSE succeeded */
                        if (mt_sense.latest) {
                                rc = get_sk_msg(devtype,
                                                mt_sense.data[2] & 0xF, /* SENSE KEY   */
                                                mt_sense.data[12],      /* ASC         */
                                                mt_sense.data[13],      /* ASCQ        */
                                                msgaddr);
                                mt_rescnt =
                                        mt_sense.data[3] << 24 |        /* INFORMATION */
                                        mt_sense.data[4] << 16 |
                                        mt_sense.data[5] << 8  |
                                        mt_sense.data[6];
                        } else {
                                usrmsg(func, "%s", "sense data do not belong to latest SCSI command: ignored\n");
                                rc = get_sk_msg(devtype, 0, 0, 0, msgaddr);
                                mt_rescnt = 0;
                        }
                }
        }

	errno = save_errno;
	RETURN (rc);
}


int get_sk_msg(devtype, key, asc, ascq, msgaddr)
char *devtype;
int key;
int asc;
int ascq;
char **msgaddr;
{
	int rc;
	if (key >= 0 && key < 15) {
		if (asc == 0 && ascq == 0) {
			*msgaddr = sk_codmsg[key].text;
		} else {
			sprintf (tp_err_msgbuf, "%s ASC=%X ASCQ=%X",
				sk_codmsg[key].text, asc, ascq);
			*msgaddr = tp_err_msgbuf;
		}
		if ((devtype && strcmp (devtype, "SD3") == 0 &&
		    key == 3 && asc == 0x30 && ascq == 0x01) ||
		    (devtype && strcmp (devtype, "3590") == 0 &&
		    key == 3 && asc == 0x30 && ascq == 0x02))
			rc = ETBLANK;
		else
			rc = sk_codmsg[key].errcat;
	} else {
		sprintf (tp_err_msgbuf, "Undefined sense key %02X", key);
		*msgaddr = tp_err_msgbuf;
		rc = -1;
	}
	return (rc);
}

int rpttperror(func, tapefd, path, cmd)
char *func;
int tapefd;
char *path;
char *cmd;
{
	char *msgaddr;
	int rc;

	if (errno == EIO) {
		if ((rc = gettperror (tapefd, path, &msgaddr)) <= 0) rc = EIO;
	} else {
		msgaddr = strerror(errno);
		rc = errno;
	}
	usrmsg (func, TP042, path, cmd, msgaddr);
	return (rc);
}
