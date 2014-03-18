/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */


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
	{"Volume overflow", ENOSPC},
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

static int get_sk_msg(char *, int, int, int, const char **);

int gettperror(const int tapefd,
               const char *const path,
               const char **msgaddr)
{
#ifndef NOTRACE
	extern char *devtype;
#else
	char *devtype;
	struct devlblinfo  *dlip;
#endif
	int rc;
	int save_errno;

	save_errno = errno;
#ifdef NOTRACE
	if (getlabelinfo (path, &dlip) < 0) {
		devtype = NULL;
		errno = save_errno;
	} else {
		devtype = dlip->devtype;
	}
#endif
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

	errno = save_errno;
	{
		char func[sizeof("gettperror")];
		strncpy(func, "gettperror", sizeof(func));
		func[sizeof(func) - 1] = '\0';
		RETURN (rc);
	}
}


static int get_sk_msg(char *devtype,
               int key,
               int asc,
               int ascq,
               const char **msgaddr)
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
		rc = sk_codmsg[key].errcat;
	} else {
		sprintf (tp_err_msgbuf, "Undefined sense key %02X", key);
		*msgaddr = tp_err_msgbuf;
		rc = -1;
	}
	return (rc);
}

int rpttperror(const char *const func,
               const int tapefd,
               const char *const path,
               const char *const cmd)
{
	const char *msgaddr;
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
