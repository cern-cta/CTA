/*
 * Copyright (C) 1997-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: omsg.c,v $ $Revision: 1.5 $ $Date: 2007/03/12 16:36:05 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>
#endif
#if defined(linux)
#include <sys/time.h>
#include <unistd.h>
#endif
#include "Ctape.h"
#include "serrno.h"
#include "msg.h"
static int msg_num = 0;
static int mymsgindex = 0;
static int orfd;
static int orport = 0;
extern fd_set readmask;

void checkorep(func, orepbuf)
char *func;
char *orepbuf;
{
	int msgr_num;
	int n;
	int reply_type;

	if (rcvmsgr (orfd, &reply_type, &n, &msgr_num, orepbuf, OPRMSGSZ) < 0)
		tplogit (func, TP002, "rcvmsgr",
		    serrno ? sys_serrlist[SERRNO] : strerror(errno));
	if (reply_type != 0x1007)
		tplogit (func, "wrong reply type %x from msgdaemon\n", reply_type);
	msg_num = 0;
}

void omsgdel(func, msg_num)
char *func;
int msg_num;
{
	while (canmsgr (msg_num, orport) < 0) {
		tplogit (func, TP002, "canmsgr",
		    serrno ? sys_serrlist[SERRNO] : strerror(errno));
		sleep (SMSGI);
	}
	msg_num = 0;
}

int testorep(readfds)
fd_set *readfds;
{
	if (FD_ISSET (orfd, readfds)) {
		FD_CLR (orfd, readfds);
		return (1);
	}
	return (0);
}

int omsgr(func, msg, logmsg)
char *func;
char *msg;
int logmsg;
{
	int n;
	int reply_type;

	if (orport == 0) {
		if ((orfd = opnmsgr (&orport)) < 0) {
			tplogit (func, TP002, "opnmsgr",
			    serrno ? sys_serrlist[SERRNO] : strerror(errno));
			serrno = ETSYS;
			return (-1);
		}
		FD_SET (orfd, &readmask);
	}
	if (msg_num)	/* cancel pending operator message */
		omsgdel (func, msg_num);

	if (logmsg)
		tplogit (func, "%s\n", msg);
	mymsgindex++;
	while (sndmsgr (msg, orport, mymsgindex) < 0) {
		tplogit (func, TP002, "sndmsgr",
		    serrno ? sys_serrlist[SERRNO] : strerror(errno));
		sleep (SMSGI);
	}
	tplogit (func, "msg sent to msgdaemon\n");

	if (rcvmsgr (orfd, &reply_type, &n, &msg_num, NULL, 0) < 0)
		tplogit (func, TP002, "rcvmsgr",
		    serrno ? sys_serrlist[SERRNO] : strerror(errno));
	else
		if (reply_type != 0x2001)
			tplogit (func, "wrong reply type %x from msgdaemon\n", reply_type);
		else
			tplogit (func, "msgdaemon ack received: msg_num=%d\n", msg_num);

	return (msg_num);
}

