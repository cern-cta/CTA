/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rlstape.c,v $ $Revision: 1.1 $ $Date: 1999/11/03 16:30:55 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <fcntl.h>
#include <pwd.h>
#include <string.h>
#include <sys/time.h>
#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>
#endif
#include "Ctape.h"
#if SACCT
#include "sacct.h"
#endif
#if !defined(linux)
extern char *sys_errlist[];
#endif
char *dvrname;
char func[16];
char hostname[CA_MAXHOSTNAMELEN+1];
int jid;
int maxfds;
char msg[OPRMSGSZ];
char orepbuf[OPRMSGSZ];
fd_set readmask;
char repbuf[133];
int rpfd;

main(argc, argv)
int	argc;
char	**argv;
{
	char *acctname;
	int c;
	unsigned int demountforce;
	int den;
	char *devtype;
	char *dgn;
	char *drive;
	char *dvn;
	gid_t gid;
	int keeprsv;
	char *loader;
	int mode;
	int msglen;
	int n;
	char name[CA_MAXUSRNAMELEN+1];
	struct passwd *pwd;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	int sonyraw;
	int tapefd;
	uid_t uid;
	int ux;
	char *vid;

	ENTRY (rlstape);

	drive = argv[1];
	vid = argv[2];
	dvn = argv[3];
	rpfd = atoi (argv[4]);
	uid = atoi (argv[5]);
	gid = atoi (argv[6]);
	acctname = argv[7];
	jid = atoi (argv[8]);
	ux = atoi (argv[9]);
	keeprsv = atoi (argv[10]);
	dgn = argv[11];
	devtype = argv[12];
	dvrname = argv[13];
	loader = argv[14];
	mode = atoi (argv[15]);
	den = atoi (argv[16]);
#if SONYRAW
	if (strcmp (devtype, "DIR1") == 0 && den == SRAW)
		sonyraw = 1;
	else
		sonyraw = 0;
#endif

	if (*vid == '\0')	/* mount req failed or killed */
		goto reply;	/* before volume was requested on drive */
	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

        /* initialize for select */

#if defined(SOLARIS) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(sgi)
        maxfds = getdtablesize();
#else
        maxfds = _NFILE;
#endif
        FD_ZERO (&readmask);

	pwd = getpwuid (uid);
	strcpy (name, pwd->pw_name);

unload_loop:
#if SONYRAW
    if (! sonyraw) {
#endif
#if defined(ADSTAR)
	while ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) < 0 &&
	    (errno == EBUSY || errno == EAGAIN))
#else
#ifndef SOLARIS
	while ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) < 0 && errno == EBUSY)
#else
	while ((tapefd = open (dvn, O_RDONLY)) < 0 && errno == EBUSY)
#endif
#endif
		sleep (UCHECKI);
	if (tapefd >= 0) {
		if (strcmp (devtype, "3480") == 0 ||
		    strcmp (devtype, "9840") == 0 ||
		    strcmp (devtype, "SD3") == 0)
			lddisplay (tapefd, dvn, 0x20, "", "", 0);
		else if (strcmp (devtype, "3590") == 0)
			lddisplay (tapefd, dvn, 0x20, "", "", 1);
		else if (strstr (devtype, "/VB"))
			lddisplay (tapefd, dvn, 0x80, "", "", 2);
		if (chkdriveready (tapefd) > 0) {
			if (*loader != 'n')
				if (unldtape (tapefd, dvn) < 0)
					configdown (drive);
#if SACCT
			tapeacct (TPUNLOAD, uid, gid, jid, dgn, drive, vid, 0, 0);
#endif
		}
		close (tapefd);
	} else {
#if defined(sun)
		if (errno != EIO)
#endif
#if defined(_IBMR2)
		if (strcmp (dvrname, "tape") || (errno != EIO && errno != ENOTREADY))
#endif
			tplogit (func, TP042, dvn, "open", sys_errlist[errno]);
	}
#if SONYRAW
    } else {
	while ((tapefd = open (dvn, O_RDWR|O_NDELAY)) < 0 && errno == EBUSY)
		sleep (UCHECKI);
	if (tapefd >= 0) {
		if (chkunitready_sony (tapefd) > 0) {
			if (unldtape_sony (tapefd, dvn) < 0)
					configdown (drive);
#if SACCT
			tapeacct (TPUNLOAD, uid, gid, jid, dgn, drive, vid, 0, 0);
#endif
		}
		close (tapefd);
	} else
		tplogit (func, TP042, dvn, "open", sys_errlist[errno]);
    }
#endif
	c = 0;

	if (*loader != 'm') {
		demountforce = 0;
		do {
			c = rbtdemount (vid, drive, dvn, loader, demountforce);
			if ((n = rbtdmntchk (&c, drive, &demountforce)) < 0)
				goto reply;
		} while (n == 1);
		if (n == 2) goto unload_loop;
	}
reply:
#ifdef TMS
	c = sendtmsmount (mode, "CA", vid, jid, name, acctname, drive);
#endif

	/* Build FREEDRV request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, FREEDRV);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_WORD (sbp, uid);
	nmarshall_WORD (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, keeprsv);
	marshall_WORD (sbp, rpfd);
	marshall_WORD (sbp, ux);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */
 
	c = send2tpd (NULL, sendbuf, NULL, (int *)0);
	if (c < 0) c = -c;
	exit (c);
}

configdown(drive)
char *drive;
{
	sprintf (msg, TP033, drive, hostname); /* ops msg */
	omsgr ("configdown", msg, 0);
	(void) Ctape_config (drive, CONF_DOWN, TPCD_SYS);
}

rbtdmntchk(c, drive, demountforce)
int *c;
char *drive;
unsigned int *demountforce;
{
	fd_set readfds;
	struct timeval rbttimeval;

	switch (*c) {
	case 0:
		return (0);
	case RBT_FAST_RETRY:
		omsgr (func, msg, 0);
		rbttimeval.tv_sec = RBTFASTRI;
		rbttimeval.tv_usec = 0;
		memcpy (&readfds, &readmask, sizeof(readmask));
		if (select (maxfds, &readfds, (fd_set *)0,
		    (fd_set *)0, &rbttimeval) > 0 && testorep (&readfds)) {
			checkorep (func, orepbuf);
			if (strncmp (orepbuf, "cancel", 6) == 0) {
				configdown (drive);
				*c = EIO;
				return (-1);
			}
		}
		return (1);
	case RBT_DMNT_FORCE:
		if (*demountforce) {
			configdown (drive);
			*c = EIO;
			return (-1);
		} else {
			*demountforce = 1;
			return (1);	/* retry */
		}
	case RBT_CONF_DRV_DN:
		configdown (drive);
		*c = EIO;
		return (-1);
	case RBT_OMSG_SLOW_R:
	case RBT_OMSGR:
		omsgr (func, msg, 0);
		checkorep (func, orepbuf);
		if (strncmp (orepbuf, "cancel", 6) == 0) {
			configdown (drive);
			*c = EIO;
			return (-1);
		}
		return (1);	/* retry */
	case RBT_UNLD_DMNT:
		return (2);	/* should unload and retry */
	default:
		configdown (drive);
		return (-1);	/* unrecoverable error */
	}
}
