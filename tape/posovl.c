/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: posovl.c,v $ $Revision: 1.17 $ $Date: 2000/03/31 15:07:57 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <pwd.h>
#include <signal.h>
#include <fcntl.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <string.h>
#include <sys/types.h>
#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>
#endif
#include "Ctape.h"
#include "marshall.h"
#if SACCT
#include "sacct.h"
#endif
#include "serrno.h"
#if !defined(linux)
extern char *sys_errlist[];
#endif
char *devtype;
char *dvrname;
char func[16];
gid_t gid;
char hostname[CA_MAXHOSTNAMELEN+1];
int jid;
char *path;
fd_set readmask;
int rpfd;
int tapefd;
uid_t uid;
main(argc, argv)
int	argc;
char	**argv;
{
	char actual_hdr1[81];
	unsigned char arg_blockid[9];
	int blksize;
	unsigned char blockid[4];
	int c;
	int cfseq;
	int den;
	char *dgn;
	char *drive;
	char fid[CA_MAXFIDLEN+1];
	int filstat;
	int flags;
	int fsec;
	int fseq;
	char hdr1[LBLBUFSZ];
	char hdr2[LBLBUFSZ];
	int i;
	int j;
	int lblcode;
	int lrecl;
	int method;
	int mode;
	int msglen;
	char name[CA_MAXUSRNAMELEN+1];
	char *p;
	struct passwd *pwd;
	char *q;
	int Qfirst;
	int Qlast;
	char recfm[CA_MAXRECFMLEN+1];
	char repbuf[REPBUFSZ];
	int retentd;
	char *sbp;
	int scsi;
	char sendbuf[REQBUFSZ];
	int sonyraw;
	char tpfid[CA_MAXFIDLEN+1];
	int ux;
	char *vid;
	char vol1[LBLBUFSZ];
	char *vsn;

	void cleanup();
	void positkilled();

	ENTRY (posovl);

	drive = argv[1];
	vid = argv[2];
	rpfd = atoi(argv[3]);
	uid = atoi (argv[4]);
	gid = atoi (argv[5]);
	jid = atoi (argv[6]);
	ux = atoi(argv[7]);
	dgn = argv[8];
	devtype = argv[9];
	dvrname = argv[10];
	mode = atoi (argv[11]);
	lblcode = atoi (argv[12]);
	vsn = argv[13];

	strcpy (arg_blockid, argv[14]);
	for (i = 0, j = 0; i < 4; i++)
		blockid[i] = arg_blockid[j++] << 4 | arg_blockid[j++];
	cfseq = atoi (argv[15]);
	strcpy (fid, argv[16]);
	filstat = atoi (argv[17]);
	fsec = atoi (argv[18]);
	fseq = atoi (argv[19]);
	method = atoi (argv[20]);
	path = argv[21];
	Qfirst = atoi (argv[22]);
	Qlast = atoi (argv[23]);
	retentd = atoi (argv[24]);

	strcpy (recfm, argv[25]);
	blksize = atoi (argv[26]);
	lrecl = atoi (argv[27]);
	den = atoi (argv[28]);
	flags = atoi (argv[29]);
 
#if _AIX
	scsi = strncmp (dvrname, "mtdd", 4);
#else
	scsi = 1;
#endif
#if SONYRAW
	if (strcmp (devtype, "DIR1") == 0 && den == SRAW)
		sonyraw = 1;
	else
		sonyraw = 0;
#endif

	c = 0;
	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

	signal (SIGINT, positkilled);

	pwd = getpwuid (uid);
	strcpy (name, pwd->pw_name);

	/* open device and check drive ready */

#if SONYRAW
	if (sonyraw) {
		tapefd = open (path, O_RDWR|O_NDELAY);
		if (tapefd < 0) {
			c = errno;
			if (errno == ENXIO)	/* drive not operational */
				configdown (drive);
			else
				usrmsg (func, TP042, path, "open",
					sys_errlist[errno]);
			goto reply;
		}
		if (chkdriveready_sony (tapefd) <= 0) {
			usrmsg (func, TP054);
			c = ETNRDY;
			goto reply;
		}
		if (fseq < 600) fseq = 7000;
		if (c = locate_sony (tapefd, path, fseq)) goto reply;
		cfseq = fseq;
	} else {
#endif
#ifndef SOLARIS
		if (!scsi)
#endif
			tapefd = open (path, O_RDONLY);
#ifndef SOLARIS
		else
			tapefd = open (path, O_RDONLY|O_NDELAY);
#endif
		if (tapefd < 0) {
#if defined(_AIX) || defined(SOLARIS)
#if _AIX
			if (!scsi && (errno == EIO || errno == ENOTREADY)) {
#else
			if (errno == EIO) {
#endif
				usrmsg (func, TP054);
				c = ETNRDY;
				goto reply;
			}
#endif
			c = errno;
			if (errno == ENXIO)	/* drive not operational */
				configdown (drive);
			else
				usrmsg (func, TP042, path, "open",
					sys_errlist[errno]);
			goto reply;
		}
		if (chkdriveready (tapefd) <= 0) {
			usrmsg (func, TP054);
			c = ETNRDY;
			goto reply;
		}
		if ((c = posittape (tapefd, path, devtype, lblcode, mode,
		    &cfseq, fid, filstat, fsec, fseq, den, flags, Qfirst, Qlast,
		    vol1, hdr1, hdr2)))
			goto reply;
#if SONYRAW
	}
#endif

	/* tape is positionned */

#if SACCT
	tapeacct (TPPOSIT, uid, gid, jid, dgn, drive, vid, cfseq, 0);
#endif
	if (lblcode == AL || lblcode == SL) {
		if (filstat != NEW_FILE) {	/* set defaults from label */
			if (fid[0] == '\0') {
				strncpy (tpfid, hdr1 + 4, 17);
				tpfid [17] = '\0';
				if ((p = strchr (tpfid, ' ')) != NULL) *p = '\0';
				strcpy (fid, tpfid);
			}
			if (hdr2[0]) {
				if (recfm[0] == '\0') {
					memset (recfm, 0, 4);
					recfm[0] = hdr2[4];
					if (lblcode == SL && hdr2[38] != ' ') {
						if (hdr2[38] == 'R')
							memcpy (recfm + 1, "BS", 2);
						else
							recfm[1] = hdr2[38];
					}
				}
				if (blksize == 0) sscanf (hdr2 + 5, "%5d", &blksize);
				if (lrecl == 0) sscanf (hdr2 + 10, "%5d", &lrecl);
			}
		} else {
			if (fid[0] == '\0') {
				p = strrchr (path, '/') + 1;
				if ((i = strlen (p) - 17) > 0) p += i;
				UPPER (p);
				strcpy (fid, p);
			}
		}
	}

	/* set default values to the fields which have not been set yet */

	if (recfm[0] == '\0')
		strcpy (recfm, "U");
	if (blksize == 0)
		if (strcmp (devtype, "SD3")) {
			if (lrecl == 0)
				blksize = 32760;
			else
				blksize = lrecl;
		} else {
			if (lrecl == 0)
				blksize = 262144;
			else if (strcmp (recfm, "F") == 0) {
				blksize = (262144 / lrecl) * lrecl;
				strcpy (recfm, "FB");
			} else
				blksize = lrecl;
		}
	if (lrecl == 0 && strcmp (recfm, "U")) lrecl = blksize;

	/* Build UPDFIL request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, UPDFIL);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build UPDFIL request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, ux);
	marshall_LONG (sbp, blksize);
	marshall_OPAQUE (sbp, blockid, 4);
	marshall_LONG (sbp, cfseq);
	marshall_STRING (sbp, fid);
	marshall_LONG (sbp, lrecl);
	marshall_STRING (sbp, recfm);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	if ((c = send2tpd (NULL, sendbuf, msglen, NULL, 0)) == 0) {
		sbp = repbuf;
		marshall_LONG (sbp, cfseq);
		if (lblcode == AL || lblcode == SL) {
			buildvollbl (vol1, vsn, lblcode, name);
			if (mode == WRITE_DISABLE || filstat == APPEND)
				for (i = 0; i < 80; i++)
					actual_hdr1[i] = hdr1[i] ? hdr1[i] : ' ';
			buildhdrlbl (hdr1, hdr2,
				fid, fsec, cfseq, retentd,
				recfm, blksize, lrecl, den, lblcode);
			if (mode == WRITE_ENABLE && filstat != APPEND)
				memcpy (actual_hdr1, hdr1, 80);
			vol1[80] = '\0';
			actual_hdr1[80] = '\0';
			hdr2[80] = '\0';
		} else {
			vol1[0] = '\0';
			actual_hdr1[0] = '\0';
			hdr2[0] = '\0';
		}
                marshall_STRING (sbp, vol1);
                marshall_STRING (sbp, actual_hdr1);
                marshall_STRING (sbp, hdr2);
		sendrep (rpfd, MSG_DATA, sbp - repbuf, repbuf);
	}
reply:
	if (c < 0) c = -c;
	if (c) {
		cleanup();
	} else {
		close (tapefd);
	}
	sendrep (rpfd, TAPERC, c);
	exit (0);
}

void cleanup()
{
	int flags;
	int msglen;
	char *q;
	char repbuf[1];
	char *sbp;
	char sendbuf[REQBUFSZ];

	tplogit (func, "cleanup started\n");
	if (tapefd >= 0)
		close (tapefd);

	/* must unload and deassign */

	flags = TPRLS_KEEP_RSV|TPRLS_UNLOAD|TPRLS_NOWAIT;
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, TPRLS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, flags);
	marshall_STRING (sbp, path);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	(void) send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
}

configdown(drive)
char *drive;
{
	char msg[OPRMSGSZ];

	sprintf (msg, TP033, drive, hostname); /* ops msg */
	usrmsg ("posovl", "%s\n", msg);
	omsgr ("configdown", msg, 0);
	(void) Ctape_config (drive, CONF_DOWN, TPCD_SYS);
}

void positkilled()
{
	cleanup();
	exit (2);
}
