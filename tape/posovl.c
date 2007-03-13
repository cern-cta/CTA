/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: posovl.c,v $ $Revision: 1.28 $ $Date: 2007/03/13 16:22:42 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
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
#include "Ctape_api.h"
#include "marshall.h"
#if SACCT
#include "sacct.h"
#endif
#include "serrno.h"
#include "tplogger_api.h"
char *devtype;
char *dvrname;
char errbuf[512];
char func[16];
gid_t gid;
char hostname[CA_MAXHOSTNAMELEN+1];
int jid;
char *path;
fd_set readmask;
int rpfd;
int tapefd;
uid_t uid;
int main(argc, argv)
int	argc;
char	**argv;
{
	char actual_hdr1[81];
	int blksize;
	unsigned char blockid[4];
	unsigned int blockid_tmp[4];
	int c;
	int cfseq;
	int den;
	struct devinfo *devinfo;
	char *dgn;
	char *domainname;
	char *drive;
	char drive_serial_no[13];
	char fid[CA_MAXFIDLEN+1];
	int filstat;
	int flags;
	int fsec;
	int fseq;
	char *fsid;
	char hdr1[LBLBUFSZ];
	char hdr2[LBLBUFSZ];
	int i;
	char inq_data[29];
	int lblcode;
	int lrecl;
	int method;
	int mode;
	int msglen;
	char *name;
	char *p;
	char *q;
	int Qfirst;
	int Qlast;
	char recfm[CA_MAXRECFMLEN+1];
	char repbuf[REPBUFSZ];
	int retentd;
	char *sbp;
	int scsi;
	char sendbuf[REQBUFSZ];
#if SONYRAW
	int sonyraw;
#endif
	char tpfid[CA_MAXFIDLEN+1];
	char uhl1[LBLBUFSZ];
	int ux;
	char *vid;
	char vol1[LBLBUFSZ];
	char *vsn;

	void cleanup();
	void positkilled();
        void configdown( char* );

	ENTRY (posovl);

        tl_init_handle( &tl_tpdaemon, "dlf" );
        tl_tpdaemon.tl_init( &tl_tpdaemon, 0 );

	drive = argv[1];
	vid = argv[2];
	rpfd = atoi(argv[3]);
	uid = atoi (argv[4]);
	gid = atoi (argv[5]);
	name = argv[6];
	jid = atoi (argv[7]);
	ux = atoi(argv[8]);
	dgn = argv[9];
	devtype = argv[10];
	dvrname = argv[11];
	mode = atoi (argv[12]);
	lblcode = atoi (argv[13]);
	vsn = argv[14];

	sscanf (argv[15], "%02x%02x%02x%02x", &blockid_tmp[0], &blockid_tmp[1],
	    &blockid_tmp[2], &blockid_tmp[3]);
	for (i = 0; i < 4; i++)
	  blockid[i] = blockid_tmp[i];

	cfseq = atoi (argv[16]);
	strcpy (fid, argv[17]);
	filstat = atoi (argv[18]);
	fsec = atoi (argv[19]);
	fseq = atoi (argv[20]);
	method = atoi (argv[21]);
	path = argv[22];
	Qfirst = atoi (argv[23]);
	Qlast = atoi (argv[24]);
	retentd = atoi (argv[25]);

	strcpy (recfm, argv[26]);
	blksize = atoi (argv[27]);
	lrecl = atoi (argv[28]);
	den = atoi (argv[29]);
	flags = atoi (argv[30]);
	fsid = argv[31];
	domainname = argv[32];
 
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
	(void) Ctape_seterrbuf (errbuf, sizeof(errbuf));
	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

	signal (SIGINT, positkilled);

	/* open device and check drive ready */

#if SONYRAW
	if (sonyraw) {
		tapefd = open (path, O_RDWR|O_NDELAY);
		if (tapefd < 0) {
			c = errno;
			if (errno == ENXIO)	/* drive not operational */
				configdown (drive);
			else {
				usrmsg (func, TP042, path, "open",
					strerror(errno));
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 3,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "path",    TL_MSG_PARAM_STR, path,
                                                    "Message", TL_MSG_PARAM_STR, "open" );
                        }
			goto reply;
		}
		if (chkdriveready_sony (tapefd) <= 0) {
			usrmsg (func, TP054);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 54, 1,
                                            "func", TL_MSG_PARAM_STR, func );
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
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 54, 1,
                                                    "func", TL_MSG_PARAM_STR, func );                        
				c = ETNRDY;
				goto reply;
			}
#endif
			c = errno;
			if (errno == ENXIO)	/* drive not operational */
				configdown (drive);
			else {
				usrmsg (func, TP042, path, "open",
					strerror(errno));
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 3,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "path",    TL_MSG_PARAM_STR, path,
                                                    "Message", TL_MSG_PARAM_STR, "open" );
                        }
			goto reply;
		}
		if (chkdriveready (tapefd) <= 0) {
			usrmsg (func, TP054);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 54, 1,
                                            "func", TL_MSG_PARAM_STR, func );                        
			c = ETNRDY;
			goto reply;
		}
		if (method == TPPOSIT_BLKID) {
			tplogit (func, "locating to blockid %02x%02x%02x%02x\n",
			    blockid[0], blockid[1], blockid[2], blockid[3]);
                        {
                                char msg[32];
                                sprintf( msg, "locating to blockid %02x%02x%02x%02x\n",
                                         blockid[0], blockid[1], blockid[2], blockid[3] );
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 6,
                                                    "func",       TL_MSG_PARAM_STR, func,
                                                    "Message",    TL_MSG_PARAM_STR, msg,
                                                    "Block ID 0", TL_MSG_PARAM_INT, blockid[0],
                                                    "Block ID 1", TL_MSG_PARAM_INT, blockid[1],
                                                    "Block ID 2", TL_MSG_PARAM_INT, blockid[2],
                                                    "Block ID 3", TL_MSG_PARAM_INT, blockid[3] );
                        }
			if ((c = locate (tapefd, path, blockid))) goto reply;
			flags |= LOCATE_DONE;
		}
		if ((c = posittape (tapefd, path, devtype, lblcode, mode,
		    &cfseq, fid, filstat, fsec, fseq, den, flags, Qfirst, Qlast,
		    vol1, hdr1, hdr2, uhl1)))
			goto reply;
		if (mode == WRITE_ENABLE)
			if ((c = read_pos (tapefd, path, blockid)))
				goto reply;
#if SONYRAW
	}
#endif

	/* tape is positionned */

#if SACCT
	tapeacct (TPPOSIT, uid, gid, jid, dgn, drive, vid, cfseq, 0);
#endif
	if (lblcode == AL || lblcode == AUL || lblcode == SL) {
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
				if (blksize == 0) {
					if (*uhl1) {
						sscanf (uhl1 + 14, "%10d", &blksize);
					} else {
						sscanf (hdr2 + 5, "%5d", &blksize);
                                        }
                                }
				if (lrecl == 0) {
					if (*uhl1) {
						sscanf (uhl1 + 24, "%10d", &lrecl);
					} else {
						sscanf (hdr2 + 10, "%5d", &lrecl);
                                        }
                                }
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
	if (blksize == 0) {
		if (strcmp (devtype, "SD3")) {
			if (lrecl == 0) {
				devinfo = Ctape_devinfo (devtype);
				blksize = devinfo->defblksize;
			} else {
				blksize = lrecl;
                        }
		} else {
			if (lrecl == 0) {
				blksize = 262144;
			} else if (strcmp (recfm, "F") == 0) {
				blksize = (262144 / lrecl) * lrecl;
				strcpy (recfm, "FB");
			} else {
				blksize = lrecl;
                        }
		}
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
		if (lblcode == AL || lblcode == AUL || lblcode == SL) {
			buildvollbl (vol1, vsn, lblcode, name);
			if (mode == WRITE_DISABLE || filstat == APPEND)
				for (i = 0; i < 80; i++)
					actual_hdr1[i] = hdr1[i] ? hdr1[i] : ' ';
			buildhdrlbl (hdr1, hdr2,
				fid, fsid, fsec, cfseq, retentd,
				recfm, blksize, lrecl, den, lblcode);
			if (mode == WRITE_ENABLE && filstat != APPEND)
				memcpy (actual_hdr1, hdr1, 80);
			vol1[80] = '\0';
			actual_hdr1[80] = '\0';
			hdr2[80] = '\0';
			if (lblcode == AUL) {
				inq_data[0] = '\0';
				(void) inquiry (tapefd, path, inq_data);
				drive_serial_no[0] = '\0';
				(void) inquiry80 (tapefd, path, drive_serial_no);
				builduhl (uhl1, cfseq, blksize, lrecl, domainname,
				    hostname, inq_data, drive_serial_no);
				uhl1[80] = '\0';
			} else
				uhl1[0] = '\0';
		} else {
			vol1[0] = '\0';
			actual_hdr1[0] = '\0';
			hdr2[0] = '\0';
			uhl1[0] = '\0';
		}
                marshall_STRING (sbp, vol1);
                marshall_STRING (sbp, actual_hdr1);
                marshall_STRING (sbp, hdr2);
		marshall_STRING (sbp, uhl1);
		sendrep (rpfd, MSG_DATA, sbp - repbuf, repbuf);
	} else {
		usrmsg (func, "%s", errbuf);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, errbuf );                        
        }
reply:
	if (c < 0) c = serrno;
	if (c) {
		cleanup();
	} else {
		close (tapefd);
	}
	sendrep (rpfd, TAPERC, c);
        tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );
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
        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                            "func",       TL_MSG_PARAM_STR, func,
                            "Message",    TL_MSG_PARAM_STR, "cleanup started" );
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
        
        /* called before each exit() */
        tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );
}

void configdown(drive)
char *drive;
{
	char msg[OPRMSGSZ];

	sprintf (msg, TP033, drive, hostname); /* ops msg */
	usrmsg ("posovl", "%s\n", msg);
        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                            "func",    TL_MSG_PARAM_STR, "posovl",
                            "Message", TL_MSG_PARAM_STR, msg );                        
	omsgr ("configdown", msg, 0);
	(void) Ctape_config (drive, CONF_DOWN, TPCD_SYS);
}

void positkilled()
{
	cleanup();
	exit (2);
}
