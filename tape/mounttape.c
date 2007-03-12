/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: mounttape.c,v $ $Revision: 1.49 $ $Date: 2007/03/12 08:06:06 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <pwd.h>
#include <signal.h>
#include <fcntl.h>
#if linux
#include <sys/mtio.h>
#endif
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/param.h>
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
#if VDQM
#include "net.h"
#include "vdqm_api.h"
#endif
#if VMGR
#include "vmgr_api.h"
#endif
#include "tplogger_api.h"

char *acctname;
char *devtype;
char *drive;
char *dvrname;
char errbuf[512];
char func[16];
gid_t gid;
char hostname[CA_MAXHOSTNAMELEN+1];
int jid;
char *loader;
int maxfds;
int mode;
char msg[OPRMSGSZ];
int msg_num;
char *name;
char orepbuf[OPRMSGSZ];
char *path;
fd_set readmask;
int rpfd;
int tapefd;
uid_t uid;
int updvsn_done = -1;
char *vid;
main(argc, argv)
int	argc;
char	**argv;
{
	int c;
	char *clienthost;
	unsigned int demountforce;
	int den;
	char density [CA_MAXDENLEN+1];
	struct devinfo *devinfo;
	char *dgn;
	char *dvn;
	int get_reply;
	char hdr1[81];
	char hdr2[81];
	int i;
	static char labels[6][4] = {"", "al", "nl", "sl", "blp", "aul"};
	int lblcode;
#if DUXV4
	char *msgaddr;
#endif
        char msg1[9];
#if linux
	struct mtop mtop;
#endif
	int n;
	int needrbtmnt;
	char *p;
	int prelabel;
	struct passwd *pwd;
	fd_set readfds;
	char repbuf[REPBUFSZ];
	char rings[9];
	char *sbp;
	int scsi;
	int side;
	int sonyraw;
	int status;
	int Tflag = 0;
	struct timeval timeval;
	int tplbl;
	int tpmode;
	int tpmounted;
	char tpvsn[CA_MAXVSNLEN+1];
	int ux;
	int vbsyretry;
	int vdqm_rc;
	int vdqm_reqid;
	int vdqm_status;
	char *vsn;
	char vol1[LBLBUFSZ];
	int vsnretry = 0;
	char *why;
	int why4a;
    
	void cleanup();
	void mountkilled();

	ENTRY (mounttape);

        tl_init_handle( &tl_tpdaemon, "dlf" );
        tl_tpdaemon.tl_init( &tl_tpdaemon, 0 );

	drive = argv[1];
	vid = argv[2];
	dvn = argv[3];
	rpfd = atoi(argv[4]);
	uid = atoi (argv[5]);
	gid = atoi (argv[6]);
	name = argv[7];
	acctname = argv[8];
	jid = atoi (argv[9]);
	ux = atoi(argv[10]);
	dgn = argv[11];
	devtype = argv[12];
	dvrname = argv[13];
	loader = argv[14];
	mode = atoi (argv[15]);
	lblcode = atoi (argv[16]);
	vsn = argv[17];
	path = argv[18];
	den = atoi (argv[19]);
	prelabel = atoi (argv[20]);
	vdqm_reqid = atoi (argv[21]);
	tpmounted = atoi (argv[22]);
	side = atoi (argv[23]);
	clienthost = argv[24];

	if (prelabel > 2) {
		prelabel -= DOUBLETM;
		Tflag = 1;
	}
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
	devinfo = Ctape_devinfo (devtype);
	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

	/* initialize for select */

#if defined(SOLARIS) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(sgi)
	maxfds = getdtablesize();
#else
	maxfds = _NFILE;
#endif
	FD_ZERO (&readmask);
	FD_ZERO (&readfds);

	signal (SIGINT, mountkilled);

#if VDQM
	vdqm_status = VDQM_UNIT_ASSIGN;
	tplogit (func, "calling vdqm_UnitStatus(VDQM_UNIT_ASSIGN)\n");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                            "func",    TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "calling vdqm_UnitStatus(VDQM_UNIT_ASSIGN)" );
	while ((vdqm_rc = vdqm_UnitStatus (NULL, NULL, dgn, NULL, drive,
		&vdqm_status, &vdqm_reqid, jid)) &&
		(serrno == SECOMERR || serrno == EVQHOLD))
			sleep (60);
	tplogit (func, "vdqm_UnitStatus returned %s\n",
		vdqm_rc ? sstrerror(serrno) : "ok");
        tl_tpdaemon.tl_log( &tl_tpdaemon, vdqm_rc ? 103 : 111, 3,
                            "func",    TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "vdqm_UnitStatus returned",
                            "Error",   TL_MSG_PARAM_STR, vdqm_rc ? sstrerror(serrno) : "ok" );        
#endif

	updvsn_done = 0;
#ifdef TMS
	vbsyretry = 0;
	while ((c = sendtmsmount (mode, "PE", vid, jid, name, acctname, drive)) == ETVBSY) {
		n = sendtmsmount (mode, "CA", vid, jid, name, acctname, drive);
		if (vbsyretry++) break;
		sleep (10);
	}
	if (c)
		goto reply;
#endif
#if VMGR
	density[0] = '\0';
	if (c = vmgrchecki (vid, vsn, dgn, density, labels[lblcode], mode, uid, gid, clienthost))
		goto reply;
#endif

	if (tpmounted) {	/* tape already mounted */
#if SONYRAW
	    if (! sonyraw) {
#endif
#ifndef SOLARIS25
		if (!scsi)
#endif
			tapefd = open (path, O_RDONLY);
#ifndef SOLARIS25
		else
			tapefd = open (path, O_RDONLY|O_NDELAY);
#endif
		if (tapefd >= 0) {
			if (chkdriveready (tapefd) > 0) goto mounted;
			else close (tapefd);
		}
#if SONYRAW
	    } else {
		tapefd = open (path, O_RDWR|O_NDELAY);
		if (tapefd >= 0) {
			if (chkdriveready_sony (tapefd) > 0) goto mounted;
			else close (tapefd);
		}
	    }
#endif
	}

	/* build mount message */

	if (mode == WRITE_DISABLE)
		strcpy (rings, "read");
	else
		strcpy (rings, "write");
	why = "";
	why4a = 0;
	msg_num = 0;
reselect_loop:
	/* send vid, vsn and flag "to be mounted" to the tape daemon */

	if (c = Ctape_updvsn (uid, gid, jid, ux, vid, vsn, 1, lblcode, mode))
		goto reply;
	updvsn_done = 1;

#if SACCT
	tapeacct (TP2MOUNT, uid, gid, jid, dgn, drive, vid, 0, why4a);
#endif

	if (*loader != 'm')
		needrbtmnt = 1;

	/* build LED display message for 3480 compatible drives */

	strcpy (msg1, vid);
	if (mode == WRITE_DISABLE)
		strcat (msg1, " .");
	if (devinfo->lddtype == 0)		/* STK */
		lddisplay (-1, path, 0x40, msg1, "", 0);
	else if (devinfo->lddtype == 1)		/* IBM */
		lddisplay (-1, path, 0x40, msg1, "", 1);
	else if (strstr (devtype, "/VB"))	/* Vision Box */
		lddisplay (-1, path, 0x18, msg1, "", 2);

	while (1) {

		/* send mount message to operator and get message number */

		sprintf (msg, TP020, vid, labels[lblcode], rings, drive, hostname,
			name, jid, why);
		if ((n = omsgr (func, msg, 1)) < 0) {
			c = n;
			goto reply;
		} else
			msg_num = n;
		get_reply = 0;
remount_loop:
		if (*loader != 'm' && needrbtmnt) {
			do {
				c = rbtmount (vid, side, drive, dvn, mode, loader);
				if ((n = rbtmountchk (&c, drive, vid, dvn, loader)) < 0)
					goto reply;
			} while (n == 1);
			if (n == 2) {
				get_reply = 1;
				goto procorep;
			}
		}

		while (1) {	/* wait for drive ready or operator cancel */
#if defined(SOLARIS) || defined(_AIX)
#if _AIX
			if (scsi) {
				if ((tapefd = open (path, O_RDWR)) >= 0) {
					tpmode = WRITE_ENABLE;
					break;
				}
				if (errno == EWRPROTECT) {
					if ((tapefd = open (path, O_RDONLY)) >= 0) {
						tpmode = WRITE_DISABLE;
						break;
					}
				}
				if (errno != EIO && errno != ENOTREADY) c = errno;
			} else {
#else
				if ((tapefd = open (path, O_RDWR)) >= 0) {
					tpmode = WRITE_ENABLE;
					break;
				}
				if (errno == EACCES) {
					if ((tapefd = open (path, O_RDONLY)) >= 0) {
						tpmode = WRITE_DISABLE;
						break;
					}
				}
				if (errno != EIO) c = errno;
#endif
#endif
#ifndef SOLARIS
#if !defined(_IBMR2) || defined(RS6000PCTA)
#if SONYRAW
			    if (! sonyraw) {
#endif
				if ((tapefd = open (path, O_RDONLY|O_NDELAY)) >= 0) {
					if (chkdriveready (tapefd) > 0) {
						tpmode = chkwriteprot (tapefd);
						break;
					} else close (tapefd);
				} else c = errno;
#if SONYRAW
			    } else {
				if ((tapefd = open (path, O_RDWR|O_NDELAY)) >= 0) {
					if (chkdriveready_sony (tapefd) > 0) {
						tpmode = chkwriteprot_sony (tapefd);
						break;
					} else close (tapefd);
				} else c = errno;
			    }
#endif
#endif
#if defined(_AIX) && defined(_IBMR2)
			}
#endif
#endif
			if (c) {
				if (errno == ENXIO)	/* drive not operational */
					configdown (drive);
				else {
					usrmsg (func, TP042, path, "open",
						strerror(errno));
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "path",    TL_MSG_PARAM_STR, path,
                                                            "Message", TL_MSG_PARAM_STR, "open",
                                                            "Error",   TL_MSG_PARAM_STR, strerror(errno) );
	                                if (strcmp (devtype, "3592") == 0) {
#if VDQM
	                                  vdqm_status = VDQM_VOL_MOUNT;
	                                  tplogit (func, "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)\n");
                                          tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                                              "func",    TL_MSG_PARAM_STR, func,
                                                              "Message", TL_MSG_PARAM_STR, "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)" );
	                                  while ((vdqm_rc = vdqm_UnitStatus (NULL, vid, dgn, NULL, drive,
		                                  &vdqm_status, NULL, jid)) &&
		                                  (serrno == SECOMERR || serrno == EVQHOLD))
			                          sleep (60);
	                                          tplogit (func, "vdqm_UnitStatus returned %s\n",
		                                           vdqm_rc ? sstrerror(serrno) : "ok");
                                                  tl_tpdaemon.tl_log( &tl_tpdaemon, vdqm_rc ? 103 : 111, 3,
                                                                      "func",    TL_MSG_PARAM_STR, func,
                                                                      "Message", TL_MSG_PARAM_STR, "vdqm_UnitStatus returned",
                                                                      "Error",   TL_MSG_PARAM_STR, vdqm_rc ? sstrerror(serrno) : "ok" );        
#endif
                                        }
				}

				goto reply;
			}
			if (testorep (&readfds)) {
				get_reply = 1;
				needrbtmnt = 0;
				break;
			}
#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))
			if (*loader == 'l')
				c = qmid3495 (&status);
#endif
#if defined(CDK)
			if (*loader == 'a')
				c = acsmountresp();
#endif
#if defined(RS6000PCTA) || defined(ADSTAR) || defined(CDK)
			if (*loader == 'l' || *loader == 'a') {
				if ((n = rbtmountchk (&c, drive, vid, dvn, loader)) < 0)
					goto reply;
				if (n == 1) goto remount_loop;
				if (n == 2) {
					get_reply = 1;
					break;
				}
			}
#endif
			memcpy (&readfds, &readmask, sizeof(readmask));
			timeval.tv_sec = UCHECKI;	/* must set each time for linux */
			timeval.tv_usec = 0;
			if (select (maxfds, &readfds, (fd_set *)0, (fd_set *)0,
				&timeval) < 0) {
				FD_ZERO (&readfds);
			}
		}

procorep:
		if (get_reply) {
			checkorep (func, orepbuf);
			if (strlen (orepbuf) == 0) continue;
			if (strncmp (orepbuf, "cancel", 6) == 0) {
				usrmsg (func, TP023, orepbuf);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 23, 2,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "orepbuf", TL_MSG_PARAM_STR, orepbuf );
				c = ETOPAB;
				goto reply;
			} else if (strcmp (orepbuf, "reselect server") == 0) {
				usrmsg (func, TP047);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 47, 1,
                                                    "func",    TL_MSG_PARAM_STR, func );
				c = ETRSLT;
				goto reply;
			} else if ((int) strlen (orepbuf) < CA_MAXUNMLEN+1) {
				/* reselect ? */
				if (devinfo->lddtype == 0)
					lddisplay (-1, path, 0x20, "", "", 0);
				else if (devinfo->lddtype == 1)
					lddisplay (-1, path, 0x20, "", "", 1);
				else if (strstr (devtype, "/VB"))
					lddisplay (-1, path, 0x80, "", "", 2);
unload_loop1:
#ifndef SOLARIS
				if ((tapefd = open (path, O_RDONLY|O_NDELAY)) >= 0) {
#else
				if ((tapefd = open (path, O_RDONLY)) >= 0) {
#endif
					if (chkdriveready (tapefd) > 0) {
						if (*loader != 'n')
							if (n = unldtape (tapefd, path)) {
								c = n;
								goto reply;
							}
#if SACCT
						tapeacct (TPUNLOAD, uid, gid, jid,
						    dgn, drive, vid, 0, TPU_RSLT);
#endif
					}
					close (tapefd);
				}
				if (*loader != 'm') {
					demountforce = 0;
					do {
						c = rbtdemount (vid, drive, dvn,
							loader, demountforce);
						if ((n = rbtdmntchk (&c, drive, &demountforce)) < 0)
							goto reply;
					} while (n == 1);
					if (n == 2)	/* tape has become ready */
						goto unload_loop1;
				}
				if (strcmp (drive, orepbuf) == 0) { /* same drive */
					why = "reselect same";
#if SACCT
					why4a = TPM_RSLT;
#endif
					goto reselect_loop;
				}
				if (Ctape_rslt (uid, gid, jid, drive, orepbuf,
				    &ux, loader, dvn)) {
					why = "reselect failed";
				} else {
#if TMS
					if (c = sendtmsmount (mode, "PE", vid,
					    jid, name, acctname, drive))
						goto reply;
#endif
					why = "reselect";
				}
#if SACCT
				why4a = TPM_RSLT;
#endif
				goto reselect_loop;
			} else continue;	/* bad reply */
		}

		/* tape is ready */

		if (devinfo->lddtype == 0)
			lddisplay (tapefd, path, 0x20, msg1, "", 0);
		else if (devinfo->lddtype == 1)
			lddisplay (tapefd, path, 0x20, msg1, "", 1);
		else if (strstr (devtype, "/VB"))
			lddisplay (tapefd, path, 0x80, msg1, "", 2);

		omsgdel (func, msg_num);

		/* check if the volume is write protected */

		if (tpmode != mode && tpmode == WRITE_DISABLE && *loader != 'm') {
			sprintf (msg, TP041, "mount", vid, drive, "write protected");
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, msg,
                                            "Command", TL_MSG_PARAM_STR, "mount",
                                            "VID",     TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, drive );
			omsgr (func, msg, 0);
			c = ETWPROT;
			goto reply;
		}
		if (tpmode != mode && *loader == 'm') {
			if (c = unldtape (tapefd, path))
				goto reply;
			if (devinfo->lddtype == 0)
				lddisplay (tapefd, path, 0x50, msg1, "wrng rng", 0);
			else if (devinfo->lddtype == 1)
				lddisplay (tapefd, path, 0x50, msg1, "wrng rng", 1);
			else if (strstr (devtype, "/VB"))
				lddisplay (tapefd, path, 0x1A, msg1, "wrong ring", 2);
			close (tapefd);
			why = "wrong ring status";
#if SACCT
			why4a = TPU_WNGR;
			tapeacct (TPUNLOAD, uid, gid, jid, dgn, drive, vid, 0, TPU_WNGR);
#endif
			continue;
		}
#if defined(_AIX) && defined(RS6000PCTA)
		if (!scsi) {
			close (tapefd);         /* to avoid errno 22 on read */
			if ((tapefd = open (path, O_RDONLY)) < 0) {
				usrmsg (func, TP042, path, "reopen", strerror(errno));
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "path",    TL_MSG_PARAM_STR, path,
                                                    "Message", TL_MSG_PARAM_STR, "reopen",
                                                    "Error",   TL_MSG_PARAM_STR, strerror(errno) );
				c = errno;
				goto reply;
			}
		}
#endif
#if SONYRAW
		if (sonyraw) break;
#endif
#if linux
		mtop.mt_op = MTSETBLK;
		mtop.mt_count = 0;
		ioctl (tapefd, MTIOCTOP, &mtop);	/* set variable block size */
#endif
#if DUXV4
		(void) gettperror (tapefd, path, &msgaddr);	/* clear eei status */
#endif

		/* position tape at BOT */

		if ((c = rwndtape (tapefd, path))) goto reply;

		/* set density and compression mode */

#if defined(ADSTAR)
#endif
#if linux
		(void) setdens (tapefd, path, devtype, den);
#endif

		/* check VOL1 label if not blp */

		if (lblcode == BLP) break;

		if ((c = readlbl (tapefd, path, vol1)) < 0) goto reply;
		if (prelabel >= 0 && c == 3) break;	/* tape is new. ok to prelabel */
		if (prelabel >= 0 && c == 2 && readlbl (tapefd, path, vol1) > 1)
			break;	/* tape has only a single or a double tapemark */
		if (c) {	/* record read is not 80 bytes long */
			c = 0;
			tplbl = NL;
		} else if (strncmp (vol1, "VOL1", 4) == 0) tplbl = AL;
		else {
			ebc2asc (vol1, 80);
			if (strncmp (vol1, "VOL1", 4) == 0) tplbl = SL;
			else tplbl = NL;
		}
		if (tplbl != NL) {
			strncpy (tpvsn, vol1 + 4, 6);
			tpvsn[6] = '\0';
			if ((p = strchr (tpvsn, ' ')) != NULL) *p = '\0';
		}
		if (prelabel >= 0) {
			if (tplbl == NL)
				sprintf (msg, TP062, vid, "is an NL tape", "");
			else
				sprintf (msg, TP062, vid, "has vsn ", tpvsn);
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 62, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, msg,
                                            "VID",     TL_MSG_PARAM_STR, vid,
                                            "VSN",     TL_MSG_PARAM_STR, tpvsn );
			strcat (msg, ", ok to continue?");
			omsgr (func, msg, 0);
			checkorep (func, orepbuf);
			if (strcmp (orepbuf, "ok") && strcmp (orepbuf, "yes")) {
				usrmsg (func, TP023, orepbuf);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 23, 2,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "orepbuf", TL_MSG_PARAM_STR, orepbuf );
				c = ETOPAB;
				goto reply;
			}
			break;	/* operator gave ok */
		}
		if (tplbl == NL && lblcode == NL) break;
		if (tplbl != NL) {
			tplogit (func, "vol1 = %s\n", vol1);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                            "func", TL_MSG_PARAM_STR, func,
                                            "vol1", TL_MSG_PARAM_STR, vol1 );
                }
		if (tplbl == AL && lblcode == AUL) tplbl = AUL;
		if (lblcode != tplbl && vsnretry) {	/* wrong label type */
			usrmsg (func, TP021, labels[lblcode], labels[tplbl]);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 21, 3,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Request", TL_MSG_PARAM_STR, labels[lblcode],
                                            "Tape",    TL_MSG_PARAM_STR, labels[tplbl] );			
			c = ETWLBL;
			goto reply;
		}
		if (tplbl != NL) {
			if (lblcode == tplbl && strcmp (tpvsn, vsn) == 0) break;
		}
		if (*loader != 'm' && vsnretry++) {
			usrmsg (func, TP039, vsn, tpvsn);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 39, 3,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Request", TL_MSG_PARAM_STR, vsn,
                                            "Tape",    TL_MSG_PARAM_STR, tpvsn );			
			c = ETWVSN;
			goto reply;
		}
		if (*loader != 'n')
			if (n = unldtape (tapefd, path)) {
				c = n;
				goto reply;
			}
		if (devinfo->lddtype == 0)
			lddisplay (tapefd, path, 0x50, msg1, "wrng vsn", 0);
		else if (devinfo->lddtype == 1)
			lddisplay (tapefd, path, 0x50, msg1, "wrng vsn", 1);
		else if (strstr (devtype, "/VB"))
			lddisplay (tapefd, path, 0x1A, msg1, "wrong vsn", 2);
		close (tapefd);
		why = "wrong vsn";
#if SACCT
		why4a = TPM_WNGV;
		tapeacct (TPUNLOAD, uid, gid, jid, dgn, drive, vid, 0, TPU_WNGV);
#endif
		if (*loader == 'm')
			continue;
		demountforce = 1;
		do {
            vsnretry++;  
			c = rbtdemount (vid, drive, dvn, loader, demountforce, vsnretry);
			if ((n = rbtdmntchk (&c, drive, &demountforce)) < 0)
				goto reply;
		} while (n == 1);
		if (n == 2)	/* tape not unloaded */
			goto reply;
		needrbtmnt = 1;
		continue;
	}

	/* tape is mounted */


    post_mount_check(tapefd, path, devtype);
    if (strcmp (devtype, "9840") == 0 ||
        strcmp (devtype, "9940") == 0 ||
        strcmp (devtype, "994B") == 0 || 
        strcmp (devtype, "T10000") == 0 ||
        strcmp (devtype, "LTO") == 0 ||
        strcmp (devtype, "3592") == 0) {
        
        /* BC Now checking the MIR */
        if (is_mir_invalid_load(tapefd, path, devtype) == 1) {
            char mirmsg[OPRMSGSZ];
            char mirrepbuf[OPRMSGSZ];
            
            sprintf (mirmsg, TP065, vid, labels[lblcode], drive, hostname,
                     name, jid);
            omsgr(func, mirmsg, 1);
            checkorep (func, mirrepbuf);
            if (strncmp (mirrepbuf, "cancel", 6) == 0) {
                tplogit(func, "Request cancelled by operator due to bad MIR\n");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "Request cancelled by operator due to bad MIR" );
                cleanup();
                sendrep (rpfd, TAPERC, ETBADMIR);
                exit(0);
            }
                
        } else {
            tplogit(func, "MIR is valid\n");
            tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                "func",    TL_MSG_PARAM_STR, func,
                                "Message", TL_MSG_PARAM_STR, "MIR is valid" );
        }
    }

#if SACCT
	tapeacct (TPMOUNTED, uid, gid, jid, dgn, drive, vid, 0, 0);
#endif
	if (c = Ctape_updvsn (uid, gid, jid, ux, vid, vsn, 0, lblcode, mode))
		goto reply;
#if VMGR
	(void) vmgr_seterrbuf (errbuf, sizeof(errbuf));
	errbuf[0] = '\0';
	if (vmgr_tpmounted (vid, mode, jid) && serrno != ENOENT) {
		if (*errbuf) {
			usrmsg (func, "%s", errbuf);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, errbuf );                       
                }
		usrmsg (func, "vmgr_tpmounted returned %s\n", sstrerror (serrno));
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "vmgr_tpmounted returned",
                                    "Error",   TL_MSG_PARAM_STR, sstrerror (serrno) );
		c = -1;
		goto reply;
	}
#endif
mounted:
#ifdef TMS
	if (c = sendtmsmount (mode, "CO", vid, jid, name, acctname, drive))
		goto reply;
#endif
#if VDQM
	vdqm_status = VDQM_VOL_MOUNT;
	tplogit (func, "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)\n");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                            "func",    TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)" );
	while ((vdqm_rc = vdqm_UnitStatus (NULL, vid, dgn, NULL, drive,
		&vdqm_status, NULL, jid)) &&
		(serrno == SECOMERR || serrno == EVQHOLD))
			sleep (60);
	tplogit (func, "vdqm_UnitStatus returned %s\n",
		vdqm_rc ? sstrerror(serrno) : "ok");
        tl_tpdaemon.tl_log( &tl_tpdaemon, vdqm_rc ? 103 : 111, 3,
                            "func",    TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "vdqm_UnitStatus returned",
                            "Error",   TL_MSG_PARAM_STR, vdqm_rc ? sstrerror(serrno) : "ok" );        
#endif

	/* do the prelabel if flag is set */

	if (prelabel >= 0) {
#if SONYRAW
	    if (! sonyraw) {
#endif
		if ((c = rwndtape (tapefd, path))) goto reply;
		close (tapefd);
		if ((tapefd = open (path, O_WRONLY)) < 0) {
			usrmsg (func, TP042, path, "reopen", strerror(errno));
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "path",    TL_MSG_PARAM_STR, path,
                                            "Message", TL_MSG_PARAM_STR, "reopen",
                                            "Error",   TL_MSG_PARAM_STR, strerror(errno) );
			c = errno;
			goto reply;
		}
		if (lblcode != NL) {
			buildvollbl (vol1, vsn, lblcode, name);
			if (lblcode == SL) asc2ebc (vol1, 80);
			if ((c = writelbl (tapefd, path, vol1)) < 0) goto reply;
			if (prelabel > 0) {
				int blksize;
				if (strcmp (devtype, "SD3"))
					blksize = 32760;
				else
					blksize = 262144;
				buildhdrlbl(hdr1, hdr2,
					"PRELABEL", vsn, 1, 1, 0,
					"U", blksize, 0, den, lblcode);
				if (lblcode == SL) asc2ebc (hdr1, 80);
				if ((c = writelbl (tapefd, path, hdr1)) < 0)
					goto reply;
				if (prelabel > 1) {
					if (lblcode == SL) asc2ebc (hdr2, 80);
					if ((c = writelbl (tapefd, path, hdr2)) < 0)
						goto reply;
				}
			}
		}
		if  ((c = wrttpmrk (tapefd, path, 1)) < 0) goto reply;
		if (devinfo->eoitpmrks == 2 || Tflag)
			if  ((c = wrttpmrk (tapefd, path, 1)) < 0) goto reply;
		if (strcmp (devtype, "SD3") == 0 && ! Tflag)	/* flush buffer */
			if  ((c = wrttpmrk (tapefd, path, 0)) < 0) goto reply;
#if SONYRAW
	    } else {
		if (c = erase_sony (tapefd, path)) goto reply;
	    }
#endif
		goto reply;
	}

	if (c == 0) {
		sbp = repbuf;
		marshall_STRING (sbp, devtype);
		marshall_WORD (sbp, den);
		marshall_WORD (sbp, lblcode);
		sendrep (rpfd, MSG_DATA, sbp - repbuf, repbuf);
	}
reply:
	if (c < 0) c = serrno;
	if (c) {
		cleanup();
	} else {
		close (tapefd);
#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))
                if (*loader == 'l')
                        closelmcp ();
#endif
#if defined(CDK)
		if (*loader == 'a')
			wait4acsfinalresp();
#endif
#if defined(DMSCAPI)
		if (*loader == 'R')
			closefbs (loader);
#endif
		if (*loader == 's')
			closesmc ();
	}
	sendrep (rpfd, TAPERC, c);

        tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );        
	exit (0);
}

Ctape_rslt(uid, gid, jid, olddrive, newdrive, ux, loader, dvn)
uid_t uid;
gid_t gid;
int jid;
char *olddrive;
char *newdrive;
int *ux;
char *loader;
char *dvn;
{
	int c;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[REPBUFSZ];
	char *sbp;
	char sendbuf[REQBUFSZ];

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, RSLT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_STRING (sbp, olddrive);
	marshall_STRING (sbp, newdrive);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */
 
	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c == 0) {
		rbp = repbuf;
		unmarshall_WORD (rbp, *ux);
		unmarshall_STRING (rbp, loader);
		unmarshall_STRING (rbp, dvn);
	} else {
		usrmsg (func, "%s", errbuf);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, errbuf );
        }
	return (c);
}

Ctape_updvsn(uid, gid, jid, ux, vid, vsn, tobemounted, lblcode, mode)
uid_t uid;
gid_t gid;
int jid;
int ux;
char *vid;
char *vsn;
int tobemounted;
int lblcode;
int mode;
{
	int c;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, UPDVSN);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, ux);
	marshall_STRING (sbp, vid);
	marshall_STRING (sbp, vsn);
	marshall_WORD (sbp, tobemounted);
	marshall_WORD (sbp, lblcode);
	marshall_WORD (sbp, mode);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */
 
	c = send2tpd (NULL, sendbuf, msglen, NULL, 0);
	if (c < 0) {
		usrmsg (func, "%s", errbuf);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, errbuf );
        }
	return (c);
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
        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                            "func",    TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "cleanup started" );
#ifdef TMS
	if (updvsn_done == 0)
		(void) sendtmsmount (mode, "CA", vid, jid, name, acctname, drive);
#endif
	if (tapefd >= 0)
		close (tapefd);
#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))
	if (*loader == 'l')
		closelmcp ();
#endif
#if defined(CDK)
	if (*loader == 'a')
		wait4acsfinalresp();
#endif
#if defined(DMSCAPI)
	if (*loader == 'R')
		closefbs();
#endif
	if (*loader == 's')
		closesmc ();

	if (msg_num)	/* cancel pending operator message */
		omsgdel (func, msg_num);

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

configdown(drive)
char *drive;
{
	sprintf (msg, TP033, drive, hostname); /* ops msg */
	usrmsg ("mounttape", "%s\n", msg);
        tl_tpdaemon.tl_log( &tl_tpdaemon, 33, 4,
                            "func",     TL_MSG_PARAM_STR, "mounttape",
                            "Message",  TL_MSG_PARAM_STR, msg,
                            "Drive",    TL_MSG_PARAM_STR, drive, 
                            "Hostname", TL_MSG_PARAM_STR, hostname );
	omsgr ("configdown", msg, 0);
	(void) Ctape_config (drive, CONF_DOWN, TPCD_SYS);
}

void mountkilled()
{
	cleanup();
	exit (2);
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
	case RBT_NORETRY:
	case RBT_SLOW_RETRY:
		*c = EIO;
		return (-1);
	case RBT_FAST_RETRY:
		omsgr (func, msg, 0);
		rbttimeval.tv_sec = RBTFASTRI;
		rbttimeval.tv_usec = 0;
		memcpy (&readfds, &readmask, sizeof(readmask));
		if (select (maxfds, &readfds, (fd_set *)0,
		    (fd_set *)0, &rbttimeval) > 0 && testorep (&readfds)) {
			checkorep (func, orepbuf);
			if (strncmp (orepbuf, "cancel", 6) == 0) {
				*c = EIO;
				return (-1);
			}
		}
		return (1);	/* retry */
	case RBT_DMNT_FORCE:
		if (*demountforce) {
			*c = EIO;
			return (-1);
		} else {
			*demountforce = 1;
			return (1);
		}
	case RBT_CONF_DRV_DN:
		configdown (drive);
		*c = EIO;
		return (-1);
	case RBT_OMSG_NORTRY:
		omsgr (func, msg, 0);
		*c = ETABSENT;
		return (-1);
	case RBT_OMSG_SLOW_R:
	case RBT_OMSGR:
		omsgr (func, msg, 0);
		checkorep (func, orepbuf);
		if (strncmp (orepbuf, "cancel", 6) == 0) {
			*c = EIO;
			return (-1);
		}
		return (1);	/* retry */
	case RBT_UNLD_DMNT:
		*c = EIO;
		return (2);	/* may be unload and retry */
	default:
		return (-1);	/* unrecoverable error */
	}
}

rbtmountchk(c, drive, vid, dvn, loader)
int *c;
char *drive;
char *vid;
char *dvn;
char *loader;
{
	unsigned int demountforce;
	int n;
	fd_set readfds;
	struct timeval rbttimeval;
	int tapefd;
    int vsnretry=0;
    
	switch (*c) {
	case 0:
		return (0);
	case RBT_NORETRY:
		*c = EIO;
		return (-1);
	case RBT_SLOW_RETRY:
		*c = ETVBSY;	/* volume in use (ifndef TMS) */
		return (-1);
	case RBT_FAST_RETRY:
		rbttimeval.tv_sec = RBTFASTRI;
		rbttimeval.tv_usec = 0;
		memcpy (&readfds, &readmask, sizeof(readmask));
		if (select (maxfds, &readfds, (fd_set *)0,
		    (fd_set *)0, &rbttimeval) > 0 && testorep (&readfds))
			return (2);	/* get & process operator reply */
		else
			return (1);	/* retry */
	case RBT_DMNT_FORCE:
		if ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) < 0) {
			*c = errno;
			return (-1);
		}
		if (chkdriveready (tapefd) > 0) {
			if ((*c = unldtape (tapefd, dvn)) < 0) {
				close (tapefd);
				return (-1);
			}
		}
		close (tapefd);
		demountforce = 1;
		do {
             vsnretry++;
			*c = rbtdemount (vid, drive, dvn, loader, demountforce, vsnretry);
			if ((n = rbtdmntchk (c, drive, &demountforce)) < 0)
				return (-1);
		} while (n == 1);
		if (n == 2)	/* tape not unloaded */
			return (-1);
		return (1);	/* retry */
	case RBT_CONF_DRV_DN:
		configdown (drive);
		*c = EIO;
		return (-1);
	case RBT_OMSG_NORTRY:
		omsgr (func, msg, 0);
		*c = ETABSENT;	/* volume not in library */
		return (-1);
	case RBT_OMSG_SLOW_R:
		omsgr (func, msg, 0);
		*c = ETVBSY;	/* volume in use or inaccessible */
		return (-1);
	case RBT_OMSGR:
		omsgr (func, msg, 0);
		return (2);	/* get & process operator reply */
	default:
		return (-1);	/* unrecoverable error */
	}
}


