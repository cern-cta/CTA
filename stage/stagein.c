/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stagein.c,v $ $Revision: 1.5 $ $Date: 1999/12/08 15:57:34 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#define W_OK 2
#else
#if ! defined(vms)
#include <unistd.h>
#else
#include <unixio.h>
#endif
#endif
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#define RFIO_KERNEL 1
#include "rfio.h"
#include "stage.h"
extern	char	*getenv();
extern	char	*getconfent();
extern	int	optind;
extern	char	*optarg;
#if !defined(linux)
extern	char	*sys_errlist[];
#endif
static gid_t gid;
static int pid;
static struct passwd *pw;
char *stghost;
char user[15];	/* login name */

main(argc, argv)
int	argc;
char	**argv;
{
	int c, i;
	void cleanup();
	static char den[6] = "";
	int dflag = 0;
	static char dgn[9] = "";
	char *dp;
	int errflg = 0;
	int fun = 0;
	int Gflag = 0;
	char Gname[15];
	uid_t Guid;
	int gflag = 0;
	struct group *gr;
	char *hsm_host;
	char hsm_path[MAXHOSTNAMELEN + MAXPATH];
	static char lbl[4] = "";
	int lflag = 0;
#if (defined(sun) && !defined(SOLARIS)) || defined(ultrix) || defined(vms) || defined(_WIN32)
	int mask;
#else
	mode_t mask;
#endif
	int msglen;
	int nargs;
	int ntries = 0;
	int numvid, numvsn;
	char *p, *q;
	char path[MAXHOSTNAMELEN + MAXPATH];
	int pflag = 0;
	char *pool_user = NULL;
	char *poolname = NULL;
	int req_type;
	char *sbp;
	char sendbuf[REQBUFSZ];
	int stagedisk = 0;
	int stagemig = 0;
	int stagetape = 0;
	int uflag = 0;
	uid_t uid;
	char vid[MAXVSN][7];
	char vsn[MAXVSN][7];
	int vflag = 0;
	char xvsn[7*MAXVSN];
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	nargs = argc;
#if !defined(vms)
#if defined(_WIN32)
	if (p = strrchr (argv[0], '\\'))
#else
	if (p = strrchr (argv[0], '/'))
#endif
#else
	if (p = strrchr (argv[0], ']'))
#endif
		p++;
	else
		p = argv[0];
	if (strncmp (p, "stagein", 7) == 0) req_type = STAGEIN;
	else if (strncmp (p, "stageout", 8) == 0) req_type = STAGEOUT;
	else if (strncmp (p, "stagewrt", 8) == 0) req_type = STAGEWRT;
	else if (strncmp (p, "stagecat", 8) == 0) req_type = STAGECAT;
	else {
		fprintf (stderr, STG34);
		errflg++;
	}

	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	numvid = 0;
	numvsn = 0;
	memset (vsn, 0, sizeof(vsn));
	while ((c = getopt (argc, argv, "A:b:C:c:d:E:F:f:Gg:h:I:KL:l:M:N:nop:q:S:s:Tt:U:u:V:v:X:z")) != EOF) {
		switch (c) {
		case 'd':
			stagetape++;
			dflag++;
			if (strlen (optarg) > sizeof(den) - 1) {
				fprintf (stderr, STG06, "-d\n");
				errflg++;
			} else
				strcpy (den, optarg);
			break;
		case 'E':
			stagetape++;
			break;
		case 'f':
			stagetape++;
			break;
		case 'G':
			Gflag++;
			if ((gr = getgrgid (gid)) == NULL) {
				fprintf (stderr, STG36, gid);
				exit (SYERR);
			}
#if !defined(vms)
			if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) {
				fprintf (stderr, STG10, gr->gr_name);
				errflg++;
			} else {
				strcpy (Gname, p);
				if ((pw = getpwnam (p)) == NULL) {
					fprintf (stderr, STG11, p);
					errflg++;
				} else
					Guid = pw->pw_uid;
			}
#else
			if ((q = getconfent ("GRPUSER", gr->gr_name, 1)) == NULL) {
				fprintf (stderr, STG10, gr->gr_name);
				errflg++;
			} else {
				if ((p = strtok (q, " \n")) == NULL) {
					fprintf (stderr, STG80, q);
					errflg++;
				}
				(void) strcpy (Gname,p);
				if ((p = strtok (NULL, " \n")) == NULL) {
					fprintf (stderr, STG81, q);
					errflg++;
				}
				Guid = atoi (p);
				if ((p = strtok (NULL, " \n")) == NULL) {
					fprintf (stderr, STG82, q);
					errflg++;
				}
				gid = atoi (p);
			}
#endif
			break;
		case 'g':
			stagetape++;
			gflag++;
			if (strlen (optarg) > sizeof(dgn) - 1) {
				fprintf (stderr, STG06, "-g\n");
				errflg++;
			} else
				strcpy (dgn, optarg);
			break;
		case 'h':
			stghost = optarg;
			break;
		case 'I':
			stagedisk++;
			break;
		case 'l':
			stagetape++;
			lflag++;
			if (strlen (optarg) > sizeof(lbl) - 1) {
				fprintf (stderr, STG06, "-l\n");
				errflg++;
			} else
				strcpy (lbl, optarg);
			break;
		case 'M':
			stagemig = 1;
			if (strchr(optarg,':') == NULL) {
				if ((hsm_host = getenv("HSM_HOST")) != NULL) {
					strcpy (hsm_path, hsm_host);
					strcat (hsm_path, ":");
					strcat (hsm_path, optarg);
					argv[optind - 1] = hsm_path;
				} else if ((hsm_host = getconfent("STG", "HSM_HOST",0)) != NULL) {
					strcpy (hsm_path, hsm_host);
					strcat (hsm_path, ":");
					strcat (hsm_path, optarg);
					argv[optind - 1] = hsm_path;
				} else {
					fprintf (stderr, STG54);
					errflg++;
				}
			}
			break;
		case 'n':
			stagetape++;
			break;
		case 'o':
			stagetape++;
			break;
		case 'p':
			poolname = optarg;
			pflag++;
			break;
		case 'q':
			stagetape++;
			break;
		case 'S':
			stagetape++;
			break;
		case 'T':
			stagetape++;
			break;
		case 't':
			stagetape++;
			break;
		case 'U':
			fun = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				fprintf (stderr, STG06, "-U\n");
				errflg++;
			}
			break;
		case 'u':
			uflag++;
			break;
		case 'V':
			stagetape++;
			errflg += getlist_of_vid ("-V", vid, &numvid);
			break;
		case 'v':
			stagetape++;
			vflag++;
			errflg += getlist_of_vid ("-v", vsn, &numvsn);
			break;
		case 'z':
			stagetape++;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (req_type != STAGEIN && req_type != STAGEOUT &&
	    optind >= argc && fun == 0) {
		fprintf (stderr, STG07);
		errflg++;
	}
	if (stagetape && stagedisk) {
		fprintf (stderr, STG35, "-I", "-dEfglnoqSTtVv");
		errflg++;
	}
	if (stagetape && stagemig) {
		fprintf (stderr, STG35, "-dEfglnoqSTtVv", "-M");
		errflg++;
	}
	if (stagedisk && stagemig) {
		fprintf (stderr, STG35, "-I", "-M");
		errflg++;
	}

#if defined(vms)
	if (!Gflag) {
		fprintf (stderr, STG83);
		errflg++;
	}
#endif
	if (errflg) {
		usage (argv[0]);
		exit (1);
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
		exit (SYERR);
	}
#endif
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

#if !defined(vms)
	if ((pw = getpwuid (uid)) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", uid);
		p = uidstr;
#else
	if ((pw = getpwnam (p = cuserid(NULL))) == NULL) {
#endif
		fprintf (stderr, STG11, p);
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (SYERR);
	}
	strcpy (user, pw->pw_name);	/* login name */

	if (numvsn || numvid) {		/* tape staging */
		if (numvid == 0) {
			for (i = 0; i < numvsn; i++)
				strcpy (vid[i], vsn[i]);
			numvid = numvsn;
		}
		if (strcmp (dgn, "CT1") == 0) strcpy (dgn, "CART");
		if (strcmp (den, "38K") == 0) strcpy (den, "38000");
		/* setting defaults (from TMS if installed) */
		for (i = 0; i < numvid; i++) {
#if TMS
			errflg += tmscheck (vid[i], vsn[i], dgn, den, lbl);
#else
			if (vflag == 0)
				strcpy (vsn[i], vid[i]);
			if (gflag == 0)
				strcpy (dgn, DEFDGN);
			if (dflag == 0)
				dflag = 1;	/* no default density */
			if (lflag == 0)
				strcpy (lbl, "sl");
#endif
		}
		if (errflg) {
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (1);
		}

		/* default parameters will be added to the argv list */

		if (vflag == 0) {
			strcpy (xvsn, vsn[0]);
			for (i = 1; i < numvid; i++)
				sprintf (xvsn + strlen (xvsn), ":%s", vsn[i]);
			nargs += 2;
		}
		if (gflag == 0)
			nargs += 2;
		if (dflag == 0)
			nargs += 2;
		if (lflag == 0)
			nargs += 2;
	}
	if (fun)
		nargs++;
	if (pflag == 0 && req_type != STAGEWRT && req_type != STAGECAT &&
	   (poolname = getenv ("STAGE_POOL")) != NULL)
		nargs += 2;
	if (uflag == 0 &&
	   (pool_user = getenv ("STAGE_USER")) != NULL)
		nargs += 2;
	if (pool_user &&
	   ((pw = getpwnam (pool_user)) == NULL || pw->pw_gid != gid)) {
		fprintf (stderr, STG11, pool_user);
		errflg++;
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_STRING (sbp, user);	/* login name */
	if (Gflag) {
		marshall_STRING (sbp, Gname);
		marshall_WORD (sbp, Guid);
	} else {
		marshall_STRING (sbp, user);
		marshall_WORD (sbp, uid);
	}
	marshall_WORD (sbp, gid);
	umask (mask = umask(0));
	marshall_WORD (sbp, mask);
	pid = getpid();
	marshall_WORD (sbp, pid);

	marshall_WORD (sbp, nargs);
	for (i = 0; i < optind; i++)
		marshall_STRING (sbp, argv[i]);
	if (numvsn || numvid) {		/* tape staging */
		if (vflag == 0) {
			marshall_STRING (sbp, "-v");
			marshall_STRING (sbp, xvsn);
		}
		if (gflag == 0) {
			marshall_STRING (sbp, "-g");
			marshall_STRING (sbp, dgn);
		}
		if (dflag == 0) {
			marshall_STRING (sbp, "-d");
			marshall_STRING (sbp, den);
		}
		if (lflag == 0) {
			marshall_STRING (sbp, "-l");
			marshall_STRING (sbp, lbl);
		}
	}
	if (pflag == 0 && poolname) {
		marshall_STRING (sbp, "-p");
		marshall_STRING (sbp, poolname);
	}
	if (uflag == 0 && pool_user) {
		marshall_STRING (sbp, "-u");
		marshall_STRING (sbp, pool_user);
	}
	for (i = optind; i < argc; i++) {
		if ((c = build_linkname (argv[i], path, sizeof(path), req_type)) == SYERR) {
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (SYERR);
		} else if (c) {
			errflg++;
			continue;
		} else {
			if (poolname && strcmp (poolname, "NOPOOL") == 0 &&
			    chkdirw (path) < 0) {
				errflg++;
				continue;
			}
			if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
				fprintf (stderr, STG38);
				errflg++;
				break;
			}
			marshall_STRING (sbp, path);
		}
	}
	if (fun) {
		if ((c = build_Upath (fun, path, sizeof(path), req_type)) == SYERR) {
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (SYERR);
		} else if (c)
			errflg++;
		else if (poolname && strcmp (poolname, "NOPOOL") == 0 &&
		    chkdirw (path) < 0) {
			errflg++;
		} else {
			if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
				fprintf (stderr, STG38);
				errflg++;
			} else
				marshall_STRING (sbp, path);
		}
	}
	if (errflg) {
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (1);
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

#if ! defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if ! defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

	while (c = send2stgd (stghost, sendbuf, msglen, 1)) {
		if (c == 0 || c == USERR || c == BLKSKPD || c == TPE_LSZ ||
		    c == MNYPARI || c == LIMBYSZ || c == CLEARED ||
		    c == ENOSPC) break;
		if (c == LNKNSUP) {	/* symbolic links not supported on that platform */
			c = USERR;
			break;
		}
		if (c != ESTNACT && ntries++ > MAXRETRY) break;
		sleep (RETRYI);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (c);
}

#if TMS
tmscheck(vid, vsn, dgn, den, lbl)
char *vid;
char *vsn;
char *dgn;
char *den;
char *lbl;
{
	int c, j;
	int errflg = 0;
	char *p;
	int repsize;
	int reqlen;
	char tmrepbuf[132];
	static char tmsden[6] = "     ";
	static char tmsdgn[7] = "      ";
	static char tmslbl[3] = "  ";
	char tmsreq[80];
	static char tmsvsn[7] = "      ";

	sprintf (tmsreq, "VIDMAP %s QUERY (GENERIC SHIFT MESSAGE", vid);
	reqlen = strlen (tmsreq);
	while (1) {
		repsize = sizeof(tmrepbuf);
		c = sysreq ("TMS", tmsreq, &reqlen, tmrepbuf, &repsize);
		switch (c) {
		case 0:
			break;
		case 8:
		case 100:
		case 312:
		case 315:
			fprintf (stderr, "%s\n", tmrepbuf);
			errflg++;
			break;
		default:
			sleep (60);
			continue;
		}
		break;
	}
	if (errflg) return (1);
	strncpy (tmsvsn, tmrepbuf, 6);
	for  (j = 0; tmsvsn[j]; j++)
		if (tmsvsn[j] == ' ') break;
	tmsvsn[j] = '\0';
	if (*vsn) {
		if (strcmp (vsn, tmsvsn)) {
			fprintf (stderr, STG15, vsn, tmsvsn);
			errflg++;
		}
	} else {
		strcpy (vsn, tmsvsn);
	}

	strncpy (tmsdgn, tmrepbuf+ 25, 6);
	for  (j = 0; tmsdgn[j]; j++)
		if (tmsdgn[j] == ' ') break;
	tmsdgn[j] = '\0';
	if (strcmp (tmsdgn, "CT1") == 0) strcpy (tmsdgn, "CART");
	if (*dgn) {
		if (strcmp (dgn, tmsdgn) != 0) {
			fprintf (stderr, STG15, dgn, tmsdgn);
			errflg++;
		}
	} else {
		strcpy (dgn, tmsdgn);
	}

	p = tmrepbuf + 32;
	while (*p == ' ') p++;
	j = tmrepbuf + 40 - p;
	strncpy (tmsden, p, j);
	tmsden[j] = '\0';
	if (*den) {
		if (strcmp (den, tmsden) != 0) {
			fprintf (stderr, STG15, den, tmsden);
			errflg++;
		}
	} else {
		strcpy (den, tmsden);
	}

	tmslbl[0] = tmrepbuf[74] - 'A' + 'a';
	tmslbl[1] = tmrepbuf[75] - 'A' + 'a';
	if (*lbl) {
		if (strcmp (lbl, "blp") && strcmp (lbl, tmslbl)) {
			fprintf (stderr, STG15, lbl, tmslbl);
			errflg++;
		}
	} else {
		strcpy (lbl, tmslbl);
	}
	return (errflg);
}
#endif

/*	chkdirw - extract directory name from full pathname
 *		and check if writable.
 *
 *	return	-1	in case of error
 *		0	if writable
 */
chkdirw(path)
char *path;
{
	char *p, *q;
	int rc;
	char sav;

	if (q = strchr (path, ':'))
		q++;
	else
		q = path;
	p = strrchr (path, '/');
	if (p == q)
		p++;
	if (strncmp (path, "/afs/", 5) == 0) {
		fprintf (stderr, STG44);
		rc = -1;
	} else {
		sav = *p;
		*p = '\0';
		rc = rfio_access (path, W_OK);
		*p = sav;
		if (rc < 0)
			fprintf (stderr, STG33, path, rfio_serror());
	}
	return (rc);
}

void cleanup(sig)
int sig;
{
	int c;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[64];

	signal (sig, SIG_IGN);

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEKILL);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
	marshall_STRING (sbp, user);	/* login name */
	marshall_WORD (sbp, gid);
	marshall_WORD (sbp, pid);
	msglen = sbp - sendbuf;
        marshall_LONG (q, msglen);	/* update length field */
	c = send2stgd (stghost, sendbuf, msglen, 0);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (USERR);
}

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s%s%s%s%s",
	  "[-A alloc_mode] [-b max_block_size] [-C charconv] [-c off|on]\n",
	  "[-d density] [-E error_action] [-F record_format] [-f file_id] [-G]\n",
	  "[-g device_group_name] [-h stage_host] [-I external_filename] [-K]\n",
	  "[-L record_length] [-l label_type] [-M hsmfile] [-N nread] [-n] [-o] [-p pool]\n",
	  "[-q file_sequence_number] [-S tape_server] [-s size] [-T] [-t retention_period]\n",
	  "[-U fun] [-u user] [-V visual_identifier(s)] [-v volume_serial_number(s)]\n",
	  "[-X xparm] pathname(s)\n");
}
