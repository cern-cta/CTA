/*
 * $Id: stagein.c,v 1.60 2006/12/14 15:14:41 itglp Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)RCSfile$ $Revision: 1.60 $ $Date: 2006/12/14 15:14:41 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#if defined(_WIN32)
#define W_OK 2
#else
#include <unistd.h>
#endif
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <limits.h>
#include "marshall.h"
#include "rfio_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cgetopt.h"
#include "Castor_limits.h"
#include "serrno.h"
#include "stage_constants.h"
#include "stage_messages.h"
#include "stage_macros.h"
#if VMGR
#include "Ctape_constants.h"   /* For WRITE_ENABLE and WRITE_DISABLE */
#include "vmgr_api.h"          /* For vmgrcheck() */
#endif
#include "stage_api.h"

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
extern	char	*getenv();
extern	char	*getconfent();
extern int getlist_of_vid _PROTO((char *, char[MAXVSN][7], int *));
extern int  rfio_access _PROTO((const char *, int));

static gid_t gid;
static int pid;
static struct passwd *pw;
char *stghost;
char user[15];	/* login name */
int nowait_flag = 0;
int tppool_flag = 0;
int volatile_tppool_flag = 0;
int nohsmcreat_flag = 0;
int noretry_flag = 0;
int rdonly_flag = 0;
int silent_flag = 0;
int side_flag = 0;
int nocopy_flag = 0;

#if TMS
static int tmscheck _PROTO((char *, char *, char *, char *, char *, int *, int *));
EXTERN_C int DLL_DECL sysreq _PROTO((char *, char *, int *, char *, int *));
#endif
#if VMGR
char vmgr_error_buffer[512];        /* Vmgr error buffer */
static int stage_vmgrcheck _PROTO((char *, char *, char *, char *, char *, int));
#endif
int stagein_chkdirw _PROTO((char *));
void cleanup _PROTO((int));
void usage _PROTO((char *));
void freehsmfiles _PROTO((int, char **));

int in_send2stgd = 0;

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
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
	char hsm_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	static char lbl[4] = "";
	int lflag = 0;
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int mask;
#else
	mode_t mask;
#endif
	int msglen;
	int nargs, nargsdelta;
	int ntries = 0;
	int nstg161 = 0;
	int numvid, numvsn;
	char *p, *q, *qnargs;
	char path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	int pflag = 0;
	char *pool_user = NULL;
	char *poolname = NULL;
	int req_type = 0;
	char *sbp;
	char sendbuf[REQBUFSZ];
	int stagedisk = 0;
	int stagemig = 0;
	int stagetape = 0;
	int copytape = 0;
	int uflag = 0;
	int Aflag = 0;
	uid_t uid;
	char vid[MAXVSN][7];
	char vsn[MAXVSN][7];
	int vflag = 0;
	char xvsn[7*MAXVSN];
	char **hsmfiles = NULL;
	int nhsmfiles = 0;
	int fseq1 = -1;
	int fseq2 = -1;
	int coff = 0;
	char *qopt = NULL;
	char *coffopt = NULL;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *qoptok = NULL;
	char *qoptforget = NULL;
	int enospc_retry = -1;
	int enospc_retryint = -1;
	int enospc_ntries = 0;
	int maxretry = MAXRETRY;
	static struct Coptions longopts[] =
	{
		{"allocation_mode",    REQUIRED_ARGUMENT,  NULL,      'A'},
		{"blocksize",          REQUIRED_ARGUMENT,  NULL,      'b'},
		{"charconv",           REQUIRED_ARGUMENT,  NULL,      'C'},
		{"concat",             REQUIRED_ARGUMENT,  NULL,      'c'},
		{"density",            REQUIRED_ARGUMENT,  NULL,      'd'},
		{"error_action",       REQUIRED_ARGUMENT,  NULL,      'E'},
		{"format",             REQUIRED_ARGUMENT,  NULL,      'F'},
		{"fid",                REQUIRED_ARGUMENT,  NULL,      'f'},
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"group_device",       REQUIRED_ARGUMENT,  NULL,      'g'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"keep",               NO_ARGUMENT,        NULL,      'K'},
		{"record_length",      REQUIRED_ARGUMENT,  NULL,      'L'},
		{"label_type",         REQUIRED_ARGUMENT,  NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"nread",              REQUIRED_ARGUMENT,  NULL,      'N'},
		{"new_fid",            NO_ARGUMENT,        NULL,      'n'},
		{"old_fid",            NO_ARGUMENT,        NULL,      'o'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"tape_server",        REQUIRED_ARGUMENT,  NULL,      'S'},
		{"side",               REQUIRED_ARGUMENT, &side_flag,   1},
		{"size",               REQUIRED_ARGUMENT,  NULL,      's'},
		{"trailer_label_off",  NO_ARGUMENT,        NULL,      'T'},
		{"retention_period",   REQUIRED_ARGUMENT,  NULL,      't'},
		{"fortran_unit",       REQUIRED_ARGUMENT,  NULL,      'U'},
		{"user",               REQUIRED_ARGUMENT,  NULL,      'u'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"vsn",                REQUIRED_ARGUMENT,  NULL,      'v'},
		{"xparm",              REQUIRED_ARGUMENT,  NULL,      'X'},
		{"copytape",           NO_ARGUMENT,        NULL,      'z'},
		{"silent",             NO_ARGUMENT,  &silent_flag,      1},
		{"nocopy",             NO_ARGUMENT,  &nocopy_flag   ,   1},
		{"nowait",             NO_ARGUMENT,  &nowait_flag,      1},
		{"tppool",             REQUIRED_ARGUMENT, &tppool_flag, 1},
		{"volatile_tppool",    NO_ARGUMENT,  &volatile_tppool_flag, 1},
		{"nohsmcreat",         NO_ARGUMENT,  &nohsmcreat_flag,  1},
		{"noretry",           NO_ARGUMENT,  &noretry_flag,  1},
		{"rdonly",            NO_ARGUMENT,  &rdonly_flag,       1},
		{NULL,                 0,                  NULL,        0}
	};

	nargs = argc;
	if (
#if defined(_WIN32)
			(p = strrchr (argv[0], '\\')) != NULL
#else
			(p = strrchr (argv[0], '/')) != NULL
#endif
			)
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
	
	if (req_type == STAGEOUT) {
		if (((p = getenv("STAGE_ENOSPC_RETRY")) != NULL) || ((p = getconfent("STG","ENOSPC_RETRY",0)) != NULL)) {
			enospc_retry = atoi(p);
			if (((p = getenv("STAGE_ENOSPC_RETRYINT")) != NULL) || ((p = getconfent("STG","ENOSPC_RETRYINT",0)) != NULL)) {
				enospc_retryint = atoi(p);
			} else {
				enospc_retryint = RETRYI;
			}
		}
		/* Anyway, if --noretry is set, it takes precedence */
		if (noretry_flag != 0) enospc_retry = 0;
	}

	uid = Guid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if ((uid < 0) || (uid >= CA_MAXUID) || (gid < 0) || (gid >= CA_MAXGID)) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	numvid = 0;
	numvsn = 0;
	memset (vsn, 0, sizeof(vsn));
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt_long (argc, argv, "A:b:C:c:d:E:F:f:Gg:h:I:KL:l:M:N:nop:q:S:s:Tt:U:u:V:v:X:z", longopts, NULL)) != -1) {
		switch (c) {
		case 'A':
			Aflag++;
			break;
		case 'b':
			break;
		case 'C':
			break;
		case 'c':
			coff++;
			coffopt = Coptarg;
			break;
		case 'd':
			stagetape++;
			dflag++;
			if (strlen (Coptarg) > sizeof(den) - 1) {
				fprintf (stderr, STG06, "-d\n");
				errflg++;
			} else
				strcpy (den, Coptarg);
			break;
		case 'E':
			stagetape++;
			break;
		case 'F':
			break;
		case 'f':
			stagetape++;
			break;
		case 'G':
			Gflag++;
			if ((gr = Cgetgrgid (gid)) == NULL) {
				if (errno != ENOENT) fprintf (stderr, STG33, "Cgetgrgid", strerror(errno));
				fprintf (stderr, STG36, gid);
				exit (SYERR);
			}
			if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) {
				fprintf (stderr, STG10, gr->gr_name);
				errflg++;
			} else {
				strcpy (Gname, p);
				if ((pw = Cgetpwnam (p)) == NULL) {
					if (errno != ENOENT) fprintf (stderr, STG33, "Cgetpwnam", strerror(errno));
					fprintf (stderr, STG11, p);
					errflg++;
				} else
					Guid = pw->pw_uid;
			}
			break;
		case 'g':
			stagetape++;
			gflag++;
			if (strlen (Coptarg) > sizeof(dgn) - 1) {
				fprintf (stderr, STG06, "-g\n");
				errflg++;
			} else
				strcpy (dgn, Coptarg);
			break;
		case 'h':
			stghost = Coptarg;
			break;
		case 'I':
			stagedisk++;
			break;
		case 'K':
			break;
		case 'L':
			break;
		case 'l':
			stagetape++;
			lflag++;
			if (strlen (Coptarg) > sizeof(lbl) - 1) {
				fprintf (stderr, STG06, "-l\n");
				errflg++;
			} else
				strcpy (lbl, Coptarg);
			break;
		case 'M':
			if (stagemig == nhsmfiles) {
				if (nhsmfiles == 0) {
					if ((hsmfiles = (char **) malloc(sizeof(char *))) == NULL) {
						fprintf(stderr,"malloc error (%s)\n",strerror(errno));
						errflg++;
					} else {
						stagemig++;
						hsmfiles[0] = NULL;
					}
				} else {
					char **dummy;
					if ((dummy = (char **) realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
						fprintf(stderr,"realloc error (%s)\n",strerror(errno));
						errflg++;
					} else {
						hsmfiles = dummy;
						stagemig++;
						hsmfiles[nhsmfiles] = NULL;
					}
				}
				if (stagemig == nhsmfiles + 1) {
					int attached = 0;
					char *dummy;

					/* Check if the option -M is attached or not */
					if (strstr(argv[Coptind - 1],"-M") == argv[Coptind - 1]) {
						attached = 1;
					}
					/* We want to know if there is no ':' in the string or, if there is such a ':' */
					/* if there is no '/' before (then is will indicate a hostname)                */
					if (! ISCASTOR(Coptarg)) {
						/* We prepend HSM_HOST only for non CASTOR-like files */
						if ((dummy = strchr(Coptarg,':')) == NULL || (dummy != Coptarg && strrchr(dummy,'/') == NULL)) {
							if ((hsm_host = getenv("HSM_HOST")) != NULL) {
								if (hsm_host[0] != '\0') {
									strcpy (hsm_path, hsm_host);
									strcat (hsm_path, ":");
								}
								strcat (hsm_path, Coptarg);
								if ((hsmfiles[nhsmfiles] = (char *) malloc((attached != 0 ? 2 : 0) + strlen(hsm_path) + 1)) == NULL) {
									fprintf(stderr,"malloc error (%s)\n",strerror(errno));
									errflg++;
								} else {
									if (attached != 0) {
										strcpy(hsmfiles[nhsmfiles],"-M");
										strcat(hsmfiles[nhsmfiles++],hsm_path);
									} else {
										strcpy(hsmfiles[nhsmfiles++],hsm_path);
									}
								}
							} else if ((hsm_host = getconfent("STG", "HSM_HOST",0)) != NULL) {
								if (hsm_host[0] != '\0') {
									strcpy (hsm_path, hsm_host);
									strcat (hsm_path, ":");
								}
								strcat (hsm_path, Coptarg);
								if ((hsmfiles[nhsmfiles] = (char *) malloc((attached != 0 ? 2 : 0) + strlen(hsm_path) + 1)) == NULL) {
									fprintf(stderr,"malloc error (%s)\n",strerror(errno));
									errflg++;
								} else {
									if (attached != 0) {
										strcpy(hsmfiles[nhsmfiles],"-M");
										strcat(hsmfiles[nhsmfiles++],hsm_path);
									} else {
										strcpy(hsmfiles[nhsmfiles++],hsm_path);
									}
								}
							} else {
								fprintf (stderr, STG54);
								errflg++;
							}
							argv[Coptind - 1] = hsmfiles[nhsmfiles - 1];
						} else {
							/* Here we believe that the user gave a hostname */
							hsmfiles[nhsmfiles++] = NULL;
            	        }
					} else {
						/* Here we believe that the user gave a CASTOR file */
						hsmfiles[nhsmfiles++] = NULL;
					}
				} else {
					fprintf (stderr, "Cannot parse hsm file %s\n", Coptarg);
					errflg++;
				}
			} else {
				fprintf (stderr, "Cannot parse hsm file %s\n", Coptarg);
				errflg++;
			}
			break;
		case 'N':
			break;
		case 'n':
			stagetape++;
			break;
		case 'o':
			stagetape++;
			break;
		case 'p':
			poolname = Coptarg;
			pflag++;
			break;
		case 'q':
			qopt = Coptarg;
			stagetape++;
			break;
		case 'S':
			stagetape++;
			break;
		case 's':
			break;
		case 'T':
			stagetape++;
			break;
		case 't':
			stagetape++;
			break;
		case 'U':
			stage_strtoi(&fun, Coptarg, &dp, 10);
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
		case 'X':
			break;
		case 'z':
			stagetape++;
			copytape++;
			break;
		case 0:
			/* Long option without short option correspondance */
			break;
		case '?':
			errflg++;
			break;
		default:
			errflg++;
			break;
		}
        if (errflg != 0) break;
	}
	if (req_type != STAGEIN && req_type != STAGEOUT &&
			Coptind >= argc && fun == 0) {
		fprintf (stderr, STG07);
		errflg++;
	}
	if (! noretry_flag) {
		if (
			(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
			(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0))
			) {
			noretry_flag = 1;
		}
	}
	if (noretry_flag != 0) maxretry = 0;
	if ((tppool_flag != 0) && (req_type != STAGEWRT) && (req_type != STAGEOUT)) {
		fprintf (stderr, STG17, "--tppool", (req_type == STAGEIN) ? "stagein" : "stagecat");
		errflg++;
	}
	/* In the following stagetape is exclusive with all other types of stagein, except */
	/* if stagetape == copytape == 1, in which case this means that only option -z */
	/* triggered the stagetape flag */
	if (stagetape && stagedisk && (! (stagetape == 1 && copytape == 1))) {
		fprintf (stderr, STG35, "-I", "-dEfglnoqSTtVv");
		errflg++;
	}
	if (stagetape && stagemig && (! (stagetape == 1 && copytape == 1))) {
		fprintf (stderr, STG35, "-dEfglnoqSTtVv", "-M");
		errflg++;
	}
	if (stagedisk && stagemig && (! (stagetape == 1 && copytape == 1))) {
		fprintf (stderr, STG35, "-I", "-M");
		errflg++;
	}
	if ((side_flag != 0) && (! stagetape)) {
		fprintf (stderr, STG17, "--side", "any request but direct 'tape' access");
		errflg++;
	}

	if (errflg != 0) {
		usage (argv[0]);
        freehsmfiles(nhsmfiles, hsmfiles);
		exit (1);
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
        freehsmfiles(nhsmfiles, hsmfiles);
		exit (SYERR);
	}
#endif
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	if ((pw = Cgetpwuid (uid)) == NULL) {
		char uidstr[8];
		if (errno != ENOENT) fprintf (stderr, STG33, "Cgetpwuid", strerror(errno));
		sprintf (uidstr, "%d", uid);
		p = uidstr;
		fprintf (stderr, STG11, p);
#if defined(_WIN32)
		WSACleanup();
#endif
        freehsmfiles(nhsmfiles, hsmfiles);
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
#if VMGR
			if (stage_vmgrcheck(vid[i], vsn[i], dgn, den, lbl, ((req_type == STAGEOUT) || (req_type == STAGEWRT)) ? WRITE_ENABLE : WRITE_DISABLE) != 0) {
#endif
#if TMS
#if VMGR
				if (serrno == ETVUNKN) {
					/* VMGR fails with acceptable serrno and TMS is available */
#endif
					if ((errflg += tmscheck (vid[i], vsn[i], dgn, den, lbl, &fseq1, &fseq2)) == 0) {
						if (stagetape && coff && (qopt != NULL)) {
							if (((numvid > 0) && (numvid != 1)) ||
								((numvsn > 0) && (numvsn != 1))) {
								fprintf (stderr, STG33, "-c off option", "requires one -V or one -v other option");
								errflg++;
							} else {
								if (((fseq1 > 0) && (fseq2 <= 0)) ||
									((fseq2 > 0) && (fseq1 <= 0)) ||
									((fseq1 > 0) && (fseq2 > 0) && (fseq2 < fseq1))) {
									fprintf (stderr, "STG33 - TMS gives %s VIDMAP inconsistent info (fseq range [%d,%d])", vid[i], fseq1, fseq2);
									errflg++;
								} else if ((fseq1 > 0) && (fseq2 > 0)) {
									char *dp;
									int qvalue;
									/* TMS says that this volume is VIDMAPped */
									/* We verify consistency with -q option value */
									stage_strtoi(&qvalue, qopt, &dp, 10);
									if ((qvalue == INT_MIN) ||
										(qvalue == INT_MAX) ||
										(*dp != '-') ||
										((*dp == '-') && (*(dp+1) != '\0'))) {
										fprintf (stderr, STG06, "-q");
										errflg++;
									} else {
 								/* When user specifies -q<fseq>, in reality the fseq on tape is fseq1+<fseq>-1 */
								/* so we have to verify that fseq1+<fseq>-1 does not exceed fseq2 */
										if ((qvalue < 0) || ((qvalue + fseq1 - 1) > fseq2)) {
											fprintf (stderr, "STG33 - For %s VIDMAP volume, -q option value must be in range [%d,%d]\n", vid[i], 1, fseq2 - fseq1 + 1);
											errflg++;
										} else {
											size_t fseqlen;
											/* The real starting fseq is fseq1+<fseq>-1 */
											fseq1 += (qvalue - 1);
											/* We create a string like q1,q2,etc... */
											/* starting from fseq1 to fseq2, so: fseq2-fseq1+1 entries */
											/* separated by as many ',' characters, each entry being maximum CA_MAXFSEQLEN length */
											fseqlen = ((fseq2-fseq1+1) * CA_MAXFSEQLEN) + (fseq2-fseq1) + 1;
											if ((qoptok = (char *) malloc(fseqlen)) == NULL) {
												fprintf (stderr, STG02, "stagein", "malloc", strerror(errno));
												errflg++;
											} else {
												int iq;
												char *qq = qoptok;
												
												*qq = '\0';
												for (iq = fseq1; iq <= fseq2; iq++) {
													sprintf(qq, "%d", iq + qvalue - fseq1);
													qq += strlen(qq);
													if (iq != fseq2) {
														*qq++ = ',';
														*qq   = '\0';
													}
												}
											}
											qoptforget = qopt;
											qopt = qoptok;
										}
									}
								}
							}
						}
					}
#if VMGR
				} else {
					/* VMGR fails with not acceptable serrno */
					errflg++;
					fprintf (stderr, STG02, vid[i][0] != '\0' ? vid[i] : vsn[i], "vmgrcheck", sstrerror(serrno));
					break;
				}
#endif
#elif (defined(VMGR))
				/* VMGR fail and TMS isn't there */
				errflg++;
				fprintf (stderr, STG02, vid[i][0] != '\0' ? vid[i] : vsn[i], "vmgrcheck", sstrerror(serrno));
				break;
#endif /* TMS */
#if VMGR
			}
#endif
#if (! (defined(VMGR) || defined(TMS)))
			/* No VMGR nor TMS */
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
		if (errflg != 0) {
#if defined(_WIN32)
			WSACleanup();
#endif
			freehsmfiles(nhsmfiles, hsmfiles);
			if (qoptok != NULL) free(qoptok);
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
			((pw = Cgetpwnam (pool_user)) == NULL || pw->pw_gid != gid)) {
		if ((pw == NULL) && (errno != ENOENT)) fprintf (stderr, STG33, "Cgetpwnam", strerror(errno));
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
	
	qnargs = sbp;	/* save pointer. The next field will be updated */
	nargsdelta = 0;
	marshall_WORD (sbp, nargs);
	for (i = 0; i < Coptind; i++) {
		if (qoptforget != NULL) {
			/* The -q option value has been overwriten */
			if (strstr(argv[i],"-q") == argv[i]) {
				nargsdelta--;
				continue;
			}
			if (argv[i] == qoptforget) {
				/* Special case when -q option and value are not attached */
				nargsdelta--;
				continue;
			}
			/* and the -c off one has to be withdrawn */
			if (strstr(argv[i],"-c") == argv[i]) {
				nargsdelta--;
				continue;
			}
			if (argv[i] == coffopt) {
				/* Special case when -c option and value are not attached */
				nargsdelta--;
				continue;
			}
		}
		marshall_STRING (sbp, argv[i]);
	}
	if (qoptforget != NULL) {
		marshall_STRING (sbp, "-q");
		marshall_STRING (sbp, qopt);
		nargsdelta += 2;
		if (! Aflag) {
			marshall_STRING (sbp, "-A");
			marshall_STRING (sbp, "deferred");
			nargsdelta += 2;
		}
	}
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
	for (i = Coptind; i < argc; i++) {
		if ((c = build_linkname (argv[i], path, sizeof(path), req_type)) == SESYSERR) {
#if defined(_WIN32)
			WSACleanup();
#endif
            freehsmfiles(nhsmfiles, hsmfiles);
			exit (SYERR);
		} else if (c) {
			errflg++;
			continue;
		} else {
			if (poolname && strcmp (poolname, "NOPOOL") == 0 &&
					stagein_chkdirw (path) < 0) {
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
		if ((c = build_Upath (fun, path, sizeof(path), req_type)) == SESYSERR) {
#if defined(_WIN32)
			WSACleanup();
#endif
            freehsmfiles(nhsmfiles, hsmfiles);
			exit (SYERR);
		} else if (c)
			errflg++;
		else if (poolname && strcmp (poolname, "NOPOOL") == 0 &&
						 stagein_chkdirw (path) < 0) {
			errflg++;
		} else {
			if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
				fprintf (stderr, STG38);
				errflg++;
			} else
				marshall_STRING (sbp, path);
		}
	}
	if (errflg != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
        freehsmfiles(nhsmfiles, hsmfiles);
		if (qoptok != NULL) free(qoptok);
		exit (1);
	}
	
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */
	
	if (nargsdelta != 0) {
		nargs += nargsdelta;
		marshall_WORD (qnargs, nargs);	/* update number of arguments field */
	}

#if ! defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if ! defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);
	
	while (1) {
		in_send2stgd = 1;
		c = send2stgd_cmd (stghost, sendbuf, msglen, 1, NULL, 0);
		in_send2stgd = 0;
		if ((req_type == STAGE_OUT) && (serrno == ENOSPC) && (enospc_retry > 0) && (enospc_retryint > 0)) {
			if (++enospc_ntries > enospc_retry) break;
			sleep(enospc_retryint);
			continue;
		}
		if (c == 0 || serrno == EINVAL || serrno == ERTBLKSKPD || serrno == ERTTPE_LSZ ||
				serrno == ERTMNYPARY || serrno == ERTLIMBYSZ || serrno == ESTCLEARED || serrno == SECHECKSUM ||
				serrno == ENOSPC || serrno == ESTKILLED || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || serrno == ESTLNKNSUP) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0) fprintf(stderr, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if (noretry_flag != 0) break; /* To be sure we always break if --noretry is in action */
		sleep (RETRYI);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
    freehsmfiles(nhsmfiles, hsmfiles);
	if (qoptok != NULL) free(qoptok);
	exit (c == 0 ? 0 : rc_castor2shift(serrno));
}

void freehsmfiles(nhsmfiles,hsmfiles)
     int nhsmfiles;
     char **hsmfiles;
{
  int i;

  if (hsmfiles == NULL) return;

  for (i = 0; i < nhsmfiles; i++) {
    if (hsmfiles[i] != NULL) {
      free(hsmfiles[i]);
    }
  }

  free(hsmfiles);
}

#if TMS
static int tmscheck(vid, vsn, dgn, den, lbl, fseq1, fseq2)
		 char *vid;
		 char *vsn;
		 char *dgn;
		 char *den;
		 char *lbl;
		 int *fseq1;
		 int *fseq2;
{
	int c, j;
	int errflg = 0;
	char *p;
	int repsize;
	int reqlen;
	char tmrepbuf[132];
	static char tmsden[6] = "     ";
	static char tmsdgn[7] = "      ";
	static char tmslbl[4] = "   ";
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
	if (errflg != 0) return (1);
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
	
	/* Indexes 74..76 contains the label type (padded with a blank if necessary at index 76) */
	tmslbl[0] = tmrepbuf[74] - 'A' + 'a';
	tmslbl[1] = tmrepbuf[75] - 'A' + 'a';
	tmslbl[2] = (tmrepbuf[76] == ' ') ? '\0' : tmrepbuf[76] - 'A' + 'a';
	if (*lbl) {
		if (strcmp (lbl, "blp") && strcmp (lbl, tmslbl)) {
			fprintf (stderr, STG15, lbl, tmslbl);
			errflg++;
		}
	} else {
		strcpy (lbl, tmslbl);
	}
    if ((fseq1 != NULL) && (fseq2 != NULL) && (strlen(tmrepbuf) >= 89)) {
      char tmpfseq[6];
      char *dp;
      /* In principle the indexes 78 to 82 contains the first fseq of a vidmapped volume */
      /*              the indexes 84 to 88 contains the 2nd   fseq of a vidmapped volume */
      memcpy(tmpfseq, tmrepbuf + 78, 5);
      tmpfseq[5] = '\0';
      stage_strtoi(fseq1, tmpfseq, &dp, 10);
      if ((*fseq1 == INT_MIN) || (*fseq1 == INT_MAX) || (*dp != '\0')) {
        *fseq1 = -1;
		fprintf (stderr, STG02, tmpfseq, "stage_strtoi", strerror(errno));
		errflg++;
      }
      memcpy(tmpfseq, tmrepbuf + 84, 5);
      tmpfseq[5] = '\0';
      stage_strtoi(fseq2, tmpfseq, &dp, 10);
      if ((*fseq2 == INT_MIN) || (*fseq2 == INT_MAX) || (*dp != '\0')) {
        *fseq2 = -1;
		fprintf (stderr, STG02, tmpfseq, "stage_strtoi", strerror(errno));
		errflg++;
      }
    }
	return (errflg);
}
#endif

#if VMGR
static int stage_vmgrcheck(vid, vsn, dgn, den, lbl, mode)
     char *vid;
     char *vsn;
     char *dgn;
     char *den;
     char *lbl;
	 int mode;
{
    uid_t uid;
    gid_t gid;
	int rc;

    /* Make sure error buffer of VMGR do not go to stderr */
    vmgr_error_buffer[0] = '\0';
    if (vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0) {
		return(serrno);
    }

    /* We need to know exact current uid/gid */
    uid = getuid();
    gid = getgid();

#if defined(_WIN32)
	if ((uid < 0) || (uid >= CA_MAXUID) || (gid < 0) || (gid >= CA_MAXGID)) {
		return(SEUSERUNKN);
    }
#endif

	if ((rc = vmgrcheck(vid,vsn,dgn,den,lbl,mode,uid,gid)) != 0) {
		serrno = rc;
		return(1);
	} else {
		return(0);
	}
}
#endif

/*	stagein_chkdirw - extract directory name from full pathname
 *		and check if writable.
 *
 *	return	-1	in case of error
 *		0	if writable
 */
int stagein_chkdirw(path)
		 char *path;
{
	char *p, *q;
	int rc;
	char sav;
	
	if ((q = strchr (path, ':')) != NULL)
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
		PRE_RFIO;
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
	
	if (in_send2stgd != 0) {
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
		c = send2stgd_cmd (stghost, sendbuf, msglen, 0, NULL, 0);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (USERR);
}

void usage(cmd)
		 char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s%s%s%s%s%s%s",
					 "[-A alloc_mode] [-b max_block_size] [-C charconv] [-c off|on]\n",
					 "[-d density] [-E error_action] [-F record_format] [-f file_id] [-G]\n",
					 "[-g device_group_name] [-h stage_host] [-I external_filename] [-K]\n",
					 "[-L record_length] [-l label_type] [-M hsmfile [-M...]] [-N nread] [-n] [-o] [-p pool]\n",
					 "[-q file_sequence_number] [-S tape_server] [-s size] [-T] [-t retention_period]\n",
					 "[-U fun] [-u user] [-V visual_identifier(s)] [-v volume_serial_number(s)]\n",
					 "[-X xparm]\n",
					 "[--nohsmcreat] [--nocopy] [--noretry] [--nowait] [--side sidenumber] [--silent]\n"
			         "[--tppool tapepool] [--rdonly] [--volatile_tppool]\n",
					 "pathname(s)\n");
}
