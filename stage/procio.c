/*
 * $Id: procio.c,v 1.56 2000/11/06 14:46:13 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procio.c,v $ $Revision: 1.56 $ $Date: 2000/11/06 14:46:13 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
#else
#include <netinet/in.h>
#include <sys/time.h>
#endif
#include <sys/stat.h>
#include <errno.h>

#include "marshall.h"
#undef  unmarshall_STRING
#define unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#if SACCT
#include "../h/sacct.h"
#endif
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include <serrno.h>
#include "osdep.h"
#include "Cns_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

extern char *optarg;
extern int optind;
extern char *rfio_serror();
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep (int, int, ...);
#endif
extern char defpoolname[CA_MAXPOOLNAMELEN + 1];
extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgcat_entry *newreq _PROTO(());
char *findpoolname();
int last_tape_file;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
static char one[2] = "1";

void procioreq _PROTO((int, char *, char *));
void procputreq _PROTO((char *, char *));
extern int isuserlevel _PROTO((char *));
int unpackfseq _PROTO((char *, int, char *, fseq_elem **, int, int *));
extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *));
extern int ask_stageout _PROTO((int, char *, struct stgcat_entry **));
extern struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, char *, char *));
extern int nextreqid _PROTO(());
int isstaged _PROTO((struct stgcat_entry *, struct stgcat_entry **, int, char *));
int maxfseq_per_vid _PROTO((struct stgcat_entry *, int, char *, char *));
extern void update_migpool _PROTO((struct stgcat_entry *, int, int));
extern int updfreespace _PROTO((char *, char *, signed64));

#ifdef MIN
#undef MIN
#endif
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#ifdef MAX
#undef MAX
#endif
#define MAX(a,b) ((a) > (b) ? (a) : (b))

void procioreq(req_type, req_data, clienthost)
		 int req_type;
		 char *req_data;
		 char *clienthost;
{
	int Aflag = 0;
	int actual_poolflag;
	char actual_poolname[CA_MAXPOOLNAMELEN + 1];
	char **argv;
	int c, i, j;
	int concat_off = 0;
	int concat_off_fseq = 0;
	int clientpid;
	static char cmdnames[4][9] = {"", "stagein", "stageout", "stagewrt"};
	int copytape = 0;
	char *dp;
	int errflg = 0;
	char *fid = NULL;
	struct stat filemig_stat;          /* For non-CASTOR HSM stat() */
	struct Cns_filestat Cnsfilestat;   /* For     CASTOR hsm stat() */
	char filemig_size[5];
	char *fseq = NULL;
	fseq_elem *fseq_list = NULL;
	struct group *gr;
	struct Cns_fileid *hsmfileids = NULL;
	u_signed64 *hsmsizes = NULL;
	char **hsmfiles = NULL;
	time_t *hsmmtimes = NULL;
	int ihsmfiles;
	int jhsmfiles;
	char *name;
	int nargs;
	int nbdskf;
	int nbtpf;
	int nhsmfiles = 0;
	int nuserlevel = 0;
	int nexplevel = 0;
	int no_upath = 0;
	char *nread = NULL;
	int numvid, numvsn;
	char *p, *q;
	char *pool_user = NULL;
	int poolflag;
	char *rbp;
	int savereqid;
	char *size = NULL;
	struct stat st;
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
	char trailing;
	int Uflag = 0;
	int Upluspath = 0;
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;
	struct waitf *wfp;
	struct waitq *wqp = NULL;
	int nhpssfiles = 0;
	int ncastorfiles = 0;

	memset ((char *)&stgreq, 0, sizeof(stgreq));
	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	strcpy (stgreq.user, user);
	unmarshall_STRING (rbp, name);
	unmarshall_WORD (rbp, stgreq.uid);
	unmarshall_WORD (rbp, stgreq.gid);
	unmarshall_WORD (rbp, stgreq.mask);
	unmarshall_WORD (rbp, clientpid);

	stglogit (func, STG92, req_type == STAGECAT ? "stagecat" : cmdnames[req_type],
						stgreq.user, stgreq.uid, stgreq.gid, clienthost);
#if SACCT
	stageacct (STGCMDR, stgreq.uid, stgreq.gid, clienthost,
						 reqid, req_type, 0, 0, NULL, "");
#endif

	nargs = req2argv (rbp, &argv);
	if (Cgetpwuid (stgreq.uid) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", stgreq.uid);
		sendrep (rpfd, MSG_ERR, STG11, uidstr);
		c = SYERR;
		goto reply;
	}
	if ((gr = Cgetgrgid (stgreq.gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, stgreq.gid);
		c = SYERR;
		goto reply;
	}
	strcpy (stgreq.group, gr->gr_name);
	numvid = 0;
	numvsn = 0;

#ifdef linux
	optind = 0;
#else
	optind = 1;
#endif
	while ((c = getopt (nargs, argv, "A:b:C:c:d:E:F:f:Gg:h:I:KL:l:M:N:nop:q:S:s:Tt:U:u:V:v:X:z")) != EOF) {
		switch (c) {
		case 'A':	/* allocation mode */
			if (strcmp (optarg, "deferred") == 0) {
				if (req_type != STAGEIN) {
					sendrep (rpfd, MSG_ERR, STG17, "-A deferred",
									 "stageout/stagewrt/stagecat");
					errflg++;
				} else
					Aflag = 1; /* deferred space allocation */
			} else if (strcmp (optarg, "immediate")) {
				sendrep (rpfd, MSG_ERR, STG06, "-A");
				errflg++;
			}
			break;
		case 'b':
			stgreq.blksize = strtol (optarg, &dp, 10);
			if (*dp != '\0' || stgreq.blksize == 0) {
				sendrep (rpfd, MSG_ERR, STG06, "-b");
				errflg++;
			}
			break;
		case 'C':	/* character conversion */
			p = strtok (optarg,",") ;
			while (p != NULL) {
				if (strcmp (p, "ebcdic") == 0) {
					stgreq.charconv |= EBCCONV;
				} else if (strcmp (p, "block") == 0) {
					stgreq.charconv |= FIXVAR;
				} else if (strcmp (p, "ascii")) {
					sendrep (rpfd, MSG_ERR, STG06, "-C");
					errflg++;
					break;
				}
				if ((p = strtok (NULL, ",")) != NULL) *(p - 1) = ',';
			}
			break;
		case 'c':	/* concatenation */
			if (strcmp (optarg, "off") == 0) {
				if (req_type != STAGEIN){
					sendrep (rpfd, MSG_ERR, STG17, "-c off",
									 "stageout/stagewrt/stagecat");
					errflg++;
				} else
					concat_off = 1;
			} else if (strcmp (optarg, "on")) {
				sendrep (rpfd, MSG_ERR, STG06, "-c");
				errflg++;
			}
			break;
		case 'd':
			strcpy (stgreq.u1.t.den, optarg);
			break;
		case 'E':
			if (strcmp (optarg, "skip") == 0)
				stgreq.u1.t.E_Tflags |= SKIPBAD;
			else if (strcmp (optarg, "keep") == 0)
				stgreq.u1.t.E_Tflags |= KEEPFILE;
			else if (strcmp (optarg, "ignoreeoi") == 0)
				stgreq.u1.t.E_Tflags |= IGNOREEOI;
			else {
				sendrep (rpfd, MSG_ERR, STG06, "-E");
				errflg++;
			}
			break;
		case 'F':
			if (strcmp (optarg, "F") && strcmp (optarg, "FB") &&
					strcmp (optarg, "FBS") && strcmp (optarg, "FS") &&
					strcmp (optarg, "U") && strcmp (optarg, "U,bin") &&
					strcmp (optarg, "U,f77") && strcmp (optarg, "F,-f77")) {
				sendrep (rpfd, MSG_ERR, STG06, "-F");
				errflg++;
			} else
				strncpy (stgreq.recfm, optarg, 3);
			break;
		case 'f':
			fid = optarg;
			break;
		case 'G':
			break;
		case 'g':
			strcpy (stgreq.u1.t.dgn, optarg);
			break;
		case 'h':
			break;
		case 'I':
			stgreq.t_or_d = 'd';
			strcpy (stgreq.u1.d.xfile, optarg);
			break;
		case 'K':
			stgreq.keep = 1;
			break;
		case 'L':
			stgreq.lrecl = strtol (optarg, &dp, 10);
			if (*dp != '\0' || stgreq.lrecl == 0) {
				sendrep (rpfd, MSG_ERR, STG06, "-L");
				errflg++;
			}
			break;
		case 'l':
			if (strcmp (optarg, "blp") ||
					(req_type != STAGEOUT && req_type != STAGEWRT))
				strcpy (stgreq.u1.t.lbl, optarg);
			else {
				sendrep (rpfd, MSG_ERR, STG17, "-l blp", "stageout/stagewrt");
				errflg++;
			}
			break;
		case 'M':
			stgreq.t_or_d = 'm';
			if (nhsmfiles == 0) {
				if ((hsmfiles   = (char **)             malloc(sizeof(char *))) == NULL ||
				    (hsmfileids = (struct Cns_fileid *) malloc(sizeof(struct Cns_fileid))) == NULL ||
				    (hsmsizes   = (u_signed64 *)        malloc(sizeof(u_signed64))) == NULL ||
				    (hsmmtimes  = (time_t *)            malloc(sizeof(time_t))) == NULL) {
					c = SYERR;
					goto reply;
				}
			} else {
				char **dummy = hsmfiles;
				struct Cns_fileid *dummy2 = hsmfileids;
				u_signed64 *dummy3 = hsmsizes;
				time_t *dummy4 = hsmmtimes;

				if ((dummy  = (char **)             realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL ||
				    (dummy2 = (struct Cns_fileid *) realloc(hsmfileids,(nhsmfiles+1) * sizeof(struct Cns_fileid))) == NULL ||
				    (dummy3 = (u_signed64 *)        realloc(hsmsizes,(nhsmfiles+1) * sizeof(u_signed64))) == NULL ||
				    (dummy4 = (time_t *)            realloc(hsmmtimes,(nhsmfiles+1) * sizeof(time_t))) == NULL) {
					c = SYERR;
					goto reply;
				}
				hsmfiles = dummy;
				hsmfileids = dummy2;
				hsmsizes = dummy3;
				hsmmtimes = dummy4;
			}
			hsmfiles[nhsmfiles++] = optarg;
			break;
		case 'N':
			nread = optarg;
			p = strtok (nread, ":");
			while (p != NULL) {
				j = strtol (p, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-N");
					errflg++;
				}
				if ((p = strtok (NULL, ":")) != NULL) *(p - 1) = ':';
			}
			break;
		case 'n':
			stgreq.u1.t.filstat = 'n';
			break;
		case 'o':
			stgreq.u1.t.filstat = 'o';
			break;
		case 'p':
			if (strcmp (optarg, "NOPOOL") == 0 ||
					isvalidpool (optarg)) {
				strcpy (stgreq.poolname, optarg);
			} else {
				sendrep (rpfd, MSG_ERR, STG32, optarg);
				errflg++;
			}
			break;
		case 'q':
			fseq = optarg;
			break;
		case 'S':
			strcpy (stgreq.u1.t.tapesrvr, optarg);
			break;
		case 's':
			size = optarg;
			p = strtok (size, ":");
			while (p != NULL) {
				j = strtol (p, &dp, 10);
				if (*dp != '\0' || j > 2047 || j <= 0) {
					sendrep (rpfd, MSG_ERR, STG06, "-s");
					errflg++;
				}
				if ((p = strtok (NULL, ":")) != NULL) *(p - 1) = ':';
			}
			break;
		case 'T':
			stgreq.u1.t.E_Tflags |= NOTRLCHK;
			break;
		case 't':
			stgreq.u1.t.retentd = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-t");
				errflg++;
			}
			break;
		case 'u':
			pool_user = optarg;
			break;
		case 'U':
			if (req_type == STAGECAT) {
				sendrep (rpfd, MSG_ERR, STG17, "-U", "stagecat");
				errflg++;
			} else
				Uflag++;
			break;
		case 'V':
			stgreq.t_or_d = 't';
			q = strtok (optarg, ":");
			while (q != NULL) {
				strcpy (stgreq.u1.t.vid[numvid], q);
				UPPER (stgreq.u1.t.vid[numvid]);
				numvid++;
				q = strtok (NULL, ":");
			}
			break;
		case 'v':
			stgreq.t_or_d = 't';
			q = strtok (optarg, ":");
			while (q != NULL) {
				strcpy (stgreq.u1.t.vsn[numvsn], q);
				UPPER (stgreq.u1.t.vsn[numvsn]);
				numvsn++;
				q = strtok (NULL, ":");
			}
			break;
		case 'X':
			if ((int) strlen (optarg) < sizeof(stgreq.u1.d.Xparm)) {
				strcpy (stgreq.u1.d.Xparm, optarg);
			} else {
				sendrep (rpfd, MSG_ERR, STG06, "-X");
				errflg++;
			}
		case 'z':
			copytape++;
			break;
		}
	}
	if (Aflag && strcmp (stgreq.poolname, "NOPOOL") == 0) {
		sendrep (rpfd, MSG_ERR, STG17, "-A deferred", "-p NOPOOL");
		errflg++;
	}
#ifndef CONCAT_OFF
	if (concat_off) {
		sendrep (rpfd, MSG_ERR, "STG17 - option -c off is not supported by this server\n");
		errflg++;
	}
#else
	if (concat_off && strcmp (stgreq.poolname, "NOPOOL") == 0) {
		sendrep (rpfd, MSG_ERR, STG17, "-c off", "-p NOPOOL");
		errflg++;
	}
	if (concat_off && stgreq.t_or_d != 't') {
		sendrep (rpfd, MSG_ERR, "STG17 - option -c off is only valid with -V option\n");
		errflg++;
	}
	if (concat_off && (nargs - optind != 0)) {
		sendrep (rpfd, MSG_ERR, "STG17 - option -c off is only valid without explicit disk files\n");
		errflg++;
	}
#endif
	if (*stgreq.recfm == 'F' && req_type != STAGEIN && stgreq.lrecl == 0) {
		sendrep (rpfd, MSG_ERR, STG20);
		errflg++;
	}
	if (stgreq.t_or_d == '\0') {
		sendrep (rpfd, MSG_ERR, STG12);
		errflg++;
	}
	if (errflg) {
		c = USERR;
		goto reply;
	}

	/* setting defaults */

	if (concat_off)
		Aflag = 1;	/* force deferred space allocation */
	if (req_type == STAGEIN && stgreq.t_or_d == 'm' && ! size)
		Aflag = 1;  /* force deferred policy if stagein of an hsm file without -s option */
	if (*stgreq.poolname == '\0') {
		poolflag = 0;
		if (req_type != STAGEWRT && req_type != STAGECAT)
			strcpy (stgreq.poolname, defpoolname);
	} else if (strcmp (stgreq.poolname, "NOPOOL") == 0) {
		poolflag = -1;
		stgreq.poolname[0] = '\0';
	} else poolflag = 1;
	if (pool_user == NULL)
		pool_user = "stage";

	if (stgreq.t_or_d == 't') {
		if (numvid == 0) {
			for (i = 0; i < numvsn; i++)
				strcpy (stgreq.u1.t.vid[i], stgreq.u1.t.vsn[i]);
			numvid = numvsn;
		}
		if (concat_off && numvid != 1) {
			sendrep (rpfd, MSG_ERR, "STG02 - option -c off is not compatible with volume spanning\n");
			errflg++;
			c = USERR;
			goto reply;
		}
		if (fseq == NULL) fseq = one;

		/* compute number of tape files */
		if ((nbtpf = unpackfseq (fseq, req_type, &trailing, &fseq_list, concat_off, &concat_off_fseq)) == 0) {
			errflg++;
		} else {
			if (concat_off) {
				int maxfseq;
				char concat_off_flag;

				if (trailing != '-' || nbtpf != 1) {
					sendrep (rpfd, MSG_ERR, "STG02 - option -c off requires exactly one tapefile, ending with '-'\n");
					errflg++;
				}
				/* We want to determine which highest fseq is already staged for this vid */
				if ((maxfseq = maxfseq_per_vid(&stgreq,poolflag,stgreq.poolname,&concat_off_flag)) > 0) {
					fseq_elem *newfseq_list = NULL;

					if (maxfseq >= concat_off_fseq) {
						int new_nbtpf = maxfseq - concat_off_fseq + (concat_off_flag == 'c' ? 1 : 2);
						/* There is overlap from concat_off_fseq to maxfseq */

						if ((newfseq_list = realloc(fseq_list,new_nbtpf * sizeof(fseq_elem))) == NULL) {
							sendrep (rpfd, MSG_ERR, "STG02 - realloc() error (%s)\n", strerror(errno));
							free(fseq_list);
							errflg++;
						}
						fseq_list = newfseq_list;
						for (i = 0; i <= (maxfseq - concat_off_fseq - (concat_off_flag == 'c' ? 1 : 0)); i++) {
							sprintf ((char *)(fseq_list + i), "%d", concat_off_fseq + i);
						}
						sprintf((char *)(fseq_list + new_nbtpf - 1), "%d", maxfseq + (concat_off_flag == 'c' ? 0 : 1));
						/* Nota : packfseq will take care of adding the leading "-" ... */
						nbtpf = new_nbtpf;
					}
				}
			}
		}
	} else nbtpf = 1;

	if (errflg) {
		c = USERR;
		goto reply;
	}

	/* compute number of disk files */
	nbdskf = nargs - optind;
	if (nbdskf == 0) {
		if (poolflag < 0) {	/* -p NOPOOL */
			sendrep (rpfd, MSG_ERR, STG07);
			c = USERR;
			goto reply;
		}
		nbdskf = nbtpf;
		no_upath = 1;
		upath[0] = '\0';
	}

	/* In case of hsm request verify the exact mapping between number of hsm files */
	/* and number of disk files.                                                   */
	if (nhsmfiles > 0) {
		if ((nargs - optind) > 0 && nbdskf != nhsmfiles) {
			sendrep (rpfd, MSG_ERR, STG19);
			c = USERR;
			goto reply;
		}
		/* And we also check that there is NO redundant hsm files (multiple equivalent ones) */
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			for (jhsmfiles = ihsmfiles + 1; jhsmfiles < nhsmfiles; jhsmfiles++) {
				if (strcmp(hsmfiles[ihsmfiles],hsmfiles[jhsmfiles]) == 0) {
					sendrep (rpfd, MSG_ERR, "Duplicate HSM file %s\n",hsmfiles[ihsmfiles]);
					c = USERR;
					goto reply;
				}
			}
		}
		/* We check that user do not mix different hsm types (hpss and castor) */
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			if (ISHPSS(hsmfiles[ihsmfiles])) {
				if (ISCASTORHOST(hsmfiles[ihsmfiles])) {
					sendrep (rpfd, MSG_ERR, STG102, "castor", "hpss", hsmfiles[ihsmfiles]);
					c = USERR;
					goto reply;
				}
				++nhpssfiles;
			}
			if (ISCASTOR(hsmfiles[ihsmfiles])) {
				if (ISHPSSHOST(hsmfiles[ihsmfiles])) {
					sendrep (rpfd, MSG_ERR, STG102, "hpss", "castor", hsmfiles[ihsmfiles]);
					c = USERR;
					goto reply;
				}
				++ncastorfiles;
                /* And we take the opportunity to decide which level of migration (user/exp) */
				if (isuserlevel(hsmfiles[ihsmfiles])) {
					nuserlevel++;
        		} else {
          			nexplevel++;
        		}
			}
		}
		/* No recognized type ? */
		if (nhpssfiles == 0 && ncastorfiles == 0) {
			sendrep (rpfd, MSG_ERR, "Cannot determine HSM file types (HPSS nor CASTOR)\n");
			c = USERR;
			goto reply;
		}
		/* Mixed CASTOR types ? */
		if (ncastorfiles > 0 && nuserlevel > 0 && nexplevel > 0) {
			sendrep (rpfd, MSG_ERR, "Mixing user-level and experiment-level CASTOR files is not allowed\n");
			c = USERR;
			goto reply;
		}
		/* At least one HPSS and one CASTOR */
		if (nhpssfiles >  0 && ncastorfiles >  0) {
			sendrep (rpfd, MSG_ERR, "HPSS (%d occurence%s) and CASTOR (%d occurence%s) files on the same command-line is not allowed\n",nhpssfiles,nhpssfiles > 1 ? "s" : "",ncastorfiles,ncastorfiles > 1 ? "s" : "");
			c = USERR;
			goto reply;
		} else if (ncastorfiles > 0) {
			/* It is a CASTOR request, so stgreq.t_or_d, previously equal to 'm', is set to 'h' */
			stgreq.t_or_d = 'h';
		}
		/* Depending in req_type we contact NameServer accordingly */
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			switch (req_type) {
			case STAGEIN:
				setegid(stgreq.gid);
				seteuid(stgreq.uid);
				switch (stgreq.t_or_d) {
				case 'h':
					/* Suppose the file is of zero size ? We can catch it right now */
					memset(&(hsmfileids[ihsmfiles]),0,sizeof(struct Cns_fileid));
					if (Cns_statx(hsmfiles[ihsmfiles], &(hsmfileids[ihsmfiles]), &Cnsfilestat) != 0) {
						sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_statx", sstrerror(serrno));
						c = USERR;
						setegid(0);
						seteuid(0);
						goto reply;
					}
					hsmmtimes[ihsmfiles] = Cnsfilestat.mtime;
					hsmsizes[ihsmfiles] = Cnsfilestat.filesize;
					break;
				case 'm':
					/* Suppose the file is of zero size ? We can catch it right now */
					if (rfio_stat(hsmfiles[ihsmfiles], &filemig_stat) < 0) {
						sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "rfio_stat", rfio_serror());
						c = USERR;
						setegid(0);
						seteuid(0);
						goto reply;
					}
					hsmmtimes[ihsmfiles] = filemig_stat.st_mtime;
					hsmsizes[ihsmfiles] = (u_signed64) filemig_stat.st_size;
					break;
				default:
					break;
				}
				setegid(0);
				seteuid(0);
				break;
			case STAGEOUT:
				/* Special case for CASTOR's HSM files : We check in advance if the file is not "busy" */
				if (stgreq.t_or_d == 'h') {
					memset(&(hsmfileids[ihsmfiles]),0,sizeof(struct Cns_fileid));
					if (Cns_statx(hsmfiles[ihsmfiles], &(hsmfileids[ihsmfiles]), &Cnsfilestat) == 0) {
						/* File already exist */
						strcpy(stgreq.u1.h.xfile,hsmfiles[ihsmfiles]);
						strcpy(stgreq.u1.h.server,hsmfileids[ihsmfiles].server);
						stgreq.u1.h.fileid = hsmfileids[ihsmfiles].fileid;
						switch (isstaged (&stgreq, &stcp, poolflag, stgreq.poolname)) {
							case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
							case STAGEPUT|CAN_BE_MIGR:
								/* And is busy with respect to our knowledge */
								sendrep (rpfd, MSG_ERR, STG37);
								c = EBUSY;
								goto reply;
							default:
								break;
						}
					}
				}
				/* There is intentionnaly no 'break' here */
			case STAGEWRT:
				setegid(stgreq.gid);
				seteuid(stgreq.uid);
				switch (stgreq.t_or_d) {
				case 'h':
					/* Overwriting an existing CASTOR HSM file is allowed (read/write mode) */
					if (Cns_creatx(hsmfiles[ihsmfiles], 0777 & ~ stgreq.mask, &(hsmfileids[ihsmfiles])) != 0) {
						sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_creatx", sstrerror(serrno));
						c = USERR;
						setegid(0);
						seteuid(0);
						goto reply;
					}
					hsmmtimes[ihsmfiles] = time(0);
					break;
				case 'm':
					/* Overwriting an existing non-CASTOR HSM file is not allowed */
					if (rfio_stat(hsmfiles[ihsmfiles], &filemig_stat) == 0) {
						sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "rfio_stat", "file already exists");
						c = USERR;
						setegid(0);
						seteuid(0);
						goto reply;
					}
					break;
				default:
					break;
				}
				setegid(0);
				seteuid(0);
				break;
			default:
				break;
			}
		}
	} else {
		if ((req_type == STAGEIN &&
				 ! Uflag && nbdskf > nbtpf && trailing != '-') ||
				(req_type != STAGEIN && nbtpf > nbdskf) ||
				(Uflag && nbdskf > 2)) {
			sendrep (rpfd, MSG_ERR, STG19);
			c = USERR;
			goto reply;
		}
		if (req_type == STAGEIN && ! Uflag && nbdskf > nbtpf) {
			fseq_list = (fseq_elem *) realloc (fseq_list, nbdskf * sizeof(fseq_elem));
			j = strtol ((char *)(fseq_list + nbtpf - 1), &dp, 10) + 1;
			for (i = nbtpf; i < nbdskf; i++, j++)
				sprintf ((char *)(fseq_list + i), "%d", j);
			nbtpf = nbdskf;
		}
		if (Uflag && nbdskf == 2)
			Upluspath = 1;
	}

	/* building catalog entries */

	ihsmfiles = -1;
	if (nhsmfiles > 0) {
		/* User either gave either no link name, either exactly nhsmfiles : we construct */
		/* anyway nhsmfiles entries...                                                   */
		nbdskf = nhsmfiles;
	}
	for (i = 0; i < nbdskf; i++) {
		ihsmfiles++;
		if (Uflag && i > 0) break;
		if (stgreq.t_or_d == 't') {
			if (fid) {
				if ((p = strchr (fid, ':')) != NULL) *p = '\0';
				if ((j = strlen (fid) - 17) > 0) fid += j;
				strcpy (stgreq.u1.t.fid, fid);
				if (p != NULL) {
					*p = ':';
					fid = p + 1;
				}
			}

			if (packfseq (fseq_list, i, nbdskf, nbtpf, trailing,
										stgreq.u1.t.fseq, CA_MAXFSEQLEN + 1)) {
				sendrep (rpfd, MSG_ERR, STG21);
				c = USERR;
				goto reply;
			}
#ifdef STAGER_DEBUG
			sendrep (rpfd, MSG_ERR, "[DEBUG] Packed fseq \"%s\"\n", stgreq.u1.t.fseq);
#endif
			if (concat_off_fseq > 0) {
				stgreq.filler[0] = 'c';
#ifdef STAGER_DEBUG
				sendrep (rpfd, MSG_ERR, "[DEBUG] Put 'c' in filler[0]\n");
#endif
			}
		} else if (stgreq.t_or_d == 'm') {
			strcpy(stgreq.u1.m.xfile,hsmfiles[ihsmfiles]);
		} else if (stgreq.t_or_d == 'h') {
			strcpy(stgreq.u1.h.xfile,hsmfiles[ihsmfiles]);
			strcpy(stgreq.u1.h.server,hsmfileids[ihsmfiles].server);
			stgreq.u1.h.fileid = hsmfileids[ihsmfiles].fileid;
		}
		if (nread) {
			if ((p = strchr (nread, ':')) != NULL) *p = '\0';
			stgreq.nread = strtol (nread, &dp, 10);
			if (p != NULL) {
				*p = ':';
				nread = p + 1;
			}
		}
		/* We have in hsmsizes[ihsmfiles] the actual_size in the NameServer    */
		/* If the user did not specified -s option (max bytes to transfer) we  */
		/* use this information.                                               */
		if (req_type == STAGEIN && nhsmfiles > 0 && ! size) {
			stgreq.size = (int) ((hsmsizes[ihsmfiles] > ONE_MB) ? (hsmsizes[ihsmfiles] / ONE_MB + 1) : 1);
		} else if (size) {
			if ((p = strchr (size, ':')) != NULL) *p = '\0';
			stgreq.size = strtol (size, &dp, 10);
			if (p != NULL) {
				*p = ':';
				size = p + 1;
			}
		}
		if (no_upath == 0)
			strcpy (upath, argv[optind+i]);

		switch (req_type) {
		case STAGEIN:
			switch (isstaged (&stgreq, &stcp, poolflag, stgreq.poolname)) {
			case STAGEIN:	/* stage in progress */
			case STAGEIN|WAITING_SPC:	/* waiting space */
				stcp->nbaccesses++;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqid = stcp->reqid;
				stcp = newreq ();
				memcpy (stcp, &stgreq, sizeof(stgreq));
				if (i > 0)
					stcp->reqid = nextreqid();
				else
					stcp->reqid = reqid;
				stcp->status = STAGEIN;
				stcp->c_time = time (0);
				stcp->a_time = stcp->c_time;
				stcp->nbaccesses++;
				stcp->status |= WAITING_REQ;
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				if (!wqp) {
					wqp = add2wq (clienthost, user,
									stcp->uid, stcp->gid, clientpid,
									Upluspath, reqid, req_type, nbdskf, &wfp, stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq);
					wqp->Aflag = Aflag;
					wqp->copytape = copytape;
#ifdef CONCAT_OFF
					wqp->concat_off_fseq = concat_off_fseq;
#endif
				}
				wfp->subreqid = stcp->reqid;
				wfp->waiting_on_req = savereqid;
				strcpy (wfp->upath, upath);
				strcpy (wqp->pool_user, pool_user);
				wqp->nb_waiting_on_req++;
				wqp->nbdskf++;
				wqp->nb_subreqs++;
				wfp++;
				if (Upluspath) {
					wfp->subreqid = stcp->reqid;
					strcpy (wfp->upath, argv[optind+1]);
				}
				break;
			case STAGED:		/* staged */
				if (rfio_stat (stcp->ipath, &st) < 0) {
					stglogit (func, STG02, stcp->ipath, "stat", rfio_serror());
					if (delfile (stcp, 0, 1, 1, "not on disk", 0, 0, 0) < 0) {
						sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
										 "rfio_unlink", rfio_serror());
						c = SYERR;
						goto reply;
					}
					goto notstaged;
				} else if (nhsmfiles > 0 && hsmmtimes[ihsmfiles] > stcp->a_time) {
					/*
					 * HSM File exists but has a modified time higher than what
					 * is known to the stager.
					 */
					if (delfile (stcp, 0, 1, 1, "mtime in nameserver > last access time", stgreq.uid, stgreq.gid, 0) < 0) {
						sendrep (rpfd, MSG_ERR,
										 STG02, stcp->ipath,
										 "rfio_unlink", rfio_serror());
						c = SYERR;
						goto reply;
					}
					goto notstaged;
				} else {
					/*
					 * File exists, now check to see if it is
					 * a partial file, and replace it if current
					 * request is for a larger size.
					 */
					if ((stcp->status == (STAGEIN|STAGED|STAGED_LSZ)) &&
							(stgreq.size > stcp->size)) {
						if (delfile (stcp, 0, 1, 1, "larger req", stgreq.uid, stgreq.gid, 0) < 0) {
							sendrep (rpfd, MSG_ERR,
											 STG02, stcp->ipath,
											 "rfio_unlink", rfio_serror());
							c = SYERR;
							goto reply;
						}
						goto notstaged;
					}
				}
			case STAGEWRT:
				if (stcp->t_or_d == 'h') {
					/* update access time in Cns */
					struct Cns_fileid Cnsfileid;
					strcpy (Cnsfileid.server, stcp->u1.h.server);
					Cnsfileid.fileid = stcp->u1.h.fileid;
					(void) Cns_setatime (NULL, &Cnsfileid);
				}
				stcp->a_time = time (0);
				stcp->nbaccesses++;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
#if SACCT
				stageacct (STGFILS, stgreq.uid, stgreq.gid,
									 clienthost, reqid, req_type, 0, 0, stcp, "");
#endif
				sendrep (rpfd, MSG_ERR, STG96,
								 strrchr (stcp->ipath, '/')+1,
								 stcp->actual_size,
								 (float)(stcp->actual_size)/(1024.*1024.),
								 stcp->nbaccesses);
				if (copytape)
					sendinfo2cptape (rpfd, stcp);
				if (*upath && strcmp (stcp->ipath, upath))
					create_link (stcp, upath);
				if (Upluspath &&
						strcmp (stcp->ipath, argv[optind+1]))
					create_link (stcp, argv[optind+1]);
				break;
			case STAGEOUT:
			case STAGEOUT|WAITING_SPC:
				sendrep (rpfd, MSG_ERR, STG37);
				c = USERR;
				goto reply;
			case STAGEOUT|CAN_BE_MIGR:
			case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
			case STAGEPUT|CAN_BE_MIGR:
				sendrep (rpfd, MSG_ERR, STG37);
				c = EBUSY;
				goto reply;
			notstaged:
			default:
				if (trailing == '-' && last_tape_file &&
						atoi (stgreq.u1.t.fseq) > last_tape_file) {
					/* requested file is not on tape */
					stglogit (func, "requested file is not on tape\n");
					nbdskf = i;
					continue;	/* exit from the loop */
				}
				stcp = newreq ();
				memcpy (stcp, &stgreq, sizeof(stgreq));
				if (i > 0)
					stcp->reqid = nextreqid();
				else
					stcp->reqid = reqid;
				stcp->status = STAGEIN;
				stcp->c_time = time (0);
				stcp->a_time = stcp->c_time;
				stcp->nbaccesses++;
				if (!wqp) {
					wqp = add2wq (clienthost, user,
									stcp->uid,
									stcp->gid,
									clientpid, Upluspath, reqid, req_type, nbdskf, &wfp, stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq);
					wqp->Aflag = Aflag;
					wqp->copytape = copytape;
#ifdef CONCAT_OFF
					wqp->concat_off_fseq = concat_off_fseq;
#endif
				}
				wfp->subreqid = stcp->reqid;
				strcpy (wfp->upath, upath);
				strcpy (wqp->pool_user, pool_user);
				if (! Aflag) {
					if ((c = build_ipath (upath, stcp, pool_user)) < 0) {
						stcp->status |= WAITING_SPC;
						strcpy (wqp->waiting_pool, stcp->poolname);
					} else if (c) {
						updfreespace (stcp->poolname, stcp->ipath,
													(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
						delreq (stcp,1);
						goto reply;
					}
				}
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				wqp->nbdskf++;
				wqp->nb_subreqs++;
				wfp++;
				if (Upluspath) {
					wfp->subreqid = stcp->reqid;
					strcpy (wfp->upath, argv[optind+1]);
				}
			}
			break;
		case STAGEOUT:
			switch (isstaged (&stgreq, &stcp, poolflag, stgreq.poolname)) {
			case NOTSTAGED:
				break;
			case STAGED:
				if (delfile (stcp, 0, 1, 1, user, stgreq.uid, stgreq.gid, 0) < 0) {
					sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
									 "rfio_unlink", rfio_serror());
					c = SYERR;
					goto reply;
				}
				break;
			/* Please note : If status is STAGEOUT|CAN_BE_MIGR|BEING_MIGR it is refused */
			/*               upper in this source */
			case STAGEOUT:
			case STAGEOUT|CAN_BE_MIGR:
			case STAGEOUT|WAITING_SPC:
				if (stgreq.t_or_d == 't' && *stgreq.u1.t.fseq == 'n') break;
				if (strcmp (user, stcp->user)) {
					sendrep (rpfd, MSG_ERR, STG37);
					c = USERR;
					goto reply;
				}
				if (delfile (stcp, 1, 1, 1, user, stgreq.uid, stgreq.gid, 0) < 0) {
					sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
									 "rfio_unlink", rfio_serror());
					c = SYERR;
					goto reply;
				}
				if ((stcp->status & (STAGEOUT|CAN_BE_MIGR)) == (STAGEOUT|CAN_BE_MIGR)) {
					/* This is a file to delete from automatic migration */
					update_migpool(stcp,-1,0);
				}
				break;
			default:
				sendrep (rpfd, MSG_ERR, STG37);
				c = USERR;
				goto reply;
			}
			stcp = newreq ();
			memcpy (stcp, &stgreq, sizeof(stgreq));
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = STAGEOUT;
			stcp->c_time = time ( 0);
			stcp->a_time = stcp->c_time;
			stcp->nbaccesses++;
			if ((c = build_ipath (upath, stcp, pool_user)) < 0) {
				stcp->status |= WAITING_SPC;
				if (!wqp) wqp = add2wq (clienthost, user,
						stcp->uid, stcp->gid, clientpid,
						Upluspath, reqid, req_type, nbdskf, &wfp, stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq);
				wfp->subreqid = stcp->reqid;
				strcpy (wfp->upath, upath);
				wqp->nbdskf++;
				wfp++;
				if (Upluspath) {
					wfp->subreqid = stcp->reqid;
					strcpy (wfp->upath, argv[optind+1]);
				}
				strcpy (wqp->pool_user, pool_user);
				strcpy (wqp->waiting_pool, stcp->poolname);
			} else if (c) {
				updfreespace (stcp->poolname, stcp->ipath,
							(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
				delreq (stcp,1);
				goto reply;
			} else {
				if (*upath && strcmp (stcp->ipath, upath))
					create_link (stcp, upath);
				if (Upluspath &&
						strcmp (stcp->ipath, argv[optind+1]))
					create_link (stcp, argv[optind+1]);
			}
#ifdef USECDB
			if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			break;
		case STAGEWRT:
			if (p = findpoolname (upath)) {
				if (poolflag < 0 ||
						(poolflag > 0 && strcmp (stgreq.poolname, p)))
					sendrep (rpfd, MSG_ERR, STG49, upath, p);
				actual_poolflag = 1;
				strcpy (actual_poolname, p);
			} else {
				if (poolflag > 0)
					sendrep (rpfd, MSG_ERR, STG50, upath);
				actual_poolflag = -1;
				actual_poolname[0] = '\0';
			}
			switch (isstaged (&stgreq, &stcp, actual_poolflag, actual_poolname)) {
			case NOTSTAGED:
				break;
			case STAGED:
				if (stcp->poolname[0] && strcmp (stcp->ipath, upath)) {
					if (delfile (stcp, 0, 1, 1, user, stgreq.uid, stgreq.gid, 0) < 0) {
						sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
										 "rfio_unlink", rfio_serror());
						c = SYERR;
						goto reply;
					}
				} else
					delreq (stcp,0);
				break;
			case STAGEOUT|PUT_FAILED:
			case STAGEOUT|PUT_FAILED|CAN_BE_MIGR:
				delreq (stcp,0);
				break;
			case STAGEWRT:
				if (stcp->t_or_d == 't' && *stcp->u1.t.fseq == 'n') break;
			default:
				sendrep (rpfd, MSG_ERR, STG37);
				c = USERR;
				goto reply;
			}
			if (stgreq.t_or_d == 'h') {
				if (rfio_stat (upath, &st) == 0) {
					u_signed64 correct_size;

					correct_size = (u_signed64) st.st_size;
#ifdef U1H_WRT_WITH_MAXSIZE
					if (stgreq.size && ((u_signed64) (stgreq.size * ONE_MB) < correct_size)) {
						/* If use specified a maxsize of bytes to transfer and if this */
						/* maxsize is lower than physical file size, then the size of */
						/* of the migrated file will be the minimum of the twos */
						correct_size = (u_signed64) (stgreq.size * ONE_MB);
					}
#endif
					/* We set the size in the name server */
					setegid(stgreq.gid);
					seteuid(stgreq.uid);
					if (Cns_setfsize(NULL,&(hsmfileids[ihsmfiles]),correct_size) != 0) {
						sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles],
							"Cns_setfsize", sstrerror(serrno));
						setegid(0);
						seteuid(0);
						c = SYERR;
						goto reply;
					}
					setegid(0);
					seteuid(0);
				} else {
					sendrep (rpfd, MSG_ERR, STG02, upath,
						"rfio_stat", rfio_serror());
					c = SYERR;
					goto reply;
				}
			}
			stcp = newreq ();
			memcpy (stcp, &stgreq, sizeof(stgreq));
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = STAGEWRT;
			strcpy (stcp->poolname, actual_poolname);
			if (stgreq.t_or_d != 'h') {
				if (rfio_stat (upath, &st) == 0) {
					stcp->actual_size = st.st_size;
					stcp->c_time = st.st_mtime;
				}
			} else {
				/* Already done before */
				stcp->actual_size = st.st_size;
				stcp->c_time = st.st_mtime;
			}
			stcp->a_time = time (0);
			strcpy (stcp->ipath, upath);
#ifdef USECDB
			if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (!wqp) wqp = add2wq (clienthost, user, stcp->uid,
									stcp->gid, clientpid, Upluspath, reqid, req_type,
									nbdskf, &wfp, stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq);
			wqp->copytape = copytape;
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			if (i < nbtpf || nhsmfiles > 0)
				wqp->nb_subreqs++;
			/* If it is CASTOR user files, then migration (explicit or not) is done under stage:st */
			/* -> StageIDflag set to 1 */
			if (nhsmfiles > 0) wqp->StageIDflag = (ncastorfiles > 0 && nuserlevel > 0) ? -1 : 0;
			wfp++;
			break;
		case STAGECAT:
			if (p = findpoolname (upath)) {
				if (poolflag < 0 ||
						(poolflag > 0 && strcmp (stgreq.poolname, p)))
					sendrep (rpfd, MSG_ERR, STG49, upath, p);
				actual_poolflag = 1;
				strcpy (actual_poolname, p);
			} else {
				if (poolflag > 0)
					sendrep (rpfd, MSG_ERR, STG50, upath);
				actual_poolflag = -1;
				actual_poolname[0] = '\0';
			}
			switch (isstaged (&stgreq, &stcp, actual_poolflag, actual_poolname)) {
			case NOTSTAGED:
				break;
			default:
				sendrep (rpfd, MSG_ERR, STG40);
				c = USERR;
				goto reply;
			}
			stcp = newreq ();
			memcpy (stcp, &stgreq, sizeof(stgreq));
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = STAGEIN | STAGED;
			strcpy (stcp->poolname, actual_poolname);
			if (rfio_stat (upath, &st) < 0) {
				sendrep (rpfd, MSG_ERR, STG02, upath, "rfio_stat",
								 rfio_serror());
				delreq (stcp,1);
				goto reply;
			}
			stcp->actual_size = st.st_size;
			stcp->c_time = st.st_mtime;
			stcp->a_time = st.st_atime;
			stcp->nbaccesses = 1;
			strcpy (stcp->ipath, upath);
#ifdef USECDB
			if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			break;
		}
	}
	savepath ();
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	if (fseq_list) free (fseq_list);
	free (argv);
	if (*(wqp->waiting_pool)) {
		wqp->nb_clnreq++;
		cleanpool (wqp->waiting_pool);
	} else if (wqp->nb_subreqs > wqp->nb_waiting_on_req)
		fork_exec_stager (wqp);
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmsizes != NULL) free(hsmsizes);
	if (hsmfileids != NULL) free(hsmfileids);
	if (hsmmtimes != NULL) free(hsmmtimes);
	return;
 reply:
	if (fseq_list) free (fseq_list);
	free (argv);
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmsizes != NULL) free(hsmsizes);
	if (hsmfileids != NULL) free(hsmfileids);
	if (hsmmtimes != NULL) free(hsmmtimes);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
						 reqid, req_type, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, req_type, c);
	if (c && wqp) {
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
			if (! wfp->waiting_on_req)
				updfreespace (stcp->poolname, stcp->ipath,
					(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
			delreq (stcp,0);
		}
		rmfromwq (wqp);
	}
}

void procputreq(req_data, clienthost)
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	int c, i;
	int clientpid;
	char *dp;
	int errflg = 0;
	char *externfile;
	int found;
	char fseq[CA_MAXFSEQLEN + 1];
	gid_t gid;
	struct group *gr;
	int Iflag = 0;
	int migrationflag = 0;
	int Mflag = 0;
	char *name;
	int nargs;
	int nbdskf;
	int numvid = 0;
	int n1 = 0;
	int n2 = 0;
	char *q;
	char *rbp;
	struct stat st;
	struct stgcat_entry *stcp;
	int subreqid;
	int Upluspath = 0;
	uid_t uid;
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;
	char vid[7];
	struct waitf *wfp;
	struct waitq *wqp;
	char **hsmfiles = NULL;
	struct stgcat_entry **hsmfilesstcp = NULL;
	int nhsmfiles = 0;
	int ihsmfiles;
	int jhsmfiles;
	int nhpssfiles = 0;
	int ncastorfiles = 0;
	int ntapefiles = 0;
	int ndiskfiles = 0;
	int nuserlevel = 0;
	int nexplevel = 0;
	extern struct passwd *stpasswd;             /* Generic uid/gid stage:st */

	rbp = req_data;
	unmarshall_STRING (rbp, user);  /* login name */
	unmarshall_STRING (rbp, name);
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	unmarshall_WORD (rbp, clientpid);

	stglogit (func, STG92, "stageput", user, uid, gid, clienthost);
	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
						 reqid, STAGEPUT, 0, 0, NULL, "");
#endif

	wqp = NULL;
	if ((gr = Cgetgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
		goto reply;
	}

#ifdef linux
	optind = 0;
#else
	optind = 1;
#endif
	while ((c = getopt (nargs, argv, "Gh:I:mM:q:U:V:")) != EOF) {
		switch (c) {
		case 'G':
			break;
		case 'h':
			break;
		case 'I':
			externfile = optarg;
			Iflag = 1;
			break;
		case 'm':
			migrationflag = 1;
			break;
		case 'M':
			Mflag = 1;
			if (nhsmfiles == 0) {
				if ((hsmfiles = (char **) malloc(sizeof(char *))) == NULL ||
                    (hsmfilesstcp = (struct stgcat_entry **) malloc(sizeof(struct stgcat_entry *))) == NULL) {
					c = SYERR;
					goto reply;
				}
			} else {
				char **dummy = hsmfiles;
				struct stgcat_entry **dummy2 = hsmfilesstcp;
				if ((dummy = (char **) realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
					c = SYERR;
					goto reply;
				}
				hsmfiles = dummy;
				if ((dummy2 = (struct stgcat_entry **) realloc(hsmfilesstcp,(nhsmfiles+1) * sizeof(struct stgcat_entry *))) == NULL) {
					c = SYERR;
					goto reply;
				}
				hsmfilesstcp = dummy2;
			}
			hsmfiles[nhsmfiles++] = optarg;
			break;
		case 'q':       /* file sequence number(s) */
			if ((q = strchr (optarg, '-')) != NULL) {
				*q = '\0';
				n2 = strtol (q + 1, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				n1 = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				*q = '-';
			} else {
				n1 = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				n2 = n1;
			}
			break;
		case 'V':       /* visual identifier */
			strcpy (vid, optarg);
			UPPER (vid);
			numvid++;
			break;
		}
	}
	if (Iflag && numvid)
		errflg++;

	if (Mflag && numvid)
		errflg++;

	if ((Iflag || Mflag) && (optind != nargs))
		errflg++;

	if (migrationflag && ! Mflag) {
		/* -m option can be used only with -M option */
		sendrep (rpfd, MSG_ERR, "STG35 - option -m requires option -M\n");
		errflg++;
	}

	if (errflg) {
		c = USERR;
		goto reply;
	}

	/* In case of hsm request verify the exact mapping between number of hsm files */
	/* and number of disk files.                                                   */
	if (nhsmfiles > 0) {
		/* We also check that there is NO redundant hsm files (multiple equivalent ones) */
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			for (jhsmfiles = ihsmfiles + 1; jhsmfiles < nhsmfiles; jhsmfiles++) {
				if (strcmp(hsmfiles[ihsmfiles],hsmfiles[jhsmfiles]) == 0) {
					sendrep (rpfd, MSG_ERR, "Duplicate HSM file %s\n",hsmfiles[ihsmfiles]);
					c = USERR;
					goto reply;
				}
			}
		}
		/* We check that user do not mix different hsm types (hpss and castor) */
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			if (ISHPSS(hsmfiles[ihsmfiles])) {
				++nhpssfiles;
			}
			if (ISCASTOR(hsmfiles[ihsmfiles])) {
				++ncastorfiles;
                /* And we take the opportunity to decide which level of migration (user/exp) */
				if (isuserlevel(hsmfiles[ihsmfiles])) {
					nuserlevel++;
        		} else {
          			nexplevel++;
        		}
			}
		}
		/* No recognizes type ? */
		if (nhpssfiles == 0 && ncastorfiles == 0) {
			sendrep (rpfd, MSG_ERR, "Cannot determine HSM file types (HPSS nor CASTOR)\n");
			c = USERR;
			goto reply;
		}
		/* Mixed CASTOR types ? */
		if (ncastorfiles > 0 && nuserlevel > 0 && nexplevel > 0) {
			sendrep (rpfd, MSG_ERR, "Mixing user-level and experiment-level CASTOR files is not allowed\n");
			c = USERR;
			goto reply;
		}
		/* At least one HPSS and one CASTOR */
		if (nhpssfiles >  0 && ncastorfiles >  0) {
			sendrep (rpfd, MSG_ERR, "HPSS (%d occurence%s) and CASTOR (%d occurence%s) files on the same command-line is not allowed\n",nhpssfiles,nhpssfiles > 1 ? "s" : "",ncastorfiles,ncastorfiles > 1 ? "s" : "");
			c = USERR;
			goto reply;
		}
	}

	if (numvid) {
		if (n1 == 0)
			n2 = n1 = 1;
		nbdskf = n2 - n1 + 1;
		for (i = n1; i <= n2; i++) {
			sprintf (fseq, "%d", i);
			found = 0;
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if (stcp->t_or_d != 't') continue;
				if (strcmp (stcp->u1.t.vid[0], vid) != 0) continue;
				if (strcmp (stcp->u1.t.fseq, fseq)) continue;
				found = 1;
				break;
			}
			if (found == 0 ||
					(stcp->status != STAGEOUT &&
					 stcp->status != (STAGEOUT|PUT_FAILED))) {
				sendrep (rpfd, MSG_ERR, STG22);
				c = USERR;
				goto reply;
			}
			if (stcp->status == STAGEOUT) {
				if (rfio_stat (stcp->ipath, &st) == 0)
					stcp->actual_size = st.st_size;
				updfreespace (stcp->poolname, stcp->ipath,
					(signed64) (((signed64) stcp->size * (signed64) ONE_MB) - (signed64) stcp->actual_size));
			}
			stcp->status = STAGEPUT;
			stcp->a_time = time (0);
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (!wqp) wqp = add2wq (clienthost, user, uid, gid,
									clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp, NULL, NULL);
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		}
	} else if (Mflag || Iflag) {
		nbdskf = Mflag ? nhsmfiles : 1;
		found = 0;
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (Mflag) {
				if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') continue;
				if (stcp->t_or_d != 'm' && nhpssfiles > 0) continue;
				if (stcp->t_or_d != 'h' && ncastorfiles > 0) continue;
				/* It can happen that we get twice the same HSM filename on the same stageput */
				/* Neverthless the first one cannot be catched a second time because it will have */
				/* to fulfill also the correction condition on its current status */
				if ((stcp->status & 0xF) == STAGEOUT ||
					(stcp->status & (STAGEOUT|PUT_FAILED)) == (STAGEOUT|PUT_FAILED)) {
					for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
						if (strcmp (stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile, hsmfiles[ihsmfiles]) != 0)
							continue;
						hsmfilesstcp[found++] = stcp;
						break;
					}
					if (found == nhsmfiles) break;
				}
			} else {
				if (stcp->t_or_d != 'd') continue;
				if (strcmp (stcp->u1.d.xfile, externfile) != 0) continue;
				found = 1;
				break;
			}
		}
		if (Iflag) {
			if (found == 0 ||
					(stcp->status != STAGEOUT &&
					 stcp->status != (STAGEOUT|PUT_FAILED))) {
				sendrep (rpfd, MSG_ERR, STG22);
				c = USERR;
				goto reply;
			}
			if (stcp->status == STAGEOUT) {
				if (rfio_stat (stcp->ipath, &st) == 0)
					stcp->actual_size = st.st_size;
				updfreespace (stcp->poolname, stcp->ipath,
					(signed64) (((signed64) stcp->size * (signed64) ONE_MB) - (signed64) stcp->actual_size));
			}
			stcp->status = STAGEPUT;
			stcp->a_time = time (0);
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (!wqp) wqp = add2wq (clienthost, user, uid, gid,
									clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp, NULL, NULL);
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		} else {
			uid_t uid_waitq;
			gid_t gid_waitq;
			char *user_waitq;
			char *user_stage = "stage";

			if (found != nhsmfiles) {
				sendrep (rpfd, MSG_ERR, "STG02 - You requested %d hsm files while I found %d of them that you currently can migrate\n",nhsmfiles,found);
				c = USERR;
				goto reply;
			}

			/* It it is CASTOR's user-level files, we change requester (uid,gid) to be stage:st */
			if (ncastorfiles > 0 && nuserlevel > 0) {
				uid_waitq = stpasswd->pw_uid;
				gid_waitq = stpasswd->pw_gid;
 				user_waitq = user_stage;
			} else {
				uid_waitq = uid;
				gid_waitq = gid;
				user_waitq = user;
			}

			for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
				/* If migration flag is set, then status must have flag CAN_BE_MIGR */
				/* If not, then status must not have flag CAN_BE_MIGR */
				if ((migrationflag != 0 && (hsmfilesstcp[ihsmfiles]->status & CAN_BE_MIGR) != CAN_BE_MIGR) ||
					(migrationflag == 0 && (hsmfilesstcp[ihsmfiles]->status & CAN_BE_MIGR) == CAN_BE_MIGR)) {
					sendrep(rpfd, MSG_ERR, STG33, hsmfiles[ihsmfiles],migrationflag ? "must have CAN_BE_MIGR state (automatic migration)\n" : "is already flagged for the automatic migration\n");
					c = USERR;
					goto reply;
				}
				if ((hsmfilesstcp[ihsmfiles]->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
					hsmfilesstcp[ihsmfiles]->status = STAGEPUT|CAN_BE_MIGR;
					hsmfilesstcp[ihsmfiles]->a_time = time (0);
				} else {
					int save_status;

					/* We make upd_stageout believe that it is a normal stageput following a stageout */
					save_status = hsmfilesstcp[ihsmfiles]->status;
					hsmfilesstcp[ihsmfiles]->status = STAGEOUT;
					if (c = upd_stageout (STAGEUPDC, hsmfilesstcp[ihsmfiles]->ipath, &subreqid, 0, NULL)) {
						hsmfilesstcp[ihsmfiles]->status = save_status;
						goto reply;
					}
					hsmfilesstcp[ihsmfiles]->status = STAGEPUT;
				}
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,hsmfilesstcp[ihsmfiles]) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				if (!wqp) wqp = add2wq (clienthost, user_waitq, uid_waitq, gid_waitq,
										clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp, NULL, NULL);
				wfp->subreqid = hsmfilesstcp[ihsmfiles]->reqid;
				wqp->nbdskf++;
				wqp->nb_subreqs++;
				/* If it is CASTOR user files, then migration (explicit or not) is done under stage:st */
				/* -> StageIDflag set to 1 */
				if (nhsmfiles > 0) wqp->StageIDflag = (ncastorfiles > 0 && nuserlevel > 0) ? (migrationflag != 0 ? 1 : -1) : 0;
				wfp++;
			}
		}
	} else {
		struct stgcat_entry *found_stcp;

		/* compute number of disk files */
		nbdskf = nargs - optind;

		/* We verify that, if there is CASTOR HSM files, there is NO mixing between userlevel and explevel ones */
		/* and, if there are also other types of files, there is NO userlevel CASTOR's ones. */
		for (i = 0; i < nbdskf; i++) {
			strcpy (upath, argv[optind+i]);
			if (c = ask_stageout (STAGEPUT, upath, &found_stcp))
				goto reply;
			switch (found_stcp->t_or_d) {
			case 'm':
			case 'h':
				switch (found_stcp->t_or_d) {
				case 'm':
					++nhpssfiles;
					break;
				case 'h':
					++ncastorfiles;
					if ((found_stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
						sendrep(rpfd, MSG_ERR, STG33, upath, "is already flagged for the automatic migration\n");
						c = USERR;
						goto reply;
					}
	                /* We take the opportunity to decide which level of migration (user/exp) */
					if (isuserlevel(found_stcp->u1.h.xfile)) {
						nuserlevel++;
        			} else {
          				nexplevel++;
	        		}
					break;
				}
                /* We will later remind the corresponding stcp */
				if (nhsmfiles == 0) {
					if ((hsmfilesstcp = (struct stgcat_entry **) malloc(sizeof(struct stgcat_entry *))) == NULL) {
						c = SYERR;
						goto reply;
					}
				} else {
					struct stgcat_entry **dummy2 = hsmfilesstcp;
					if ((dummy2 = (struct stgcat_entry **) realloc(hsmfilesstcp,(nhsmfiles+1) * sizeof(struct stgcat_entry *))) == NULL) {
						c = SYERR;
						goto reply;
					}
					hsmfilesstcp = dummy2;
				}
				hsmfilesstcp[nhsmfiles++] = found_stcp;
				break;
			case 't':
				++ntapefiles;
				break;
			case 'd':
				++ndiskfiles;
				break;
			default:
				sendrep (rpfd, MSG_ERR, STG33, upath, "unknown type");
				c = USERR;
				goto reply;
			}
		}
		/* The stager branch is the following : */
		/* if ('d' or 'm') then                 */
		/*   filecopy (through rfio)            */
		/* else                                 */
		/*   if ('t') then                      */
		/*     tape_to_tape                     */
		/*   else             -- then it is 'h' */
		/*     castor_migration                 */
		/*   endif                              */
		/* endif                                */

		/* In the following we make sure that   */
		/* those conditions are fullfilled at   */
		/* fork_exec time.                      */

		/* It is not allowed to mix ('d' or 'm') with other types */
		if ((ndiskfiles > 0 || nhpssfiles > 0) && (ntapefiles > 0 || ncastorfiles > 0)) {
			sendrep (rpfd, MSG_ERR, "STG02 - Mixing ('tape' or 'non-CASTOR') files with other types is not allowed\n");
			c = USERR;
			goto reply;
		}
		/* It is not allowed to mix 't' with 'h' type */
		if (ntapefiles > 0 && ncastorfiles > 0) {
			sendrep (rpfd, MSG_ERR, "STG02 - Mixing 'tape' files with 'CASTOR' files is not allowed\n");
			c = USERR;
			goto reply;
		}
		/* CASTOR files ? */
		if (ncastorfiles > 0) {
			/* Mixed CASTOR types */
			if (nuserlevel > 0 && nexplevel > 0) {
				sendrep (rpfd, MSG_ERR, "STG02 - Mixing user-level and experiment-level CASTOR files is not allowed\n");
				c = USERR;
				goto reply;
			}
			/* Simulate a STAGEUPDC call on all CASTOR HSM files */
			for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
				int save_status;

				/* We make upd_stageout believe that it is a normal stageput following a stageupdc followinga stageout */
				/* This will force Cns_setfsize to be called. */
				save_status = hsmfilesstcp[ihsmfiles]->status;
				hsmfilesstcp[ihsmfiles]->status = STAGEOUT;
				if (c = upd_stageout (STAGEUPDC, hsmfilesstcp[ihsmfiles]->ipath, &subreqid, 0, hsmfilesstcp[ihsmfiles])) {
					hsmfilesstcp[ihsmfiles]->status = save_status;
					goto reply;
				}
				hsmfilesstcp[ihsmfiles]->status = STAGEPUT;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,hsmfilesstcp[ihsmfiles]) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
			}
		}

		for (i = 0; i < nbdskf; i++) {
			strcpy (upath, argv[optind+i]);
			if (ncastorfiles <= 0) {
				/* We call upd_stageout as normal for all files except CASTOR HSM ones */
				if (c = upd_stageout (STAGEPUT, upath, &subreqid, 0, NULL))
					goto reply;
			}
			if (!wqp) wqp = add2wq (clienthost, user, uid, gid,
									clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp, NULL, NULL);
			wfp->subreqid = subreqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			if (nuserlevel > 0) wqp->StageIDflag = (migrationflag != 0 ? 1 : -1);
			wfp++;
		}
	}
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	free (argv);
	fork_exec_stager (wqp);
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmfilesstcp != NULL) free(hsmfilesstcp);
	return;
 reply:
	free (argv);
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmfilesstcp != NULL) free(hsmfilesstcp);
#if SACCT
	stageacct (STGCMDC, uid, gid, clienthost,
						 reqid, STAGEPUT, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, STAGEPUT, c);
	if (c && wqp) {
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
			if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
				update_migpool(stcp,-1,-1);
				stcp->status = STAGEOUT|PUT_FAILED|CAN_BE_MIGR;
			} else {
				stcp->status = STAGEOUT|PUT_FAILED;
			}
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
		}
		rmfromwq (wqp);
	}
}

int
isstaged(cur, p, poolflag, poolname)
		 struct stgcat_entry *cur;
		 struct stgcat_entry **p;
		 int poolflag;
		 char *poolname;
{
	int found = 0;
	int thisfseq;
	int i;
	struct stgcat_entry *stcp;

	last_tape_file = 0;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		/* if ((stcp->status & WAITING_REQ) == WAITING_REQ) continue; */
		if (cur->t_or_d != stcp->t_or_d) continue;
		/* if no pool specified, the file may reside in any pool */
		if (poolflag == 0) {
			if (stcp->poolname[0] == '\0') continue;
		} else
			/* if a specific pool was requested, the file must be there */
			if (poolflag > 0) {
				if (strcmp (poolname, stcp->poolname)) continue;
			} else
				/* if -p NOPOOL, the file should not reside in a pool */
				if (poolflag < 0) {
					if (stcp->poolname[0]) continue;
				}
		if (cur->t_or_d == 't') {
			for (i = 0; i < MAXVSN; i++)
				if (strcmp (cur->u1.t.vid[i], stcp->u1.t.vid[i])) break;
			if (i < MAXVSN) continue;
			for (i = 0; i < MAXVSN; i++)
				if (strcmp (cur->u1.t.vsn[i], stcp->u1.t.vsn[i])) break;
			if (i < MAXVSN) continue;
			if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) continue;
			if (cur->u1.t.fseq[0] != 'u') {
				if ((stcp->status & 0xF000) == LAST_TPFILE)
					last_tape_file = atoi (stcp->u1.t.fseq);
				if (stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-' &&
					stcp->filler[0] == 'c' &&
					atoi(stcp->u1.t.fseq) <= atoi(cur->u1.t.fseq)) {
					/* If it has a trailing '-' && it is a concat_off && fseq <= cur one */
					/* then we found it. */
					found = 1;
					break;
				} else if (strcmp (cur->u1.t.fseq, stcp->u1.t.fseq)) {
					continue;
				}
			} else {
				if (strcmp (cur->u1.t.fid, stcp->u1.t.fid)) continue;
			}
		} else if (cur->t_or_d == 'm') {
			if (strcmp (cur->u1.m.xfile, stcp->u1.m.xfile)) continue;
		} else if (cur->t_or_d == 'h') {
			if (strcmp (cur->u1.h.server, stcp->u1.h.server) != 0 || cur->u1.h.fileid != stcp->u1.h.fileid) continue;
			/* Invariants already exist and the are same - was this file renamed ? */
			if (strcmp (cur->u1.h.xfile, stcp->u1.h.xfile)) {
				sendrep(rpfd, MSG_ERR, STG101, cur->u1.h.xfile, stcp->u1.h.xfile);
				strcpy(stcp->u1.h.xfile,cur->u1.h.xfile);
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs ();
			}
		} else {
			if (strcmp (cur->u1.d.xfile, stcp->u1.d.xfile)) continue;
		}
		found = 1;
		break;
	}
	if (! found) {
		return (NOTSTAGED);
	} else {
		*p = stcp;
		if ((stcp->status & 0xF0) == STAGED) {
			return (STAGED);
		} else {
			return (stcp->status);
		}
	}
}

int
maxfseq_per_vid(cur, poolflag, poolname, concat_off_flag)
		 struct stgcat_entry *cur;
		 int poolflag;
		 char *poolname;
		 char *concat_off_flag;
{
	int found = 0;
	int i;
	struct stgcat_entry *stcp;
	int result = 0;

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if (stcp->t_or_d != 't') continue;
		/* if no pool specified, the file may reside in any pool */
		if (poolflag == 0) {
			if (stcp->poolname[0] == '\0') continue;
		} else
			/* if a specific pool was requested, the file must be there */
			if (poolflag > 0) {
				if (strcmp (poolname, stcp->poolname)) continue;
			} else
				/* if -p NOPOOL, the file should not reside in a pool */
				if (poolflag < 0) {
					if (stcp->poolname[0]) continue;
				}
		for (i = 0; i < MAXVSN; i++)
			if (strcmp (cur->u1.t.vid[i], stcp->u1.t.vid[i])) break;
		if (i < MAXVSN) continue;
		for (i = 0; i < MAXVSN; i++)
			if (strcmp (cur->u1.t.vsn[i], stcp->u1.t.vsn[i])) break;
		if (i < MAXVSN) continue;
		if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) continue;
		if ((found = atoi(stcp->u1.t.fseq)) < result) continue;
		*concat_off_flag = stcp->filler[0];
		result = found;
	}
	return(result);
}

unpackfseq(fseq, req_type, trailing, fseq_list, concat_off, concat_off_fseq)
		 char *fseq;
		 int req_type;
		 char *trailing;
		 fseq_elem **fseq_list;
		 int concat_off;
		 int *concat_off_fseq;
{
	char *dp;
	int i;
	int n1, n2;
	int nbtpf;
	char *p, *q;
	int lasttpf = 0;
	int have_qn = 0;
	int have_qu = 0;

	*trailing = *(fseq + strlen (fseq) - 1);
	if (*trailing == '-') {
		if (req_type != STAGEIN) {
			sendrep (rpfd, MSG_ERR, STG18);
			return (0);
		}
		*(fseq + strlen (fseq) - 1) = '\0';
	}
	switch (*fseq) {
	case 'n':
		if (req_type == STAGEIN) {
			sendrep (rpfd, MSG_ERR, STG17, "-qn", "stagein");
			return (0);
		}
		++have_qn;
	case 'u':
		if (strlen (fseq) == 1) {
			nbtpf = 1;
		} else {
			nbtpf = strtol (fseq + 1, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-q");
				return (0);
			}
		}
		*fseq_list = (fseq_elem *) calloc (nbtpf, sizeof(fseq_elem));
		for (i = 0; i < nbtpf; i++) {
			sprintf ((char *)(*fseq_list + i), "%c", *fseq);
			lasttpf = atoi((char *)(*fseq_list + i));
		}
		++have_qu;
		break;
	default:
		nbtpf = 0;
		p = strtok (fseq, ",");
		while (p != NULL) {
			if ((q = strchr (p, '-')) != NULL) {
				*q = '\0';
				n2 = strtol (q + 1, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				n1 = strtol (p, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				*q = '-';
			} else {
				n1 = strtol (p, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				n2 = n1;
			}
			if (n1 <= 0 || n2 < n1) {
				sendrep (rpfd, MSG_ERR, STG06, "-q");
				return (0);
			}
			nbtpf += n2 - n1 + 1;
			if ((p = strtok (NULL, ",")) != NULL) *(p - 1) = ',';
		}
		*fseq_list = (fseq_elem *) calloc (nbtpf, sizeof(fseq_elem));
		nbtpf = 0;
		p = strtok (fseq, ",");
		while (p != NULL) {
			if ((q = strchr (p, '-')) != NULL) {
				*q = '\0';
				n2 = strtol (q + 1, &dp, 10);
				n1 = strtol (p, &dp, 10);
				*q = '-';
			} else {
				n1 = strtol (p, &dp, 10);
				n2 = n1;
			}
			for (i = n1; i <= n2; i++, nbtpf++) {
				sprintf ((char *)(*fseq_list + nbtpf), "%d", i);
				lasttpf = i;
			}
			p = strtok (NULL, ",");
		}
	}
	/* If the last entry is a '-' and concat_off flag is set and there is at least one fseq identified */
	if (nbtpf > 0 && *trailing == '-' && concat_off) {
		if (concat_off_fseq == NULL) {
			sendrep (rpfd, MSG_ERR, "unpackfseq : Internal error : concat_off is ON and concat_off_fseq == NULL\n");
			return (0);
		}
		/* The fseq just before the '-' is the last element of the fseq_elem list */
		if ((*concat_off_fseq = lasttpf) <= 0) {
			sendrep (rpfd, MSG_ERR, "unpackfseq : Internal error : concat_off is ON and nbtpf > 0 and *trailing == '-' and atoi(fseq_list[nbtpf-1]) <= 0\n");
			free(*fseq_list);
			*fseq_list = NULL;
			return (0);
		}
	}
	if (have_qu == 0 && have_qn == 0) {
		/* We verify the fseq list (if more than one element) */
		i = 1;
 retry_verif:
		for (; i < nbtpf; i++) {
			if (strcmp((char *)(*fseq_list + i),(char *)(*fseq_list + i - 1)) == 0) {
				int j, new_nbtpf;
				fseq_elem *new_fseq_list;

				if ((new_fseq_list = (fseq_elem *) calloc (nbtpf - 1, sizeof(fseq_elem))) == NULL) {
					sendrep (rpfd, MSG_ERR, "STG02 - calloc error (%s)\n", strerror(errno));
					free(*fseq_list);
					return(-1);
				}
				sendrep (rpfd, MSG_ERR, "STG02 - Duplicated file sequence %s reduced by one\n", (char *)(*fseq_list + i));
				new_nbtpf = 0;
				for (j = 0; j < nbtpf; j++) {
					if (j == i) continue;
					strcpy((char *)(new_fseq_list + new_nbtpf++),(char *)(*fseq_list + j));
				}
				free(*fseq_list);
				*fseq_list = new_fseq_list;
				nbtpf = new_nbtpf;
				goto retry_verif;
			}
		}
	}
	return (nbtpf);
}
