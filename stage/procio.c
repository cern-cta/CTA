/*
 * $Id: procio.c,v 1.18 2000/04/14 13:54:05 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procio.c,v $ $Revision: 1.18 $ $Date: 2000/04/14 13:54:05 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
struct waitq *add2wq();
char *findpoolname();
int last_tape_file;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif

/* This macro returns TRUE is the file is an hpss one */
#define ISHPSS(xfile)   ( strncmp (xfile, "/hpss/"  , 6) == 0 || strstr (xfile, ":/hpss/"  ) != NULL)

/* This macro returns TRUE is the file is an castor one */
#define ISCASTOR(xfile) ( strncmp (xfile, "/castor/", 8) == 0 || strstr (xfile, ":/castor/") != NULL)

void procioreq _PROTO((int, char *, char *));
void procputreq _PROTO((char *, char *));

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
	int clientpid;
	static char cmdnames[4][9] = {"", "stagein", "stageout", "stagewrt"};
	int copytape = 0;
	char *dp;
	int errflg = 0;
	char *fid = NULL;
	struct stat filemig_stat;
	char filemig_size[5];
	char *fseq = NULL;
	fseq_elem *fseq_list = NULL;
	struct group *gr;
	char *name;
	int nargs;
	int nbdskf;
	int nbtpf;
	struct stgcat_entry *newreq();
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
	char **hsmfiles = NULL;
	int nhsmfiles = 0;
	int ihsmfiles;
	int jhsmfiles;

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
	if (getpwuid (stgreq.uid) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", stgreq.uid);
		sendrep (rpfd, MSG_ERR, STG11, uidstr);
		c = SYERR;
		goto reply;
	}
	if ((gr = getgrgid (stgreq.gid)) == NULL) {
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
				if ((hsmfiles = (char **) malloc(sizeof(char *))) == NULL) {
					c = SYERR;
					goto reply;
				}
			} else {
				char **dummy = hsmfiles;
				if ((dummy = (char **) realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
					c = SYERR;
					goto reply;
				}
				hsmfiles = dummy;
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
				if (*dp != '\0' || j > 2047) {
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
	if (concat_off && strcmp (stgreq.poolname, "NOPOOL") == 0) {
		sendrep (rpfd, MSG_ERR, STG17, "-c off", "-p NOPOOL");
		errflg++;
	}
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
		if (fseq == NULL) fseq = "1";

		/* compute number of tape files */
		if ((nbtpf = unpackfseq (fseq, req_type, &trailing, &fseq_list)) == 0)
			errflg++;
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
			int nhpssfiles = 0;
			int ncastorfiles = 0;

		if (nbdskf != nhsmfiles) {
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
						++nhpssfiles;
					}
					if (ISCASTOR(hsmfiles[ihsmfiles])) {
						++ncastorfiles;
					}
				}
				/* No recognizes type ? */
				if (nhpssfiles == 0 && ncastorfiles == 0) {
					sendrep (rpfd, MSG_ERR, "Cannot determine HSM file types (HPSS nor CASTOR)\n");
					c = USERR;
					goto reply;
				}
				/* At least one HPSS and one CASTOR */
				if (nhpssfiles >  0 && ncastorfiles >  0) {
					sendrep (rpfd, MSG_ERR, "HPSS (%d occurence%s) and CASTOR (%d occurence%s) files on the same command-line is not allowed\n",nhpssfiles,nhpssfiles > 1 ? "s" : "",ncastorfiles,ncastorfiles > 1 ? "s" : "");
					c = USERR;
					goto reply;
				}
				/* More than one HPSS not supported for the moment */
				if (nhpssfiles > 1) {
					sendrep (rpfd, MSG_ERR, "More than one HPSS (%d occurence%s) files on the same command-line is not allowed\n",nhpssfiles,nhpssfiles > 1 ? "s" : "");
					c = USERR;
					goto reply;
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

	ihsmfiles = 0;
	for (i = 0; i < nbdskf; i++) {
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
		} else if (stgreq.t_or_d == 'm') {
			strcpy(stgreq.u1.m.xfile,hsmfiles[ihsmfiles++]);
		}
		if (nread) {
			if ((p = strchr (nread, ':')) != NULL) *p = '\0';
			stgreq.nread = strtol (nread, &dp, 10);
			if (p != NULL) {
				*p = ':';
				nread = p + 1;
			}
		}
		if (size) {
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
					sendrep(rpfd, MSG_ERR, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
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
					sendrep(rpfd, MSG_ERR, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				if (!wqp) {
					wqp = add2wq (clienthost, user,
												stcp->uid, stcp->gid, clientpid,
												Upluspath, reqid, req_type, nbdskf, &wfp);
					wqp->Aflag = Aflag;
					wqp->copytape = copytape;
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
				stcp->a_time = time (0);
				stcp->nbaccesses++;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					sendrep(rpfd, MSG_ERR, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
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
												stcp->uid, stcp->gid, clientpid,
												Upluspath, reqid, req_type, nbdskf, &wfp);
					wqp->Aflag = Aflag;
					wqp->copytape = copytape;
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
													stcp->size*1024*1024);
						delreq (stcp,1);
						goto reply;
					}
				}
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					sendrep(rpfd, MSG_ERR, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
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
			case STAGEOUT:
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
			stcp->c_time = time (0);
			stcp->a_time = stcp->c_time;
			stcp->nbaccesses++;
			if ((c = build_ipath (upath, stcp, pool_user)) < 0) {
				stcp->status |= WAITING_SPC;
				if (!wqp) wqp = add2wq (clienthost, user,
																stcp->uid, stcp->gid, clientpid,
																Upluspath, reqid, req_type, nbdskf, &wfp);
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
											stcp->size*1024*1024);
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
				sendrep(rpfd, MSG_ERR, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
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
				delreq (stcp,0);
				break;
			case STAGEWRT:
				if (stcp->t_or_d == 't' && *stcp->u1.t.fseq == 'n') break;
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
			stcp->status = STAGEWRT;
			strcpy (stcp->poolname, actual_poolname);
			if (rfio_stat (upath, &st) == 0) {
				stcp->actual_size = st.st_size;
				stcp->c_time = st.st_mtime;
			}
			stcp->a_time = time (0);
			strcpy (stcp->ipath, upath);
#ifdef USECDB
			if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
				sendrep(rpfd, MSG_ERR, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (!wqp) wqp = add2wq (clienthost, user, stcp->uid,
															stcp->gid, clientpid, Upluspath, reqid, req_type,
															nbdskf, &wfp);
			wqp->copytape = copytape;
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			if (i < nbtpf || nhsmfiles > 0)
				wqp->nb_subreqs++;
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
				sendrep(rpfd, MSG_ERR, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
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
	return;
 reply:
	if (fseq_list) free (fseq_list);
	free (argv);
	if (hsmfiles != NULL) free(hsmfiles);
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
											stcp->size*1024*1024);
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
	char *hsmfile;
	int Iflag = 0;
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
	if ((gr = getgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
		goto reply;
	}
#ifdef linux
	optind = 0;
#else
	optind = 1;
#endif
	while ((c = getopt (nargs, argv, "Gh:I:M:q:U:V:")) != EOF) {
		switch (c) {
		case 'G':
			break;
		case 'h':
			break;
		case 'I':
			externfile = optarg;
			Iflag = 1;
			break;
		case 'M':
			hsmfile = optarg;
			Mflag = 1;
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

	if (errflg) {
		c = USERR;
		goto reply;
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
											stcp->size*1024*1024 - (int)stcp->actual_size);
			}
			stcp->status = STAGEPUT;
			stcp->a_time = time (0);
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				sendrep(rpfd, MSG_ERR, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (!wqp) wqp = add2wq (clienthost, user, uid, gid,
															clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp);
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		}
	} else if (Mflag || Iflag) {
		nbdskf = 1;
		found = 0;
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (Mflag) {
				if (stcp->t_or_d != 'm') continue;
				if (strcmp (stcp->u1.m.xfile, hsmfile) != 0) continue;
			} else {
				if (stcp->t_or_d != 'd') continue;
				if (strcmp (stcp->u1.d.xfile, externfile) != 0) continue;
			}
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
										stcp->size*1024*1024 - (int)stcp->actual_size);
		}
		stcp->status = STAGEPUT;
		stcp->a_time = time (0);
#ifdef USECDB
		if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
			sendrep(rpfd, MSG_ERR, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		if (!wqp) wqp = add2wq (clienthost, user, uid, gid,
														clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp);
		wfp->subreqid = stcp->reqid;
		wqp->nbdskf++;
		wqp->nb_subreqs++;
		wfp++;
	} else {
		/* compute number of disk files */
		nbdskf = nargs - optind;

		for (i = 0; i < nbdskf; i++) {
			strcpy (upath, argv[optind+i]);
			if (c = upd_stageout (STAGEPUT, upath, &subreqid))
				goto reply;
			if (!wqp) wqp = add2wq (clienthost, user, uid, gid,
															clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp);
			wfp->subreqid = subreqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		}
	}
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	free (argv);
	fork_exec_stager (wqp);
	return;
 reply:
	free (argv);
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
			stcp->status = STAGEOUT|PUT_FAILED;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				sendrep(rpfd, MSG_ERR, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
		}
		rmfromwq (wqp);
	}
}

isstaged(cur, p, poolflag, poolname)
		 struct stgcat_entry *cur;
		 struct stgcat_entry **p;
		 int poolflag;
		 char *poolname;
{
	int found = 0;
	int i;
	struct stgcat_entry *stcp;

	last_tape_file = 0;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
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
				if (strcmp (cur->u1.t.fseq, stcp->u1.t.fseq)) continue;
			} else {
				if (strcmp (cur->u1.t.fid, stcp->u1.t.fid)) continue;
			}
		} else if (cur->t_or_d == 'm') {
			if (strcmp (cur->u1.m.xfile, stcp->u1.m.xfile)) continue;
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

unpackfseq(fseq, req_type, trailing, fseq_list)
		 char *fseq;
		 int req_type;
		 char *trailing;
		 fseq_elem **fseq_list;
{
	char *dp;
	int i;
	int n1, n2;
	int nbtpf;
	char *p, *q;
	char tmp_fseq[CA_MAXFSEQLEN+1];

	if (strlen (fseq) > CA_MAXFSEQLEN) {
		sendrep (rpfd, MSG_ERR, STG06, "-q");
		return (0);
	}
	strcpy (tmp_fseq, fseq);
	*trailing = *(tmp_fseq + strlen (tmp_fseq) - 1);
	if (*trailing == '-') {
		if (req_type != STAGEIN) {
			sendrep (rpfd, MSG_ERR, STG18);
			return (0);
		}
		*(tmp_fseq + strlen (tmp_fseq) - 1) = '\0';
	}
	switch (*tmp_fseq) {
	case 'n':
		if (req_type == STAGEIN) {
			sendrep (rpfd, MSG_ERR, STG17, "-qn", "stagein");
			return (0);
		}
	case 'u':
		if (strlen (tmp_fseq) == 1) {
			nbtpf = 1;
		} else {
			nbtpf = strtol (tmp_fseq + 1, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-q");
				return (0);
			}
		}
		*fseq_list = (fseq_elem *) calloc (nbtpf, sizeof(fseq_elem));
		for (i = 0; i < nbtpf; i++)
			sprintf ((char *)(*fseq_list + i), "%c", *tmp_fseq);
		break;
	default:
		nbtpf = 0;
		p = strtok (tmp_fseq, ",");
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
		p = strtok (tmp_fseq, ",");
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
			for (i = n1; i <= n2; i++, nbtpf++)
				sprintf ((char *)(*fseq_list + nbtpf), "%d", i);
			p = strtok (NULL, ",");
		}
	}
	return (nbtpf);
}
