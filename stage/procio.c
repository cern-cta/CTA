/*
 * $Id: procio.c,v 1.4 1999/07/21 20:09:03 jdurand Exp $
 *
 * $Log: procio.c,v $
 * Revision 1.4  1999/07/21 20:09:03  jdurand
 * Initialize all variable pointers to NULL
 *
 * Revision 1.3  1999/07/20 17:29:17  jdurand
 * Added Id and Log CVS's directives
 *
 */

/*
 * Copyright (C) 1993-1999 by CERN/CN/PDP/DH
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)procio.c	1.41 01/14/99 CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
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
#ifdef DB
#include <Cdb_api.h>
#include <errno.h>
#include "wrapdb.h"
#endif /* DB */

extern char *optarg;
extern int optind;
extern char *rfio_serror();
#if defined(IRIX64)
extern int sendrep (int, int, ...);
#endif
extern char defpoolname[MAXPOOLNAMELEN];
extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
struct waitq *add2wq();
char *findpoolname();
int last_tape_file;

#ifdef DB
extern db_fd **db_stgcat;     /* DB connection on STGCAT catalog */
extern db_fd **db_stgipath;   /* DB connection on STGIPATH catalog */
#endif /* DB */

procioreq(req_type, req_data, clienthost)
int req_type;
char *req_data;
char *clienthost;
{
	int Aflag = 0;
	int actual_poolflag;
	char actual_poolname[MAXPOOLNAMELEN];
	char **argv = NULL;
	int c, i, j;
	int concat_off = 0;
	int clientpid;
	static char cmdnames[4][9] = {"", "stagein", "stageout", "stagewrt"};
	int copytape = 0;
	char *dp = NULL;
	int errflg = 0;
	char *fid = NULL;
	struct stat filemig_stat;
	char filemig_size[5];
	char *fseq = NULL;
	fseq_elem *fseq_list = NULL;
	struct group *gr = NULL;
	char *name = NULL;
	int nargs;
	int nbdskf;
	int nbtpf;
	struct stgcat_entry *newreq();
	int no_upath = 0;
	char *nread = NULL;
	int numvid, numvsn;
	char *p = NULL;
    char *q = NULL;
	char *pool_user = NULL;
	int poolflag;
	char *rbp = NULL;
	int savereqid;
	char *size = NULL;
	struct stat st;
	struct stgcat_entry *stcp = NULL;
	struct stgcat_entry stgreq;
	char trailing;
	int Uflag = 0;
	int Upluspath = 0;
	char upath[MAXHOSTNAMELEN + MAXPATH];
	char *user = NULL;
	struct waitf *wfp = NULL;
	struct waitq *wqp = NULL;

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
	optind = 1;
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
			while (p) {
				if (strcmp (p, "ebcdic") == 0) {
					stgreq.charconv |= EBCCONV;
				} else if (strcmp (p, "block") == 0) {
					stgreq.charconv |= FIXVAR;
				} else if (strcmp (p, "ascii")) {
					sendrep (rpfd, MSG_ERR, STG06, "-C");
					errflg++;
					break;
				}
				if (p = strtok (NULL, ",")) *(p - 1) = ',';
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
			strcpy (stgreq.u1.m.xfile, optarg);
			break;
		case 'N':
			nread = optarg;
			p = strtok (nread, ":");
			while (p) {
				j = strtol (p, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-N");
					errflg++;
				}
				if (p = strtok (NULL, ":")) *(p - 1) = ':';
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
			while (p) {
				j = strtol (p, &dp, 10);
				if (*dp != '\0' || j > 2047) {
					sendrep (rpfd, MSG_ERR, STG06, "-s");
					errflg++;
				}
				if (p = strtok (NULL, ":")) *(p - 1) = ':';
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
			while (q) {
				strcpy (stgreq.u1.t.vid[numvid], q);
				UPPER (stgreq.u1.t.vid[numvid]);
				numvid++;
				q = strtok (NULL, ":");
			}
			break;
		case 'v':
			stgreq.t_or_d = 't';
			q = strtok (optarg, ":");
			while (q) {
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

	/* Fetching HSM file size */

	if (req_type == STAGEIN && stgreq.t_or_d == 'm' && !size) {
		if (rfio_stat (stgreq.u1.m.xfile, &filemig_stat) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, stgreq.u1.m.xfile,
				"rfio_stat", rfio_serror());
			c = USERR;
			goto reply;
		}
		sprintf (filemig_size, "%d", (filemig_stat.st_size/(1024*1024)) + 1);
		size = filemig_size;
	}

	/* Overwriting an existing HSM file is not allowed */

	if ((req_type == STAGEOUT || req_type == STAGEWRT) && stgreq.t_or_d == 'm') {
		if (rfio_stat (stgreq.u1.m.xfile, &filemig_stat) == 0) {
			sendrep (rpfd, MSG_ERR, STG02, stgreq.u1.m.xfile,
				"rfio_stat", "file already exists");
			c = USERR;
			goto reply;
		}
	}

	/* building catalog entries */

	for (i = 0; i < nbdskf; i++) {
		if (Uflag && i > 0) break;
		if (stgreq.t_or_d == 't') {
			if (fid) {
				if (p = strchr (fid, ':')) *p = '\0';
				if ((j = strlen (fid) - 17) > 0) fid += j;
				strcpy (stgreq.u1.t.fid, fid);
				if (p) {
					*p = ':';
					fid = p + 1;
				}
			}

			if (packfseq (fseq_list, i, nbdskf, nbtpf, trailing,
			    stgreq.u1.t.fseq, MAXFSEQ)) {
				sendrep (rpfd, MSG_ERR, STG21);
				c = USERR;
				goto reply;
			}
		}
		if (nread) {
			if (p = strchr (nread, ':')) *p = '\0';
			stgreq.nread = strtol (nread, &dp, 10);
			if (p) {
				*p = ':';
				nread = p + 1;
			}
		}
		if (size) {
			if (p = strchr (size, ':')) *p = '\0';
			stgreq.size = strtol (size, &dp, 10);
			if (p) {
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
#ifdef DB
				if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
					c = SYERR;
					goto reply;
				}
#endif /* DB */
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
#ifdef DB
				if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
					c = SYERR;
					goto reply;
				}
#endif /* DB */
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
#ifdef DB
				if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
					c = SYERR;
					goto reply;
				}
#endif /* DB */
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
#ifdef DB
				if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
					c = SYERR;
					goto reply;
				}
#endif /* DB */
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
#ifdef DB
						if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
							sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
							c = SYERR;
							goto reply;
						}
#endif /* DB */
					} else if (c) {
						updfreespace (stcp->poolname, stcp->ipath,
							stcp->size*1024*1024);
						delreq (stcp);
						goto reply;
					}
				}
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
#ifdef DB
			if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
				sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				c = SYERR;
				goto reply;
			}
#endif /* DB */
			if ((c = build_ipath (upath, stcp, pool_user)) < 0) {
				stcp->status |= WAITING_SPC;
#ifdef DB
				if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
					c = SYERR;
					goto reply;
				}
#endif /* DB */
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
				delreq (stcp);
				goto reply;
			} else {
				if (*upath && strcmp (stcp->ipath, upath))
					create_link (stcp, upath);
				if (Upluspath &&
				    strcmp (stcp->ipath, argv[optind+1]))
					create_link (stcp, argv[optind+1]);
			}
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
					delreq (stcp);
				break;
			case STAGEOUT|PUT_FAILED:
				delreq (stcp);
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
#ifdef DB
			if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
				sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				c = SYERR;
				goto reply;
			}
#endif /* DB */
			if (!wqp) wqp = add2wq (clienthost, user, stcp->uid,
				stcp->gid, clientpid, Upluspath, reqid, req_type,
				nbdskf, &wfp);
			wqp->copytape = copytape;
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			if (i < nbtpf)
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
#ifdef DB
			if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
				sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				c = SYERR;
				goto reply;
			}
#endif /* DB */
			if (rfio_stat (upath, &st) < 0) {
				sendrep (rpfd, MSG_ERR, STG02, upath, "rfio_stat",
					rfio_serror());
				delreq (stcp);
				goto reply;
			}
			stcp->actual_size = st.st_size;
			stcp->c_time = st.st_mtime;
			stcp->a_time = st.st_atime;
			stcp->nbaccesses = 1;
			strcpy (stcp->ipath, upath);
#ifdef DB
			if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
				sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				c = SYERR;
				goto reply;
			}
#endif /* DB */
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
	return;
reply:
	if (fseq_list) free (fseq_list);
	free (argv);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
		reqid, req_type, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, req_type, c);
	if (c && wqp) {
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
#ifdef DB
		if (wrapCdb_fetch(db_stgcat, wfp->subreqid, (void **) &stcp, NULL, 0) != 0) {
			sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
			if (stcp != NULL)
				free(stcp);
			return;
		}
#else /* DB */        
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
#endif /* DB */
			if (! wfp->waiting_on_req)
				updfreespace (stcp->poolname, stcp->ipath,
					stcp->size*1024*1024);
			delreq (stcp);
		}
		rmfromwq (wqp);
	}
#ifdef DB
	if (stcp != NULL)
		free(stcp);
#endif /* DB */
}

procputreq(req_data, clienthost)
char *req_data;
char *clienthost;
{
	char **argv = NULL;
	int c, i;
	int clientpid;
	char *dp = NULL;
	int errflg = 0;
	char *externfile = NULL;
	int found;
	char fseq[MAXFSEQ];
	gid_t gid;
	struct group *gr = NULL;
	char *hsmfile = NULL;
	int Iflag = 0;
	int Mflag = 0;
	char *name = NULL;
	int nargs;
	int nbdskf;
	int numvid = 0;
	int n1 = 0;
	int n2 = 0;
	char *q = NULL;
	char *rbp = NULL;
	struct stat st;
	struct stgcat_entry *stcp = NULL;
	int subreqid;
	int Upluspath = 0;
	uid_t uid;
	char upath[MAXHOSTNAMELEN + MAXPATH];
	char *user = NULL;
	char vid[7];
	struct waitf *wfp = NULL;
	struct waitq *wqp = NULL;
#ifdef DB
	char *db_data = NULL;
	size_t db_size;
#endif /* DB */

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
	optind = 1;
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
			if (q = strchr (optarg, '-')) {
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
#ifdef DB
			if (Cdb_altkey_fetch(db_stgcat, "u1.t.vid0", vid, (void **) &db_data, &db_size) == 0) {
				char *ptr = NULL;
                char *ptrmax = NULL;

				ptrmax = ptr = db_data;
				ptrmax += db_size;
				do {
					int reqid;
	
					reqid = atoi(ptr);
					/* In case of a string, sizeof() == strlen() + 1 */
					/* ptr points to the reqid (as a string, e.g. a master key) */
		   			if (wrapCdb_fetch(db_stgcat,reqid,(void **) &stcp, NULL, 0) == 0) {
						if (stcp != NULL) {
							stcp->status = STAGEOUT|PUT_FAILED;
							if (strcmp (stcp->u1.t.vid[0], vid) != 0) goto docontinue0;
							if (strcmp (stcp->u1.t.fseq, fseq)) goto docontinue0;
							found = 1;
							break;
						}
					}
				docontinue0:
					ptr += (strlen(ptr) + 1);
				} while (ptr < ptrmax);
			}
#else /* DB */
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if (stcp->t_or_d != 't') continue;
				if (strcmp (stcp->u1.t.vid[0], vid) != 0) continue;
				if (strcmp (stcp->u1.t.fseq, fseq)) continue;
				found = 1;
				break;
			}
#endif /* DB */
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
#ifdef DB
			if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
				sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				c = SYERR;
				goto reply;
			}
#endif /* DB */
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
#ifdef DB
		if (Mflag)
			if (Cdb_altkey_fetch(db_stgcat, "u1.m.xfile", hsmfile, (void **) &stcp, NULL) == 0)
				found = 1;
			else
				if (Cdb_altkey_fetch(db_stgcat, "u1.d.xfile", externfile, (void **) &stcp, NULL) == 0)
				found = 1;
#else /* DB */
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
#endif /* DB */
		if (found == 0 ||
		    (stcp->status != STAGEOUT &&
		    stcp->status != (STAGEOUT|PUT_FAILED))) {
			sendrep (rpfd, MSG_ERR, STG22);
			c = USERR;
			goto reply;
		}
		if (stcp->status == STAGEOUT) {
			if (rfio_stat (stcp->ipath, &st) == 0) {
				stcp->actual_size = st.st_size;
#ifdef DB
				if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
					c = SYERR;
					goto reply;
				}
#endif /* DB */
			}
			updfreespace (stcp->poolname, stcp->ipath,
				stcp->size*1024*1024 - (int)stcp->actual_size);
		}
		stcp->status = STAGEPUT;
		stcp->a_time = time (0);
#ifdef DB
		if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
			sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
			c = SYERR;
			goto reply;
		}
#endif /* DB */
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
#ifdef DB
	if (stcp != NULL)
		free(stcp);
#endif /* DB */
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
#ifdef DB
			if (wrapCdb_fetch(db_stgcat, wfp->subreqid, (void **) &stcp, NULL, 0) != 0) {
				sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				if (stcp != NULL)
					free(stcp);
				return;
			}
#else /* DB */
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
#endif /* DB */
			stcp->status = STAGEOUT|PUT_FAILED;
#ifdef DB
			if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
				sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				if (stcp != NULL)
					free(stcp);
				return;
			}
#endif /* DB */
		}
		rmfromwq (wqp);
	}
#ifdef DB
	if (stcp != NULL)
		free(stcp);
#endif /* DB */
}

isstaged(cur, p, poolflag, poolname)
struct stgcat_entry *cur;
struct stgcat_entry **p;
int poolflag;
char *poolname;
{
	int found = 0;
	int i;
	struct stgcat_entry *stcp = NULL;
#ifdef DB
	char *db_data = NULL;
	char *db_vid[MAXVSN];
	size_t db_size[MAXVSN];
	int  min_index = -1;
#endif

	last_tape_file = 0;
#ifdef DB
	if (cur->t_or_d == 't') {
		/* We want all the vid[] members to be the same */
		int  ndb_vid[MAXVSN];
		int   all_the_same = 1;
		int   entered = 0;
		for (i = 0; i < MAXVSN; i++) {
			/* Do some initialization */
			db_vid[i]  = NULL;
			ndb_vid[i] = 0;
			db_size[i] = 0;
		}

		/* ------------------------------------- */
		/* Fetch alternate keys matching the VID */
		/* ------------------------------------- */
		for (i = 0; i < MAXVSN; i++) {

			if (strlen(cur->u1.t.vid[i]) > 0) {
				char *newkey = NULL;
                int   status;
                char *data = NULL;
                
                ++entered;
                if ((newkey = (char *) malloc(strlen("u1.t.vid") + count_digits(i,10) + 1)) == NULL) {
                	stglogit (func, STG05);
                    exit(SYERR);
                }
                strcpy(newkey,"u1.t.vid");
                sprintf(newkey + strlen("u1.t.vid"),"%d",i);
                data = NULL;
                
                status = Cdb_altkey_fetch(db_stgcat, newkey, cur->u1.t.vid[i], (void **) &data, &(db_size[i]));
                
                free(newkey);
                if (status != 0) {
                	/* Just to be sure */
                	all_the_same = 0;
                    break;
                }
                
                db_vid[i] = data;
                /* we got the entry */
                /* Let's evaluate the number of entries in it */
                {
                	char *ptr = NULL;
                    char *ptrmax = NULL;
                    ptrmax = ptr = db_vid[i];
                    ptrmax += db_size[i];
                    do {
                    	ptr += (strlen(ptr) + 1);
                        ++ndb_vid[i];
                    } while (ptr < ptrmax);
                }
                /* And which entry has the lowest number of indexes */
                if (min_index >= 0) {
                	if (ndb_vid[i] < ndb_vid[min_index])
                      min_index = i;
                } else {
                  min_index=i;
                }
                
            }
        }
        
        if (entered > 0 && all_the_same == 1) {
        	/* We have found the MAXVSN entries in the u1.t.vid[] respective databases */
        	/* We now need the list of "reqid" that match all of them at the time      */
        	/* This means, the list of "reqid" that appears in all db_vid[] lists */

        	/* Free all the indexes but the one that has the lowest number of entries  */
        	for (i = 0; i < MAXVSN; i++) {
            	if (i != min_index) {
                	if (db_vid[i] != NULL) {
                      free(db_vid[i]);
                    }
                }
            }
            
            /* ---------------------------------------------------------------------- */
            /* Fetch the (reduced) matched reqid corresponding to found alternate key */
            /* ---------------------------------------------------------------------- */
            {
            	char *ptr = NULL;
                char *ptrmax = NULL;

                ptrmax = ptr = db_vid[min_index];
                ptrmax += db_size[min_index];
                do {
                	int reqid;
                    
                    reqid = atoi(ptr);
                    
                    if (stcp != NULL)
                    	free(stcp);
                    stcp = NULL;
                    
                    if (wrapCdb_fetch(db_stgcat,reqid,(void **) &stcp, NULL, 0) == 0) {
                    	for (i = 0; i < MAXVSN; i++)
                        	if (strcmp (cur->u1.t.vid[i], stcp->u1.t.vid[i])) {
                              break;
                            }
                        if (i < MAXVSN) {
                        	goto docontinue1;
                        }
                        for (i = 0; i < MAXVSN; i++)
                        	if (strcmp (cur->u1.t.vsn[i], stcp->u1.t.vsn[i])) {
                              break;
                            }
                        if (i < MAXVSN) {
                        	goto docontinue1;
                        }
                        if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) {
                        	goto docontinue1;
                        }
                        if (cur->u1.t.fseq[0] != 'u') {
                        	if ((stcp->status & 0xF000) == LAST_TPFILE) {
                            	last_tape_file = atoi (stcp->u1.t.fseq);
                            }
                            if (strcmp (cur->u1.t.fseq, stcp->u1.t.fseq)) {
                            	goto docontinue1;
                            }
                        } else {
                        	if (strcmp (cur->u1.t.fid, stcp->u1.t.fid)) {
                            	goto docontinue1;
                            }
                        }
                        found = 1;
                        break;
                    }
                docontinue1:
                    ptr += (strlen(ptr) + 1);
                } while (ptr < ptrmax);
            }
            
            /* --------------------------------------------------------------- */
            /* If found: db_vid[min_index] points to the possible reqid's */
            /* --------------------------------------------------------------- */
            if (found == 0) {
            	free(db_vid[min_index]);
            }
        } else {
        	/* All the entries do not exist. For sure there is no other "reqid" that   */
        	/* match all the vid of record "cur"                                       */
        	for (i = 0; i < MAXVSN; i++) {
            	if (db_vid[i] != NULL)
                	free(db_vid[i]);
            }
        }
        if (stcp != NULL)
        	free(stcp);
        stcp = NULL;
    } else if (cur->t_or_d == 'm') {
    	/* -------------------------------------------------------------- */
    	/* If it works: db_vid[0] and db_size[0] will be filled */
      	/* -------------------------------------------------------------- */
    	if (Cdb_altkey_fetch(db_stgcat, "u1.m.xfile", cur->u1.m.xfile, (void **) &(db_vid[0]), &(db_size[0])) == 0) {
          min_index = 0;
          found = 1;
        }
    } else {
    	/* -------------------------------------------------------------- */
    	/* If it works: db_vid[0] and db_size[0] will be filled */
    	/* -------------------------------------------------------------- */
    	if (Cdb_altkey_fetch(db_stgcat, "u1.d.xfile", cur->u1.d.xfile, (void **) &(db_vid[0]), &(db_size[0])) == 0) {
          min_index = 0;
          found = 1;
        }
    }
    /* Do the poolfalg condition at the end, instead */
    if (found != 0) {
    	/* We have found an alternate entry */
    	/* Is there any "reqid" that match the "poolname" condition ? */
    	char *ptr = NULL;
        char *ptrmax = NULL;

        found = 0;
        ptrmax = ptr = db_vid[min_index];
        ptrmax += db_size[min_index];
        do {
        	int reqid;
            
            reqid = atoi(ptr);
            /* In case of a string, sizeof() == strlen() + 1 */
            /* ptr points to the reqid (as a string, e.g. a master key) */
            if (stcp != NULL)
            	free(stcp);
            stcp = NULL;
            
            if (wrapCdb_fetch(db_stgcat,reqid,(void **) &stcp, NULL, 0) == 0) {
              /* if no pool specified, the file may reside in any pool */
            	if (poolflag == 0) {
                	if (stcp->poolname[0] == '\0') goto docontinue2;
                } else
                  /* if a specific pool was requested, the file must be there */
                	if (poolflag > 0) {
                    	if (strcmp (poolname, stcp->poolname)) goto docontinue2;
                    } else
                      /* if -p NOPOOL, the file should not reside in a pool */
                    	if (poolflag < 0) {
                        	if (stcp->poolname[0]) goto docontinue2;
                        }
                /* Still making sure that is a correct entry */
                if (cur->t_or_d == 't') {
                	for (i = 0; i < MAXVSN; i++)
                    	if (strcmp (cur->u1.t.vid[i], stcp->u1.t.vid[i])) break;
                    if (i < MAXVSN) goto docontinue2;
                    for (i = 0; i < MAXVSN; i++)
                    	if (strcmp (cur->u1.t.vsn[i], stcp->u1.t.vsn[i])) break;
                    if (i < MAXVSN) goto docontinue2;
                    if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) goto docontinue2;
                    if (cur->u1.t.fseq[0] != 'u') {
                    	if ((stcp->status & 0xF000) == LAST_TPFILE) {
                          last_tape_file = atoi (stcp->u1.t.fseq);
                        }
                        if (strcmp (cur->u1.t.fseq, stcp->u1.t.fseq)) goto docontinue2;
                    } else {
                      if (strcmp (cur->u1.t.fid, stcp->u1.t.fid)) goto docontinue2;
                    }
                } else if (cur->t_or_d == 'm') {
                  if (strcmp (cur->u1.m.xfile, stcp->u1.m.xfile)) goto docontinue2;
                } else {
                  if (strcmp (cur->u1.d.xfile, stcp->u1.d.xfile)) goto docontinue2;
                }
            }
            found = 1;
            break;
        docontinue2:
            ptr += (strlen(ptr) + 1);
        } while (ptr < ptrmax);
    }
#else /* DB */
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
#endif /* DB */

  /* ----------------------------------- */
  /* If the entry is found, then stcp    */
  /* will have to point to the structure */
  /* ----------------------------------- */

	if (! found) {
#ifdef DB
		if (stcp != NULL)
			free(stcp);
#endif /* DB */
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
	char *dp = NULL;
	int i;
	int n1, n2;
	int nbtpf;
	char *p = NULL;
    char *q = NULL;

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
		for (i = 0; i < nbtpf; i++)
			sprintf ((char *)(*fseq_list + i), "%c", *fseq);
		break;
	default:
		nbtpf = 0;
		p = strtok (fseq, ",");
		while (p) {
			if (q = strchr (p, '-')) {
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
			if (p = strtok (NULL, ",")) *(p - 1) = ',';
		}
		*fseq_list = (fseq_elem *) calloc (nbtpf, sizeof(fseq_elem));
		nbtpf = 0;
		p = strtok (fseq, ",");
		while (p) {
			if (q = strchr (p, '-')) {
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
