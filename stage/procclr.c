/*
 * $Id: procclr.c,v 1.12 2000/02/11 11:06:51 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procclr.c,v $ $Revision: 1.12 $ $Date: 2000/02/11 11:06:51 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <grp.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#undef  unmarshall_STRING
#define unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#if SACCT
#include "../h/sacct.h"
#endif
#include "osdep.h"

void procclrreq _PROTO((char *, char *));

extern char *optarg;
extern int optind;
extern char *rfio_serror();
extern char func[16];
extern int nbcat_ent;
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgpath_entry *stpe;	/* end of stage path catalog */
extern struct stgpath_entry *stps;	/* start of stage path catalog */
extern struct waitq *waitqp;

void procclrreq(req_data, clienthost)
char *req_data;
char *clienthost;
{
	char **argv;
	int c, i, j;
	int cflag = 0;
	char *dp;
	int errflg = 0;
	int found;
	char *fseq = NULL;
	gid_t gid;
	char group[CA_MAXGRPNAMELEN + 1];
	struct group *gr;
	char *lbl = NULL;
	char *linkname = NULL;
	char *mfile = NULL;
	int minfree = 0;
	int nargs;
	int numvid = 0;
	char *path = NULL;
	int poolflag = 0;
	char poolname[CA_MAXPOOLNAMELEN + 1];
	char *q;
	char *rbp;
	int rflag = 0;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	uid_t uid;
	char *user;
	char vid[MAXVSN][7];
	char *xfile = NULL;

	poolname[0] = '\0';
	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
		reqid, STAGECLR, 0, 0, NULL, "");
#endif

	if ((gr = getgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
		goto reply;
	}
	strcpy (group, gr->gr_name);
#ifdef linux
	optind = 0;
#else
	optind = 1;
#endif
	while ((c = getopt (nargs, argv, "cGh:I:iL:l:M:m:P:p:q:r:V:")) != EOF) {
		switch (c) {
		case 'c':
			cflag++;
			break;
		case 'G':
			break;
		case 'h':
			break;
		case 'I':
			xfile = optarg;
			break;
		case 'i':
			break;
		case 'L':
			linkname = optarg;
			break;
		case 'l':	/* label type (al, nl, sl or blp) */
			lbl = optarg;
			break;
		case 'M':
			mfile = optarg;
			break;
		case 'm':
                        minfree = strtol (optarg, &dp, 10);
                        if (*dp != '\0' || minfree > 100) {
                                sendrep (rpfd, MSG_ERR, STG06, "-m");
                                errflg++;
                        }
                        break;
		case 'P':
			path = optarg;
			if (*path == '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-P");
				errflg++;
			}
			break;
		case 'p':
			if (strcmp (optarg, "NOPOOL") == 0 ||
			    isvalidpool (optarg)) {
				strcpy (poolname, optarg);
			} else {
				sendrep (rpfd, MSG_ERR, STG32, optarg);
				errflg++;
			}
			break;
		case 'q':	/* file sequence number(s) */
			fseq = optarg;
			break;
		case 'r':
			/* optarg is equal to emove_from_hsm */
			/* because we only allows this in the stageclr command line */
			rflag = 1;
			break;
		case 'V':	/* visual identifier(s) */
			q = strtok (optarg, ":");
			while (q != NULL) {
				strcpy (vid[numvid], q);
				UPPER (vid[numvid]);
				numvid++;
				q = strtok (NULL, ":");
			}
			break;
		}
	}
	/* -L linkname and -remove_from_hsm is not allowed */
	if (linkname && rflag)
		errflg++;

	if (errflg) {
		c = USERR;
		goto reply;
	}
	c = 0;
	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;
	found = 0;
	if (linkname) {
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if (strcmp (linkname, stpp->upath) == 0) {
				found = 1;
				break;
			}
		}
		if (found) {
			dellink (stpp);
			savepath ();
		} else {
			sendrep (rpfd, MSG_ERR, STG33, linkname, "file not found");
			c = USERR;
			goto reply;
		}
	} else if (path) {
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if (strcmp (path, stpp->upath) == 0) {
				found = 1;
				break;
			}
		}
		if (found) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stpp->reqid == stcp->reqid) break;
			}
			if (cflag && stcp->poolname[0] &&
			    enoughfreespace (stcp->poolname, minfree)) {
				c = ENOUGHF;
				goto reply;
			}
			if (cflag && uid == 0 &&	/* probably garbage collector */
			    checklastaccess (stcp->poolname, stcp->a_time)) {
				c = EBUSY;
				goto reply;
			}
			if ((i = check_delete (stcp, gid, uid, group, user, rflag)) > 0) {
				c = i;
				goto reply;
			}
		} else {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if (strcmp (path, stcp->ipath) == 0) {
					found = 1;
					if (cflag && stcp->poolname[0] &&
					    enoughfreespace (stcp->poolname, minfree)) {
						c = ENOUGHF;
						goto reply;
					}
					if (cflag && uid == 0 &&
					    checklastaccess (stcp->poolname, stcp->a_time)) {
						c = EBUSY;
						goto reply;
					}
					if ((i = check_delete (stcp, gid, uid,
					    group, user, rflag)) > 0) {
						c = i;
						goto reply;
					}
				}
			}
		}
		if (! found) {
			sendrep (rpfd, MSG_ERR, STG33, path, "file not found");
			c = USERR;
			goto reply;
		}
	} else {
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (poolflag < 0) {	/* -p NOPOOL */
				if (stcp->poolname[0]) continue;
			} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
			if (numvid) {
				if (stcp->t_or_d != 't') continue;
				for (j = 0; j < numvid; j++)
					if (strcmp (stcp->u1.t.vid[0], vid[j]) == 0) break;
				if (j == numvid) continue;
			}
			if (lbl) {
				if (stcp->t_or_d != 't') continue;
				if (strcmp (lbl, stcp->u1.t.lbl)) continue;
			}
			if (fseq) {
				if (stcp->t_or_d != 't') continue;
				if (strcmp (fseq, stcp->u1.t.fseq)) continue;
			}
			if (xfile) {
				if (stcp->t_or_d != 'd') continue;
				if (strcmp (xfile, stcp->u1.d.xfile)) continue;
			}
			if (mfile) {
				if (stcp->t_or_d != 'm') continue;
				if (strcmp (mfile, stcp->u1.m.xfile)) continue;
			}
			found = 1;
			if (cflag && stcp->poolname[0] &&
			    enoughfreespace (stcp->poolname, minfree)) {
				c = ENOUGHF;
				goto reply;
			}
			if ((i = check_delete (stcp, gid, uid, group, user, rflag)) > 0) {
				c = i;
				goto reply;
			}
			stcp += i;
		}
		if (! found) {
			sendrep (rpfd, MSG_ERR, STG33, "", "file not found");
			c = USERR;
		}
	}
reply:
	free (argv);
	sendrep (rpfd, STAGERC, STAGECLR, c);
}

check_delete(stcp, gid, uid, group, user, rflag)
struct stgcat_entry *stcp;
gid_t gid;
uid_t uid;
char *group;
char *user;
int rflag; /* True if HSM source file has to be removed */
{
	int found;
	int i;
	int savereqid;
	struct waitf *wfp;
	struct waitq *wqp;

/*	return	<0	request deleted
 *		 0	running request (status set to CLEARED and req signalled)
 *		>0	in case of error
 */

	if (strcmp (group, STGGRP) && strcmp (group, stcp->group) && uid != 0) {
		sendrep (rpfd, MSG_ERR, STG33, "", "permission denied");
		return (USERR);
	}
	if ((stcp->status & 0xF0) == STAGED ||
	    stcp->status == (STAGEOUT | PUT_FAILED)) {
                if (delfile (stcp, 0, 1, 1, user, uid, gid, rflag) < 0) {
                        sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_unlink",
                                rfio_serror());
                        return (USERR);
		}
	} else if (stcp->status == STAGEOUT || stcp->status == STAGEALLOC) {
                if (delfile (stcp, 1, 1, 1, user, uid, gid, rflag) < 0) {
                        sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_unlink",
                                rfio_serror());
                        return (USERR);
		}
	} else {	/* the request should be in the active/wait queue */
		found = 0;
		for (wqp = waitqp; wqp; wqp = wqp->next) {
			for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
				if (wfp->subreqid == stcp->reqid) {
					found = 1;
					break;
				}
			}
			if (found) {
				savereqid = reqid;
				reqid = wqp->reqid;
				sendrep (wqp->rpfd, MSG_ERR, STG95, "request", user);
				reqid = savereqid;
				wqp->status =  CLEARED;
				/* is there an active stager overlay for this file? */
				if (wqp->ovl_pid) {
					stglogit (func, "killing process %d\n",
						wqp->ovl_pid);
					kill (wqp->ovl_pid, SIGINT);
					wqp->ovl_pid = 0;
				}
				return (0);
			}
		}
		sendrep (rpfd, MSG_ERR,
		    "internal error: status=%x but req not in waitq\n",
		    stcp->status);
		return (USERR);
	}
	return (-1);
}
