/*
 * $Id: procfilchg.c,v 1.9 2001/07/12 07:38:49 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procfilchg.c,v $ $Revision: 1.9 $ $Date: 2001/07/12 07:38:49 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <sys/stat.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include "marshall.h"
#define local_unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#if SACCT
#include "../h/sacct.h"
#endif
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include "serrno.h"
#include "osdep.h"
#include "Cgetopt.h"
#include "Cns_api.h"
#include "stage_api.h"

void procfilchgreq _PROTO((int, int, char *, char *));

extern char func[16];
extern int reqid;
extern int rpfd;
extern int req2argv _PROTO((char *, char ***));
extern int upd_staged _PROTO((char *));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *));
extern int stglogit _PROTO(());
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif

extern char defpoolname[CA_MAXPOOLNAMELEN + 1];
static int reqid_flag = 0;
static int status_flag = 0;
static int poolname_flag = 0;
extern int check_hsm_type_light _PROTO((char *, char *));
extern int isvalidpool _PROTO((char *));
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *));
extern int savereqs _PROTO(());
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *));
extern void rwcountersfs _PROTO((char *, char *, int, int));

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif


void
procfilchgreq(req_type, magic, req_data, clienthost)
		 int req_type;
		 int magic;
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	int c, i;
	gid_t gid;
	int nargs;
	uid_t uid;
	char *user;
	char *rbp;
	int clientpid;
	int errflg = 0;
	char *hsmfile = NULL;
	char t_or_d;
	int thisreqid = -1;
	int donereqid = 0;
	int thisstatus = -1;
	int donestatus = 0;
	char *dp;
	struct stgcat_entry *stcp;
	int found = 0;
	char poolname[CA_MAXPOOLNAMELEN + 1];
	int poolflag = 0;
	int ifileclass;
	static struct Coptions longopts[] =
	{
		{"host",               REQUIRED_ARGUMENT,  NULL,               'h'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,               'M'},
		{"poolname",           REQUIRED_ARGUMENT,  &poolname_flag,       1},
		{"reqid",              REQUIRED_ARGUMENT,  &reqid_flag,          1},
		{"status",             REQUIRED_ARGUMENT,  &status_flag,         1},
		{NULL,                 0,                  NULL,                 0}
	};

	poolname[0] = '\0';
	reqid_flag = 0;
	status_flag = 0;

	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);

#ifndef __INSURE__
	stglogit (func, STG92, "stagefilchg", user, uid, gid, clienthost);
#endif

	unmarshall_WORD (rbp, clientpid);
	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
						 reqid, STAGEFILCHG, 0, 0, NULL, "");
#endif

	Coptind = 1;
	Copterr = 0;
	while ((c = Cgetopt_long (nargs, argv, "h:M:p:", longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			break;
		case 'M':
			hsmfile = Coptarg;
			if (check_hsm_type_light(Coptarg,&(t_or_d)) != 0) {
				sendrep(rpfd, MSG_ERR, "STG02 - Bad HSM filename\n");
				c = USERR;
				goto reply;
			}
			if (t_or_d != 'h') {
				sendrep(rpfd, MSG_ERR, "STG02 - Only CASTOR HSM filename supported\n");
				c = USERR;
				goto reply;
			}
			break;
		case 'p':
			if (strcmp (Coptarg, "NOPOOL") == 0 ||
					isvalidpool (Coptarg)) {
				strcpy (poolname, Coptarg);
			} else {
				sendrep (rpfd, MSG_ERR, STG32, Coptarg);
				errflg++;
			}
			break;
		case 0:
			if ((reqid_flag != 0) && (! donereqid)) {
				thisreqid = strtol (Coptarg, &dp, 10);
				if ((*dp != '\0') || (((thisreqid == LONG_MIN) || (thisreqid == LONG_MAX)) && (errno == ERANGE))) {
					sendrep (rpfd, MSG_ERR, STG06, (*dp != '\0') ? "--reqid" : "--reqid (out of range)");
					errflg++;
				}
				donereqid = 1;
			}
			if ((status_flag != 0) && (! donestatus)) {
				thisstatus = strtol (Coptarg, &dp, 0);
				if ((*dp != '\0') || (((thisstatus == LONG_MIN) || (thisstatus == LONG_MAX)) && (errno == ERANGE))) {
					sendrep (rpfd, MSG_ERR, STG06, (*dp != '\0') ? "--status" : "--status (out of range)");
					errflg++;
				}
				/* The only status change supported here are STAGEOUT|CAN_BE_MIGR */
				if ((thisstatus != (STAGEOUT|CAN_BE_MIGR)) && (! errflg)) {
					sendrep (rpfd, MSG_ERR, STG06, "--status");
					errflg++;
				}
				donestatus = 1;
			}
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

	if (errflg != 0) {
		c = USERR;
		goto reply;
	}

	if (poolname[0] == '\0') {
		/* Apply the default poolname */
		strcpy(poolname,defpoolname);
	}

	if ((nargs > Coptind) && (hsmfile || donereqid || donestatus)) {
		sendrep(rpfd, MSG_ERR, "STG02 - Options and parameters are exclusive\n");
		c = USERR;
		goto reply;
	}

	if (nargs > Coptind) {
		/* User choosed the 'parameter' way of using stagechng */
		for  (i = Coptind; i < nargs; i++)
			if ((c = upd_staged (argv[i])) != 0)
				goto reply;
		goto reply;
	}

	if (strcmp (poolname, "NOPOOL") == 0) {
		poolflag = -1;
	}

	if (hsmfile || donereqid || donestatus) {
		if (! hsmfile) {
			sendrep(rpfd, MSG_ERR, "STG02 - Supply of -M option is mandatory\n");
			c = USERR;
			goto reply;
		}
		if (! (donestatus)) {
			sendrep(rpfd, MSG_ERR, "STG02 - Supply of --status is mandatory\n");
			c = USERR;
			goto reply;
		}
		/* User choosed the 'option' way of using stagechng */
		for (stcp = stcs; stcp < stce; stcp++) {
			struct Cns_fileid Cnsfileid;       /* For     CASTOR hsm IDs */
			struct Cns_filestat Cnsfilestat;   /* For     CASTOR hsm stat() */
			extern struct passwd start_passwd; /* Start uid/gid stage:st */
			int changed;

			changed = 0;
			if (stcp->reqid == 0) break;
			if (donereqid && stcp->reqid != thisreqid) continue; /* --reqid */
			if (stcp->t_or_d != 'h') continue; /* Only CASTOR HSM filename supported */
			/* Grab the fileclass */
			if ((ifileclass = upd_fileclass(NULL,stcp)) < 0) {
				c = USERR;
				goto reply;
			}
			if (poolflag < 0) { /* -p NOPOOL */
				if (stcp->poolname[0]) continue;
			} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
			if (strcmp(stcp->u1.h.xfile, hsmfile) != 0) continue; /* -M */
			found++;
			/* We check if we have permissions to do so using the NameServer */
			memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
			strcpy(Cnsfileid.server,stcp->u1.h.server);
			Cnsfileid.fileid = stcp->u1.h.fileid;
			setegid(gid);
			seteuid(uid);
			if (Cns_statx(hsmfile, &Cnsfileid, &Cnsfilestat) != 0) {
				setegid(start_passwd.pw_gid);
				seteuid(start_passwd.pw_uid);
				sendrep (rpfd, MSG_ERR, STG02, hsmfile, "Cns_statx", sstrerror(serrno));
				c = USERR;
				goto reply;
			}
			setegid(start_passwd.pw_gid);
			seteuid(start_passwd.pw_uid);

			/* From now on we assume that permission to change parameters on this file is granted */

			if (donestatus) { /* --status */
				switch (thisstatus) {
				case STAGEOUT|CAN_BE_MIGR:
					/* Will work only if this stcp is in STAGEOUT|CAN_BE_MIGR|PUT_FAILED */
					switch (stcp->status) {
					case STAGEOUT|CAN_BE_MIGR|PUT_FAILED:
						/* We simulate a stageout followed by a stageupdc */
						stcp->status = STAGEOUT;
						rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT);
						if ((c = upd_stageout(STAGEUPDC, NULL, NULL, 1, stcp)) != 0) {
							if (c != CLEARED) {
								goto reply;
							} else {
								/* This is not formally an error to do an updc on a zero-length file */
								c = 0;
								changed++;
							}
						} else {
							changed++;
						}
						break;
					default:
						sendrep(rpfd, MSG_ERR, STG02, hsmfile, "--status", "should be in STAGEOUT|CAN_BE_MIGR|PUT_FAILED status");
						c = USERR;
						goto reply;			
					}
					break;
				default:
					sendrep(rpfd, MSG_ERR, STG02, hsmfile, "--status", "only --status CAN_BE_MIGR option value supported");
					c = USERR;
					goto reply;			
				}
				/* Either no executions */
				/* Either success and upd_stageout will take care about updating the catalog */
			}
			if (changed) {
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs();
            }
		}
		if (! found) {
			sendrep (rpfd, MSG_ERR, STG153, hsmfile, "file not found", poolname);
			c = USERR;
			goto reply;
		} else {
			c = 0;
		}
	} else {
		sendrep(rpfd, MSG_ERR, "STG02 - Supply of options xor parameters is mandatory\n");
		c = USERR;
		goto reply;
	}


 reply:
	free (argv);
	sendrep (rpfd, STAGERC, STAGEFILCHG, c);
}


