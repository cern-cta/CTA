/*
 * $Id: procfilchg.c,v 1.17 2002/01/23 13:52:00 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procfilchg.c,v $ $Revision: 1.17 $ $Date: 2002/01/23 13:52:00 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
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
#include "rfio_api.h"

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
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern int stglogit _PROTO(());
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
extern char *findpoolname _PROTO((char *));

extern char defpoolname[CA_MAXPOOLNAMELEN + 1];
static int mintime_beforemigr_flag = 0;
static int reqid_flag = 0;
static int retenp_on_disk_flag = 0;
static int status_flag = 0;
static int poolname_flag = 0;
extern int check_hsm_type_light _PROTO((char *, char *));
extern int isvalidpool _PROTO((char *));
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern int mintime_beforemigr _PROTO((int));
extern int retenp_on_disk _PROTO((int));
extern int max_setretenp _PROTO((char *));
extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *, int));
extern int savereqs _PROTO(());
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *));
extern void rwcountersfs _PROTO((char *, char *, int, int));
extern u_signed64 findblocksize _PROTO((char *));
extern int updfreespace _PROTO((char *, char *, signed64));
extern int stageput_check_hsm _PROTO((struct stgcat_entry *, uid_t, gid_t, int));

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
	int c, i, rc;
	gid_t gid;
	int nargs;
	uid_t uid;
	char *user;
	char *rbp;
	int clientpid;
	int errflg = 0;
	char *hsmfile = NULL;
	char t_or_d;
	int thismintime_beforemigr = -1;
	int donemintime_beforemigr = 0;
	int thisreqid = -1;
	int donereqid = 0;
	int thisretenp_on_disk = -1;
	int doneretenp_on_disk = 0;
	int thisunit_retenp = ONE_SECOND; /* Default is second */
	int thisunit_mintime = ONE_SECOND; /* Default is second */
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
		{"mintime_beforemigr", REQUIRED_ARGUMENT,  &mintime_beforemigr_flag, 1},
		{"poolname",           REQUIRED_ARGUMENT,  &poolname_flag,       1},
		{"reqid",              REQUIRED_ARGUMENT,  &reqid_flag,          1},
		{"retenp_on_disk",     REQUIRED_ARGUMENT,  &retenp_on_disk_flag, 1},
		{"status",             REQUIRED_ARGUMENT,  &status_flag,         1},
		{NULL,                 0,                  NULL,                 0}
	};

	poolname[0] = '\0';
	mintime_beforemigr_flag = 0;
	reqid_flag = 0;
	retenp_on_disk_flag = 0;
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
			   reqid, STAGEFILCHG, 0, 0, NULL, "", (char) 0);
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
			if ((mintime_beforemigr_flag != 0) && (! donemintime_beforemigr)) {
				/* If the latest character is 'S' or 's', 'H' or 'h', 'D' or 'd', 'Y' or 'y' */
				/* we accept it */
				if (strlen(Coptarg) > 1) {
					switch (Coptarg[strlen(Coptarg) - 1]) {
					case 'S':
					case 's':
						/* Second - already the default */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					case 'H':
					case 'h':
						thisunit_mintime = ONE_HOUR; /* Hour */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					case 'D':
					case 'd':
						thisunit_mintime = ONE_DAY; /* Day */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					case 'Y':
					case 'y':
						thisunit_mintime = ONE_YEAR; /* Year */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					default:
						/* If there is another non-digit character the stage_strtoi() call will catch it */
						break;
					}
				}
				if (stage_strtoi(&thismintime_beforemigr, Coptarg, &dp, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG06, (serrno != ERANGE) ? "--mintime_beforemigr" : "--mintime_beforemigr (out of range)");
					errflg++;
				}
				if (thismintime_beforemigr < 0) thismintime_beforemigr = -1;
				donemintime_beforemigr = 1;
			}
			if ((reqid_flag != 0) && (! donereqid)) {
				if (stage_strtoi(&thisreqid, Coptarg, &dp, 10) != 0) {
					sendrep (rpfd, MSG_ERR, STG06, (serrno != ERANGE) ? "--reqid" : "--reqid (out of range)");
					errflg++;
				}
				donereqid = 1;
			}
			if ((retenp_on_disk_flag != 0) && (! doneretenp_on_disk)) {
				/* If the latest character is 'S' or 's', 'H' or 'h', 'D' or 'd', 'Y' or 'y' */
				/* we accept it */
				if (strlen(Coptarg) > 1) {
					switch (Coptarg[strlen(Coptarg) - 1]) {
					case 'S':
					case 's':
						/* Second - already the default */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					case 'H':
					case 'h':
						thisunit_retenp = ONE_HOUR; /* Hour */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					case 'D':
					case 'd':
						thisunit_retenp = ONE_DAY; /* Day */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					case 'Y':
					case 'y':
						thisunit_retenp = ONE_YEAR; /* Year */
						Coptarg[strlen(Coptarg) - 1] = '\0';
						break;
					default:
						/* If there is another non-digit character the stage_strtoi() call will catch it */
						break;
					}
				}
				if (stage_strtoi(&thisretenp_on_disk, Coptarg, &dp, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG06, (serrno != ERANGE) ? "--retenp_on_disk" : "--retenp_on_disk (out of range)");
					errflg++;
				}
				if (thisretenp_on_disk < 0) thisretenp_on_disk = -1;
				doneretenp_on_disk = 1;
			}
			if ((status_flag != 0) && (! donestatus)) {
				if (stage_strtoi(&thisstatus, Coptarg, &dp, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG06, (serrno != ERANGE) ? "--status" : "--status (out of range)");
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

	if (thisunit_mintime != ONE_SECOND) {
		/* We check that --mintime_beforemigr is not out of range */
		if (thismintime_beforemigr > (INT_MAX / thisunit_mintime)) {
			sendrep (rpfd, MSG_ERR, STG06, "--mintime_beforemigr (out of range)");
			errflg++;
		} else {
			thismintime_beforemigr *= thisunit_mintime;
		}
	}

	if (thisunit_retenp != ONE_SECOND) {
		/* We check that --retenp_on_disk is not out of range */
		if (thisretenp_on_disk > (INT_MAX / thisunit_retenp)) {
			sendrep (rpfd, MSG_ERR, STG06, "--retenp_on_disk (out of range)");
			errflg++;
		} else {
			thisretenp_on_disk *= thisunit_retenp;
		}
	}

	if (errflg != 0) {
		c = USERR;
		goto reply;
	}

	if (poolname[0] == '\0') {
		/* Apply the default poolname */
		strcpy(poolname,defpoolname);
	}

	if ((nargs > Coptind) && (hsmfile || donemintime_beforemigr || donereqid || doneretenp_on_disk || donestatus)) {
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
	} else if (doneretenp_on_disk) { /* --retenp_on_dik */
		/* Note: MAX_SETRETENP can be < 0, e.g. allow no retention period set... */
		if ((max_setretenp(poolname) != 0) && (thisretenp_on_disk > (max_setretenp(poolname) * ONE_DAY))) {
			/* This apply unless user gave special value corresponding to AS_LONG_AS_POSSIBLE */
			if (thisretenp_on_disk != AS_LONG_AS_POSSIBLE) {
				sendrep (rpfd, MSG_ERR, STG147, "--retenp_on_disk", max_setretenp(poolname), (max_setretenp(poolname) > 1 ? "days" : "day"));
				c = USERR;
				goto reply;
			}
		}
	}

	if (hsmfile || donemintime_beforemigr || donereqid || doneretenp_on_disk || donestatus) {
		if (! hsmfile) {
			sendrep(rpfd, MSG_ERR, "STG02 - Supply of -M option is mandatory\n");
			c = USERR;
			goto reply;
		}
		if (! (donemintime_beforemigr || doneretenp_on_disk || donestatus)) {
			sendrep(rpfd, MSG_ERR, "STG02 - Supply of --mintime_beforemigr, --retenp_on_disk or --status is mandatory\n");
			c = USERR;
			goto reply;
		}
		/* User choosed the 'option' way of using stagechng */
		for (stcp = stcs; stcp < stce; stcp++) {
			struct Cns_fileid Cnsfileid;       /* For     CASTOR hsm IDs */
			struct Cns_filestat Cnsfilestat;   /* For     CASTOR hsm stat() */
			extern struct passwd start_passwd; /* Start uid/gid stage:st */
			int changed, loop_break;
			int currentmintime_beforemigr;
			struct stgcat_entry thisstcp;
			char checkpath[CA_MAXPATHLEN+1];
			char tmpbuf1[21];
			char tmpbuf2[21];
			u_signed64 hsmsize;

			changed = loop_break = 0;
			if (stcp->reqid == 0) break;
			if (donereqid && stcp->reqid != thisreqid) continue; /* --reqid */
			if (stcp->t_or_d != 'h') continue; /* Only CASTOR HSM filename supported */
			if (donestatus)
				/* Should be a complete record if user want to change status */
				if ((stcp->u1.h.server[0] == '\0') || (stcp->u1.h.fileid == 0)) continue;
			/* Grab the fileclass */
			if (poolflag < 0) { /* -p NOPOOL */
				if (stcp->poolname[0]) continue;
			} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
			if ((ifileclass = upd_fileclass(NULL,stcp)) < 0) {
				c = USERR;
				goto reply;
			}
			if (strcmp(stcp->u1.h.xfile, hsmfile) != 0) continue; /* -M */
			/* We check if the name of this file in the name server is really the one we have in input */
			if (Cns_getpath(stcp->u1.h.server, stcp->u1.h.fileid, checkpath) == 0) {
				if (strcmp(checkpath, stcp->u1.h.xfile) != 0) {
					sendrep (rpfd, MSG_ERR, STG157, stcp->u1.h.xfile, checkpath, u64tostr((u_signed64) stcp->u1.h.fileid, tmpbuf1, 0), stcp->u1.h.server);
					strncpy(stcp->u1.h.xfile, checkpath, (CA_MAXHOSTNAMELEN+MAXPATH));
					stcp->u1.h.xfile[(CA_MAXHOSTNAMELEN+MAXPATH)] = '\0';
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					savereqs();
					/* So, by definition, stcp->u1.h.xfile and hsmfile do not match anymore */
					continue;
				}
			}

			found++;

			if (donestatus) {
				/* We check if we have permissions to do so using the NameServer */
				memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
				strcpy(Cnsfileid.server,stcp->u1.h.server);
				Cnsfileid.fileid = stcp->u1.h.fileid;
				setegid(gid);
				seteuid(uid);
				rc = Cns_statx(hsmfile, &Cnsfileid, &Cnsfilestat);
				setegid(start_passwd.pw_gid);
				seteuid(start_passwd.pw_uid);
				if (rc != 0) {
					sendrep (rpfd, MSG_ERR, STG02, hsmfile, "Cns_statx", sstrerror(serrno));
					c = USERR;
					goto reply;			
				}
				/* Thanks to Cns_stat we can also verify the size of the file */
				hsmsize = Cnsfilestat.filesize;
			}

			/* From now on we assume that permission to change status for this file is granted */

			thisstcp = *stcp;
			if (donemintime_beforemigr) { /* --mintime_beforemigr */
				/* Changing mintime_beforemigr is possible only on records with STAGEOUT|CAN_BE_MIGR status */
				/* or STAGEOUT */
				if ((stcp->status != (STAGEOUT|CAN_BE_MIGR)) && (stcp->status != STAGEOUT)) {
					sendrep(rpfd, MSG_ERR, STG02, hsmfile, "--mintime", "should be in STAGEOUT|CAN_BE_MIGR or STAGEOUT status");
					c = USERR;
					goto reply;			
				}
				if ((currentmintime_beforemigr = thisstcp.u1.h.mintime_beforemigr) < 0)
					currentmintime_beforemigr = mintime_beforemigr(ifileclass); /* We take the default */
				if ((thisstcp.u1.h.mintime_beforemigr = thismintime_beforemigr) != stcp->u1.h.mintime_beforemigr) {
					changed++;
					sendrep (rpfd, MSG_OUT, STG152, hsmfile,
							 currentmintime_beforemigr,
							 (currentmintime_beforemigr > 1 ? "s" : ""),
							 (stcp->u1.h.mintime_beforemigr < 0 ? "fileclass" : "explicit"),
							 thismintime_beforemigr >= 0 ? thismintime_beforemigr : mintime_beforemigr(ifileclass),
							 (thismintime_beforemigr >= 0 ? thismintime_beforemigr : mintime_beforemigr(ifileclass)) > 1 ? "s" : "",
							 (thismintime_beforemigr < 0 ? "fileclass" : "explicit"));
				} else {
					sendrep (rpfd, MSG_OUT, STG150, hsmfile,
							 thismintime_beforemigr >= 0 ? thismintime_beforemigr : mintime_beforemigr(ifileclass),
							 (thismintime_beforemigr >= 0 ? thismintime_beforemigr : mintime_beforemigr(ifileclass)) > 1 ? "s" : "",
							 (thismintime_beforemigr < 0 ? "fileclass" : "explicit"));
				}
			}
			if (doneretenp_on_disk) { /* --retenp_on_disk */
				if ((thisstcp.u1.h.retenp_on_disk = thisretenp_on_disk) != stcp->u1.h.retenp_on_disk) {
					changed++;
				}
			}
			if (donestatus) { /* --status */
				u_signed64 actual_size_block;
				struct stat st;
				int save_stcp_status;

				switch (thisstatus) {
				case STAGEOUT|CAN_BE_MIGR:
					/* Will work only if this stcp is in STAGEOUT|CAN_BE_MIGR|PUT_FAILED or STAGEOUT|PUT_FAILED */
					switch (stcp->status) {
					case STAGEOUT|PUT_FAILED:
					case STAGEOUT|CAN_BE_MIGR|PUT_FAILED:

						/* We have to find if there are other stcp's refering the same file */
						/* If yes - we have to check also their status and delete them */
						/* Note that this is safe to continue the loop from where we are: */
						/* If being there this mean that we are dealing with the first of */
						/* the entries in the catalog that match the option values */
						/* and we know by construction that delreq() will not realloc() the */
						/* memory so our current pointer, stcp, will be safe even if we delete */
						/* any other entry that is after */
						if (! donereqid) {
							/* No need to do this loop if reqid was explicitely given */
							struct stgcat_entry *stcp2;

							for (stcp2 = stcp+1; stcp2 < stce; stcp2++) {
								if (stcp2->reqid == 0) break;
								if (stcp2->t_or_d != 'h') continue;
								if (poolflag < 0) { /* -p NOPOOL */
									if (stcp2->poolname[0]) continue;
								} else if (*poolname && strcmp (poolname, stcp2->poolname)) continue;
								/* By definition we made sure that stcp contains valid [server,fileid] */
								if ((stcp->u1.h.fileid != 0) &&
									(stcp2->u1.h.fileid != stcp->u1.h.fileid)) continue;
								if ((stcp->u1.h.server[0] != '\0') &&
									(strcmp(stcp2->u1.h.server,stcp->u1.h.server) != 0)) continue;
								/* From now on we should be able to say that stcp2 and stcp really */
								/* refers to the SAME CASTOR HSM file */
								/* The last protection is to check if they have the same internal */
								/* path - this must match - otherwise it indicates that there are two */
								/* entries in the catalog that use two different physical */
								/* files themselves refering to the SAME CASTOR HSM file */
								/* This is a consistency problem for us */
								if (strcmp(stcp->ipath,stcp2->ipath) != 0) {
									sendrep (rpfd, MSG_ERR, STG169, hsmfile, stcp->ipath, stcp2->ipath);
									c = USERR;
									goto reply;			
								}
								/* We repeat the test on the status */
								switch (stcp2->status) {
								case STAGEOUT|PUT_FAILED:
								case STAGEOUT|CAN_BE_MIGR|PUT_FAILED:
									break;
								default:
									sendrep(rpfd, MSG_ERR, STG170, hsmfile, stcp->reqid, stcp2->reqid);
									c = USERR;
									goto reply;			
								}
								/* Good - we can delete this entry - it has no indicence on */
								/* migration counters btw, since it was already in PUT_FAILED */
								/* e.g. not a candidate */
								delreq(stcp2);
                                stcp2--;
							}
						}
						/* From now on there should be only ONE entry to deal with */
						/* Either because --reqid was specified, either because */
						/* we deleted all but stcp entries that match option values */
						/* We simulate a stageout followed by a stageupdc */
						PRE_RFIO;
						if (RFIO_STAT(stcp->ipath, &st) == 0) {
							stcp->actual_size = st.st_size;
							if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
								actual_size_block = stcp->actual_size;
							}
						} else {
							stglogit (func, STG02, stcp->ipath, RFIO_STAT_FUNC(stcp->ipath), rfio_serror());
							/* No block information - assume mismatch with actual_size will be acceptable */
							actual_size_block = stcp->actual_size;
						}
						/* We grabbed the size in the name server before. We verify consistency */
						if (hsmsize != stcp->actual_size) {
							sendrep(rpfd, MSG_ERR, STG171, hsmfile, u64tostr((u_signed64) hsmsize, tmpbuf1, 0), u64tostr((u_signed64) stcp->actual_size, tmpbuf2, 0));
							c = USERR;
							goto reply;			
						}
						save_stcp_status = stcp->status;
						stcp->status = STAGEOUT;
						updfreespace (
							stcp->poolname,
							stcp->ipath,
							(signed64) ((signed64) actual_size_block - (signed64) stcp->size * (signed64) ONE_MB)
							);
						rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT);
						if (! donereqid) {
							/* No specific reqid, this mean that other entries could have been deleted and it is */
							/* really a new starting point - if there was a tape pool yet assigned we remove it */
							/* so that it is automatically expanded */
							stcp->u1.h.tppool[0] = '\0';
						}
						if ((c = upd_stageout(STAGEUPDC, NULL, NULL, 1, stcp, 1)) != 0) {
							if (c != CLEARED) {
								stcp->status = save_stcp_status;
#ifdef USECDB
								if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
									stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
								}
#endif
								savereqs();
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
						sendrep(rpfd, MSG_ERR, STG02, hsmfile, "--status", "should be in STAGEOUT|CAN_BE_MIGR|PUT_FAILED or STAGEOUT|PUT_FAILED status");
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
				/* In any case, we have already performed cross-check with the rest of the catalog */
				/* and another pass is not anymore necessary */
				loop_break = 1;
			}
			if (changed) {
#ifdef USECDB
				if (donemintime_beforemigr) { /* --mintime_beforemigr */
					stcp->u1.h.mintime_beforemigr = thisstcp.u1.h.mintime_beforemigr;
				}
				if (doneretenp_on_disk) { /* --retenp_on_disk */
					stcp->u1.h.retenp_on_disk = thisstcp.u1.h.retenp_on_disk;
				}
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs();
			}
			if (loop_break) break;
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


