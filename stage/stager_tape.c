/*
 * $Id: stager_tape.c,v 1.2 2001/12/05 18:20:41 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Do we want to maintain maxsize for the transfert... ? */
/* #define SKIP_FILEREQ_MAXSIZE */

/* Do you want to hardcode the limit of number of file sequences on a tape ? */
/* Not the default in TAPE mode */
/* #define MAX_TAPE_FSEQ 9999 */

/* Do you want to hardcode the limit of number of files per rtcpc() request ? */
#define MAX_RTCPC_FILEREQ 1000

/* If you want to force a specific tape server, compile it with: */
/* -DTAPESRVR=\"your_tape_server_hostname\" */
/* #define TAPESRVR "shd63" */

#define USE_SUBREQID

#ifdef STAGE_CSETPROCNAME
#define STAGE_CSETPROCNAME_FORMAT_DISK "%s %s %s"
#define STAGE_CSETPROCNAME_FORMAT_TAPE "%s %s %s.%d"
#define STAGE_CSETPROCNAME_FORMAT_CASTOR "%s %s %s.%d %s"
#include "Csetprocname.h"
#endif

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_tape.c,v $ $Revision: 1.2 $ $Date: 2001/12/05 18:20:41 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#ifndef _WIN32
#include <unistd.h>                 /* For getcwd() etc... */
#include <sys/time.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include "log.h"
#include "rtcp_api.h"
#include "serrno.h"
#include "Castor_limits.h"
#include "stage_api.h" /* Because of struct definitions and stage_strtoi() */
#include "u64subr.h"
#include "osdep.h"
#include "Cpwd.h"
#include "Cgrp.h"

#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
EXTERN_C void DLL_DECL stager_usrmsg _PROTO(());
EXTERN_C void DLL_DECL stager_migmsg _PROTO(());
#else
EXTERN_C void DLL_DECL stager_usrmsg _PROTO((int, ...));
EXTERN_C void DLL_DECL stager_migmsg _PROTO((int, ...));
#endif
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());

int stage_tape _PROTO(());
void cleanup _PROTO(());
void stagekilled _PROTO(());
int build_rtcpcreq _PROTO((int *, tape_list_t ***, struct stgcat_entry *, struct stgcat_entry *, struct stgcat_entry *, struct stgcat_entry *));
void free_rtcpcreq _PROTO((tape_list_t ***));
int unpackfseq _PROTO((char *, int, char *, fseq_elem **));
int stager_tape_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
void stager_tape_log_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
extern int (*rtcpc_ClientCallback) _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
void init_hostname _PROTO(());
int get_subreqid _PROTO((struct stgcat_entry *));

char func[16];                      /* This executable name in logging */
int Aflag;                          /* Allocation flag */
int api_flag;                       /* Api flag, .e.g we will NOT re-arrange the file sequences in case of a tape request */
int concat_off_fseq;                /* Fseq where begin concatenation off */
int silent;                         /* Tells if we are running in silent mode or not */
int use_subreqid;                   /* Tells if we allow asynchroneous callbacks from RTCOPY */
int nbcat_ent = -1;                 /* Number of catalog entries in stdin */
int nbcat_ent_current = -1;         /* Number of catalog entries currently processed - used in callback */
int istart = -1;                    /* Index of first entry currently processed - used in callback */
int iend = -1;                      /* Index of last entry currently processed - used in callback */
char hostname[CA_MAXHOSTNAMELEN+1]; /* Current hostname */
int reqid = 0;                      /* Request id */
int key;                            /* Request id associated key */
int rpfd = -1;                      /* Reply socket file descriptor */
struct stgcat_entry *stce = NULL;   /* End of stage catalog */
struct stgcat_entry *stcs = NULL;   /* Start of stage catalog */
struct stgcat_entry *stcp_end = NULL; /* End of tmp inline stage catalog */
struct stgcat_entry *stcp_start = NULL; /* Start of tmp inline stage catalog */
int nrtcpcreqs = 0;                 /* Number of rtcpcreq structures in circular list */
tape_list_t **rtcpcreqs = NULL;     /* rtcp request itself (circular list) */

struct passwd start_passwd;
int callback_error = 0;             /* Flag to tell us that there was an error in the callback */
int fatal_callback_error = 0;       /* Flag to tell us that there error in the callback was fatal or not */
int nhpss = 0;
int ncastor = 0;
int rtcpc_kill_cleanup = 0;         /* Flag to tell if we have to call rtcpc_kill() in the signal handler */
int callback_fseq = 0;              /* Last fseq as transfered OK and seen in the stage_tape callback */
int callback_nok = 0;               /* Number of fseq transfered OK and seen in the stage_tape callback */
int dont_change_srv = 0;            /* If != 0 means that we will not change tape_server if rtcpc() retry - Only stage_tape() */
#ifdef STAGE_CSETPROCNAME
static char sav_argv0[1024+1];
#endif

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

#ifdef STAGER_DEBUG
#ifdef RETRYI
#undef RETRYI
#endif
#define RETRYI 5
#ifndef SLEEP_DEBUG
#define SLEEP_DEBUG 10
#endif
EXTERN_C int DLL_DECL dumpTapeReq _PROTO((tape_list_t *));
EXTERN_C int DLL_DECL dumpFileReq _PROTO((file_list_t *));
#define STAGER_RTCP_DUMP(rtcpreq) {              \
	tape_list_t *dumptl = rtcpreq;               \
	do {                                         \
		file_list_t *dumpfl;                     \
		dumpTapeReq(dumptl);                     \
		dumpfl = dumptl->file;                   \
		do {                                     \
			dumpFileReq(dumpfl);                 \
			dumpfl = dumpfl->next;               \
		} while (dumptl->file != dumpfl);        \
		dumptl = dumptl->next;                   \
	} while (rtcpreq != dumptl);                 \
}
#endif

/* These declarations and macros will make sure that MSG_ERR messages will always */
/* be logged via stglogit() in sendrep.c [especially for migrators]               */
int save_euid, save_egid;
#define SAVE_EID {                               \
	save_euid = geteuid();                       \
	save_egid = getegid();                       \
	SETEID(0,0);                                 \
}
#define RESTORE_EID {                            \
	SETEID(save_euid,save_egid);                 \
}

/* This macro resumes all the sensible variables for rtcpc() */
/* that needs to correctly initialized by the client.        */
#define STAGER_FILEREQ_INIT(filereq) {           \
	filereq.VolReqID = -1;                       \
	filereq.jobID = -1;                          \
	filereq.position_method = -1;                \
	filereq.tape_fseq = -1;                      \
	filereq.disk_fseq = -1;                      \
	filereq.blocksize = -1;                      \
	filereq.recordlength = -1;                   \
	filereq.retention = -1;                      \
	filereq.def_alloc = -1;                      \
	filereq.rtcp_err_action = -1;                \
	filereq.tp_err_action = -1;                  \
	filereq.convert = -1;                        \
	filereq.check_fid = NEW_FILE;                \
	filereq.concat = NOCONCAT;                   \
	filereq.maxsize = 0;                         \
	filereq.stageSubreqID = -1;                  \
	filereq.err.max_tpretry = -1;                \
	filereq.err.max_cpretry = -1;                \
	filereq.err.severity = RTCP_OK;              \
}
#define STAGER_TAPEREQ_INIT(tapereq) {           \
	tapereq.err.max_tpretry = -1;                \
	tapereq.err.max_cpretry = -1;                \
}
#define STAGER_RTCP_INIT(filereq,tapereq) {      \
	STAGER_FILEREQ_INIT(filereq);                \
	STAGER_TAPEREQ_INIT(tapereq);                \
}

/* The following macros will set the correct uid/gid or euid/egid for tape operations */

#define SETTAPEEID(thiseuid,thisegid) {          \
	setegid(start_passwd.pw_gid);                \
	seteuid(start_passwd.pw_uid);                \
	setegid(thisegid);                           \
	seteuid(thiseuid);                           \
}

#define SETTAPEID(thisuid,thisgid) {             \
	setegid(start_passwd.pw_gid);                \
	seteuid(start_passwd.pw_uid);                \
	setgid(thisgid);                             \
	setuid(thisuid);                             \
}

/* The following macros will set the correct uid/gid or euid/egid for nameserver operations */

#define SETID(thisuid,thisgid) {                 \
	setegid(start_passwd.pw_gid);                \
	seteuid(start_passwd.pw_uid);                \
	setgid(thisgid);                             \
	setuid(thisuid);                             \
}

#define SETEID(thiseuid,thisegid) {              \
	setegid(start_passwd.pw_gid);                \
	seteuid(start_passwd.pw_uid);                \
	setegid(thisegid);                           \
	seteuid(thiseuid);                           \
}

/* This macro call the cleanup at exit if necessary */
/* If hsm_status != NULL then we know we were doing */
/* CASTOR migration.                                */
/* It can be that rtcp() fails because of some      */
/* protocol error, but from the stager point of     */
/* view, we can know of the migration of all the    */
/* files did perform ok.                            */
/* The subtility with hsm_ignore[] is that if we    */
/* exit with USERR there will be no retry - if we   */
/* exit with SYERR there will be a retry...         */
#define RETURN(exit_code) {                      \
    int correct_exit_code;                       \
                                                 \
    correct_exit_code = exit_code;               \
	if (exit_code != 0) {                        \
		cleanup();                               \
	}                                            \
	if (rtcpcreqs != NULL) {                     \
		free_rtcpcreq(&rtcpcreqs);               \
		rtcpcreqs = NULL;                        \
	}                                            \
	return(correct_exit_code);                   \
}

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

#ifdef STAGE_CSETPROCNAME
int main(argc,argv,envp)
		 int argc;
		 char **argv;
		 char **envp;
#else
int main(argc,argv)
		 int argc;
		 char **argv;
#endif
{
	int c;
	int l;
	int nretry;
#ifdef STAGER_DEBUG
	int i;
#endif
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
	struct passwd *this_passwd;
	struct passwd root_passwd;
#ifdef __INSURE__
	char *tmpfile;
	FILE *f;
#endif

	strcpy (func, "stager");
	stglogit (func, "function entered\n");

#ifdef STAGE_CSETPROCNAME
	strncpy(sav_argv0,argv[0],1024);
	sav_argv0[1024] = '\0';
	if (Cinitsetprocname(argc,argv,envp) != 0) {
		stglogit(func,"### Cinitsetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif

#ifdef STAGER_DEBUG
	{
		int i;

		sendrep(atoi (argv[3]), MSG_ERR, "[DEBUG] argc = %d\n", argc);

		for (i = 0; i < argc; i++) {
			sendrep(atoi (argv[3]), MSG_ERR, "[DEBUG] argv[%d] = %s\n", i, argv[i]);
		}
	}
#endif
	reqid = atoi (argv[1]);
#ifdef STAGER_DEBUG
	sendrep(atoi (argv[3]), MSG_ERR, "[DEBUG] reqid = %d\n", reqid);
#endif
	key = atoi (argv[2]);
#ifdef STAGER_DEBUG
	sendrep(atoi (argv[3]), MSG_ERR, "[DEBUG] key = %d\n", key);
#endif
	rpfd = atoi (argv[3]);
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] rpfd = %d\n", rpfd);
#endif
	nbcat_ent = atoi (argv[4]);
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] nbcat_ent = %d\n", nbcat_ent);
#endif
	nretry = atoi (argv[5]);
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] nretry = %d\n", nretry);
#endif
	Aflag = atoi (argv[6]);
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] Aflag = %d\n", Aflag);
#endif
	concat_off_fseq = atoi (argv[7]);
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] concat_off_fseq = %d\n", concat_off_fseq);
#endif
	silent = atoi (argv[8]);
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] silent = %d\n", silent);
#endif
#ifdef USE_SUBREQID
	use_subreqid = atoi (argv[9]);
#else
	use_subreqid = 0;
	if (use_subreqid != atoi(argv[9])) {
		/* We says in the log-file that this feature is hardcoded to not supported by stager.c */
		/* whereas the stgdaemon specified it in the waitq. This means that the callbacks are */
		/* forced to all synchroneous. */
		stglogit (func, "Asynchroneous callback specified by stgdaemon disabled\n");
	}
#endif
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] use_subreqid = %d\n", use_subreqid);
#endif
	api_flag = atoi (argv[10]);
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] api_flag = %d\n", api_flag);
#endif
#ifdef __INSURE__
	tmpfile = argv[11];
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] tmpfile = %s\n", tmpfile);
#endif
#endif

	if ((stcs = (struct stgcat_entry *) malloc (nbcat_ent * sizeof(struct stgcat_entry))) == NULL) {
		exit (SYERR);
	}
	stcp = stcs;

#ifdef __INSURE__
	if ((f = fopen(tmpfile,"r")) == NULL) {
		free(stcs);
		exit (SYERR);
	}
	fread(stcp,sizeof(struct stgcat_entry),nbcat_ent,f);
	fclose(f);
	remove(tmpfile);
	stce = stcs + nbcat_ent;
#else
	while ((l = read (0, &stgreq, sizeof(stgreq)))) {
		if (l == sizeof(stgreq)) {
			memcpy (stcp, &stgreq, sizeof(stgreq));
			stcp++;
		}
	}
	stce = stcp;
	close (0);
#endif

#ifdef STAGER_DEBUG
	{
		int i;

		for (i = 0; i < nbcat_ent; i++) {
			dump_stcp(rpfd, stcs + i, &sendrep);
        }
	}
#endif

#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(rpfd, MSG_ERR, "[DEBUG] GO ON WITH gdb /usr/local/bin/stager_tape %d, then break %d\n",getpid(),__LINE__ + 6);
	sendrep(rpfd, MSG_ERR, "[DEBUG] sleep(%d)\n", SLEEP_DEBUG);
	sleep(SLEEP_DEBUG);
	RESTORE_EID;
#endif

	/* Initialize hostname */
	init_hostname();

	/* We get information on current uid/gid */
	if ((this_passwd = Cgetpwuid(getuid())) == NULL) {
		stglogit(func, "### Cannot Cgetpwuid(%d) (%s)\n",(int) getuid(), strerror(errno));
		stglogit(func, "### Please check existence of current uid %d in password file\n", (int) getuid());
 		exit (SYERR);
	}
	start_passwd = *this_passwd;
	if (Cgetgrgid(start_passwd.pw_gid) == NULL) {
		stglogit(func, "### Cannot Cgetgrgid(%d) (%s)\n",start_passwd.pw_gid,strerror(errno));
		stglogit(func, "### Please check existence of group %d (gid of account \"%s\") in group file\n", start_passwd.pw_gid, start_passwd.pw_name);
 		exit (SYERR);
	}

	/* We check that we currently run under root account */
	if ((this_passwd = Cgetpwnam("root")) == NULL) {
		stglogit(func, "### Cannot Cgetpwnam(\"%s\") (%s)\n","root",strerror(errno));
		stglogit(func, "### Please check existence of account \"%s\" in password file\n", "root");
	} else {
		root_passwd = *this_passwd;
		if (Cgetgrgid(root_passwd.pw_gid) == NULL) {
			stglogit(func, "### Cannot Cgetgrgid(%d) (%s)\n",root_passwd.pw_gid,strerror(errno));
			stglogit(func, "### Please check existence of group %d (gid of account \"%s\") in group file\n", (int) root_passwd.pw_gid, "root");
	 		/* If "root" group does not exist in group file (!), let's continue anyway */
		} else {
			if ((root_passwd.pw_uid != start_passwd.pw_uid) ||
				(root_passwd.pw_gid != start_passwd.pw_gid)) {
				stglogit(func, "### Warning : You, \"%s\" (uid=%d,gid=%d), are NOT running under \"root\" (uid=%d,gid=%d) account. Functionnality might very well be extremely limited.\n", start_passwd.pw_name, start_passwd.pw_uid, start_passwd.pw_gid, root_passwd.pw_uid, root_passwd.pw_gid);
			}
		}
	}

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->t_or_d != 't') {
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "### HSM file is of unvalid type ('%c')\n",stcp->t_or_d);
			RESTORE_EID;
			free(stcs);
			exit(USERR);
		}
	}

	if (concat_off_fseq > 0) {
		if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
			/* By precaution, we allow concat_off only for stagein here again */
			SAVE_EID;
    	    sendrep(rpfd, MSG_ERR, "### concat_off option not allowed in write-to-tape\n");
			RESTORE_EID;
			free(stcs);
	        exit(USERR);
		}
		if (! Aflag) {
			/* By precaution, we allow concat_off only for deferred allocation */
			SAVE_EID;
    	    sendrep(rpfd, MSG_ERR, "### concat_off option allowed only in deffered allocation mode\n");
			RESTORE_EID;
			free(stcs);
	        exit(USERR);
		}
		if (use_subreqid != 0) {
			/* By precaution, we do not allow async callback if concat_off_fseq is set */
			SAVE_EID;
    	    sendrep(rpfd, MSG_ERR, "### concat_off option is not compatible with async callback\n");
			RESTORE_EID;
			free(stcs);
	        exit(USERR);
		}
	}
    if (use_subreqid != 0) {
		if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "### async callback is not allowed in write-to-tape\n");
			RESTORE_EID;
			free(stcs);
			exit(USERR);
		}
    }
	(void) umask (stcs->mask);

	signal (SIGINT, stagekilled);        /* If client died */
	signal (SIGTERM, stagekilled);       /* If killed from administrator */
	if (nretry) sleep (RETRYI);

	/* Redirect RTCOPY log message directly to user's console */
	rtcp_log = (void (*) _PROTO((int, CONST char *, ...))) (silent != 0 ? &stager_migmsg : &stager_usrmsg);

	/* -------- TAPE TO TAPE ----------- */

#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] GO ON WITH gdb /usr/local/bin/stager_tape %d, then break stage_tape\n",getpid());
	sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] sleep(%d)\n",SLEEP_DEBUG);
	sleep(SLEEP_DEBUG);
	RESTORE_EID;
#endif
#ifdef STAGE_CSETPROCNAME
	if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_TAPE,
					sav_argv0,
					"STARTING",
					"???",
					0
					) != 0) {
		stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif

	/* We detect if tape server have been explicitely given on command-line */
	if (stcs->u1.t.tapesrvr[0] != '\0') {
		dont_change_srv = 1;
	}

	c = stage_tape();

	free(stcs);
	exit (c);
}

int stage_tape() {
	int rtcp_rc, save_serrno;
#ifdef MAX_RTCPC_FILEREQ
	int issplitted;
	int nsplitted = 0;
#endif

	SETTAPEEID(stcs->uid,stcs->gid);

	/* Set CallbackClient */
	rtcpc_ClientCallback = &stager_tape_callback;

    stcp_start = stcs;
    stcp_end = stce;
	nbcat_ent_current = nbcat_ent;
	istart = 0;
	iend = nbcat_ent - 1;

#ifdef MAX_RTCPC_FILEREQ
	if (nsplitted == 0) {
		/* First round */
	check_issplitted:
		issplitted = 0;
		if ((iend - istart + 1) > MAX_RTCPC_FILEREQ) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, "STG02 - stage_tape : MAX_RTCPC_FILEREQ=%d reached. Request will be sequentially splitted (round No %d)\n",MAX_RTCPC_FILEREQ, ++nsplitted);
			RESTORE_EID;
			iend = MAX_RTCPC_FILEREQ + istart - 1;
			stcp_end = stcp_start + MAX_RTCPC_FILEREQ;
			nbcat_ent_current = MAX_RTCPC_FILEREQ;
			issplitted = 1;
		}
	} else {
	next_issplitted:
		/* Second or more round */
		/* By definition a tape request is always sequential, so we just need to increment the correct pointers */
		stcp_start = stcp_end;
		stcp_end = stce;
		nbcat_ent_current = stcp_end - stcp_start;
		istart = ++iend;
		iend = nbcat_ent - 1;
		callback_nok = 0; /* This will be seen as an independant new request */
		goto check_issplitted;
	}
#endif

	if (build_rtcpcreq(&nrtcpcreqs, NULL, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
		RETURN (USERR);
	}
	if (nrtcpcreqs <= 0) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "stage_tape", "Cannot determine number of tapes",sstrerror(serrno));
		RESTORE_EID;
		RETURN (USERR);
	}

	/* Build the request */
	if (rtcpcreqs != NULL) {
		free_rtcpcreq(&rtcpcreqs);
		rtcpcreqs = NULL;
	}
	if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "", "build_rtcpcreq",sstrerror (serrno));
		RESTORE_EID;
		RETURN (SYERR);
	}


#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] Calling rtcpc()\n");
	STAGER_RTCP_DUMP(rtcpcreqs[0]);
	sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] sleep(%d)\n",SLEEP_DEBUG);
	sleep(SLEEP_DEBUG);
	RESTORE_EID;
#endif

 reselect:
	rtcpc_kill_cleanup = 1;
	fatal_callback_error = callback_error = 0;
#ifdef STAGE_CSETPROCNAME
	if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_TAPE,
					sav_argv0,
					"STARTING",
					rtcpcreqs[0]->tapereq.vid,
					0
					) != 0) {
		stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif
	rtcp_rc = rtcpc(rtcpcreqs[0]);
	save_serrno = serrno;
	rtcpc_kill_cleanup = 0;

    if (callback_error != 0) {
		/* This is a callback error - considered as fatal */
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "stage_tape", "callback", sstrerror(serrno));
		RESTORE_EID;
		RETURN (SYERR);
	}

	if (rtcp_rc < 0) {
		if (rtcpc_CheckRetry(rtcpcreqs[0]) == TRUE) {
			tape_list_t *tl;
			/* Rtcopy bits suggest to retry */
			CLIST_ITERATE_BEGIN(rtcpcreqs[0],tl) {
		   		*tl->tapereq.unit = '\0';
		   		if ( dont_change_srv == 0 ) *tl->tapereq.server = '\0'; 
			} CLIST_ITERATE_END(rtcpcreqs[0],tl);
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc",sstrerror(save_serrno));
			if (save_serrno == ETVBSY) {
				sendrep (rpfd, MSG_ERR, "STG47 - Re-selecting another tape server in %d seconds\n", RETRYI);
				sleep(RETRYI);
			} else {
				sendrep (rpfd, MSG_ERR, "STG47 - Re-selecting another tape server\n");
			}
			RESTORE_EID;
			if (concat_off_fseq > 0) {
				file_list_t *fl;
				int save_tapereq_err_max_tpretry = rtcpcreqs[0]->tapereq.err.max_tpretry;
				int save_tapereq_err_max_cpretry = rtcpcreqs[0]->tapereq.err.max_cpretry;
				int save_filereq_err_max_tpretry = -1;
				int save_filereq_err_max_cpretry = -1;

				/* We have to find what was the latest filereq that failed */
				CLIST_ITERATE_BEGIN(rtcpcreqs[0]->file,fl) {
					if (fl->filereq.proc_status != RTCP_FINISHED) {
						/* Should be this one - remember that -c off request are processed sequentially */
						save_filereq_err_max_tpretry = fl->filereq.err.max_tpretry;
						save_filereq_err_max_cpretry = fl->filereq.err.max_cpretry;
						break;
					}
				} CLIST_ITERATE_END(rtcpcreqs[0]->file,fl);
				/* In concat off mode we have to take care of what has been already done */
				if (callback_nok > 0) {
					if (callback_fseq >= atoi((stce-1)->u1.t.fseq)) {
						/* And we were staging some tape sequence already beyond the last [-q <tape_fseq>-] entry */
						/* Note: callback_fseq always contain the last STAGED tape sequence */
						stcp_start = stce - 1;
						concat_off_fseq = ++callback_fseq;
						sprintf(stcp_start->u1.t.fseq, "%d", concat_off_fseq);
						strcat(stcp_start->u1.t.fseq, "-");
						/* We reset those entries : it will look like a new and independant tape request */
						callback_nok = callback_fseq = 0;
						/* We reset the number of effective entries */
						nbcat_ent = 1;
					}
				}
				/* Build the request */
				if (rtcpcreqs != NULL) {
					free_rtcpcreq(&rtcpcreqs);
					rtcpcreqs = NULL;
				}
				if (build_rtcpcreq(&nrtcpcreqs, NULL, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
					RETURN (USERR);
				}
				if (nrtcpcreqs <= 0) {
					serrno = SEINTERNAL;
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, "stage_tape", "Cannot determine number of tapes",sstrerror(serrno));
					RESTORE_EID;
					RETURN (USERR);
				}
				if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, "", "build_rtcpcreq",sstrerror (serrno));
					RESTORE_EID;
					RETURN (SYERR);
				}
				rtcpcreqs[0]->tapereq.err.max_tpretry = save_tapereq_err_max_tpretry;
				rtcpcreqs[0]->tapereq.err.max_cpretry = save_tapereq_err_max_cpretry;
				/* We restore the latest max_[cp|tp]retry we found */
				CLIST_ITERATE_BEGIN(rtcpcreqs[0]->file,fl) {
					if (fl->filereq.proc_status != RTCP_FINISHED) {
						/* Should be this one - remember that -c off request are processed sequentially */
						fl->filereq.err.max_tpretry = save_filereq_err_max_tpretry;
						fl->filereq.err.max_cpretry = save_filereq_err_max_cpretry;
						break;
					}
				} CLIST_ITERATE_END(rtcpcreqs[0]->file,fl);
			}
			goto reselect;
		}

		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc", sstrerror(save_serrno));
		RESTORE_EID;
		RETURN ((save_serrno == ETHELD) ? ETHELDERR : USERR);

	} else if (rtcp_rc > 0) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Unknown error code (>0)");
		RESTORE_EID;
		RETURN (SYERR);
#ifdef MAX_RTCPC_FILEREQ
	} else {
		/* Request is successful. If it has been internally splitted we continue the work */
		if (issplitted != 0) {
			/* By definition we processed the window [stcp_start,stcp_end[, that correspond to indexes */
			/* [istart,iend], while the whole window is [stcp,stce[, that correpond to indexes */
			/* [0,nbcat_ent - 1] */
			goto next_issplitted;
		}
#endif
	}

	RETURN (0);
}
void cleanup() {
	if (rtcpc_kill_cleanup != 0) {
		/* rtcpc() API cleanup */
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, "[DEBUG-CLEANUP] Calling rtcpc_kill()\n");
		RESTORE_EID;
#endif
		rtcpc_kill();
	}
}

void stagekilled() {
	cleanup();
	exit (REQKILD);
}

/* Conversion routine between catalog entries list to a single tapereq rtcpc() structure          */
/* Keep in mind that, several catalog entries as input or not, it required that all entries       */
/* have the same "type", e.g. they are ALL tape ('t') request, hsm stagein requests or hsm        */
/* stagewrt requests. The following routine will give unknown result if catalog entries in        */
/* input do mix this flavor.                                                                      */
/* In case of a tape ('t') request : everything has to be build in one go                         */
/* In case of a stagewrt   request : vid, hsm_totalsize[] and hsm_transferedsize are used[]       */
/* In case of a stagein    request : vid, hsm_totalsize[] and hsm_transferedsize[] are used       */

/* You can "interrogate" this routine by calling it with a NULL second argument : you             */
/* will then get in return the number of rtcpc requests.                                          */

int build_rtcpcreq(nrtcpcreqs_in, rtcpcreqs_in, stcs, stce, fixed_stcs, fixed_stce)
		 int *nrtcpcreqs_in;                      /* [Output] Number of created rtcpc requests     */
		 tape_list_t ***rtcpcreqs_in;             /* [Output] Array [0..*nrtcpcreqs_in] of requests   */
		 struct stgcat_entry *stcs;            /* [Input] Loop Start point                      */
		 struct stgcat_entry *stce;            /* [Input] Loop End point                        */
		 struct stgcat_entry *fixed_stcs;      /* [Input] Start point                           */
		 struct stgcat_entry *fixed_stce;      /* [Input] End point                             */
{
	/* General variables */
	struct stgcat_entry *stcp = NULL;        /* Current catalog entry */
	int ivid;                                /* Loop on vid list if 't' */
	int i;                                   /* Loop counter on rtcpc requests */
	int nbtpf, j;
	char trailing2;
	int n;
	int nbfiles, nbfiles_orig, nbfiles_ok;
	file_list_t *fl;
	int last_disk_fseq;
	fseq_elem *fseq_list = NULL;
	int stcp_nbtpf = 0;
	int stcp_inbtpf = 0;
	int j_ok;

	if (nrtcpcreqs_in == NULL || fixed_stcs == NULL || fixed_stce == NULL) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Bad arguments to build_rtcpcreq",sstrerror(serrno));
		RESTORE_EID;
		return(-1);
	}

	/* We calculate how many rtcpc requests we need. */
	*nrtcpcreqs_in = 0;

	/* We loop on all catalog entries */
	for (stcp = fixed_stcs; stcp < fixed_stce; stcp++) {
		/* This is a tape request : we will skip our outer loop */
		for (ivid = 0; ivid < MAXVSN; ivid++) {
			if (stcp->u1.t.vid[ivid][0] == '\0') {
				/* No more tape vid */
				break;
			}
			++(*nrtcpcreqs_in);
		}
		break;
	}

	if (rtcpcreqs_in == NULL) {
		/* We were running in the "interrogate" mode */
		return(0);
	}
	
	/* We allocate the array */
	if ((*rtcpcreqs_in = (tape_list_t **) calloc(*nrtcpcreqs_in,sizeof(tape_list_t *))) == NULL) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
		RESTORE_EID;
		serrno = SEINTERNAL;
		return(-1);
	}
	
	/* We allocate content of the array */
	for (i = 0; i < *nrtcpcreqs_in; i++) {
		if (((*rtcpcreqs_in)[i] = calloc(1,sizeof(tape_list_t))) == NULL) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
			RESTORE_EID;
			serrno = SEINTERNAL;
			return(-1);
		}
	}
	
	/* We makes sure it is a circular list and correctly initialized */
	for (i = 0; i < *nrtcpcreqs_in; i++) {
		switch (i) {
		case 0:
			(*rtcpcreqs_in)[i]->prev = (*rtcpcreqs_in)[*nrtcpcreqs_in - 1];
			(*rtcpcreqs_in)[i]->next = (*rtcpcreqs_in)[*nrtcpcreqs_in > 1 ? i + 1 : 0];
			break;
		default:
			if (i == *nrtcpcreqs_in - 1) {
				(*rtcpcreqs_in)[i]->prev = (*rtcpcreqs_in)[i - 1]; /* Nota : we are there only if *nrtcpcreqs_in > 1 */
				(*rtcpcreqs_in)[i]->next = (*rtcpcreqs_in)[0];
			} else {
				(*rtcpcreqs_in)[i]->prev = (*rtcpcreqs_in)[i-1];
				(*rtcpcreqs_in)[i]->next = (*rtcpcreqs_in)[i+1];
			}
			break;
		}
		STAGER_TAPEREQ_INIT((*rtcpcreqs_in)[i]->tapereq);
	}

	/* We initialize the rtcpcreq counter */
	i = -1;

	for (ivid = 0; ivid < MAXVSN; ivid++) {

		if (stcs->u1.t.vid[ivid][0] == '\0') {
			/* No more tape vid */
			break;
		}
		i++;
		if (stcs < fixed_stcs || stcs >= fixed_stce) {
			continue;
		}
		strcpy((*rtcpcreqs_in)[i]->tapereq.vid      , stcs->u1.t.vid[ivid]);
#ifdef TMS
		if (strcmp(stcs->u1.t.lbl,"blp") == 0) {
#endif
			strcpy((*rtcpcreqs_in)[i]->tapereq.label    , stcs->u1.t.lbl      );
#ifdef TMS
		}
#endif
#ifndef TMS
		strcpy((*rtcpcreqs_in)[i]->tapereq.vsn      , stcs->u1.t.vsn[ivid]);
		strcpy((*rtcpcreqs_in)[i]->tapereq.dgn      , stcs->u1.t.dgn      );
		strcpy((*rtcpcreqs_in)[i]->tapereq.density  , stcs->u1.t.den      );
#endif
#ifdef TAPESRVR
		strcpy((*rtcpcreqs_in)[i]->tapereq.server   , TAPESRVR );
#else
		strcpy((*rtcpcreqs_in)[i]->tapereq.server   , stcs->u1.t.tapesrvr );
#endif /* TAPESRVR */
		switch (stcs->status & 0xF) {
		case STAGEWRT:
		case STAGEPUT:
			(*rtcpcreqs_in)[i]->tapereq.mode = WRITE_ENABLE;
			break;
		default:
			(*rtcpcreqs_in)[i]->tapereq.mode = WRITE_DISABLE;
			break;
		}
	}

	n = -1;

	nbtpf = 0;
	/* We compute the number of tape files */
	for (stcp = stcs; stcp < stce; stcp++) {
		char trailing_tmp;
		fseq_elem *fseq_list_tmp = NULL;
		char save_fseq[CA_MAXFSEQLEN+1];
		int nbtpf_tmp;

		strcpy(save_fseq,stcp->u1.t.fseq);
		if ((nbtpf_tmp = unpackfseq(stcp->u1.t.fseq, stcp->status, &trailing_tmp, &fseq_list_tmp)) <= 0) {
			return(-1);
		}
		if (fseq_list_tmp != NULL) {
			free(fseq_list_tmp);
		}
		strcpy(stcp->u1.t.fseq,save_fseq);
		nbtpf += nbtpf_tmp;
	}
	/* We determine the number of filereqs in nbfiles_ok */
	nbfiles = nbfiles_orig = (nbcat_ent_current > nbtpf) ? nbcat_ent_current : nbtpf;
	/* If the number of yet OK transfered fseq is > 0, there is less filereqs to build */
	if (callback_nok > 0) {
		/* But beware : if this number is equal or higher than the number of files */
		/* this mean that there is a concatenation. We will take that into account later */
		nbfiles -= callback_nok;
		nbfiles_ok = (nbfiles > 0 ? nbfiles : 1);
	} else {
		nbfiles_ok = nbfiles;
	}
	/* We create the filereq structures */
	/* Note that if there is more than one tapereq, All but the first will contain only the last */
	/* of the filereq. */
	for (i = 0; i < *nrtcpcreqs_in; i++) {
		if (i == 0) {
			if ((fl = (file_list_t *) calloc (nbfiles_ok, sizeof(file_list_t))) == NULL) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
				RESTORE_EID;
				serrno = SEINTERNAL;
				return(-1);
			}
		} else {
			if ((fl = (file_list_t *) calloc (1, sizeof(file_list_t))) == NULL) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
				RESTORE_EID;
				serrno = SEINTERNAL;
				return(-1);
			}
		}
		/* We makes sure it is a circular list and correctly initialized */
		for (j = 0; j < (i == 0 ? nbfiles_ok : 1); j++) {
			switch (j) {
			case 0:
				fl[j].prev = &(fl[(i == 0 ? nbfiles_ok : 1) - 1]);
				fl[j].next = &(fl[(i == 0 ? nbfiles_ok : 1) > 1 ? j + 1 : 0]);
				break;
			default:
				if (j == (i == 0 ? nbfiles_ok : 1) - 1) {
					fl[j].prev = &(fl[j - 1]); /* Nota : we are there only if (i == 0 ? nbfiles_ok : 1) > 1 */
					fl[j].next = &(fl[0]);
				} else {
					fl[j].prev = &(fl[j-1]);
					fl[j].next = &(fl[j+1]);
				}
				break;
			}
			/* Set sensible variables */
			STAGER_FILEREQ_INIT(fl[j].filereq);
		}
		(*rtcpcreqs_in)[i]->file = fl;
	}

	/* fl, the first of the filereqs, will be our reference from which we will copy afterwards */
	/* to all remaining tapereqs the last of the filereqs (volume spanning).                   */
	fl = (*rtcpcreqs_in)[0]->file;
	last_disk_fseq = 0;
	j_ok = -1;
	for (stcp = fixed_stcs, j = 0; j < nbfiles_orig; j++) {
		/* If some files have yet been transfered we skip them - it is a retry */
		if (callback_nok > 0) {
			if (nbfiles <= 0 && j != (nbfiles_orig - 1)) {
				/* This is a concatenation not yet finished */
				/* Only the last disk file is to be processed and it is a volume spanning */
				goto next_entry;
			} else if (j < callback_nok) {
				/* This entry has yet been transfered */
				goto next_entry;
			} else {
				/* Not only the last disk file is to be processed */
				j_ok++;
			}
		} else {
			j_ok++;
		}
		/* We compute the file sequence list for this entry */
		if (stcp_nbtpf > 0 && stcp_inbtpf >= stcp_nbtpf) {
			stcp_nbtpf = 0;
			stcp_inbtpf = 0;
			if (fseq_list != NULL) {
				free(fseq_list);
				fseq_list = NULL;
			}
		}
		if (fseq_list == NULL) {
			if ((stcp_nbtpf = unpackfseq(stcp->u1.t.fseq, stcp->status, &trailing2, &fseq_list)) <= 0) {
				return(-1);
			}
		}
		if (! Aflag) {
			strcpy (fl[j_ok].filereq.file_path, stcp->ipath);
		} else {
			strcpy (fl[j_ok].filereq.file_path, ".");
		}
		if (concat_off_fseq > 0 || api_flag == 0) {
			if (++last_disk_fseq < nbcat_ent_current) {
				fl[j_ok].filereq.disk_fseq = last_disk_fseq;
			} else {
				fl[j_ok].filereq.disk_fseq = nbcat_ent_current;
			}
		} else {
			/* In the API mode we don't care of the disk_fseq : it is always */
			/* the index of the current structure */
			if (last_disk_fseq == 0) last_disk_fseq = 1;
			fl[j_ok].filereq.disk_fseq = last_disk_fseq;
		}
		if (strcmp (stcp->recfm, "U,b") == 0) {
			strcpy (fl[j_ok].filereq.recfm, "U");
			fl[j_ok].filereq.convert = NOF77CW;
		} else if (strcmp (stcp->recfm, "U,f") == 0) {
			strcpy (fl[j_ok].filereq.recfm, "U");
		} else if (strcmp (stcp->recfm, "F,-") == 0) {
			strcpy (fl[j_ok].filereq.recfm, "F");
			fl[j_ok].filereq.convert = NOF77CW;
		} else {
			strcpy (fl[j_ok].filereq.recfm, stcp->recfm);
		}
		if (strlen(stcp->u1.t.fid) > CA_MAXFIDLEN) {
			/* Take the LAST 17 characters of fid */
			strncpy (fl[j_ok].filereq.fid, &(stcp->u1.t.fid[strlen(stcp->u1.t.fid) - CA_MAXFIDLEN]), CA_MAXFIDLEN);
		} else {
			strcpy (fl[j_ok].filereq.fid, stcp->u1.t.fid);
		}
		sprintf (fl[j_ok].filereq.stageID, "%d.%d@%s", reqid, key, hostname);
		if (concat_off_fseq > 0 || api_flag == 0) {
			switch (stcp->u1.t.fseq[0]) {
			case 'n':
				if (j > (nbtpf - 1)) {
					/* Last (tape file, disk file) association already reached */
					fl[j_ok].filereq.concat = CONCAT;
				}
				fl[j_ok].filereq.position_method = TPPOSIT_EOI;
				break;
			case 'u':
				fl[j_ok].filereq.position_method = TPPOSIT_FID;
				break;
			default:
				fl[j_ok].filereq.position_method = TPPOSIT_FSEQ;
				fl[j_ok].filereq.tape_fseq = atoi (fseq_list[stcp_inbtpf++]);
#ifdef MAX_TAPE_FSEQ
				if (fl[j_ok].filereq.tape_fseq > MAX_TAPE_FSEQ) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, "STG02 - %s : Tape sequence must be <= %d\n", stcp->u1.t.vid, MAX_TAPE_FSEQ);
					RESTORE_EID;
					RETURN (USERR);
				}
#endif
				break;
			}
		} else {
			/* This is the API call */
			/* We use the stcp_inbtpf following counter to know it we are at the fist tape_fseq for this stgcat entry... */
			switch (stcp->u1.t.fseq[0]) {
			case 'n':
				if (stcp_inbtpf > 1) {
					/* Last (tape file, disk file) association already reached */
					fl[j_ok].filereq.concat = CONCAT;
				}
				fl[j_ok].filereq.position_method = TPPOSIT_EOI;
				break;
			case 'u':
				fl[j_ok].filereq.position_method = TPPOSIT_FID;
				break;
			default:
				fl[j_ok].filereq.position_method = TPPOSIT_FSEQ;
				fl[j_ok].filereq.tape_fseq = atoi (fseq_list[stcp_inbtpf]);
#ifdef MAX_TAPE_FSEQ
				if (fl[j_ok].filereq.tape_fseq > MAX_TAPE_FSEQ) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, "STG02 - %s : Tape sequence must be <= %d\n", stcp->u1.t.vid, MAX_TAPE_FSEQ);
					RESTORE_EID;
					RETURN (USERR);
				}
#endif
				break;
			}
			stcp_inbtpf++;
		}
		if (stcp->blksize > 0) {
			fl[j_ok].filereq.blocksize = stcp->blksize;
		}
		if (stcp->lrecl > 0) {
			fl[j_ok].filereq.recordlength = stcp->lrecl;
		}
		if (stcp->u1.t.retentd > 0) {
			fl[j_ok].filereq.retention = stcp->u1.t.retentd;
		}
		fl[j_ok].filereq.def_alloc = Aflag;
		if ((stcp->u1.t.E_Tflags & SKIPBAD) == SKIPBAD) {
			fl[j_ok].filereq.rtcp_err_action |= SKIPBAD;
		}
		if ((stcp->u1.t.E_Tflags & KEEPFILE) == KEEPFILE) {
			fl[j_ok].filereq.rtcp_err_action |= KEEPFILE;
		}
		if ((stcp->u1.t.E_Tflags & IGNOREEOI) == IGNOREEOI) {
			fl[j_ok].filereq.tp_err_action |= IGNOREEOI;
		}
		if ((stcp->u1.t.E_Tflags & NOTRLCHK) == NOTRLCHK) {
			fl[j_ok].filereq.tp_err_action |= NOTRLCHK;
		}
		if (stcp->charconv > 0) {
			fl[j_ok].filereq.convert = stcp->charconv;
		}
		switch (stcp->status & 0xF) {
		case STAGEWRT:
		case STAGEPUT:
			if (stcp->u1.t.filstat == 'o') {
				fl[j_ok].filereq.check_fid = CHECK_FILE;
			}
			break;
		default:
			fl[j_ok].filereq.check_fid = CHECK_FILE;
			break;
		}
		if (stcp->u1.t.fseq[0] != 'n') {
			if (trailing2 == '-') {
				if (concat_off_fseq > 0 && fl[j_ok].filereq.tape_fseq >= concat_off_fseq) {
					if (j >= (nbcat_ent_current - 1)) {
						/* And last disk file is reached */
						/* e.g. tpread/tpwrite -V <vid> -q<j_ok>- -c off "." */
						fl[j_ok].filereq.concat = NOCONCAT_TO_EOD;
					} else {
						/* This is not the last disk file */
						/* e.g. tpread/tpwrite -V <vid> -q<j_ok>  -c off "." */
						fl[j_ok].filereq.concat = NOCONCAT;
					}
				} else {
					if (api_flag == 0) {
						/* command-line version */
						/* equivalent to tpread -V <vid> -q<j_ok>- f1 f2 etc... */
						if (j >= (nbtpf - 1)) {
							/* And last (tape file, disk file) association is reached */
							/* If it is the last disk file, we ask to concat to end of disk, not otherwise */
							fl[j_ok].filereq.concat = (last_disk_fseq >= nbcat_ent_current ? CONCAT_TO_EOD : NOCONCAT_TO_EOD);
						} else {
							/* And last (tape file, disk file) association is not reached */
							/* If it is the last disk file, we ask to concat, not otherwise */
							fl[j_ok].filereq.concat = (last_disk_fseq >= nbcat_ent_current ? CONCAT        : NOCONCAT       );
						}
					} else {
						/* API version : there is always one and only one disk file associated to it... */
						if (stcp_inbtpf > 1) {
							fl[j_ok].filereq.concat = CONCAT_TO_EOD;
						} else {
							fl[j_ok].filereq.concat = CONCAT;
						}
					}
				}
			} else {
				if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
					/* equivalent to tpwrite -V <vid> -q<j_ok> f1 f2 ... */
					/* If the last tape file is yet computed */
					/* we ask to concat, not otherwise */
					if (api_flag == 0) {
						/* Command-line version */
						fl[j_ok].filereq.concat = (j > (nbtpf - 1) ? CONCAT : NOCONCAT);
					} else {
						/* API version : there is always one and only one disk file associated to it... */
						fl[j_ok].filereq.concat = (stcp_inbtpf > 1 ? CONCAT : NOCONCAT);
					}
				} else {
					if (concat_off_fseq > 0 && fl[j_ok].filereq.tape_fseq >= concat_off_fseq) {
						if (j >= (nbcat_ent_current - 2)) {
							/* And the tape file just before that last one is reached */
							/* e.g. tpread/tpwrite -V <vid> -q<j_ok> -c off "." */
							fl[j_ok].filereq.concat = NOCONCAT;
						} else {
							/* This is not the last tape file */
							/* e.g. tpread/tpwrite -V <vid> -q<j_ok>  -c off "." */
							fl[j_ok].filereq.concat = NOCONCAT_TO_EOD;
						}
					} else {
						/* equivalent to tpread -V <vid> -q<j_ok> f1 f2 ... */
						/* If the last (tape file, disk file) association is yet computed */
						/* we ask to concat, not otherwise */
						if (api_flag == 0) {
							/* Command-line version */
							fl[j_ok].filereq.concat = (last_disk_fseq > nbcat_ent_current ? CONCAT : NOCONCAT);
						} else {
							/* API version : there is always one and only one disk file associated to it... */
							fl[j_ok].filereq.concat = (stcp_inbtpf > 1 ? CONCAT : NOCONCAT);
						}
					}
				}
			}
			if (callback_nok > 0 && callback_fseq > 0 && nbfiles <= 0 && j == (nbfiles_orig - 1)) {
				/* We are dealing with the last file of a concatenation : starting fseq depends on previous round */
				fl[j_ok].filereq.tape_fseq = callback_fseq + 1;
#ifdef MAX_TAPE_FSEQ
				if (fl[j_ok].filereq.tape_fseq > MAX_TAPE_FSEQ) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, "STG02 - %s : Tape sequence must be <= %d\n", stcp->u1.t.vid, MAX_TAPE_FSEQ);
					RESTORE_EID;
					RETURN (USERR);
				}
#endif
			}
		}
		fl[j_ok].filereq.maxnbrec = stcp->nread;

#ifndef SKIP_FILEREQ_MAXSIZE
		if (stcp->size > 0) {
			/* filereq.maxsize is in bytes */
			fl[j_ok].filereq.maxsize = (u_signed64) ((u_signed64) stcp->size * ONE_MB);
		}
#endif
		if (use_subreqid != 0) {
			fl[j_ok].filereq.stageSubreqID = get_subreqid(stcp);
		}
	next_entry:
		if (concat_off_fseq > 0 || api_flag == 0) {
			if ((stcp + 1) < fixed_stce) stcp++;
		} else {
			if (stcp_nbtpf > 0 && stcp_inbtpf >= stcp_nbtpf) {
				if ((stcp + 1) < fixed_stce) {
					stcp++;
					last_disk_fseq++;
				}
			}
		}
	}

	/* We copy the last of the fl structures to all the other tapereqs */
	for (i = 1; i < *nrtcpcreqs_in; i++) {
		/* Set sensible variables */
		STAGER_FILEREQ_INIT((*rtcpcreqs_in)[i]->file[0].filereq);
		/* Copy back content of the last of the filereqs */
		memcpy((*rtcpcreqs_in)[i]->file,(*rtcpcreqs_in)[0]->file->prev, sizeof(file_list_t));
		/* It has to be a volume spanning */
		(*rtcpcreqs_in)[i]->file[0].filereq.concat |= VOLUME_SPANNING;
		/* We makes sure it is a circular list */
		(*rtcpcreqs_in)[i]->file[0].prev = &((*rtcpcreqs_in)[i]->file[0]);
		(*rtcpcreqs_in)[i]->file[0].next = &((*rtcpcreqs_in)[i]->file[0]);
	}

	if (fseq_list != NULL) {
		free(fseq_list);
		fseq_list = NULL;
	}

	return(0);
}

int unpackfseq(fseq, req_type, trailing, fseq_list)
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
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(rpfd, MSG_ERR, "[DEBUG-XXX] unpackfseq : Analysing fseq = %s\n", fseq);
	RESTORE_EID;
#endif

	*trailing = *(fseq + strlen (fseq) - 1);
	if (*trailing == '-') {
		if ((req_type & 0xF) != STAGEIN) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG18);
			RESTORE_EID;
			return (0);
		}
		*(fseq + strlen (fseq) - 1) = '\0';
	}
	switch (*fseq) {
	case 'n':
		if ((req_type & 0xF) == STAGEIN) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG17, "-qn", "stagein");
			RESTORE_EID;
			return (0);
		}
	case 'u':
		if (strlen (fseq) == 1) {
			nbtpf = 1;
		} else {
			stage_strtoi(&nbtpf, fseq + 1, &dp, 10);
			if (*dp != '\0') {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG06, "-q");
				RESTORE_EID;
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
		while (p != NULL) {
			if ((q = strchr (p, '-')) != NULL) {
				*q = '\0';
				stage_strtoi(&n2, q + 1, &dp, 10);
				if (*dp != '\0') {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					RESTORE_EID;
					return (0);
				}
				stage_strtoi(&n1, p, &dp, 10);
				if (*dp != '\0') {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					RESTORE_EID;
					return (0);
				}
				*q = '-';
			} else {
				stage_strtoi(&n1, p, &dp, 10);
				if (*dp != '\0') {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					RESTORE_EID;
					return (0);
				}
				n2 = n1;
			}
			if (n1 <= 0 || n2 < n1) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG06, "-q");
				RESTORE_EID;
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
				stage_strtoi(&n2, q + 1, &dp, 10);
				stage_strtoi(&n1, p, &dp, 10);
				*q = '-';
			} else {
				stage_strtoi(&n1, p, &dp, 10);
				n2 = n1;
			}
			for (i = n1; i <= n2; i++, nbtpf++)
				sprintf ((char *)(*fseq_list + nbtpf), "%d", i);
			p = strtok (NULL, ",");
		}
	}
	return (nbtpf);
}

void free_rtcpcreq(rtcpcreq)
		 tape_list_t ***rtcpcreq;
{
	/* We have to care of one thing : rtcpcreq is a circular list               */
	/* and it contains a pointer to a file_list_t which is also a circular list */
	/* but that have been allocated one (with a calloc())                       */
	
	tape_list_t *first_rtcpcreq, *current_rtcpcreq;
	
	if (rtcpcreq == NULL) {
		return;
	}
	if (*rtcpcreq == NULL) {
		return;
	}
	
	/* We loop on rtcpcreq circular list */
	first_rtcpcreq = current_rtcpcreq = (*rtcpcreq)[0];
	if (first_rtcpcreq != NULL) {
		while (1) {
			if (current_rtcpcreq->file != NULL) {
				free(current_rtcpcreq->file);
			}
			/* Next rtcpcreq */
			if (current_rtcpcreq->next == first_rtcpcreq) {
				/* We reached the end */
				free(first_rtcpcreq);
				free(*rtcpcreq);
				*rtcpcreq = NULL;
				break;
			}
			current_rtcpcreq = current_rtcpcreq->next;
		}
	}
}

int stager_tape_callback(tapereq,filereq)
	rtcpTapeRequest_t *tapereq;
	rtcpFileRequest_t *filereq;
{
	if (tapereq == NULL || filereq == NULL) {
		serrno = EINVAL;
		callback_error = 1;
		return(-1);
	}

	stager_tape_log_callback(tapereq,filereq);

#ifdef STAGE_CSETPROCNAME
	if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_TAPE,
					sav_argv0,
					(filereq->cprc == 0 && filereq->proc_status == RTCP_FINISHED) ? "COPIED" : "COPYING",
					tapereq->vid,
					filereq->tape_fseq
					) != 0) {
		stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif

	/* Successful ? */
	if (filereq->cprc == 0 && filereq->proc_status == RTCP_FINISHED) {
		callback_fseq = filereq->tape_fseq;
		++callback_nok;
	}

	return(0);
}

void init_hostname()
{
	/* Get localhostname */
	if (gethostname(hostname,CA_MAXHOSTNAMELEN + 1) != 0) {
		stglogit(func, "Cannot get local hostname (%s) - forcing \"localhost\"\n", strerror(errno));
		strcpy(hostname,"localhost");
    }
}

int get_subreqid(stcp)
     struct stgcat_entry *stcp;
{
  int i;
  for (i = 0; i < nbcat_ent; i++) {
    if (stcp->reqid == stcs[i].reqid) return(i);
  }
  return(-1);
}


void stager_tape_log_callback(tapereq,filereq)
	rtcpTapeRequest_t *tapereq;
	rtcpFileRequest_t *filereq;
{
	char tmpbuf1[21];
	char tmpbuf2[21];
	char tmpbuf3[21];

	SAVE_EID;
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG-CALLBACK] VID.FSEQ=%s.%d, File No %d (%s), filereq->cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s\n",
		tapereq->vid,
		(int) filereq->tape_fseq,
		(int) filereq->disk_fseq,
		filereq->file_path,
		(int) filereq->cprc,
		u64tostr((u_signed64) filereq->bytes_in, tmpbuf1, 0),
		u64tostr((u_signed64) filereq->bytes_out, tmpbuf2, 0),
		u64tostr((u_signed64) filereq->host_bytes, tmpbuf3, 0));
	sendrep(rpfd, MSG_ERR, "[DEBUG-CALLBACK] VID.FSEQ=%s.%d, filereq->proc_status=%d (0x%lx), filereq->err.severity=%d (0x%lx), filereq->err.errorcode=%d (0x%lx), tapereq->err.severity=%d (0x%lx), tapereq->err.errorcode=%d (0x%lx)\n",
		tapereq->vid,
		(int) filereq->tape_fseq,
		(int) filereq->proc_status,
		(unsigned long) filereq->proc_status,
		(int) filereq->err.severity,
		(unsigned long) filereq->err.severity,
		(int) filereq->err.errorcode,
		(unsigned long) filereq->err.errorcode,
		(int) tapereq->err.severity,
		(unsigned long) tapereq->err.severity,
		(int) tapereq->err.errorcode,
		(unsigned long) tapereq->err.errorcode);
	sendrep(rpfd, MSG_ERR, "[DEBUG-CALLBACK] VID.FSEQ=%s.%d, errno=%d (%s), serrno=%d (%s)\n",
		tapereq->vid,
		(int) filereq->tape_fseq,
		errno,
		strerror(errno),
		serrno,
		strerror(serrno));
#else
	/* We log every callback for log-survey and administration reason */
	if (filereq->cprc == 0) {
		stglogit(func, "%s.%d, File No %d (%s), cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s\n",
						tapereq->vid,
						(int) filereq->tape_fseq,
						(int) filereq->disk_fseq,
						filereq->file_path,
						(int) filereq->cprc,
						u64tostr((u_signed64) filereq->bytes_in, tmpbuf1, 0),
						u64tostr((u_signed64) filereq->bytes_out, tmpbuf2, 0),
						u64tostr((u_signed64) filereq->host_bytes, tmpbuf3, 0));
	} else {
		stglogit(func, "%s.%d, File No %d (%s), cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s, proc_status=%d (0x%lx), filereq->err.severity=%d (0x%lx), filereq->err.errorcode=%d (0x%lx), tapereq->err.severity=%d (0x%lx), tapereq->err.errorcode=%d (0x%lx), errno=%d (%s), serrno=%d (%s)\n",
						tapereq->vid,
						(int) filereq->tape_fseq,
						(int) filereq->disk_fseq,
						filereq->file_path,
						(int) filereq->cprc,
						u64tostr((u_signed64) filereq->bytes_in, tmpbuf1, 0),
						u64tostr((u_signed64) filereq->bytes_out, tmpbuf2, 0),
						u64tostr((u_signed64) filereq->host_bytes, tmpbuf3, 0),
						(int) filereq->proc_status,
						(unsigned long) filereq->proc_status,
						(int) filereq->err.severity,
						(unsigned long) filereq->err.severity,
						(int) filereq->err.errorcode,
						(unsigned long) filereq->err.errorcode,
						(int) tapereq->err.severity,
						(unsigned long) tapereq->err.severity,
						(int) tapereq->err.errorcode,
						(unsigned long) tapereq->err.errorcode,
						errno,
						strerror(errno),
						serrno,
						sstrerror(serrno));
	}
#endif
	RESTORE_EID;
}
