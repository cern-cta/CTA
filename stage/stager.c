/*
 * $Id: stager.c,v 1.120 2001/02/02 22:42:47 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Do we want to maintain maxsize for the transfert... ? */
/* #define SKIP_FILEREQ_MAXSIZE */

/* If you want to force a specific tape server, compile it with: */
/* -DTAPESRVR=\"your_tape_server_hostname\" */
/* Otherwise you can also specify two tape servers depending if a tape ends with odd or an even number: */
/* #define TAPESRVR_ODD "shd65" */
/* #define TAPESRVR_EVEN "shd79" */
#define USE_SUBREQID

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager.c,v $ $Revision: 1.120 $ $Date: 2001/02/02 22:42:47 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#ifndef _WIN32
#include <unistd.h>                 /* For getcwd() etc... */
#include <sys/time.h>
#endif
#include <grp.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include "Cns_api.h"
#include "log.h"
#include "rfio_api.h"
#include "rtcp_api.h"
#include "serrno.h"
#include "stage.h"
#include "stage_api.h"
#include "vmgr_api.h"
#include "Ctape_api.h"
#include "Castor_limits.h"
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
EXTERN_C int DLL_DECL rfio_parseln _PROTO((char *, char **, char **, int));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());

int stagein_castor_hsm_file _PROTO(());
int stagewrt_castor_hsm_file _PROTO(());
int stage_tape _PROTO(());
int filecopy _PROTO((struct stgcat_entry *, int, char *));
void cleanup _PROTO(());
void stagekilled _PROTO(());
int build_rtcpcreq _PROTO((int *, tape_list_t ***, struct stgcat_entry *, struct stgcat_entry *, struct stgcat_entry *, struct stgcat_entry *));
char *hsmpath _PROTO((struct stgcat_entry *));
int hsmidx _PROTO((struct stgcat_entry *));
void free_rtcpcreq _PROTO((tape_list_t ***));
int unpackfseq _PROTO((char *, int, char *, fseq_elem **));
void stager_log_callback _PROTO((int, char *));
int stager_hsm_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
int stager_tape_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
void stager_hsm_or_tape_log_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
extern int (*rtcpc_ClientCallback) _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
int isuserlevel _PROTO((char *));
int getnamespace _PROTO((char *, char *));
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
char last_vid[CA_MAXVIDLEN+1];      /* Last vid returned by vmgr_gettape or Cns_getsegattrs */
char vid[CA_MAXVIDLEN+1];           /* Vid returned by vmgr_gettape or Cns_getsegattrs */
char vsn[CA_MAXVIDLEN+1];           /* Vsn returned by vmgr_gettape or vmgr_querytape */
char dgn[CA_MAXDENLEN+1];           /* Dgn returned by vmgr_gettape or vmgr_querytape */
char aden[CA_MAXDENLEN+1];          /* Aden returned by vmgr_gettape or vmgr_querytape */
char model[CA_MAXMODELLEN+1];       /* Model returned by vmgr_gettape or vmgr_querytape */
char lbltype[3];                    /* Lbltype returned by vmgr_gettape or vmgr_querytape */
int fseq = -1;                      /* Fseq returned by vmgr_gettape or vmgr_querytape */
char tmpbuf[21];                    /* Printout of u_signed64 quantity temp. buffer */
int nrtcpcreqs = 0;                 /* Number of rtcpcreq structures in circular list */
tape_list_t **rtcpcreqs = NULL;     /* rtcp request itself (circular list) */

u_signed64 *hsm_totalsize = NULL;   /* Total size per hsm file */
u_signed64 *hsm_transferedsize = NULL; /* Yet transfered size per hsm file */
int *hsm_nsegments = NULL;          /* Total number of segments per hsm file */
int *hsm_oksegment = NULL;          /* Valid segment index in the total segment list */
int *hsm_startsegment = NULL;       /* Valid segment start index in the total segment list */
int *hsm_endsegment = NULL;         /* Valid segment end index in the total segment list */
int *hsm_status = NULL;             /* Global status per hsm file */
struct Cns_segattrs **hsm_segments = NULL; /* Total list of segments per hsm file */
int *hsm_fseq = NULL;               /* Current file sequence on current tape (vid) */
typedef unsigned char blockid_t[4];
blockid_t nullblockid = { '\0', '\0', '\0', '\0' };
blockid_t *hsm_blockid = NULL;      /* Current file sequence on current tape (vid) */
int *hsm_flag = NULL;               /* Flag saying to hsmidx() not to consider this entry while scanning */
char **hsm_vid = NULL;              /* Current vid pointer or NULL if not in current rtcpc request */
char cns_error_buffer[512];         /* Cns error buffer */
char vmgr_error_buffer[512];        /* Vmgr error buffer */
int Flags = 0;                      /* Tape flag for vmgr_updatetape */
int stagewrt_nomoreupdatetape = 0;  /* Tell if the last updatetape from callback already did the job */
struct passwd start_passwd;
int nuserlevel = 0;                 /* Number of user-level files */
int nexplevel = 0;                  /* Number of experiment-level files */
int callback_error = 0;             /* Flag to tell us that there was an error in the callback */
int nhpss = 0;
int ncastor = 0;
int rtcpc_kill_cleanup = 0;         /* Flag to tell if we have to call rtcpc_kill() in the signal handler */
int callback_fseq = 0;              /* Last fseq as transfered OK and seen in the stage_tape callback */
int callback_nok = 0;               /* Number of fseq transfered OK and seen in the stage_tape callback */
int dont_change_srv = 0;            /* If != 0 means that we will not change tape_server if rtcpc() retry - Only stage_tape() */
char tape_pool[CA_MAXPOOLNAMELEN + 1]; /* Global tape pool for write migration */

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

#ifdef STGSEGMENT_OK
#undef STGSEGMENT_OK
#endif
#define STGSEGMENT_OK '-'

#ifdef STGSEGMENT_NOTOK
#undef STGSEGMENT_NOTOK
#endif
#define STGSEGMENT_NOTOK ' '

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

/* This macro allocates HSM arrays */
#define ALLOCHSM {                               \
  if ((hsm_totalsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))        == NULL ||              \
      (hsm_nsegments = (int *) calloc(nbcat_ent,sizeof(int)))                      == NULL ||              \
      (hsm_segments = (struct Cns_segattrs **) calloc(nbcat_ent,sizeof(struct Cns_segattrs *))) == NULL || \
      (hsm_oksegment = (int *) calloc(nbcat_ent,sizeof(int)))                      == NULL ||              \
      (hsm_startsegment = (int *) calloc(nbcat_ent,sizeof(int)))                   == NULL ||              \
      (hsm_endsegment = (int *) calloc(nbcat_ent,sizeof(int)))                     == NULL ||              \
      (hsm_transferedsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))   == NULL ||              \
      (hsm_fseq = (int *) calloc(nbcat_ent,sizeof(int)))                           == NULL ||              \
      (hsm_blockid = (blockid_t *) calloc(nbcat_ent,sizeof(blockid_t)))            == NULL ||              \
      (hsm_flag = (int *) calloc(nbcat_ent,sizeof(int)))                           == NULL ||              \
      (hsm_status = (int *) calloc(nbcat_ent,sizeof(int)))                         == NULL ||              \
      (hsm_vid = (char **) calloc(nbcat_ent,sizeof(char *)))                       == NULL) {              \
    SAVE_EID;                                    \
    sendrep (rpfd, MSG_ERR, STG02, "stagein/wrt/put_castor_hsm_file", "malloc error",strerror(errno));     \
    RESTORE_EID;                                 \
    RETURN (USERR);                              \
  }                                              \
}

/* This macro frees anything allocated previously... */
#define FREEHSM {                                                            \
  if (hsm_totalsize != NULL) free(hsm_totalsize);                            \
  if (hsm_transferedsize != NULL) free(hsm_transferedsize);                  \
  if (hsm_segments != NULL && hsm_nsegments != NULL) {                       \
    int isegments;                                                           \
    for (isegments = 0; isegments < nbcat_ent; isegments++) {                \
      if (hsm_segments[isegments] != NULL) free(hsm_segments[isegments]);    \
    }                                                                        \
  }                                                                          \
  if (hsm_segments != NULL) free(hsm_segments);                              \
  if (hsm_nsegments != NULL) free(hsm_nsegments);                            \
  if (hsm_oksegment != NULL) free(hsm_oksegment);                            \
  if (hsm_startsegment != NULL) free(hsm_startsegment);                      \
  if (hsm_endsegment != NULL) free(hsm_endsegment);                          \
  if (hsm_fseq != NULL) free(hsm_fseq);                                      \
  if (hsm_blockid != NULL) free(hsm_blockid);                                \
  if (hsm_flag != NULL) free(hsm_flag);                                      \
  if (hsm_status != NULL) free(hsm_status);                                  \
  if (hsm_vid != NULL) free(hsm_vid);                                        \
}
  
/* This macro call the cleanup at exit if necessary */
/* If hsm_status != NULL then we know we were doing */
/* CASTOR migration.                                */
/* It can be that rtcp() fails because of some      */
/* protocol error, but from the stager point of     */
/* view, we can know of the migration of all the    */
/* files did perform ok.                            */
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
    if (hsm_status != NULL) {                    \
      int ihsm_status;                           \
      int global_found_error = 0;                \
      for (ihsm_status = 0; ihsm_status < nbcat_ent; ihsm_status++) { \
        if (hsm_status[ihsm_status] != 1) {      \
          global_found_error = 1;                \
          break;                                 \
        }                                        \
      }                                          \
      if (global_found_error == 0) {             \
        correct_exit_code = 0;                   \
      }                                          \
    }                                            \
	FREEHSM;                                     \
	return(correct_exit_code);                   \
}

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

int main(argc, argv)
		 int argc;
		 char **argv;
{
	int c;
	int l;
	int nretry;
	int i;
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

	/* Init if of crucial variables for the signal handler */
	vid[0] = '\0';

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
	SAVE_EID;
	sendrep(rpfd, MSG_ERR, "[DEBUG] GO ON WITH gdb /usr/local/bin/stager %d, then break %d\n",getpid(),__LINE__ + 6);
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

	if (stcs->t_or_d == 'm' || stcs->t_or_d == 'h') {
		/* We cannot mix HPSS and CASTOR files... */

		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->t_or_d == 'm') {
				nhpss++;
			} else if (stcp->t_or_d == 'h') {
				ncastor++;
			} else {
				SAVE_EID;
				sendrep(rpfd, MSG_ERR, "### HSM file is of unknown type ('%c')\n",stcp->t_or_d);
				RESTORE_EID;
				exit(SYERR);
			}
		}
		if ((nhpss == 0 && ncastor == 0) ||
			(nhpss  > 0 && ncastor  > 0)) {
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "### Recognized %d HPSS files, %d CASTOR files\n",nhpss,ncastor);
			RESTORE_EID;
			free(stcs);
			exit(SYERR);
		}
	}

	if (concat_off_fseq > 0) {
		if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
			/* By precaution, we allow concat_off only for stagein here again */
			SAVE_EID;
    	    sendrep(rpfd, MSG_ERR, "### concat_off option not allowed in write-to-tape\n");
			RESTORE_EID;
			free(stcs);
	        exit(SYERR);
		}
		if (! Aflag) {
			/* By precaution, we allow concat_off only for deferred allocation */
			SAVE_EID;
    	    sendrep(rpfd, MSG_ERR, "### concat_off option allowed only in deffered allocation mode\n");
			RESTORE_EID;
			free(stcs);
	        exit(SYERR);
		}
		if (use_subreqid != 0) {
			/* By precaution, we do not allow async callback if concat_off_fseq is set */
			SAVE_EID;
    	    sendrep(rpfd, MSG_ERR, "### concat_off option is not compatible with async callback\n");
			RESTORE_EID;
			free(stcs);
	        exit(SYERR);
		}
	}
    if (use_subreqid != 0) {
		if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
			/* By precaution, we disallow async callbacks if anything but stagein */
			SAVE_EID;
    	    sendrep(rpfd, MSG_ERR, "### async callback is not allowed in write-to-tape\n");
			RESTORE_EID;
			free(stcs);
	        exit(SYERR);
		}
    }
	(void) umask (stcs->mask);

	signal (SIGINT, stagekilled);        /* If client died */
	signal (SIGTERM, stagekilled);       /* If killed from administrator */
	if (nretry) sleep (RETRYI);

	/* We always set stager logger callback */
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN/WRT/PUT] Setting stage_setlog()\n");
		RESTORE_EID;
#endif
	if (stage_setlog((void (*) _PROTO((int, char *))) &stager_log_callback) != 0) {
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "### Cannot set stager API callback function (%s)\n",sstrerror(serrno));
		RESTORE_EID;
		free(stcs);
		exit(SYERR);
	}

    /* -------- DISK TO DISK OR HPSS MIGRATION ----------- */

	if (stcs->t_or_d == 'd' || stcs->t_or_d == 'm') {
		int exit_code;

        for (stcp = stcs; stcp < stce; stcp++) {
          if ((exit_code = filecopy (stcp, key, hostname)) & 0xFF) {
            exit (SYERR);
          }
        }
		free(stcs);
		exit((exit_code >> 8) & 0xFF);
	}

	/* Redirect RTCOPY log message directly to user's console */
	rtcp_log = (void (*) _PROTO((int, CONST char *, ...))) (silent != 0 ? &stager_migmsg : &stager_usrmsg);

	if (stcs->t_or_d == 't') {

      /* -------- TAPE TO TAPE ----------- */

#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] GO ON WITH gdb /usr/local/bin/stager %d, then break stage_tape\n",getpid());
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] sleep(%d)\n",SLEEP_DEBUG);
		sleep(SLEEP_DEBUG);
		RESTORE_EID;
#endif
		c = stage_tape ();
	} else {

      /* -------- CASTOR MIGRATION ----------- */

#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN/WRT/PUT] Setting Cns_errbuf(), vmgr_errbuf()\n");
		RESTORE_EID;
#endif
		if (Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0 ||
			vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0) {
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "### Cannot set Cns or Vmgr API error buffer(s) callback function (%s)\n",sstrerror(serrno));
			RESTORE_EID;
			free(stcs);
			exit(SYERR);
		}
		if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
			if (stcs->t_or_d == 'h') {
				int have_tppool = 0;
				/* We check if there the tppool member is specified for any of the */
				/* stcp_input in case of CASTOR files */
				for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
					if (stcp->u1.h.tppool[0] != '\0') {
						++have_tppool;
					}
				}
				if (have_tppool > 0) {
					if (have_tppool != nbcat_ent) {
						/* If the number of tape pools is > 0 but not equal to total number of inputs - error */
						SAVE_EID;
						sendrep (rpfd, MSG_ERR, STG116);
						RESTORE_EID;
						free(stcs);
						exit(SYERR);
					} else {
						/* We check that they are all the same */
						for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
							if (i == 0) continue;
							if (strcmp((stcp-1)->u1.h.tppool, stcp->u1.h.tppool) != 0) {
								SAVE_EID;
								sendrep (rpfd, MSG_ERR, STG117,
										i-1,
										(stcp-1)->u1.h.xfile,
										i,
										stcp->u1.h.xfile,
										(stcp-1)->u1.h.tppool,
										stcp->u1.h.tppool);
								RESTORE_EID;
								free(stcs);
								exit(SYERR);
							}
						}
						/* Everything is ok */
						strcpy(tape_pool, stcs->u1.h.tppool);
					}
				} else {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG121);
					RESTORE_EID;
					free(stcs);
					exit(SYERR);
				}
			}
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] GO ON WITH gdb /usr/local/bin/stager %d, then break stagewrt_castor_hsm_file\n",getpid());
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] sleep(%d)\n",SLEEP_DEBUG);
			sleep(SLEEP_DEBUG);
			RESTORE_EID;
#endif
			c = stagewrt_castor_hsm_file ();
		} else {
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] GO ON WITH gdb /usr/local/bin/stager %d, then break stagein_castor_hsm_file\n",getpid());
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] sleep(%d)\n",SLEEP_DEBUG);
			sleep(SLEEP_DEBUG);
			RESTORE_EID;
#endif
			c = stagein_castor_hsm_file ();
		}
	}

	free(stcs);
	exit (c);
}

int isuserlevel(path)
     char *path;
{
  char *p[4];
  int c, rc;

  /* A user-level files begins like this: /xxx/yyy.zz/user/ */
  /*                                      1   2      3    4 */
  /* So we search up to 4 '/' and requires that string between */
  /* 3rd and 4th ones is "user" */

  /* Search No 1 */
  if ((p[0] = strchr(path,'/')) == NULL)
    return(0);
  /* Search No 2 */
  if ((p[1] = strchr(++p[0],'/')) == NULL)
    return(0);
  /* Search No 3 */
  if ((p[2] = strchr(++p[1],'/')) == NULL)
    return(0);
  /* Search No 4 */
  if ((p[3] = strchr(++p[2],'/')) == NULL)
    return(0);

  c = p[3][0];
  p[3][0] = '\0';

  rc = (strcmp(p[2],"user") == 0 ? 1 : 0);

  p[3][0] = c;
  return(rc);
}

int getnamespace(namespace,path)
     char *namespace;
     char *path;
{
  char *p[4];
  int c;

  /* A user-level files begins like this: /xxx/yyy.zz/user/ */
  /*                                      1   2      3    4 */

  /* Search No 1 */
  if ((p[0] = strchr(path,'/')) == NULL)
    return(-1);
  /* Search No 2 */
  if ((p[1] = strchr(++p[0],'/')) == NULL)
    return(-1);
  /* Search No 3 */
  if ((p[2] = strchr(++p[1],'/')) == NULL)
    return(-1);
  /* Search No 4 */
  if ((p[3] = strchr(++p[2],'/')) == NULL)
    return(-1);

  c = p[3][0];
  p[3][0] = '\0';

  strcpy(namespace,p[2]);

  p[3][0] = c;
  return(0);
}


/* This routine will return the index in the nbcat_ent entries of an HSM file       */
/* provided the associated hsm_flag[] is == 0                                       */
int hsmidx(stcp)
     struct stgcat_entry *stcp;
{
	struct stgcat_entry *stcx;
	int i;

	for (stcx = stcs, i = 0; stcx < stce; stcx++, i++) {
		if (ncastor > 0) {
			if (strcmp(stcx->u1.h.xfile,stcp->u1.h.xfile) == 0 && hsm_flag[i] == 0) {
				return(i);
			}
		} else {
			if (strcmp(stcx->u1.m.xfile,stcp->u1.m.xfile) == 0 && hsm_flag[i] == 0) {
				return(i);
			}
		}
	}

	return(-1);
}

char *hsmpath(stcp)
		 struct stgcat_entry *stcp;
{
	char *castor_hsm;
	char *end_host_hsm;
	char *host_hsm;
	char save_char;

	if (stcp == NULL) {
		serrno = SEINTERNAL;
		return(NULL);
	}

	/* Search for "/castor" in the file to stagein */
	if ((castor_hsm = CASTORFILE(ncastor > 0 ? stcp->u1.h.xfile : stcp->u1.m.xfile)) == NULL) {
		serrno = EINVAL;
		return(NULL);
	}
	if (castor_hsm != (ncastor > 0 ? stcp->u1.h.xfile : stcp->u1.m.xfile)) {
		/* We extract the host information from the hsm file */
		if ((end_host_hsm = strchr((ncastor > 0 ? stcp->u1.h.xfile : stcp->u1.m.xfile),':')) == NULL) {
			serrno = EINVAL;
			return(NULL);
		}
		if (end_host_hsm <= (ncastor > 0 ? stcp->u1.h.xfile : stcp->u1.m.xfile)) {
			serrno = EINVAL;
			return(NULL);
		}
		/* We replace the first ':' with a null character */
		save_char = end_host_hsm[0];
		end_host_hsm[0] = '\0';
		host_hsm = (ncastor > 0 ? stcp->u1.h.xfile : stcp->u1.m.xfile);
		/* If the hostname begins with castor... then the user explicitely gave  */
		/* a nameserver host - otherwise he might have get the HSM_HOST of hpss  */
		/* or nothing or something wrong. In those three last cases, we will let */
		/* the nameserver API get default CNS_HOST                               */
		if (strstr(host_hsm,"hpss") != host_hsm) {
			/* It is an explicit and valid castor nameserver : the API will be able */
			/* to connect directly to this host. No need to do a putenv ourself.    */
			end_host_hsm[0] = save_char;
			return(ncastor > 0 ? stcp->u1.h.xfile : stcp->u1.m.xfile);
		} else {
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "[DEBUG-XXX] Hsm Host %s is incompatible with a /castor file. Default CNS_HOST (from shift.conf) will apply\n",host_hsm);
		RESTORE_EID;
#endif
		}
		end_host_hsm[0] = save_char;
	}
	return(castor_hsm);
}

int stagein_castor_hsm_file() {
	int mode;
	int rtcp_rc;
	struct Cns_filestat statbuf;
	struct stgcat_entry *stcp = stcs;
	char *castor_hsm;
	int i;
	int new_tape;
	struct stgcat_entry *stcp_tmp = NULL;
#ifdef STAGER_DEBUG
	char tmpbuf1[21];
	char tmpbuf2[21];
#endif
    struct devinfo *devinfo;
    int isegments;
    int jsegments;
    int ksegments;
    int save_serrno;

    ALLOCHSM;

    /* Set CallbackClient */
    rtcpc_ClientCallback = &stager_hsm_callback;

	/* We initialize those size arrays */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		struct Cns_fileid Cnsfileid;

		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
			RESTORE_EID;
			RETURN (SYERR);
		}
		/* check permissions in parent directory, get file size */
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling Cns_statx(path=\"%s\",&Cnsfileid={server=\"%s\",fileid=%s},&statbuf)\n",
                castor_hsm,
                Cnsfileid.server,
                u64tostr(Cnsfileid.fileid, tmpbuf1, 0));
		RESTORE_EID;
#endif
        SETEID(stcp->uid,stcp->gid);
		if (Cns_statx (castor_hsm, &Cnsfileid, &statbuf) < 0) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx",
							 sstrerror (serrno));
			RESTORE_EID;
			RETURN (SYERR);
		}
		/* check file permissions */
		mode = S_IREAD;
		if (statbuf.uid != stcp->uid) {
			mode >>= 3;
			if (statbuf.gid != stcp->gid)
				mode >>= 3;
		}
		if ((statbuf.filemode & mode) != mode) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx",
							 sstrerror (EACCES));
			RESTORE_EID;
			RETURN (USERR);
		}
        /* Get the segment attributes */
        if (Cns_getsegattrs(castor_hsm,&Cnsfileid,&(hsm_nsegments[i]),&(hsm_segments[i])) != 0) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_getsegattrs",
							 sstrerror (serrno));
			RESTORE_EID;
			RETURN (SYERR);
		}
		hsm_totalsize[i] = statbuf.filesize; /* Can be changed if -s option - see few lines below */
		hsm_transferedsize[i] = 0;

		/* Determine which valid copy to use */
		hsm_startsegment[i] = -1;
		hsm_endsegment[i] = -1;
		for (isegments = 0; isegments < hsm_nsegments[i]; isegments++) {
			if (hsm_segments[i][isegments].s_status != STGSEGMENT_OK) {
				/* This copy number is not available for recall */
	            /* We makes sure that all segments with the same copy number are really invalid */
				for (jsegments = 0; jsegments < hsm_nsegments[i]; jsegments++) {
					if (jsegments == isegments) continue;
					if (hsm_segments[i][jsegments].copyno != hsm_segments[i][isegments].copyno) continue;
					if (hsm_segments[i][jsegments].s_status == STGSEGMENT_OK) {
						SAVE_EID;
						sendrep (rpfd, MSG_ERR, STG137, castor_hsm, hsm_segments[i][isegments].copyno, jsegments, isegments);
						RESTORE_EID;
						hsm_segments[i][jsegments].s_status = STGSEGMENT_NOTOK;
					}
            	}
				continue;
			}

			/* We have found a copy number to start with */
			/* We now search to the end in the index list, based */
			/* onto the copy number which have to be the same for */
			/* all segments of the same file */

			/* PLEASE NOTE THAT THE FOLLOWING ASSUMES THAT ALL SEGMENTS ARE GROUPED BY COPY NUMBER */

			for (jsegments = isegments; jsegments < hsm_nsegments[i]; jsegments++) {
				if (hsm_segments[i][jsegments].copyno != hsm_segments[i][isegments].copyno) {
					/* We have reached a segment which does not have the same copynumber */
					/* this means that the previous one should be our candidate */

					/* Please note that by construction, here, jsegments > isegments, always */
					hsm_startsegment[i] = isegments;
					hsm_endsegment[i] = (jsegments - 1);
					break;
				} else if (jsegments == (hsm_nsegments[i] - 1)) {
					/* Or we basically have reached the end of the segments list */
					/* In this case this is is normal to not have touched any segment with another */
					/* copy number */
					/* If this last segment is not the first one, we just check that its status */
					/* is correctly STGSEGMENT_OK */
					if (jsegments != isegments) {
						if (hsm_segments[i][jsegments].s_status == STGSEGMENT_OK) {
							hsm_startsegment[i] = isegments;
							hsm_endsegment[i] = jsegments;
							break;
						} else {
							/* This copy number is not available for recall */
				            /* We makes sure that all segments with the same copy number are really invalid */
							for (ksegments = 0; ksegments < hsm_nsegments[i]; ksegments++) {
								if (ksegments == jsegments) continue;
								if (hsm_segments[i][ksegments].copyno != hsm_segments[i][jsegments].copyno) continue;
								if (hsm_segments[i][ksegments].s_status == STGSEGMENT_OK) {
									SAVE_EID;
									sendrep (rpfd, MSG_ERR, STG137, castor_hsm, hsm_segments[i][ksegments].copyno, ksegments, jsegments);
									RESTORE_EID;
									hsm_segments[i][ksegments].s_status = STGSEGMENT_NOTOK;
								}
			            	}
							break;
						}
					} else {
						/* This copy number is is one single segment */
						hsm_startsegment[i] = isegments;
						hsm_endsegment[i] = jsegments;
						break;
					}
				}
				/* This segment does not have status STGSEGMENT_OK. This is not ok for us because we did */
				/* have not yet determined the end inside of the segment list with the same copy number */
				if (hsm_segments[i][jsegments].s_status != STGSEGMENT_OK) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG136, castor_hsm, hsm_segments[i][isegments].copyno, isegments, jsegments, hsm_segments[i][isegments].copyno);
					RESTORE_EID;
					for (ksegments = 0; ksegments < hsm_nsegments[i]; ksegments++) {
						if (ksegments == jsegments) continue;
						if (hsm_segments[i][ksegments].copyno != hsm_segments[i][jsegments].copyno) continue;
						if (hsm_segments[i][ksegments].s_status == STGSEGMENT_OK) {
							sendrep (rpfd, MSG_ERR, STG137, castor_hsm, hsm_segments[i][jsegments].copyno, ksegments, jsegments);
							hsm_segments[i][ksegments].s_status = STGSEGMENT_NOTOK;
						}
        	    	}
					break;
				}
			}
			/* We exit of the loop if both hsm_startsegment and hsm_endsegment are set */
			if ((hsm_startsegment[i] >= 0) && (hsm_endsegment[i] >= 0)) {
				/* With a small consistency check... */
				if (hsm_endsegment[i] < hsm_startsegment[i]) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "end segment < start segment", sstrerror(SEINTERNAL));
					RESTORE_EID;
					RETURN (SYERR);
				}
				break;
			}
		}

		/* So we have finally scanned everything */
		if (hsm_startsegment[i] < 0) {
			/* We found no start segment ? */
			if (hsm_totalsize[i] > 0) {
				/* But file is not of zero-size !? */
				if (hsm_nsegments[i] <= 0) {
					/* This file is very probably BEEING migrated ! Then it is normal */
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm,
							"File probably currently beeing migrated",sstrerror(EBUSY));
					RESTORE_EID;
					RETURN (EBUSY);
				} else {
					/* But here, we found no valid segment - are they all STGSEGMENT_NOTOK */
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm,
							"File has not valid segment",sstrerror(USERR));
					RESTORE_EID;
					RETURN (USERR);
				}
			} else {
				/* This is really an empty file... */
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, castor_hsm,
						"Empty file (size == 0, no valid segment information)",sstrerror(USERR));
				RESTORE_EID;
				RETURN (USERR);
			}
		}

		/* The test on the number of segments beeing done, we now correct the */
		/* totalsize to transfer versus -s argument */
		if (stcp->size > 0) {
			u_signed64 new_totalsize;

			new_totalsize = (u_signed64) ((u_signed64) stcp->size * ONE_MB);

			/* If the maximum size to transfer does not exceed physical size then */
			/* we change this field.                                              */
			if (new_totalsize < hsm_totalsize[i]) {
				hsm_totalsize[i] = new_totalsize;
			}
		}
#ifndef USE_SUBREQID
		if (hsm_segments[i][hsm_startsegment[i]].segsize < hsm_totalsize[i]) {
			/* The stagein of this CASTOR file will require multiple segments */
			/* and we know in advance that this is supported, when async callback */
			/* is disabled only if this file is the last one on the command-line */
			if (stcp != (stce - 1)) {
				SAVE_EID;
				serrno = SEOPNOTSUP;
				sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Requires > 1 segment to be recalled [supported with this version only if this is the last -M option]",
								 sstrerror (serrno));
				RESTORE_EID;
				RETURN (USERR);
			}
		}
#endif
	}

 getseg:
	/* We initalize current vid, fseq and blockid's */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		hsm_vid[i] = NULL;
		hsm_fseq[i] = -1;
		hsm_oksegment[i] = -1;
		memset(hsm_blockid[i],0,sizeof(blockid_t));
	}

	/* We initialize the latest vid from vmgr_gettape() */
	last_vid[0] = '\0';

	/* We loop on all the requests, choosing those that requires the same tape */
	new_tape = 0;
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
#ifdef STAGER_DEBUG
		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
			RESTORE_EID;
			RETURN (SYERR);
		}
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : totalsize=%s, transferedsize=%s\n",
                castor_hsm,
                u64tostr(hsm_totalsize[i], tmpbuf1, 0),
                u64tostr(hsm_transferedsize[i], tmpbuf2, 0));
		RESTORE_EID;
#endif
		if (hsm_totalsize[i] > hsm_transferedsize[i]) {
			u_signed64 virtual_size;
			u_signed64 previous_virtual_size;

			/* We search at which segment to start the transfer. It depends on the size */
			/* yet transfered and the size of each of the segments for file No i */
			virtual_size = previous_virtual_size = 0;
			for (isegments = hsm_startsegment[i]; isegments <= hsm_endsegment[i]; isegments++) {
				/* We add the size of this segment to total virtual_size up to this segment */
				/* and compares with the size physically yet transfered */
				virtual_size += hsm_segments[i][isegments].segsize;
				if (virtual_size > hsm_transferedsize[i]) {
					/* Well... in principle if this segment is NOT the first of the segments */
					/* then what we had already transfered should end up exactly to the previous */
					/* virtual size, e.g. the sum of all previous segments */
					if (isegments > hsm_startsegment[i]) {
						if (hsm_transferedsize[i] != previous_virtual_size) {
							SAVE_EID;
							sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "yet transfered size does not match the sum of all previous segments",
							 sstrerror (SEINTERNAL));
							RESTORE_EID;
							RETURN (SYERR);
						}
					}
					hsm_oksegment[i] = isegments;
					break;
                }
				previous_virtual_size = virtual_size;
			}
			if (hsm_oksegment[i] < 0) {
				/* Impossible ! */
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "could not find segment to transfer",
					sstrerror (SEINTERNAL));
				RESTORE_EID;
				RETURN (SYERR);
			}
			hsm_vid[i] = hsm_segments[i][hsm_oksegment[i]].vid;
			hsm_fseq[i] = hsm_segments[i][hsm_oksegment[i]].fseq;
			memcpy(hsm_blockid[i],hsm_segments[i][hsm_oksegment[i]].blockid,sizeof(blockid_t));
			hsm_oksegment[i]++;
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : Got vid.fseq=%s.%d (blockid '\\%d\\%d\\%d\\%d') last_vid=%s\n",
							castor_hsm,
							hsm_vid[i],hsm_fseq[i],
							(int) hsm_blockid[i][0],
							(int) hsm_blockid[i][1],
							(int) hsm_blockid[i][2],
							(int) hsm_blockid[i][3],
							last_vid);
			RESTORE_EID;
#endif
			
			/* We check previous vid returned by Cns_getsegattrs if any */
			if (last_vid[0] != '\0') {
				if (strcmp(last_vid,hsm_vid[i]) != 0) {
					/* We are moving from one tape to another : we will */
					/* run current and partial rtcp request             */
					new_tape = 1;
					/* We reset the last found one : we will have to redo it */
					hsm_vid[i] = NULL;
					hsm_oksegment[i]--;
				}
			} else {
				/* We initialize this first vid */
				strcpy(last_vid,hsm_vid[i]);
				if (stcp == (stce - 1)) {
					/* And this will be the only one */
					new_tape = 1;
				}
			}

			if (new_tape == 0 && stcp == (stce - 1)) {
				/* This is the special case where all file HSM files share the same vid */
				/* from the first not yet completely transfered up to the last one */
				/* Then the first test (when last_vid[0] == '\0') leads only to */
				/* strcpy(last_vid,hsm_vid[i]) */
				/* the second, third, etc... leads only to strcmp(last_vid,hsm_vid[i]) == 0 */
				new_tape = 1;
			}

			if (new_tape != 0) {
				break;
			}
		}
	}

	if (last_vid[0] != '\0' && new_tape == 0) {
		/* This means that we found something and remaining entries where already staged */
		new_tape = 1;
	}

	SETTAPEEID(stcs->uid,stcs->gid);

	if (new_tape != 0) {
		while (1) {
			int tape_status;

			/* Wait for the corresponding tape to be available */
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : Calling vmgr_querytape(vid=\"%s\",vsn,dgn,aden,lbltype,model,NULL,NULL,NULL,NULL,NULL,NULL,NULL)\n",castor_hsm,last_vid);
			RESTORE_EID;
#endif
			vid[0] = '\0';
			if (vmgr_querytape (last_vid, vsn, dgn, aden, lbltype, model, NULL, NULL,
								NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, &tape_status) == 0) {
				if (((tape_status & DISABLED) == DISABLED) ||
					((tape_status & EXPORTED) == EXPORTED) ||
					((tape_status & ARCHIVED) == ARCHIVED)) {
					SAVE_EID;
					sendrep(rpfd, MSG_ERR, STG134, last_vid,
                            ((tape_status & DISABLED) == DISABLED) ? "DISABLED" :
                            (((tape_status & EXPORTED) == EXPORTED) ? "EXPORTED" : "ARCHIVED"));
					RESTORE_EID;
					vid[0] = '\0';
					RETURN (USERR);
				}
				strcpy(vid,last_vid);
				break;
			}
			vid[0] = '\0';
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "vmgr_querytape",
							 sstrerror (serrno));
			sendrep (rpfd, MSG_ERR, "STG47 - %s : Retrying Volume Manager request in %d seconds\n", castor_hsm, RETRYI);
			RESTORE_EID;
			sleep(RETRYI);
		}
		/* We grab the optimal blocksize */
		devinfo = Ctape_devinfo(model);
	}

	/* Build the request from where we found vid (included) up to our next (excluded) */
	nbcat_ent_current = 0;
	istart = -1;
	iend = -1;
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		if (hsm_vid[i] != NULL) {
			++nbcat_ent_current;
			if (istart < 0) {
				istart = i;
			} else {
				iend = i;
			}
		}
	}

	if (istart >= 0 && iend < 0) {
		/* This means that was only one entry */
		if (nbcat_ent_current != 1) {
			serrno = SEINTERNAL;
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Internal error : istart >= 0 && iend < 0 && nbcat_ent_current != 1",
							 sstrerror (serrno));
			RESTORE_EID;
			RETURN (USERR);
		}
		iend = istart;
	}

	/* nbcat_ent_current will be the number of entries that will use vid */
	if (nbcat_ent_current == 0 || istart < 0 || iend < 0) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Internal error : nbcat_ent_current == 0 || istart < 0 || iend < 0",
						 sstrerror (serrno));
		RESTORE_EID;
		RETURN (USERR);
	}

	/* window [stcp_start,stcp_end(excluded)] will be a temporary set of catalog entries */
	if (stcp_start != NULL) {
		free(stcp_start);
		stcp_start = NULL;
	}
	if ((stcp_end = stcp_start = (struct stgcat_entry *) calloc(nbcat_ent_current,sizeof(struct stgcat_entry))) == NULL) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "malloc error",strerror(errno));
		RESTORE_EID;
		free(stcp_start);
		RETURN (USERR);
	}
	stcp_end += nbcat_ent_current;

	/* we fill temporary window [stcp_start,stcp_end] */
	for (stcp = stcs, i = 0, stcp_tmp = stcp_start; stcp < stce; stcp++, i++) {
		if (hsm_vid[i] != NULL) {
			*stcp_tmp = *stcp;
			/* We also force the blocksize while migrating */
			stcp_tmp->blksize = devinfo->defblksize;
			stcp_tmp++;
		}
	}


	/* We "interrogate" for the total number of structures */
	if (build_rtcpcreq(&nrtcpcreqs, NULL, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
		RETURN (USERR);
	}
	if (nrtcpcreqs <= 0) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "Cannot determine number of tapes",sstrerror(serrno));
		RESTORE_EID;
		RETURN (USERR);
	}

	/* We reset all the hsm_flag[] values that are candidates for search */
	for (i = 0; i < nbcat_ent; i++) {
		if (hsm_totalsize[i] > hsm_transferedsize[i]) {
			hsm_flag[i] = 0;            /* This one will be a candidate while searching */
		} else {
			hsm_flag[i] = 1;            /* We will certainly NOT return again this file while searching */
		}
	}

	/* We build the request */
	if (rtcpcreqs != NULL) {
		free_rtcpcreq(&rtcpcreqs);
	   	rtcpcreqs = NULL;
	}
	if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "build_rtcpcreq",sstrerror (serrno));
		RESTORE_EID;
		free(stcp_start);
		RETURN (SYERR);
	}

	/* By construction, rtcpcreqs has only one indice: 0 */
	reselect:
#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling rtcpc()\n");
	STAGER_RTCP_DUMP(rtcpcreqs[0]);
	sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] sleep(%d)\n",SLEEP_DEBUG);
	sleep(SLEEP_DEBUG);
	RESTORE_EID;
#endif
	Flags = 0;
	rtcpc_kill_cleanup = 1;
	rtcp_rc = rtcpc(rtcpcreqs[0]);
	save_serrno = serrno;
	rtcpc_kill_cleanup = 0;
	if (rtcp_rc < 0) {

		if (callback_error != 0) {
			/* This is a callback error - considered as fatal */
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "callback error", sstrerror(serrno));
			RESTORE_EID;
			free(stcp_start);
			RETURN (SYERR);
		}

		if (rtcpc_CheckRetry(rtcpcreqs[0]) == TRUE) {
			tape_list_t *tl;
			/* Rtcopy bits suggest to retry */
			CLIST_ITERATE_BEGIN(rtcpcreqs[0],tl) {
		   		*tl->tapereq.unit = '\0';
		   		if ( dont_change_srv == 0 ) *tl->tapereq.server = '\0'; 
			} CLIST_ITERATE_END(rtcpcreqs[0],tl);
			SAVE_EID;
			if (save_serrno == ETVBSY) {
				sendrep (rpfd, MSG_ERR, "STG47 - Re-selecting another tape server in %d seconds\n", RETRYI);
				sleep(RETRYI);
			} else {
				sendrep (rpfd, MSG_ERR, "STG47 - Re-selecting another tape server\n");
			}
			RESTORE_EID;
			goto reselect;
		} else {
			int forced_exit = 0;

			/* Rtcopy bits suggest to stop */
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, "", "rtcpc",sstrerror(save_serrno));
			/* We reset all the hsm_flag[] values */
			for (i = 0; i < nbcat_ent; i++) {
				if (hsm_totalsize[i] > hsm_transferedsize[i]) {
					hsm_flag[i] = 0;            /* This one will be a candidate while searching */
				} else {
					hsm_flag[i] = 1;            /* We will certainly NOT return again this file while searching */
				}
			}

			for (stcp_tmp = stcp_start, i = 0; stcp_tmp < stcp_end; stcp_tmp++, i++) {
				/* Note that if there is a medium error it it catched in the callback */
				if (rtcpcreqs[0]->file[i].filereq.err.errorcode == ENOENT) {
					/* We check if the failure was because of a missing file ? */
					int ihsm;

					if ((ihsm = hsmidx(stcp_tmp)) < 0) {
						serrno = SEINTERNAL;
						sendrep (rpfd, MSG_ERR, STG02, stcp_tmp->u1.h.xfile, "Cannot find internal hsm index",sstrerror(serrno));
						forced_exit = 1;
						break;
					} else {
						/* Entry did not exist */
						serrno = ENOENT;
						sendrep (rpfd, MSG_ERR, STG02, stcp_tmp->u1.h.xfile, "Empty file",sstrerror(serrno));
						forced_exit = 1;
						break;
					}
				}
			}
			RESTORE_EID;
			if (forced_exit) {
				free(stcp_start);
				RETURN (serrno == ENOENT ? USERR : SYERR);
			}
		}
	} else if (rtcp_rc > 0) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, "rtcpc() : Unknown error code (returned %d)\n", rtcp_rc);
		RESTORE_EID;
		free(stcp_start);
		RETURN (SYERR);
	}
	free(stcp_start);
	stcp_start = NULL;
	/* We checked if there is pending things to stagein */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		if (hsm_transferedsize[i] < hsm_totalsize[i]) {
			/* Not finished */
			goto getseg;
		}
	}
	RETURN(0);
}

int stagewrt_castor_hsm_file() {
	int fseq;
	int i;
	struct stat statbuf;
	int rtcp_rc;
	struct stgcat_entry *stcp = stcs;
	char *castor_hsm;
#ifdef STAGER_DEBUG
	char tmpbuf[21];
#endif
	extern char* poolname2tapepool _PROTO((char *));
    struct devinfo *devinfo;
    int save_serrno;

    ALLOCHSM;

	/* Set CallbackClient */
	rtcpc_ClientCallback = &stager_hsm_callback;

	/* We initialize those size arrays */
	i = 0;
	for (stcp = stcs; stcp < stce; stcp++, i++) {
		SETEID(stcp->uid,stcp->gid);
		if (rfio_stat (stcp->ipath, &statbuf) < 0) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_stat",rfio_serror());
			RESTORE_EID;
			RETURN (USERR);
		}
		if (statbuf.st_size == 0) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, "STG02 - %s is empty\n", stcp->ipath);
			RESTORE_EID;
			RETURN (USERR);
		}

		/* Search for castor a-la-unix path in the file to stagewrt */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
			RESTORE_EID;
			RETURN (USERR);
		}

		hsm_totalsize[i] = (u_signed64) statbuf.st_size;
		if ((stcs->status & STAGEWRT) == STAGEWRT) {
			/* Here we support limitation of number of bytes in write-to-tape */
			if (stcp->size > 0) {
				u_signed64 new_totalsize;

				new_totalsize = (u_signed64) ((u_signed64) stcp->size * ONE_MB);

				/* If the amount of bytes to transfer is asked to be lower than the physical */
				/* size of the file, we reflect this in the corresponding field.             */
				if (new_totalsize < hsm_totalsize[i]) {
					hsm_totalsize[i] = new_totalsize;
				}
			}
		}
		hsm_transferedsize[i] = 0;
		hsm_fseq[i] = -1;
		hsm_vid[i] = NULL;
	}

	while (1) {
		u_signed64 totalsize_to_transfer;
		u_signed64 estimated_free_space;
		int vmgr_gettape_nretry;
		int vmgr_gettape_iretry;

		totalsize_to_transfer = 0;

		for (i = 0; i < nbcat_ent; i++) {
			totalsize_to_transfer += (hsm_totalsize[i] - hsm_transferedsize[i]);
		}

		if (totalsize_to_transfer <= 0) {
			/* Finished */
			break;
		}

		/* We loop until everything is staged */
	gettape:
		Flags = 0;

		SETTAPEEID(stcs->uid,stcs->gid);

		vmgr_gettape_nretry = 3;
		vmgr_gettape_iretry = 0;

		while (1) {
			stagewrt_nomoreupdatetape = 0;

#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] "
					"Calling vmgr_gettape(tape_pool=\"%s\",size=%s,NULL,vid,vsn,dgn,aden,lbltype,model,&fseq,&estimated_free_space)\n",
					tape_pool, u64tostr((u_signed64) statbuf.st_size, tmpbuf, 0));
			RESTORE_EID;
#endif
			if (vmgr_gettape (
					tape_pool,              /* poolname  (input - can be NULL) */
					totalsize_to_transfer,  /* size      (input)        */
					NULL,                   /* condition (input - can be NULL) */
					vid,                    /* vid       (output)              */
					vsn,                    /* vsn       (output)              */
					dgn,                    /* dgn       (output)              */
					aden,                   /* density   (output)              */
					lbltype,                /* lbltype   (output)              */
					model,                  /* model     (output)              */
					&fseq,                  /* fseq      (output)              */
					&estimated_free_space   /* freespace (output)              */
					) == 0) {
					break;
			}

			/* Makes sure vid variable has not been overwritten... (to be checked with Jean-Philippe) */
			vid[0] = '\0';
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, "", "vmgr_gettape",sstrerror (serrno));
			RESTORE_EID;
			if (++vmgr_gettape_iretry > vmgr_gettape_nretry) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "", "Number of retries exceeded on vmgr_gettape. Exit.",sstrerror (serrno));
				RESTORE_EID;
				RETURN (SYERR);
			}
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, "Trying on another migration pool in %d seconds (if any) [Retry No %d/%d]\n",
					RETRYI, vmgr_gettape_iretry, vmgr_gettape_nretry);
			RESTORE_EID;
			sleep(RETRYI);
			}
			/* From now on, "vid" is in status TAPE_BUSY */

			/* We grab the optimal blocksize */
			devinfo = Ctape_devinfo(model);

			/* We check which hsm files are concerned */
			i = 0;
			istart = -1;
			iend = -1;
			stcp_start = stcp_end = NULL;

			for (stcp = stcs; stcp < stce; stcp++, i++) {
				/* We always force the blocksize while migrating */
				stcp->blksize = devinfo->defblksize;
				if (hsm_transferedsize[i] >= hsm_totalsize[i]) {
					/* Yet transfered */
					continue;
				}
				/* This is the first of file to continue transfer with */
				istart = i;
				stcp_start = stcp;
				/* We found the first of the files that is not yet totally transfered */
				/* We now search up to which one we can go trying not to fragment     */
				if (estimated_free_space >= (hsm_totalsize[i] - hsm_transferedsize[i]) && i != (nbcat_ent - 1)) {
					/* New estimated free space available */
					estimated_free_space -= (hsm_totalsize[i] - hsm_transferedsize[i]);
					for (++stcp, ++i; stcp < stce; stcp++, i++) {
						if (estimated_free_space >= (hsm_totalsize[i] - hsm_transferedsize[i])) {
							estimated_free_space -= (hsm_totalsize[i] - hsm_transferedsize[i]);
							continue;
						} else {
							/* Not enough room for next file */
							iend = --i;
							stcp_end = stcp;
							break;
						}
					}
					if (iend < 0) {
						/* We reached the end of the list, and there is still enough free space */
						/* This mean that we can build a full rtcpc() request.                  */
						iend = nbcat_ent - 1;
						stcp_end = stce;
					}
				} else {
					/* Vmgr claims that there is not already enough room for the first file, or */
					/* we know this is the last of the file.                                    */
					/* We will, so, build a rtcpc() request only for this one. Others, if any,  */
					/* will wait the next round.                                                */
					iend = istart;
					stcp_end = ++stcp;
				}
				break;
			}

			if (istart < 0 || iend < 0) {
				/* This cannot be */
				serrno = SEINTERNAL;
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "Cannot find where to begin",sstrerror(serrno));
				RESTORE_EID;
				Flags = 0;
				RETURN (USERR);
			}

			/* From now on we know that stcp[istart,iend] fulfill a-priori the requirement */
			nbcat_ent_current = iend - istart + 1;
			for (i = 0; i < nbcat_ent; i++) {
				if (i >= istart && i <= iend) {
					hsm_vid[i] = vid;
					hsm_fseq[i] = fseq++;
				} else {
					hsm_vid[i] = NULL;
					hsm_fseq[i] = -1;
				}
			}

#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Use [vid,vsn,dgn,aden,lbltype,fseqs]=[%s,%s,%s,%s,%s,%d to %d]\n",
					vid,vsn,dgn,aden,lbltype,hsm_fseq[istart],hsm_fseq[iend]);
			RESTORE_EID;
#endif

			/* We "interrogate" for the number of structures */
			if (build_rtcpcreq(&nrtcpcreqs, NULL, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
				Flags = 0;
				RETURN (USERR);
			}

			/* It has to be one exactly (one vid exactly !) */
			if (nrtcpcreqs != 1) {
				serrno = SEINTERNAL;
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "Number of rtcpc structures different vs. deterministic value of 1",sstrerror(serrno));
				RESTORE_EID;
				Flags = 0;
				RETURN (USERR);
			}

			/* We reset all the hsm_flags entries */
			for (i = 0; i < nbcat_ent; i++) {
				if (hsm_totalsize[i] > hsm_transferedsize[i]) {
					hsm_flag[i] = 0;            /* This one will be a candidate while searching */
				} else {
					hsm_flag[i] = 1;            /* We will certainly NOT return again this file while searching */
				}
			}

			/* Build the request from where we started (included) up to our next (excluded) */
			if (rtcpcreqs != NULL) {
				free_rtcpcreq(&rtcpcreqs);
				rtcpcreqs = NULL;
			}

			if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "build_rtcpcreq",sstrerror (serrno));
				RESTORE_EID;
				Flags = 0;
				RETURN (SYERR);
			}

#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling rtcpc()\n");
			STAGER_RTCP_DUMP(rtcpcreqs[0]);
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] sleep(%d)\n",SLEEP_DEBUG);
			sleep(SLEEP_DEBUG);
			RESTORE_EID;
#endif

	reselect:
			Flags = 0;
			rtcpc_kill_cleanup = 1;
			rtcp_rc = rtcpc(rtcpcreqs[0]);
			save_serrno = serrno;
			rtcpc_kill_cleanup = 0;

			if (rtcp_rc < 0) {
				int j;

				/* rtcpc failed */
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, vid, "rtcpc",sstrerror (serrno));
				RESTORE_EID;

				if (callback_error != 0) {
					/* This is a callback error - considered as fatal */
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "callback error", sstrerror(serrno));
					RESTORE_EID;
					RETURN (SYERR);
				}

				if (rtcpc_CheckRetry(rtcpcreqs[0]) == TRUE) {
					tape_list_t *tl;
					/* Rtcopy bits suggest to retry */
					CLIST_ITERATE_BEGIN(rtcpcreqs[0],tl) {
						*tl->tapereq.unit = '\0';
						if ( dont_change_srv == 0 ) *tl->tapereq.server = '\0'; 
					} CLIST_ITERATE_END(rtcpcreqs[0],tl);
					SAVE_EID;
					if (save_serrno == ETVBSY) {
						sendrep (rpfd, MSG_ERR, "STG47 - Re-selecting another tape server in %d seconds\n", RETRYI);
						sleep(RETRYI);
					} else {
						sendrep (rpfd, MSG_ERR, "STG47 - %s\n", "Re-selecting another tape server\n");
					}
					RESTORE_EID;
					goto reselect;
				}

				i = 0;
				for (j = istart; j <= iend; j++, i++) {
					/* Note that if there is a medium error it is catched in the callback */
					if (rtcpcreqs[0]->file->filereq.err.errorcode == ENOENT) {
						/* Tape info very probably inconsistency with, for ex., TMS */
						SAVE_EID;
						sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc",sstrerror(serrno));
						RESTORE_EID;
						Flags = 0;
						RETURN(USERR);
					}
				}
				if (Flags != TAPE_FULL && stagewrt_nomoreupdatetape == 0) {
					/* If Flags is TAPE_FULL then it has already been set by the callback which called vmgr_updatetape */
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=\"%s\",BytesWriten=0,CompressionFactor=0,FilesWriten=0,Flags=%d)\n",vid,Flags);
					RESTORE_EID;
#endif
				if (vmgr_updatetape (vid, (u_signed64) 0, 0, 0, Flags) != 0) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",sstrerror (serrno));
					RESTORE_EID;
				}
			}
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, "Retrying Volume Manager request in %d seconds\n", RETRYI);
			RESTORE_EID;
			sleep(RETRYI);
			goto gettape;
		} else {
			if (Flags != TAPE_FULL && stagewrt_nomoreupdatetape == 0) {
				/* If Flags is TAPE_FULL then it has already been set by the callback */
#ifdef STAGER_DEBUG
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=\"%s\",ByteWriten=0,CompressionFactor=0,filesWriten0,Flags=0)\n",vid);
				RESTORE_EID;
#endif
				if (vmgr_updatetape (vid, (u_signed64) 0, 0, 0, 0) != 0) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",sstrerror (serrno));
					RESTORE_EID;
				}
			}
		}
	}
	Flags = 0;
	RETURN(0);
}

int stage_tape() {
	int rtcp_rc, save_serrno;

    SETTAPEEID(stcs->uid,stcs->gid);

    /* Set CallbackClient */
    rtcpc_ClientCallback = &stager_tape_callback;

	/* We detect if tape server have been explicitely given on command-line */
	if (stcs->u1.t.tapesrvr[0] != '\0') {
		dont_change_srv = 1;
	}

    stcp_start = stcs;
    stcp_end = stce;
	nbcat_ent_current = nbcat_ent;
	istart = 0;
	iend = nbcat_ent - 1;

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
	rtcp_rc = rtcpc(rtcpcreqs[0]);
	save_serrno = serrno;
	rtcpc_kill_cleanup = 0;

    if (callback_error != 0) {
		/* This is a callback error - considered as fatal */
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "stage_tape", "callback error", sstrerror(serrno));
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
			if (save_serrno == ETVBSY) {
				sendrep (rpfd, MSG_ERR, "STG47 - Re-selecting another tape server in %d seconds\n", RETRYI);
				sleep(RETRYI);
			} else {
				sendrep (rpfd, MSG_ERR, "STG47 - %s\n", "Re-selecting another tape server\n");
			}
			RESTORE_EID;
			goto reselect;
		}

		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc", sstrerror(save_serrno));
		RESTORE_EID;
		RETURN (USERR);

	} else if (rtcp_rc > 0) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Unknown error code (>0)");
		RESTORE_EID;
		RETURN (SYERR);
	}

	RETURN (0);
}

int filecopy(stcp, key, hostname)
		 struct stgcat_entry *stcp;
		 int key;
		 char *hostname;
{
	char buf[256];
	int c;
	char command[2*(CA_MAXHOSTNAMELEN+1+MAXPATH)+CA_MAXHOSTNAMELEN+1+196];
	char *filename;
	char *host;
	FILE *rf;
	char stageid[CA_MAXSTGRIDLEN+1];
	EXTERN_C int rfiosetopt _PROTO((int, int *, int));
	EXTERN_C int rfio_pread _PROTO((char *, int, int, FILE *));

    SETTAPEEID(stcs->uid,stcs->gid);

	/*
	 * @@@ TO BE MOVED TO cpdskdsk.sh @@@
	 */

	if (! stcp->ipath[0]) {
		/* Deferred allocation (Aflag) */
		sprintf (stageid, "%d.%d@%s", reqid, key, hostname);
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, "Calling stage_updc_filcp(stageid=%s,...)\n",stageid);
		RESTORE_EID;
#endif
		if (stage_updc_filcp (
								stageid,                 /* Stage ID      */
								use_subreqid != 0 ? get_subreqid(stcp) : -1, /* subreqid      */
								-1,                      /* Copy rc       */
								NULL,                    /* Interface     */
								0,                       /* Size          */
								0,                       /* Waiting time  */
								0,                       /* Transfer time */
								0,                       /* block size    */
								NULL,                    /* drive         */
								NULL,                    /* fid           */
								0,                       /* fseq          */
								0,                       /* lrecl         */
								NULL,                    /* recfm         */
								stcp->ipath              /* path          */
								) != 0) {
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, STG02, stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.d.xfile,
					"stage_updc_filcp", sstrerror (serrno));
			RESTORE_EID;
			RETURN (USERR);
		}
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, "[DEBUG-FILECOPY] Internal path setted to %s\n", stcp->ipath);
		RESTORE_EID;
#endif
	}

	(void) rfio_parseln (stcp->ipath, &host, &filename, NORDLINKS);

	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);
	if (host)
		sprintf (command, "%s:%s/cpdskdsk", host, BIN);
	else 
		sprintf (command, "%s/cpdskdsk", BIN);
	sprintf (command+strlen(command), " -Z %d.%d@%s", reqid, key, hostname);

	if ((stcp->t_or_d == 'd') && (stcp->u1.d.Xparm[0]))
		sprintf (command+strlen(command), " -X %s", stcp->u1.d.Xparm);

	/* Writing file */
	if (((stcp->status & STAGEWRT) == STAGEWRT) || ((stcp->status & STAGEPUT) == STAGEPUT)) {
		sprintf (command+strlen(command), " -s %d",(int) stcp->actual_size);
		sprintf (command+strlen(command), " %s", stcp->ipath);
		if (stcp->t_or_d == 'm')
			sprintf (command+strlen(command), " %s", stcp->u1.m.xfile);
		else
			sprintf (command+strlen(command), " %s", stcp->u1.d.xfile);
	} else {
		/* Reading file */
		if (stcp->size)
			sprintf (command+strlen(command), " -s %d",
							 (int) stcp->size * ONE_MB);
		if (stcp->t_or_d == 'm')
			sprintf (command+strlen(command), " %s", stcp->u1.m.xfile);
		else
			sprintf (command+strlen(command), " %s", stcp->u1.d.xfile);	    
		sprintf (command+strlen(command), " %s", stcp->ipath);
	}
#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(rpfd, MSG_ERR, "[DEBUG-FILECOPY] Execing %s\n",command);
	RESTORE_EID;
#else
	SAVE_EID;
	stglogit (func, "execing command : %s\n", command);
	RESTORE_EID;
#endif

	SETID(stcp->uid,stcp->gid);
	rf = rfio_popen (command, "r");
	if (rf == NULL) {
		/* This way we will sure that it will be logged... */
		sendrep(rpfd, MSG_ERR, "STG02 - %s : %s\n", command, rfio_serror());
		RETURN (SYERR);
	}

	while ((c = rfio_pread (buf, 1, sizeof(buf)-1, rf)) > 0) {
		buf[c] = '\0';
		SAVE_EID;
		sendrep (rpfd, RTCOPY_OUT, "%s", buf);
		RESTORE_EID;
	}

	c = rfio_pclose (rf);
	if (c != 0) {
		sendrep(rpfd, MSG_ERR, "STG02 - %s : %s\n", "rfio_pclose", rfio_serror());
	}
	RETURN (c);
}

void cleanup() {
	/* Safety cleanup - executed ONLY in case of write-to-tape */
	if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
		if (vid[0] != '\0') {
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep (rpfd, MSG_ERR, "[DEBUG-CLEANUP] Calling vmgr_updatetape(vid=\"%s\",BytesWriten=0,CompressionFactor=0,Files0,Flags=0)\n",vid);
			RESTORE_EID;
#endif
			if (vmgr_updatetape (vid, (u_signed64) 0, 0, 0, 0) != 0) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
								 sstrerror (serrno));
				RESTORE_EID;
			}
		}
	}
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
	int loop_break;                          /* Loop break flag, only if 't' */
	char *castor_hsm;
	int ihsm;                                /* Correct hsm index */
	int nfile_list = 0;                      /* Number of file lists in case of hsm */

	if (nrtcpcreqs_in == NULL || fixed_stcs == NULL || fixed_stce == NULL) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Bad arguments to build_rtcpcreq",sstrerror(serrno));
		RESTORE_EID;
		return(-1);
	}

	/* We calculate how many rtcpc requests we need. */
	*nrtcpcreqs_in = 0;

	/* Loop break flag - only if 't' */
	loop_break = 0;

	/* We loop on all catalog entries */
	for (stcp = fixed_stcs; stcp < fixed_stce; stcp++) {
		/* We depend on the request type */
		switch (stcp->t_or_d) {
		case 't':
			/* This is a tape request : we will skip our outer loop */
			for (ivid = 0; ivid < MAXVSN; ivid++) {
				if (stcp->u1.t.vid[ivid][0] == '\0') {
					/* No more tape vid */
					break;
				}
				++(*nrtcpcreqs_in);
			}
			/* Say what we will exit the loop */
			loop_break = 1;
			break;
		default:
			/* This is an hsm read or write request */
			/* ++(*nrtcpcreqs_in); */
			*nrtcpcreqs_in = 1;
			break;
		}
		if (loop_break != 0) {
			break;
		}
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

	/* Loop break flag - only if 't' */
	loop_break = 0;

	/* We fill the content of the array */
	for (stcp = stcs; stcp < stce; stcp++) {
		/* We depend on the request type */
		switch (stcp->t_or_d) {
		case 't':
			/* This is a tape request */
			for (ivid = 0; ivid < MAXVSN; ivid++) {
				if (stcp->u1.t.vid[ivid][0] == '\0') {
					/* No more tape vid */
					break;
				}
				i++;
				if (stcp < fixed_stcs || stcp >= fixed_stce) {
					continue;
				}
				strcpy((*rtcpcreqs_in)[i]->tapereq.vid      , stcp->u1.t.vid[ivid]);
#ifdef TMS
				if (strcmp(stcp->u1.t.lbl,"blp") == 0) {
#endif
					strcpy((*rtcpcreqs_in)[i]->tapereq.label    , stcp->u1.t.lbl      );
#ifdef TMS
				}
#endif
#ifndef TMS
				strcpy((*rtcpcreqs_in)[i]->tapereq.vsn      , stcp->u1.t.vsn[ivid]);
				strcpy((*rtcpcreqs_in)[i]->tapereq.dgn      , stcp->u1.t.dgn      );
				strcpy((*rtcpcreqs_in)[i]->tapereq.density  , stcp->u1.t.den      );
#endif
#ifdef TAPESRVR
				strcpy((*rtcpcreqs_in)[i]->tapereq.server   , TAPESRVR );
#else
#if (defined(TAPESRVR_ODD) && defined(TAPESRVR_EVEN))
				if (((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '0') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '2') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '4') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '6') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '8')) {
					strcpy((*rtcpcreqs_in)[i]->tapereq.server   , TAPESRVR_ODD );
				} else if (((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '1') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '3') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '5') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '7') ||
					((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '9')) {
					strcpy((*rtcpcreqs_in)[i]->tapereq.server   , TAPESRVR_EVEN );
				} else {
					strcpy((*rtcpcreqs_in)[i]->tapereq.server   , stcp->u1.t.tapesrvr );
				}
#else
				strcpy((*rtcpcreqs_in)[i]->tapereq.server   , stcp->u1.t.tapesrvr );
#endif /* TAPESRVR_ODD || TAPESRVR_EVEN */
#endif /* TAPESRVR */
				switch (stcp->status & 0xF) {
				case STAGEWRT:
				case STAGEPUT:
					(*rtcpcreqs_in)[i]->tapereq.mode = WRITE_ENABLE;
					break;
				default:
					(*rtcpcreqs_in)[i]->tapereq.mode = WRITE_DISABLE;
					break;
				}
			}
            {
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
				nbfiles = nbfiles_orig = (nbcat_ent > nbtpf) ? nbcat_ent : nbtpf;
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
						if (++last_disk_fseq < nbcat_ent) {
							fl[j_ok].filereq.disk_fseq = last_disk_fseq;
						} else {
							fl[j_ok].filereq.disk_fseq = nbcat_ent;
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
					sprintf (fl[j_ok].filereq.stageID, "%d.%d@%s", reqid, key,
									 hostname);
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
#ifdef CONCAT_OFF
							if (concat_off_fseq > 0 && fl[j_ok].filereq.tape_fseq >= concat_off_fseq) {
								if (j >= (nbcat_ent - 1)) {
									/* And last disk file is reached */
									/* e.g. tpread/tpwrite -V <vid> -q<j_ok>- -c off "." */
									fl[j_ok].filereq.concat = NOCONCAT_TO_EOD;
								} else {
									/* This is not the last disk file */
									/* e.g. tpread/tpwrite -V <vid> -q<j_ok>  -c off "." */
									fl[j_ok].filereq.concat = NOCONCAT;
								}
							} else {
#endif /* CONCAT_OFF */
								if (api_flag == 0) {
									/* command-line version */
									/* equivalent to tpread -V <vid> -q<j_ok>- f1 f2 etc... */
									if (j >= (nbtpf - 1)) {
										/* And last (tape file, disk file) association is reached */
										/* If it is the last disk file, we ask to concat to end of disk, not otherwise */
										fl[j_ok].filereq.concat = (last_disk_fseq >= nbcat_ent ? CONCAT_TO_EOD : NOCONCAT_TO_EOD);
									} else {
										/* And last (tape file, disk file) association is not reached */
										/* If it is the last disk file, we ask to concat, not otherwise */
										fl[j_ok].filereq.concat = (last_disk_fseq >= nbcat_ent ? CONCAT        : NOCONCAT       );
									}
								} else {
									/* API version : there is always one and only one disk file associated to it... */
									if (stcp_inbtpf > 1) {
										fl[j_ok].filereq.concat = CONCAT_TO_EOD;
									} else {
										fl[j_ok].filereq.concat = CONCAT;
									}
								}
#ifdef CONCAT_OFF
							}
#endif /* CONCAT_OFF */
						} else {
							if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
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
									if (j >= (nbcat_ent - 2)) {
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
										fl[j_ok].filereq.concat = (last_disk_fseq > nbcat_ent ? CONCAT : NOCONCAT);
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
						if (stcp + 1 < fixed_stce) stcp++;
					} else {
						if (stcp_nbtpf > 0 && stcp_inbtpf >= stcp_nbtpf) {
							if (stcp + 1 < fixed_stce) {
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

				/* Say we will exit the loop */
				loop_break = 1;

			}
			break;
		default:
			/* This is an hsm request */
			if (hsm_totalsize == NULL || hsm_transferedsize == NULL) {
				serrno = SEINTERNAL;
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "No hsm_totalsize or hsm_transferedsize array (should be != NULL)",sstrerror(serrno));
				RESTORE_EID;
				return(-1);
			}
			if ((ihsm = hsmidx(stcp)) < 0) {
				serrno = SEINTERNAL;
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Cannot find internal hsm index",sstrerror(serrno));
				RESTORE_EID;
				return(-1);
			}
			if (hsm_totalsize[ihsm] == 0) {
				serrno = SEINTERNAL;
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Bad size parameter (should be > 0)",sstrerror(serrno));
				RESTORE_EID;
				return(-1);
			}
			if (hsm_transferedsize[ihsm] > hsm_totalsize[ihsm]) {
				serrno = SEINTERNAL;
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Yet transfered size claimed to be > Hsm total size...",sstrerror(serrno));
				RESTORE_EID;
				return(-1);
			}
			/* hsm mode : only one tape */
			if (i < 0) {
				i++;
			}
			if (stcp < fixed_stcs || stcp >= fixed_stce) {
				continue;
			}
			strcpy((*rtcpcreqs_in)[i]->tapereq.vid     ,vid    );
#ifdef TMS
			if (strcmp(lbltype,"blp") == 0) {
#endif
				strcpy((*rtcpcreqs_in)[i]->tapereq.label   ,lbltype);
#ifdef TMS
			}
#endif
#ifndef TMS
			strcpy((*rtcpcreqs_in)[i]->tapereq.vsn     ,vsn    );
			strcpy((*rtcpcreqs_in)[i]->tapereq.dgn     ,dgn    );
			strcpy((*rtcpcreqs_in)[i]->tapereq.density ,aden   );
#endif
#ifdef TAPESRVR
			strcpy((*rtcpcreqs_in)[i]->tapereq.server   , TAPESRVR );
#else
#if (defined(TAPESRVR_ODD) && defined(TAPESRVR_EVEN))
			if (((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '0') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '2') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '4') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '6') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '8')) {
				strcpy((*rtcpcreqs_in)[i]->tapereq.server   , TAPESRVR_ODD );
			} else if (((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '1') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '3') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '5') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '7') ||
				((*rtcpcreqs_in)[i]->tapereq.vid[strlen((*rtcpcreqs_in)[i]->tapereq.vid) - 1] == '9')) {
				strcpy((*rtcpcreqs_in)[i]->tapereq.server   , TAPESRVR_EVEN );
			}
#endif  /* TAPESRVR_ODD || TAPESRVR_EVEN */
#endif /* TAPESRVR */
			if ((*rtcpcreqs_in)[i]->file == NULL) {
				/* First file for this VID */
				if (((*rtcpcreqs_in)[i]->file = (file_list_t *) calloc(1,sizeof(file_list_t))) == NULL) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
					RESTORE_EID;
					serrno = SEINTERNAL;
					return(-1);
				}
				++nfile_list;
				/* Makes sure it is a circular list */
				(*rtcpcreqs_in)[i]->file->prev = (*rtcpcreqs_in)[i]->file;
				(*rtcpcreqs_in)[i]->file->next = (*rtcpcreqs_in)[i]->file;
			} else {
				file_list_t *dummy;
                int          i2;

				if ((dummy = (file_list_t *) realloc((*rtcpcreqs_in)[i]->file,(nfile_list + 1) * sizeof(file_list_t))) == NULL) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "realloc",strerror(errno));
					RESTORE_EID;
					serrno = SEINTERNAL;
					return(-1);
				}
                /* Please note that nfile_list is not yet incremented, so last element is really [nfile_list] */
				memset((void *) &(dummy[nfile_list]), 0, sizeof(file_list_t));
				(*rtcpcreqs_in)[i]->file = dummy;

                /* We makes sure it is a circular list and correctly initialized */
                for (i2 = 0; i2 < nfile_list; i2++) {
                  switch (i2) {
                  case 0:
                    (*rtcpcreqs_in)[i]->file[i2].prev = &((*rtcpcreqs_in)[i]->file[nfile_list - 1]);
                    (*rtcpcreqs_in)[i]->file[i2].next = &((*rtcpcreqs_in)[i]->file[nfile_list > 1 ? i2 + 1 : 0]);
                    break;
                  default:
                    if (i2 == nfile_list - 1) {
                      (*rtcpcreqs_in)[i]->file[i2].prev = &((*rtcpcreqs_in)[i]->file[i2 - 1]);
                      (*rtcpcreqs_in)[i]->file[i2].next = &((*rtcpcreqs_in)[i]->file[0]);
                    } else {
                      (*rtcpcreqs_in)[i]->file[i2].prev = &((*rtcpcreqs_in)[i]->file[i2 - 1]);
                      (*rtcpcreqs_in)[i]->file[i2].next = &((*rtcpcreqs_in)[i]->file[i2 + 1]);
                    }
                    break;
                  }
                }
				/* Makes sure it is a circular list */
				(*rtcpcreqs_in)[i]->file[0].prev              = &((*rtcpcreqs_in)[i]->file[nfile_list]);
				(*rtcpcreqs_in)[i]->file[nfile_list - 1].next = &((*rtcpcreqs_in)[i]->file[nfile_list]);
				(*rtcpcreqs_in)[i]->file[nfile_list].prev     = &((*rtcpcreqs_in)[i]->file[nfile_list - 1]);
				(*rtcpcreqs_in)[i]->file[nfile_list].next     = &((*rtcpcreqs_in)[i]->file[0]);
				++nfile_list;
			}
			STAGER_FILEREQ_INIT((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq);
			sprintf ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.stageID, "%d.%d@%s", reqid, key, hostname);
			strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.file_path, stcp->ipath);
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.disk_fseq = nfile_list;
			strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.recfm, "F");
            /*
             * For HSM files - if the file is renamed, then the check on fid will NOT work.
             * We set fid on write, but not on read, to prevent recall failure on renamed files.
             */

			switch (stcp->status & 0xF) {
			case STAGEWRT:
			case STAGEPUT:
				if ((castor_hsm = hsmpath(stcp)) == NULL) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
					RESTORE_EID;
					RETURN (USERR);
				}
				if (strlen(castor_hsm) > CA_MAXFIDLEN) {
					/* Length more than CA_MAXFIDLEN, our target is of size [CA_MAXFIDLEN+1], the last one if for '\0' */
					strncpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.fid, &(castor_hsm[strlen(castor_hsm) - CA_MAXFIDLEN]), CA_MAXFIDLEN);
				} else {
					strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.fid, castor_hsm);
				}
				/* We makes sure that last field is '\0' */
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.fid[CA_MAXFIDLEN] = '\0';
				break;
			default:
				break;
			}

			if (memcmp(hsm_blockid[ihsm],nullblockid,sizeof(blockid_t)) != 0) {
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.position_method = TPPOSIT_BLKID;
				memcpy((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.blockid,hsm_blockid[ihsm],sizeof(blockid_t));
			} else {
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.position_method = TPPOSIT_FSEQ;
			}
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.tape_fseq = hsm_fseq[ihsm];
			/* We set the hsm_flag[ihsm] so that this entry cannot be returned twice */
			hsm_flag[ihsm] = 1;
#ifndef SKIP_FILEREQ_MAXSIZE
			/* Here we support maxsize in the rtcopy structure only if explicit STAGEWRT */
			if ((stcs->status & STAGEWRT) == STAGEWRT) {
				/* Here we support limitation of number of bytes in write-to-tape */
				if (stcp->size > 0) {
					u_signed64 dummysize;
					dummysize = (u_signed64) stcp->size;
					dummysize *= (u_signed64) ONE_MB;
					dummysize -= (u_signed64) hsm_transferedsize[ihsm];
					(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.maxsize = dummysize;
				}
            } else {
				/* Here we do NOT support limitation of number of bytes */
				if (stcp->size > 0) {
    	          /* But user (unfortunately) specified such a number... We overwrite it if necessasry */
					u_signed64 dummysize;

					dummysize = (u_signed64) stcp->size;
					dummysize *= (u_signed64) ONE_MB;
					/* If stcp->size (in MB) in lower than totalsize, we change maxsize value */
					if (dummysize < hsm_totalsize[ihsm]) dummysize = hsm_totalsize[ihsm];
					dummysize -= (u_signed64) hsm_transferedsize[ihsm];
					(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.maxsize = dummysize;
				}
			}
#endif /* SKIP_FILEREQ_MAXSIZE */
			if (use_subreqid != 0) {
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.stageSubreqID = get_subreqid(stcp);
			}
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.offset = hsm_transferedsize[ihsm];
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.def_alloc = Aflag;
			if (! Aflag) {
				strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.file_path, stcp->ipath);
			} else {
				strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.file_path, ".");
			}
			(*rtcpcreqs_in)[i]->file[nfile_list-1].tape = (*rtcpcreqs_in)[i];
			if (stcp->blksize > 0) {
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.blocksize = stcp->blksize;
			}
			switch (stcp->status & 0xF) {
			case STAGEWRT:
			case STAGEPUT:
				/* This is an hsm write request */
				(*rtcpcreqs_in)[i]->tapereq.mode            = WRITE_ENABLE;
				break;
			default:
				/* This is an hsm read request */
				(*rtcpcreqs_in)[i]->tapereq.mode            = WRITE_DISABLE;
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.check_fid = CHECK_FILE;
				break;
			}
			break;
		}
		if (loop_break != 0) {
			break;
		}
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
			nbtpf = strtol (fseq + 1, &dp, 10);
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
				n2 = strtol (q + 1, &dp, 10);
				if (*dp != '\0') {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					RESTORE_EID;
					return (0);
				}
				n1 = strtol (p, &dp, 10);
				if (*dp != '\0') {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					RESTORE_EID;
					return (0);
				}
				*q = '-';
			} else {
				n1 = strtol (p, &dp, 10);
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

void stager_log_callback(level,message)
     int level;
     char *message;
{
	SAVE_EID;
#ifdef STAGER_DEBUG
	/* In debug mode we always want to have all the messages in the stager log-file */
	sendrep(rpfd,MSG_ERR,"%s",message);
#else
	/* In migration mode we want to make sure that everything is logged */
	sendrep(rpfd, (level == LOG_INFO) ? MSG_OUT : MSG_ERR,"%s",message);
#endif
	RESTORE_EID;
}

int stager_hsm_callback(tapereq,filereq)
	rtcpTapeRequest_t *tapereq;
	rtcpFileRequest_t *filereq;
{
#ifdef STAGER_DEBUG
	char tmpbuf1[21];
	char tmpbuf2[21];
	char tmpbuf3[21];
#endif
	int compression_factor = 0;		/* times 100 */
	int stager_client_callback_i = -1;
	int stager_client_true_i = -1;
	struct stgcat_entry *stcp;
	char *castor_hsm;
	struct Cns_fileid Cnsfileid;

	if (tapereq == NULL || filereq == NULL) {
		serrno = EINVAL;
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "### Received invalid callback (tapereq=0x%lx,filereq=0x%lx)\n", (unsigned long) tapereq, (unsigned long) filereq);
		RESTORE_EID;
		callback_error = 1;
		return(-1);
	}

	stager_hsm_or_tape_log_callback(tapereq,filereq);

	stager_client_callback_i = filereq->disk_fseq - 1;
	stager_client_true_i = stager_client_callback_i + istart;
	stcp = &(stcp_start[stager_client_callback_i]);

	if (stager_client_callback_i < 0 || stager_client_callback_i > iend || stager_client_true_i >= nbcat_ent) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, filereq->file_path, "rtcpc() ClientCallback",strerror(errno));
		RESTORE_EID;
		callback_error = 1;
		return(-1);
	}

	SETEID(stcp->uid,stcp->gid);

	/* Search for castor a-la-unix path in the file to stagewrt */
	if ((castor_hsm = hsmpath(stcp)) == NULL) {
		SAVE_EID;
		sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
		RESTORE_EID;
		callback_error = 1;
		return(-1);
	}

	/* Successful or end of tape */
	/* - 1st line :     OK and flagged as finished (consistency check) */
	/* - 2nd line : NOT OK and flagged as failed   (consistency check) */
	/* - 3rd line :      + and EndOfVolume in write mode               */
	/* PS: in read mode, the EndOFVolume would be flagged ETEOV        */ 
	if (((filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED)) ||
		((filereq->cprc <  0) && ((filereq->err.severity & RTCP_FAILED) == RTCP_FAILED) &&
			(
				(filereq->err.errorcode == ENOSPC && (((stcs->status & STAGEWRT) == STAGEWRT) ||
														((stcs->status & STAGEPUT) == STAGEPUT))) ||
				((stcs->status & STAGEIN) == STAGEIN)
			)
		)
	) {

#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN/WRT/PUT-CALLBACK] bytes_in = %s, bytes_out = %s, host_bytes = %s\n",
				u64tostr(filereq->bytes_in, tmpbuf1, 0),
				u64tostr(filereq->bytes_out, tmpbuf2, 0),
				u64tostr(filereq->host_bytes, tmpbuf3, 0));
		RESTORE_EID;
#endif

 		if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
			/* This is the tpwrite callback */
			int tape_flag = filereq->cprc < 0 ? TAPE_FULL : (stager_client_callback_i == iend ? 0 : TAPE_BUSY);


			{
				/* gcc compiler bug - fixed or forbidden register was spilled. */
				/* This may be due to a compiler bug or to impossible asm      */
				/* statements or clauses.                                      */
				u_signed64 dummyvalue;

				dummyvalue = filereq->bytes_in;
				hsm_transferedsize[stager_client_true_i] += dummyvalue;
			}

			if (stager_client_callback_i == iend) {
				stagewrt_nomoreupdatetape = 1;
			}
			if (filereq->host_bytes > 0 && filereq->bytes_out > 0) {
				compression_factor = filereq->host_bytes * 100 / filereq->bytes_out;
			}
			Flags = tape_flag;
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling vmgr_updatetape(vid=\"%s\",BytesWriten=%s,CompressionFactor=%d,FilesWriten=%d,Flags=%d)\n",
				tapereq->vid,
				u64tostr(filereq->bytes_in, tmpbuf1, 0),
				compression_factor,
				1,
				Flags);
			RESTORE_EID;
#endif
			if (vmgr_updatetape(tapereq->vid,
								filereq->bytes_in,
								compression_factor,
								1,
								Flags
				) != 0) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape", sstrerror(serrno));
				RESTORE_EID;
				callback_error = 1;
				return(-1);
			}

			if (filereq->bytes_in <= 0) {
				SAVE_EID;
				sendrep (rpfd, MSG_ERR, STG123,
						tapereq->vid,
						filereq->tape_fseq,
						filereq->cprc < 0 ? "full" : (stager_client_callback_i == iend ? "free" : "busy")
						);
				RESTORE_EID;
			} else {
#ifdef STAGER_DEBUG
				SAVE_EID;
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] istart = %d , iend = %d, stager_client_callback_i = %d -> stager_client_true_i = %d\n",istart, iend, stager_client_callback_i, stager_client_true_i);
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_nsegments[%d] = %d\n",stager_client_true_i,hsm_nsegments[stager_client_true_i]);
				RESTORE_EID;
#endif
				if (hsm_nsegments[stager_client_true_i] == 0) {
					/* And this is the first of the segments */
					if ((hsm_segments[stager_client_true_i] = (struct Cns_segattrs *) malloc(sizeof(struct Cns_segattrs))) == NULL) {
						SAVE_EID;
						sendrep (rpfd, MSG_ERR, STG02, vid, "malloc", strerror(errno));
						RESTORE_EID;
						serrno = SEINTERNAL;
						callback_error = 1;
						return(-1);
					}
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].fsec = %d\n",stager_client_true_i,hsm_nsegments[stager_client_true_i],1);
					RESTORE_EID;
#endif
					hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].fsec = 1;
				} else {
					struct Cns_segattrs *dummy;

					if ((dummy = (struct Cns_segattrs *) realloc(hsm_segments[stager_client_true_i],
																(hsm_nsegments[stager_client_true_i] + 1) *
																sizeof(struct Cns_segattrs))) == NULL) {
						SAVE_EID;
						sendrep (rpfd, MSG_ERR, STG02, vid, "realloc", strerror(errno));
						RESTORE_EID;
						serrno = SEINTERNAL;
						callback_error = 1;
						return(-1);
					}
					hsm_segments[stager_client_true_i] = dummy;
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].fsec = %d\n",
						stager_client_true_i,
						hsm_nsegments[stager_client_true_i],
						hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i] - 1].fsec + 1);
					RESTORE_EID;
#endif
					hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].fsec =
						hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i] - 1].fsec + 1;
				}
#ifdef STAGER_DEBUG
				SAVE_EID;
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].copyno = %d\n",
					stager_client_true_i,hsm_nsegments[stager_client_true_i],0);
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].segsize = %d\n",
					stager_client_true_i,hsm_nsegments[stager_client_true_i],filereq->bytes_in);
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].compression = %d\n",
					stager_client_true_i,hsm_nsegments[stager_client_true_i],compression_factor);
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].s_status = '-'\n",
					stager_client_true_i,hsm_nsegments[stager_client_true_i]);
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].vid = \"%s\"\n",
					stager_client_true_i,hsm_nsegments[stager_client_true_i],tapereq->vid);
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].fseq = %d\n",
					stager_client_true_i,hsm_nsegments[stager_client_true_i],filereq->tape_fseq);
				RESTORE_EID;
#endif
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].copyno = 0;
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].segsize = filereq->bytes_in;
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].compression = compression_factor;
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].s_status = '-';
				strcpy(hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].vid,tapereq->vid);
				memcpy(hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].blockid,filereq->blockid,sizeof(blockid_t));
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].fseq = filereq->tape_fseq;
				hsm_nsegments[stager_client_true_i]++;
				if (hsm_transferedsize[stager_client_true_i] >= hsm_totalsize[stager_client_true_i]) {
					/* tpwrite of this file is over */
					strcpy(Cnsfileid.server,stcp->u1.h.server);
					Cnsfileid.fileid = stcp->u1.h.fileid;
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling Cns_setsegattrs(\"%s\",&Cnsfileid={server=\"%s\",fileid=%s},nbseg=%d,segments)\n",
						castor_hsm,
						Cnsfileid.server,
						u64tostr(Cnsfileid.fileid,tmpbuf1,0),
						hsm_nsegments[stager_client_true_i]);
					RESTORE_EID;
#endif
					if (Cns_setsegattrs(castor_hsm,
										&Cnsfileid,
										hsm_nsegments[stager_client_true_i],
										hsm_segments[stager_client_true_i]) < 0) {
						SAVE_EID;
						sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setsegattrs", sstrerror(serrno));
						RESTORE_EID;
						callback_error = 1;
						return(-1);
					}
					/* If we reach this part of the code then we know undoubtly */
					/* that the transfer of this HSM file IS ok                 */
					hsm_status[stager_client_true_i] = 1;
				}
			}
		} else {
			{
				/* gcc compiler bug - fixed or forbidden register was spilled. */
				/* This may be due to a compiler bug or to impossible asm      */
				/* statements or clauses.                                      */
				u_signed64 dummyvalue;

				dummyvalue = filereq->bytes_out;
				hsm_transferedsize[stager_client_true_i] += dummyvalue;
			}

			/* This is the tpread callback */
			if (hsm_transferedsize[stager_client_true_i] >= hsm_totalsize[stager_client_true_i]) {
				strcpy(Cnsfileid.server,stcp->u1.h.server);
				Cnsfileid.fileid = stcp->u1.h.fileid;
#ifdef STAGER_DEBUG
				SAVE_EID;
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN/CALLBACK] Calling Cns_setatime(%s,&Cnsfileid={server=\"%s\",fileid=%s})\n",
					castor_hsm,
					Cnsfileid.server,
					u64tostr(Cnsfileid.fileid,tmpbuf1,0));
				RESTORE_EID;
#endif
				if (Cns_setatime (castor_hsm, &Cnsfileid) < 0) {
					SAVE_EID;
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setatime", sstrerror(serrno));
					RESTORE_EID;
					callback_error = 1;
					return(-1);
				}
				/* If we reach this part of the code then we know undoubly */
				/* that the transfer of this HSM file IS ok                */
				hsm_status[stager_client_true_i] = 1;
			}
		}
	} else if (filereq->cprc != 0) {
		char *THIS_DISABLED = "DISABLED";
		char *THIS_EXPORTED = "EXPORTED";
		char *THIS_TAPE_RDONLY = "TAPE_RDONLY";
		char *THIS_ARCHIVED = "ARCHIVED";
		int this_flag = 0;
		int is_this_flag = 0;
		char *this_string = NULL;

		switch (serrno) {
		case ETWLBL:                          /* Wrong label type */
		case ETWVSN:                          /* Wrong vsn */
		case ETHELD:                          /* Volume held (TMS) */
		case ETVUNKN:                         /* Volume unknown or absent (TMS) */
		case ETOPAB:                          /* Operator cancel */
			this_flag = DISABLED;
			this_string = THIS_DISABLED;
			is_this_flag = 1;
			break;
		case ETABSENT:                        /* Volume absent (TMS) */
			this_flag = EXPORTED;
			this_string = THIS_EXPORTED;
			is_this_flag = 1;
			break;
		case ETNXPD:                          /* File not expired */
		case ETWPROT:                         /* Cartridge physically write protected or tape read-only (TMS) */
			this_flag = TAPE_RDONLY;
			this_string = THIS_TAPE_RDONLY;
			is_this_flag = 1;
			break;
		case ETARCH:                          /* Volume in inactive library */
			this_flag = ARCHIVED;
			this_string = THIS_ARCHIVED;
			is_this_flag = 1;
			break;
		case EACCES:                          /* Volume protected (12 in TMS) - do nothing ? */
			break;
		default:
			/* what shall we do here ? */
			break;
		}
		if (is_this_flag == 0) {
			if (filereq->err.errorcode == ETPARIT ||
				filereq->err.errorcode == ETUNREC ||
				filereq->err.errorcode == ETLBL ||
				tapereq->err.errorcode == ETPARIT ||
				tapereq->err.errorcode == ETLBL ||
				tapereq->err.errorcode == ETUNREC) {
				/* serrno was not of any help but we can anyway detect there was something serious with this tape */
				SAVE_EID;
 				if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
					sendrep(rpfd, MSG_ERR, "### [%s] For tape %s, global error code %d not enough, but tapereq->err.errorcode=%d (0x%lx) and filereq->err.errorcode=%d (0x%lx) - Safety action is to flag the tape as read-only\n",
						castor_hsm,
						tapereq->vid,
						serrno,
						(int) tapereq->err.errorcode,
						(unsigned long) tapereq->err.errorcode,
						(int) filereq->err.errorcode,
						(unsigned long) filereq->err.errorcode);
					this_flag = TAPE_RDONLY;
					this_string = THIS_TAPE_RDONLY;
				} else {
					sendrep(rpfd, MSG_ERR, "### [%s] For tape %s, global error code %d not enough, but tapereq->err.errorcode=%d (0x%lx) and filereq->err.errorcode=%d (0x%lx) - Safety action is to disable the tape \n",
						castor_hsm,
						tapereq->vid,
						serrno,
						(int) tapereq->err.errorcode,
						(unsigned long) tapereq->err.errorcode,
						(int) filereq->err.errorcode,
						(unsigned long) filereq->err.errorcode);
					this_flag = DISABLED;
					this_string = THIS_DISABLED;
				}
				RESTORE_EID;
				is_this_flag = 1;
			}
		}
		if (is_this_flag != 0) {
			SAVE_EID;
			sendrep(rpfd, MSG_ERR, "### [%s] Setting tape %s to %s state\n", castor_hsm, tapereq->vid, this_string);
			if (vmgr_updatetape(tapereq->vid, 0, 0, 0, this_flag) != 0) {
				sendrep (rpfd, MSG_ERR, STG02, tapereq->vid, "vmgr_updatetape", sstrerror(serrno));
			}
			RESTORE_EID;
		}
		callback_error = 1;
		/* By resetting this variable we makes sure that our decision is not overwriten by the cleanup() */
		vid[0] = '\0';
		return(-1);
	}

	return(0);
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

	stager_hsm_or_tape_log_callback(tapereq,filereq);

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
		stglogit("stager", "Cannot get local hostname (%s) - forcing \"localhost\"\n", strerror(errno));
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


void stager_hsm_or_tape_log_callback(tapereq,filereq)
	rtcpTapeRequest_t *tapereq;
	rtcpFileRequest_t *filereq;
{
	char tmpbuf1[21];
	char tmpbuf2[21];
	char tmpbuf3[21];

	SAVE_EID;
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG-CALLBACK] VID.FSEQ=%s.%d, File No %d (%s), filereq->cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s, filereq->proc_status=%d (0x%lx), filereq->err.severity=%d (0x%lx), filereq->err.errorcode=%d (0x%lx), tapereq->err.severity=%d (0x%lx), tapereq->err.errorcode=%d (0x%lx), errno=%d (%s), serrno=%d (%s)\n",
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
		stglogit(func, "%s.%d, File No %d (%s), cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s, proc_status=%d (0x%lx), filereq->err.severity=%d (0x%lx), filereq->err.errorcode=%d (0x%lx), tapereq->err.severity=%d (0x%lx), tapereq->err.errorcode=%d (0x%lx), bytes_in=%s, bytes_out=%s, host_bytes=%s, errno=%d (%s), serrno=%d (%s)\n",
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
						u64tostr((u_signed64) filereq->bytes_in, tmpbuf1, 0),
						u64tostr((u_signed64) filereq->bytes_out, tmpbuf2, 0),
						u64tostr((u_signed64) filereq->host_bytes, tmpbuf3, 0),
						errno,
						strerror(errno),
						serrno,
						sstrerror(serrno));
	}
#endif
	RESTORE_EID;
}

/*
 * Last Update: "Friday 02 February, 2001 at 22:59:29 CET by Jean-Damien DURAND (<A HREF='mailto:Jean-Damien.Durand@cern.ch'>Jean-Damien.Durand@cern.ch</A>)"
 */

