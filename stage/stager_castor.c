/*
 * $Id: stager_castor.c,v 1.40 2003/05/12 12:49:19 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/* Do we want to maintain maxsize for the transfert... ? */
/* #define SKIP_FILEREQ_MAXSIZE */

/* Do you want to do the Cns_setsegattrs() under real uid/gid of the owner of file in Castor Name Server ? */
#define SETSEGATTRS_WITH_OWNER_UID_GID

/* Do you want to do a Cns_statx() or use the yet known uid/gid from stager catalog (recommended) ? */
#define SETSEGATTRS_WITH_OWNER_UID_GID_FROM_CATALOG

/* Do you want to hardcode the limit of number of files per rtcpc() request ? */
#define MAX_RTCPC_FILEREQ 1000

/* Do you want disk to disk copy done a la third party, e.g. transfer not going through this process */
/* if source file is not local ? */
#define DISK2DISK_COPY_REMOTE

/* If you want to force a specific tape server, compile it with: */
/* -DTAPESRVR=\"your_tape_server_hostname\" */

#define USE_SUBREQID

/* If you want to always migrate the full data to tape do (default is to migrated a predicted nb of bytes, not up to EOF): */
/* #define FULL_STAGEWRT_HSM */

#ifdef STAGE_CSETPROCNAME
#define STAGE_CSETPROCNAME_FORMAT_CASTOR "%s %s %s/%d.%d %s"
#include "Csetprocname.h"
#endif

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_castor.c,v $ $Revision: 1.40 $ $Date: 2003/05/12 12:49:19 $ CERN IT-DS/HSM Jean-Damien Durand";
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
#include "Cns_api.h"
#include "log.h"
#include "rfio_api.h"
#include "rtcp_api.h"
#include "serrno.h"
#include "stage_messages.h"
#include "stage_macros.h"
#include "stage_struct.h"
#include "vmgr_api.h"
#include "Ctape_api.h"
#include "Castor_limits.h"
#include "u64subr.h"
#include "osdep.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "stage_api.h"       /* For stage_updc_tppos() and stage_updc_filcp() */

#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
EXTERN_C void DLL_DECL stager_usrmsg _PROTO(());
#else
EXTERN_C void DLL_DECL stager_usrmsg _PROTO((int, ...));
#endif
EXTERN_C int DLL_DECL rfio_parseln _PROTO((char *, char **, char **, int));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int *, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());
extern char *stglogflags _PROTO((char *, char *, u_signed64));

int stagein_castor_hsm_file _PROTO(());
int stagewrt_castor_hsm_file _PROTO(());
void cleanup _PROTO(());
void stagekilled _PROTO(());
int build_rtcpcreq _PROTO((int *, tape_list_t ***, struct stgcat_entry *, struct stgcat_entry *, struct stgcat_entry *, struct stgcat_entry *));
char *hsmpath _PROTO((struct stgcat_entry *));
int hsmidx_vs_ipath _PROTO((char *));
int hsmidx _PROTO((struct stgcat_entry *));
int hsm_is_ignored _PROTO((struct stgcat_entry *));
void free_rtcpcreq _PROTO((tape_list_t ***));
int stager_hsm_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
void stager_hsm_log_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
void stager_process_error _PROTO((int, rtcpTapeRequest_t *, rtcpFileRequest_t *, char *));
extern int (*rtcpc_ClientCallback) _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
void init_hostname _PROTO(());
int get_subreqid _PROTO((struct stgcat_entry *));
#ifdef hpux
int stage_copyfile _PROTO(());
#else
int stage_copyfile _PROTO((char *, char *, mode_t, int, u_signed64, u_signed64 *));
#endif
int copyfile _PROTO((int, int, char *, char *, u_signed64, u_signed64 *));
void stcplog _PROTO((int, char *));

char func[16];                      /* This executable name in logging */
int Aflag;                          /* Allocation flag */
int api_flag;                       /* Api flag, .e.g we will NOT re-arrange the file sequences in case of a tape request */
u_signed64 api_flags;               /* Flags themselves of the Api call */
uid_t rtcp_uid;                     /* Forced uid */
gid_t rtcp_gid;                     /* Forced gid */
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
int last_side;                      /* Last side returned by vmgr_gettape or Cns_getsegattrs */
char vid[CA_MAXVIDLEN+1];           /* Vid returned by vmgr_gettape or Cns_getsegattrs */
int side;                           /* Side returned by vmgr_gettape or Cns_getsegattrs */
char vsn[CA_MAXVIDLEN+1];           /* Vsn returned by vmgr_gettape or vmgr_querytape */
char dgn[CA_MAXDENLEN+1];           /* Dgn returned by vmgr_gettape or vmgr_querytape */
char aden[CA_MAXDENLEN+1];          /* Aden returned by vmgr_gettape or vmgr_querytape */
char model[CA_MAXMODELLEN+1];       /* Model returned by vmgr_gettape or vmgr_querytape */
char lbltype[CA_MAXLBLTYPLEN+1];    /* Lbltype returned by vmgr_gettape or vmgr_querytape */
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
mode_t *hsm_mode = NULL;            /* Used only to use correct st_mode for internal copy of CASTOR file between pools */
int *hsm_flag = NULL;               /* Flag saying to hsmidx() not to consider this entry while scanning */
int *hsm_ignore = NULL;             /* Flag saying to completely ignore this entry - use for migration stagewrt */
int *hsm_map = NULL;                /* Map File Disk No <-> Index in [stcs,stce-1] */
char **hsm_vid = NULL;              /* Current vid pointer or NULL if not in current rtcpc request */
int *hsm_side = NULL;               /* Current side of current vid pointer or NULL if not in current rtcpc request */
char cns_error_buffer[512];         /* Cns error buffer */
char vmgr_error_buffer[512];        /* Vmgr error buffer */
int Flags = 0;                      /* Tape flag for vmgr_updatetape */
int stagewrt_nomoreupdatetape = 0;  /* Tell if the last updatetape from callback already did the job */
int error_already_processed = 0;    /* Set by the callback if it processes error */
struct passwd start_passwd;
int callback_error = 0;             /* Flag to tell us that there was an error in the callback */
int fatal_callback_error = 0;       /* Flag to tell us that there was a fatal error in the callback */
int callback_forced_noretry = 0;    /* Flag to tell us that the callback do NOT want to see retry when mover says so */
int rtcpc_kill_cleanup = 0;         /* Flag to tell if we have to call rtcpc_kill() in the signal handler */
int dont_change_srv = 0;            /* If != 0 means that we will not change tape_server */
char tape_pool[CA_MAXPOOLNAMELEN + 1]; /* Global tape pool for write migration */
int retryi_vmgr_gettape_enospc = 0; /* Retry Interval if vmgr_gettape() returns ENOSPC */
#ifdef STAGE_CSETPROCNAME
static char sav_argv0[1024+1];
#endif
int ismig_log = 0; /* Will ask to have few more entries in mig_log rather than in log */

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

static struct vmgr_tape_info vmgr_tapeinfo;

#ifdef STGSEGMENT_OK
#undef STGSEGMENT_OK
#endif
#define STGSEGMENT_OK(segment) ( ((segment).s_status == '-') && ((segment).vid[0] != '\0') && (vmgr_querytape((segment).vid, (segment).side, &vmgr_tapeinfo, NULL) == 0) && (! (((vmgr_tapeinfo.status & DISABLED) == DISABLED) || ((vmgr_tapeinfo.status & EXPORTED) == EXPORTED) || (((vmgr_tapeinfo.status & ARCHIVED) == ARCHIVED)))) )

#ifdef STGSEGMENT_NOTOK
#undef STGSEGMENT_NOTOK
#endif
#define STGSEGMENT_NOTOK ' '

#ifdef STGSEGMENT_STATUS
#undef STGSEGMENT_STATUS
#endif
#define STGSEGMENT_STATUS(segment) ( (((segment).s_status == '-') && ((segment).vid[0] != '\0')) ? ((vmgr_querytape((segment).vid, (segment).side, &vmgr_tapeinfo, NULL) == 0) ? (((vmgr_tapeinfo.status & DISABLED) == DISABLED) ? "DISABLED" : (((vmgr_tapeinfo.status & EXPORTED) == EXPORTED) ? "EXPORTED" : (((vmgr_tapeinfo.status & ARCHIVED) == ARCHIVED) ? "ARCHIVED" : "?"))) : "?") : "?" )

#ifdef RETRYI_VMGR_GETTAPE_ENOSPC_MAX
#undef RETRYI_VMGR_GETTAPE_ENOSPC_MAX
#endif
#define RETRYI_VMGR_GETTAPE_ENOSPC_MAX 3600

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
	SETEID(start_passwd.pw_gid,start_passwd.pw_uid); \
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
      (hsm_mode = (mode_t *) calloc(nbcat_ent,sizeof(mode_t)))                     == NULL ||              \
      (hsm_ignore = (int *) calloc(nbcat_ent,sizeof(int)))                         == NULL ||              \
      (hsm_map = (int *) calloc(nbcat_ent,sizeof(int)))                            == NULL ||              \
      (hsm_status = (int *) calloc(nbcat_ent,sizeof(int)))                         == NULL ||              \
      (hsm_vid = (char **) calloc(nbcat_ent,sizeof(char *)))                       == NULL ||              \
      (hsm_side = (int *) calloc(nbcat_ent,sizeof(int)))                           == NULL) {              \
    SAVE_EID;                                    \
    sendrep (&rpfd, MSG_ERR, STG02, "stagein/wrt/put_castor_hsm_file", "malloc error",strerror(errno));     \
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
  if (hsm_mode != NULL) free(hsm_mode);                                      \
  if (hsm_ignore != NULL) free(hsm_ignore);                                  \
  if (hsm_map != NULL) free(hsm_map);                                        \
  if (hsm_status != NULL) free(hsm_status);                                  \
  if (hsm_vid != NULL) free(hsm_vid);                                        \
  if (hsm_side != NULL) free(hsm_side);                                      \
}
  
/* This macro will set a hsm_ignore[] field depending */
/* on use_subreqid availability and current serrno */
/* It is used ONLY in the HSM callback */
#define SET_HSM_IGNORE(ihsm_ignore,error_code) { \
  if (! use_subreqid) { \
    fatal_callback_error = 1; \
  } else { \
    switch (error_code) { \
    case ENOENT: \
    case EISDIR: \
    case EPERM: \
    case EACCES: \
      hsm_ignore[ihsm_ignore] = USERR; \
      break; \
    case SECOMERR: \
    case SECONNDROP: \
      hsm_ignore[ihsm_ignore] = SECOMERR; \
      break; \
    default: \
      hsm_ignore[ihsm_ignore] = SYERR; \
      break; \
    } \
  } \
} \

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
    if (hsm_status != NULL) {                    \
      int ihsm_status;                           \
      int global_found_error = 0;                \
      int have_ignore = 0;                       \
      for (ihsm_status = 0; ihsm_status < nbcat_ent; ihsm_status++) { \
        if (hsm_ignore[ihsm_status] != 0) {      \
          have_ignore = 1;                       \
          break;                                 \
        }                                        \
        if (hsm_status[ihsm_status] != 1) {      \
          global_found_error = 1;                \
          break;                                 \
        }                                        \
      }                                          \
      if ((global_found_error == 0) && (! have_ignore)) { \
        correct_exit_code = 0;                   \
      } else {                                   \
        int global_userr = 0;                    \
        int global_syerr = 0;                    \
        for (ihsm_status = 0; ihsm_status < nbcat_ent; ihsm_status++) { \
          if (! hsm_ignore[ihsm_status]) continue; \
          switch (hsm_ignore[ihsm_status]) {     \
          case SECOMERR:                         \
            global_syerr++;                      \
            break;                               \
          default:                               \
            global_userr++;                      \
            break;                               \
          }                                      \
        }                                        \
        correct_exit_code = (global_syerr ? SYERR : (global_userr ? USERR : exit_code)); \
      }                                          \
    }                                            \
	FREEHSM;                                     \
	return(correct_exit_code);                   \
}

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
#ifdef STAGER_DEBUG
	int thisrpfd;
#endif
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

	strcpy (func, "stager_castor");
	stglogit (func, "function entered\n");

	/* Some intialization */
	lbltype[0] = '\0';

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
		thisrpfd = atoi(argv[3]);
		
		sendrep(&thisrpfd, MSG_ERR, "[DEBUG] argc = %d\n", argc);

		for (i = 0; i < argc; i++) {
			sendrep(&thisrpfd, MSG_ERR, "[DEBUG] argv[%d] = %s\n", i, argv[i]);
		}
	}
#endif
	reqid = atoi (argv[1]);
#ifdef STAGER_DEBUG
	sendrep(&thisrpfd, MSG_ERR, "[DEBUG] reqid = %d\n", reqid);
#endif
	key = atoi (argv[2]);
#ifdef STAGER_DEBUG
	sendrep(&thisrpfd, MSG_ERR, "[DEBUG] key = %d\n", key);
#endif
	rpfd = atoi (argv[3]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] rpfd = %d\n", rpfd);
#endif
	nbcat_ent = atoi (argv[4]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] nbcat_ent = %d\n", nbcat_ent);
#endif
	nretry = atoi (argv[5]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] nretry = %d\n", nretry);
#endif
	Aflag = atoi (argv[6]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] Aflag = %d\n", Aflag);
#endif
	concat_off_fseq = atoi (argv[7]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] concat_off_fseq = %d\n", concat_off_fseq);
#endif
	silent = atoi (argv[8]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] silent = %d\n", silent);
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
	sendrep(&rpfd, MSG_ERR, "[DEBUG] use_subreqid = %d\n", use_subreqid);
#endif
	api_flag = atoi (argv[10]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] api_flag = %d\n", api_flag);
#endif
	api_flags = strtou64(argv[11]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] api_flags = %s\n", stglogflags(NULL,NULL,(u_signed64) api_flags));
#endif
	rtcp_uid = atoi (argv[12]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] rtcp_uid = %d\n", (int) rtcp_uid);
#endif
	rtcp_gid = atoi (argv[13]);
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] rtcp_gid = %d\n", (int) rtcp_gid);
#endif
#ifdef __INSURE__
	tmpfile = argv[14];
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] tmpfile = %s\n", tmpfile);
#endif
#endif

	/* Init if of crucial variables for the signal handler */
	vid[0] = '\0';
	side = -1;
	if ((api_flags & STAGE_MIGLOG) == STAGE_MIGLOG) ismig_log++;

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
			dump_stcp(&rpfd, stcs + i, &sendrep);
        }
	}
#endif

#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(&rpfd, MSG_ERR, "[DEBUG] GO ON WITH gdb /usr/local/bin/stager_castor %d, then break %d\n",getpid(),__LINE__ + 6);
	sendrep(&rpfd, MSG_ERR, "[DEBUG] sleep(%d)\n", SLEEP_DEBUG);
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

	/* We accept only CASTOR files ('h') or internal copy between pools ('d') */

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->t_or_d == 'd') {
			if (stcp->u1.d.xfile[0] == '\0') {
				SAVE_EID;
				sendrep(&rpfd, MSG_ERR, "### Record for internal copy of HSM file (type '%c') incomplete (stcp->u1.d.xfile == \"\")\n",stcp->t_or_d);
				RESTORE_EID;
				free(stcs);
				exit(USERR);
			}
			/* Internal copy is ok ONLY for STAGEIN requests */
			if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
				SAVE_EID;
				sendrep(&rpfd, MSG_ERR, "### Record for internal copy of HSM file (type '%c') should be for STAGEIN only\n", stcp->t_or_d);
				RESTORE_EID;
				free(stcs);
				exit(USERR);
			}
			continue;
		}
		if (stcp->t_or_d != 'h') {
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "### HSM file is of unvalid type ('%c')\n",stcp->t_or_d);
			RESTORE_EID;
			free(stcs);
			exit(USERR);
		}
	}

	if (concat_off_fseq > 0) {
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "### concat_off option not allowed in CASTOR mode\n");
		RESTORE_EID;
		free(stcs);
		exit(USERR);
	}

	(void) umask (stcs->mask);

	signal (SIGINT, stagekilled);        /* If client died */
	signal (SIGTERM, stagekilled);       /* If killed from administrator */
	if (nretry) sleep (RETRYI);

	/* Redirect RTCOPY log message directly to user's console */
	rtcp_log = (void (*) _PROTO((int, CONST char *, ...))) &stager_usrmsg;

	/* -------- CASTOR MIGRATION ----------- */

#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN/WRT/PUT] Setting Cns_errbuf(), vmgr_errbuf()\n");
	RESTORE_EID;
#endif
	if (Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0 ||
		vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0) {
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "### Cannot set Cns or Vmgr API error buffer(s) callback function (%s)\n",sstrerror(serrno));
		RESTORE_EID;
		free(stcs);
		exit(SYERR);
	}
#ifdef STAGE_CSETPROCNAME
	if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_CASTOR,
					 sav_argv0,
					 "STARTING",
					 "???",
					 -1,
					 0,
					 "???"
		) != 0) {
		stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif
	/* We detect if tape server have been explicitely given on command-line */
	if (stcs->u1.t.tapesrvr[0] != '\0') {
		dont_change_srv = 1;
	}

	if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
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
				sendrep (&rpfd, MSG_ERR, STG116);
				RESTORE_EID;
				free(stcs);
				exit(USERR);
			} else {
				/* We check that they are all the same */
				for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
					if (i == 0) continue;
					if (strcmp((stcp-1)->u1.h.tppool, stcp->u1.h.tppool) != 0) {
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG117,
								 i-1,
								 (stcp-1)->u1.h.xfile,
								 i,
								 stcp->u1.h.xfile,
								 (stcp-1)->u1.h.tppool,
								 stcp->u1.h.tppool);
						RESTORE_EID;
						free(stcs);
						exit(USERR);
					}
				}
				/* Everything is ok */
				strcpy(tape_pool, stcs->u1.h.tppool);
			}
		} else {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG121);
			RESTORE_EID;
			free(stcs);
			exit(USERR);
		}
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] GO ON WITH gdb /usr/local/bin/stager_castor %d, then break stagewrt_castor_hsm_file\n",getpid());
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] sleep(%d)\n",SLEEP_DEBUG);
		sleep(SLEEP_DEBUG);
		RESTORE_EID;
#endif
		c = stagewrt_castor_hsm_file ();
	} else {
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] GO ON WITH gdb /usr/local/bin/stager_castor %d, then break stagein_castor_hsm_file\n",getpid());
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] sleep(%d)\n",SLEEP_DEBUG);
		sleep(SLEEP_DEBUG);
		RESTORE_EID;
#endif
		c = stagein_castor_hsm_file ();
	}

	free(stcs);
	exit (c);
}

/* This routine will return the index in the nbcat_ent entries of an HSM file       */
/* given its internal path */
int hsmidx_vs_ipath(ipath)
	char *ipath;
{
	struct stgcat_entry *stcx;
	int i;
	char *host = NULL;
	char *filename = NULL;
	char save_ipath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];	/* internal path */
	char host1[CA_MAXHOSTNAMELEN+1];
	char path1[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
	char host2[CA_MAXHOSTNAMELEN+1];
	char path2[(CA_MAXHOSTNAMELEN+MAXPATH)+1];

	strcpy(save_ipath,ipath);

	/* Bad argument */
	if (ipath == NULL) return(-1);
	/* Null ipath */
	if (ipath[0] == '\0') return(-1);
	/* Not yet allocated */
	if (strcmp(ipath,".") == 0) return(-1);

	(void) rfio_parseln (save_ipath, &host, &filename, NORDLINKS);
	if (host != NULL) {
		strcpy(host1,host);
	} else {
		host1[0] = '\0';
	}
	if (filename != NULL) {
		strcpy(path1,filename);
	} else {
		path1[0] = '\0';
	}
	for (stcx = stcs, i = 0; stcx < stce; stcx++, i++) {
		/* if hsm_flag[] != 0 this mean it has already been transfered in a previous rtcpc() call */
		/* if hsm_ignore[] != 0 this mean we have to ignore it */
		/* if hsm_status[] != 0 this mean it has already been transfered in current rtcpc() call */
		if ((hsm_flag[i] != 0) || (hsm_ignore[i] != 0) || (hsm_status[i] != 0)) continue;
		strcpy(save_ipath,stcx->ipath);
		(void) rfio_parseln (save_ipath, &host, &filename, NORDLINKS);
		if (host != NULL) {
			strcpy(host2,host);
		} else {
			host2[0] = '\0';
		}
		if (filename != NULL) {
			strcpy(path2,filename);
		} else {
			path2[0] = '\0';
		}
		if ((strcmp(host1,host2) == 0) && (strcmp(path1,path2) == 0)) {
			return(i);
		}
	}

	return(-1);
}

/* This routine will return the index in the nbcat_ent entries of an HSM file       */
/* provided the associated hsm_flag[] and hsm_ignore[] are == 0                     */
int hsmidx(stcp)
	struct stgcat_entry *stcp;
{
	struct stgcat_entry *stcx;
	int i;

	for (stcx = stcs, i = 0; stcx < stce; stcx++, i++) {
		/* if hsm_flag[] != 0 this mean it has already been transfered in a previous rtcpc() call */
		/* if hsm_ignore[] != 0 this mean we have to ignore it */
		/* if hsm_status[] != 0 this mean it has already been transfered in current rtcpc() call */
		if ((hsm_flag[i] != 0) || (hsm_ignore[i] != 0) || (hsm_status[i] != 0)) continue;
		if (strcmp(stcx->u1.h.xfile,stcp->u1.h.xfile) == 0) {
			return(i);
		}
	}

	return(-1);
}

/* This routine will return the index in the nbcat_ent entries of an HSM file       */
/* provided the associated hsm_ignored[] is != 0                                    */
int hsmidx_is_ignored(stcp)
	struct stgcat_entry *stcp;
{
	struct stgcat_entry *stcx;
	int i;

	for (stcx = stcs, i = 0; stcx < stce; stcx++, i++) {
		if (hsm_ignore[i] == 0) continue;
		if (strcmp(stcx->u1.h.xfile,stcp->u1.h.xfile) == 0) {
			return(i);
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
	if ((castor_hsm = CASTORFILE(stcp->u1.h.xfile)) == NULL) {
		serrno = EINVAL;
		return(NULL);
	}
	if (castor_hsm != stcp->u1.h.xfile) {
		/* We extract the host information from the hsm file */
		if ((end_host_hsm = strchr(stcp->u1.h.xfile,':')) == NULL) {
			serrno = EINVAL;
			return(NULL);
		}
		if (end_host_hsm <= stcp->u1.h.xfile) {
			serrno = EINVAL;
			return(NULL);
		}
		/* We replace the first ':' with a null character */
		save_char = end_host_hsm[0];
		end_host_hsm[0] = '\0';
		host_hsm = stcp->u1.h.xfile;
		/* If the hostname begins with castor... then the user explicitely gave  */
		/* a nameserver host - otherwise he might have get the HSM_HOST of hpss  */
		/* or nothing or something wrong. In those three last cases, we will let */
		/* the nameserver API get default CNS_HOST                               */
		if (strstr(host_hsm,"hpss") != host_hsm) {
			/* It is an explicit and valid castor nameserver : the API will be able */
			/* to connect directly to this host. No need to do a putenv ourself.    */
			end_host_hsm[0] = save_char;
			return(stcp->u1.h.xfile);
		} else {
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "[DEBUG-XXX] Hsm Host %s is incompatible with a /castor file. Default CNS_HOST (from shift.conf) will apply\n",host_hsm);
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
	int rfio_stat_rc;
	struct Cns_filestat statbuf;
	struct stgcat_entry *stcp = stcs;
	char *castor_hsm = NULL;
	int i, j;
	int new_tape;
	int something_to_do;
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
    int last_found;        /* In case of use_subreqid == 0 */
	char fseq_for_log[1025]; /* 1024 characters max (+'\0') */
	int last_fseq_for_log = -1;
	char *p_fseq = NULL;
	char this_string[CA_MAXFSEQLEN+1];
	char *this_cont = NULL;
	char *start_fseq = "";
	char *cont_fseq = "-";
	char *new_fseq = ",";
	
    ALLOCHSM;

    /* Set CallbackClient */
    rtcpc_ClientCallback = &stager_hsm_callback;

	/* We initialize those size arrays */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		struct Cns_fileid Cnsfileid;
		struct stat64 sbuf;

		if (stcp->t_or_d == 'd') {
			/* Check totalsize for this internal copy */

			SETEID(stcp->uid,stcp->gid);

			PRE_RFIO;
			if (((rfio_stat_rc = rfio_stat64 (stcp->u1.d.xfile, &sbuf)) == 0) && (S_ISDIR(sbuf.st_mode) || S_ISCHR(sbuf.st_mode)
#if !defined(_WIN32)
																				|| S_ISBLK(sbuf.st_mode)
#endif
				)) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.d.xfile, "rfio_stat64",
						 "Not a regular file");
				RESTORE_EID;
				RETURN (USERR);
			} else if (rfio_stat_rc == 0) {
				hsm_totalsize[i] = (u_signed64) sbuf.st_size;
				hsm_mode[i] = sbuf.st_mode;
				hsm_transferedsize[i] = 0;
			} else {
				struct stgcat_entry stcp_input;
				struct stgcat_entry *stcp_output = NULL;
				int nstcp_output = 0;

				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.d.xfile, "rfio_stat64", rfio_serror());
				RESTORE_EID;
				/* We have lost the CASTOR filename here */
				if (stage_setlog(&stcplog) != 0) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.d.xfile, "stage_setlog", sstrerror(serrno));
					RETURN(SYERR);
				}
				/* We ask for it */
				memset((void *) &stcp_input,0,sizeof(struct stgcat_entry));
				stcp_input.reqid = stcp->reqid;
				if (stage_qry('h',
							  (u_signed64) STAGE_ALL|STAGE_REQID, /* Flags */
							  NULL,                        /* Hostname */
							  1,                           /* nstcp_input */
							  &stcp_input,                 /* stcp_input */
							  &nstcp_output,               /* nstcp_output */
							  &stcp_output,                /* stcp_output */
							  NULL,                        /* nstpp_output */
							  NULL                         /* stpp_output */
					) != 0) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.d.xfile, "stage_qry", sstrerror(serrno));
					if (stcp_output != NULL) free(stcp_output);
					RETURN(SYERR);
				}
				if (nstcp_output != 1) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.d.xfile, "stage_qry", sstrerror(SEINTERNAL));
					if (stcp_output != NULL) free(stcp_output);
					RETURN(SYERR);
				}
				if ((stcp_output[0].status != STAGEIN) || (stcp_output[0].reqid != stcp->reqid)) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.d.xfile, "stage_qry", sstrerror(SEINTERNAL));
					if (stcp_output != NULL) free(stcp_output);
					RETURN(SYERR);
				}
				/* We simply copy stcp_output[0] into stcp... */
				memcpy((void *) stcp,(void *) stcp_output,sizeof(struct stgcat_entry));
				/* and go on with normal procedure with a tape mount */
				if (stcp_output != NULL) free(stcp_output);
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG33, stcp->u1.d.xfile, "Switching to tape mount mode");
				RESTORE_EID;
				goto forced_tape_mount;
			}
			/* check file permissions */
			mode = S_IREAD;
			if (sbuf.st_uid != stcp->uid) {
				mode >>= 3;
				if (sbuf.st_gid != stcp->gid)
					mode >>= 3;
			}
			if ((sbuf.st_mode & mode) != mode) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.d.xfile, "rfio_stat64", sstrerror (EACCES));
				RESTORE_EID;
				RETURN (USERR);
			}
			RESTORE_EID;

			/* The test on the number of segments being done, we now correct the */
			/* totalsize to transfer versus -s argument */
			if (stcp->size > 0) {
				u_signed64 new_totalsize;

				new_totalsize = stcp->size;

				/* If the maximum size to transfer does not exceed physical size then */
				/* we change this field.                                              */
				if (new_totalsize < hsm_totalsize[i]) {
					hsm_totalsize[i] = new_totalsize;
				}
			}

			continue;
		}

	  forced_tape_mount:

		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
			RESTORE_EID;
			RETURN (SYERR);
		}
		/* check permissions in parent directory, get file size */
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling Cns_statx(path=\"%s\",&Cnsfileid={server=\"%s\",fileid=%s},&statbuf)\n",
				castor_hsm,
				Cnsfileid.server,
				u64tostr(Cnsfileid.fileid, tmpbuf1, 0));
		RESTORE_EID;
#endif
        SETEID(stcp->uid,stcp->gid);
		if (Cns_statx (castor_hsm, &Cnsfileid, &statbuf) < 0) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx",
					 sstrerror (serrno));
			RESTORE_EID;
			RETURN (USERR);
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
			sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx",
					 sstrerror (EACCES));
			RESTORE_EID;
			RETURN (USERR);
		}
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling Cns_getsegattrs(path=\"%s\",&Cnsfileid={server=\"%s\",fileid=%s},&(hsm_nsegments[%d]),&(hsm_segments[%d]))\n",
				castor_hsm,
				Cnsfileid.server,
				u64tostr(Cnsfileid.fileid, tmpbuf1, 0),
				i,
				i);
		RESTORE_EID;
#endif
        /* Get the segment attributes */
        if (Cns_getsegattrs(castor_hsm,&Cnsfileid,&(hsm_nsegments[i]),&(hsm_segments[i])) != 0) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Cns_getsegattrs",
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
			if (! STGSEGMENT_OK(hsm_segments[i][isegments])) {
				/* This copy number is not available for recall */
				/* We makes sure that all segments with the same copy number are really invalid */
				for (jsegments = 0; jsegments < hsm_nsegments[i]; jsegments++) {
					if (jsegments == isegments) continue;
					if (hsm_segments[i][jsegments].copyno != hsm_segments[i][isegments].copyno) continue;
					if (hsm_segments[i][jsegments].s_status == '-') {
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG137, castor_hsm, hsm_segments[i][isegments].copyno, jsegments + 1, isegments + 1);
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
						if (STGSEGMENT_OK(hsm_segments[i][jsegments])) {
							hsm_startsegment[i] = isegments;
							hsm_endsegment[i] = jsegments;
							break;
						} else {
							/* This copy number is not available for recall */
				            /* We makes sure that all segments with the same copy number are really invalid */
							for (ksegments = 0; ksegments < hsm_nsegments[i]; ksegments++) {
								if (ksegments == jsegments) continue;
								if (hsm_segments[i][ksegments].copyno != hsm_segments[i][jsegments].copyno) continue;
								if (hsm_segments[i][ksegments].s_status == '-') {
									SAVE_EID;
									sendrep (&rpfd, MSG_ERR, STG137, castor_hsm, hsm_segments[i][ksegments].copyno, ksegments + 1, jsegments + 1);
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
				if (! STGSEGMENT_OK(hsm_segments[i][jsegments])) {
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG136, castor_hsm, hsm_segments[i][isegments].copyno, isegments, jsegments, hsm_segments[i][isegments].copyno);
					RESTORE_EID;
					for (ksegments = 0; ksegments < hsm_nsegments[i]; ksegments++) {
						if (ksegments == jsegments) continue;
						if (hsm_segments[i][ksegments].copyno != hsm_segments[i][jsegments].copyno) continue;
						if (hsm_segments[i][ksegments].s_status == '-') {
							sendrep (&rpfd, MSG_ERR, STG137, castor_hsm, hsm_segments[i][jsegments].copyno, ksegments + 1, jsegments + 1);
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
					sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "end segment < start segment", sstrerror(SEINTERNAL));
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
					/* This file is very probably BEING migrated ! Then it is normal */
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, castor_hsm,
							 "stagein", "File probably currently being migrated");
					RESTORE_EID;
					RETURN (EBUSY);
				} else {
					/* But here, we found no valid segment - are they all STGSEGMENT_NOTOK */
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "stagein", "Required tape segments are not all accessible");
					RESTORE_EID;
					RETURN (USERR);
				}
			} else {
				/* This is really an empty file... */
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, castor_hsm,
						 "stagein", "Empty file");
				RESTORE_EID;
				RETURN (USERR);
			}
		}

		/* The test on the number of segments being done, we now correct the */
		/* totalsize to transfer versus -s argument */
		if (stcp->size > 0) {
			u_signed64 new_totalsize;

			new_totalsize = stcp->size;

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
				sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Requires > 1 segment to be recalled [supported with this version only if this is the last -M option]",
						 sstrerror (serrno));
				RESTORE_EID;
				RETURN (USERR);
			}
		}
#endif
	}

  getseg:
	/* We initalize current vid, side, fseq and blockid's */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		hsm_vid[i] = NULL;
		hsm_side[i] = -1;
		hsm_fseq[i] = -1;
		hsm_oksegment[i] = -1;
		memset(hsm_blockid[i],0,sizeof(blockid_t));
	}

	/* We initialize the latest vid and side from vmgr_querytape() */
	last_vid[0] = '\0';
	last_side = -1;

	/* We loop on all the requests, choosing those that requires the same tape */
	new_tape = 0;
	something_to_do = 0;
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {

		if (stcp->t_or_d == 'd') {
			if (hsm_status[i] != 0) {
				/* Yet done */
				continue;
			}
			/* Internal copy - we do it as soon as we meet it - Internal copy has to be successful in ONE pass */

#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s -> %s (%s bytes)\n",
    	            stcp->u1.d.xfile,
    	            stcp->ipath,
        	        u64tostr(hsm_totalsize[i], tmpbuf1, 0));
			RESTORE_EID;
#endif

			/* We do almost exactly as in CASTOR/rfio/rfcp via BIN/cpdskdsk */
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] GO ON WITH gdb /usr/local/bin/stager_castor %d, then break stage_copyfile\n",getpid());
			sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] sleep(%d)\n",SLEEP_DEBUG);
			sleep(SLEEP_DEBUG);
			RESTORE_EID;
#endif
			SETEID(stcp->uid,stcp->gid);
			if ((rtcp_rc = stage_copyfile(stcp->u1.d.xfile,
										  stcp->ipath,
										  hsm_mode[i],
										  get_subreqid(stcp),
										  hsm_totalsize[i],
										  &(hsm_transferedsize[i]))) != 0) {
				RETURN(rtcp_rc);
			}
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s copied (%s out of %s bytes)\n",
					stcp->u1.d.xfile,
					u64tostr(hsm_totalsize[i], tmpbuf1, 0),
					u64tostr(hsm_totalsize[i], tmpbuf2, 0)
					);
			RESTORE_EID;
#endif
			hsm_status[i] = hsm_flag[i] = 1;
			SETEID(start_passwd.pw_uid,start_passwd.pw_gid);
			continue;
		}

#ifdef STAGER_DEBUG
		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
			RESTORE_EID;
			RETURN (SYERR);
		}
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : totalsize=%s, transferedsize=%s\n",
                castor_hsm,
                u64tostr(hsm_totalsize[i], tmpbuf1, 0),
                u64tostr(hsm_transferedsize[i], tmpbuf2, 0));
		RESTORE_EID;
#endif
		if (hsm_totalsize[i] > hsm_transferedsize[i]) {
			u_signed64 virtual_size;
			u_signed64 previous_virtual_size;
			int do_not_process_any_other_segment;
			
			something_to_do = 1;

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
							sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "yet transfered size does not match the sum of all previous segments",
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
				sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "could not find segment to transfer",
						 sstrerror (SEINTERNAL));
				RESTORE_EID;
				RETURN (SYERR);
			}
			hsm_vid[i] = hsm_segments[i][hsm_oksegment[i]].vid;
			hsm_side[i] = hsm_segments[i][hsm_oksegment[i]].side;
			hsm_fseq[i] = hsm_segments[i][hsm_oksegment[i]].fseq;
			memcpy(hsm_blockid[i],hsm_segments[i][hsm_oksegment[i]].blockid,sizeof(blockid_t));
			/* We keep in mind that we have found a vid/side to use */
			strcpy(last_vid,hsm_vid[i]);
			last_side = hsm_side[i];
			new_tape = 1;
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : Got vid/side.fseq=%s/%d.%d (blockid '\\%d\\%d\\%d\\%d')\n",
					castor_hsm,
					hsm_vid[i],hsm_side[i],hsm_fseq[i],
					(int) hsm_blockid[i][0],
					(int) hsm_blockid[i][1],
					(int) hsm_blockid[i][2],
					(int) hsm_blockid[i][3]);
			RESTORE_EID;
#endif
			
			/* We have found hsm_vid[i]/hsm_side[i] */
			/* We check if other HSM files also requires this tape */

			/* But we must be careful: if current file cannot be staged using one single segment AND */
			/* if asyncrhoneous callback is disabled (use_subreqid == 0) then we cannot continue with */
			/* any other file: the order of the command must be followed exaxctly. */
			/* For example, suppose that file i   is on tape t1 fseq f1 */
			/*                                               t2 fseq f2 */
			/*                           file i+n is on tape t1 fseq f2 */
			/* Then we cannot optimize the recall like this: t1.[f1,f2] + t2.f2 */
			/* because file i+n (e.g. t1.f2) would then be staged before file i (e.g. t1.f2 + t2.f2) */

			/* This logic of making current non-totally-transfered file will be exactly the next transfered */
			/* one in case of asyncrhroneous callback disabled is extended to any number of segments, that is */
			/* If current file, No i, by definition not yet totally transfered because we are here indeed, */
			/* will NOT be totally staged using the segment hsm_segments[i][hsm_oksegment[i]] then we cannot */
			/* go further */

			do_not_process_any_other_segment = 0;
			if (! use_subreqid) {
				if (hsm_segments[i][hsm_oksegment[i]].segsize < (hsm_totalsize[i] - hsm_transferedsize[i])) {
					do_not_process_any_other_segment = 1;
				}
			}

			if ((do_not_process_any_other_segment == 0) && (stcp != (stce - 1))) {
				stcp++;
				last_found = i;
				for (j = i + 1; stcp < stce; stcp++, j++) {
					if (stcp->t_or_d == 'd') continue;
#ifdef STAGER_DEBUG
					/* Search for castor a-la-unix path in the file to stagein */
					if ((castor_hsm = hsmpath(stcp)) == NULL) {
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
						RESTORE_EID;
						RETURN (SYERR);
					}
					SAVE_EID;
					sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : totalsize=%s, transferedsize=%s\n",
			                castor_hsm,
			                u64tostr(hsm_totalsize[j], tmpbuf1, 0),
		    	            u64tostr(hsm_transferedsize[j], tmpbuf2, 0));
					RESTORE_EID;
#endif
					if (hsm_totalsize[j] > hsm_transferedsize[j]) {
						u_signed64 virtual_size;
						u_signed64 previous_virtual_size;

						/* We search at which segment to start the transfer. It depends on the size */
						/* yet transfered and the size of each of the segments for file No i */
						virtual_size = previous_virtual_size = 0;
						for (isegments = hsm_startsegment[j]; isegments <= hsm_endsegment[j]; isegments++) {
							/* We add the size of this segment to total virtual_size up to this segment */
							/* and compares with the size physically yet transfered */
							virtual_size += hsm_segments[j][isegments].segsize;
							if (virtual_size > hsm_transferedsize[j]) {
								/* Well... in principle if this segment is NOT the first of the segments */
								/* then what we had already transfered should end up exactly to the previous */
								/* virtual size, e.g. the sum of all previous segments */
								if (isegments > hsm_startsegment[j]) {
									if (hsm_transferedsize[j] != previous_virtual_size) {
										SAVE_EID;
										sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "yet transfered size does not match the sum of all previous segments",
												 sstrerror (SEINTERNAL));
										RESTORE_EID;
										RETURN (SYERR);
									}
								}
								hsm_oksegment[j] = isegments;
								break;
		    	            }
							previous_virtual_size = virtual_size;
						}
						if (hsm_oksegment[j] < 0) {
							/* Impossible ! */
							SAVE_EID;
							sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "could not find segment to transfer",
									 sstrerror (SEINTERNAL));
							RESTORE_EID;
							RETURN (SYERR);
						}
					}
					/* We accept this only if hsm_vid[j] matches hsm_vid[i] as well as for side */
					if ((strcmp(hsm_segments[j][hsm_oksegment[j]].vid, hsm_vid[i]) == 0) &&
						(hsm_segments[j][hsm_oksegment[j]].side == hsm_side[i])) {
						if (use_subreqid) {
							/* No pb : we can stagein in any order */
							hsm_vid[j] = hsm_segments[j][hsm_oksegment[j]].vid;
							hsm_side[j] = hsm_segments[j][hsm_oksegment[j]].side;
							hsm_fseq[j] = hsm_segments[j][hsm_oksegment[j]].fseq;
							memcpy(hsm_blockid[j],hsm_segments[j][hsm_oksegment[j]].blockid,sizeof(blockid_t));
#ifdef STAGER_DEBUG
							/* Search for castor a-la-unix path in the file to stagein */
							SAVE_EID;
							sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : Added vid/side.fseq=%s/%d.%d (blockid '\\%d\\%d\\%d\\%d')\n",
									castor_hsm,
									hsm_vid[j],hsm_side[j],hsm_fseq[j],
									(int) hsm_blockid[j][0],
									(int) hsm_blockid[j][1],
									(int) hsm_blockid[j][2],
									(int) hsm_blockid[j][3]);
							RESTORE_EID;
#endif
							last_found = j;
						} else if (j == (last_found + 1)) {
							/* We are lucky : next stcp's segment shares also the same vid/side */

							hsm_vid[j] = hsm_segments[j][hsm_oksegment[j]].vid;
							hsm_side[j] = hsm_segments[j][hsm_oksegment[j]].side;
							hsm_fseq[j] = hsm_segments[j][hsm_oksegment[j]].fseq;
							memcpy(hsm_blockid[j],hsm_segments[j][hsm_oksegment[j]].blockid,sizeof(blockid_t));
#ifdef STAGER_DEBUG
							/* Search for castor a-la-unix path in the file to stagein */
							SAVE_EID;
							sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : Added vid/side.fseq=%s/%d.%d (blockid '\\%d\\%d\\%d\\%d')\n",
									castor_hsm,
									hsm_vid[j],hsm_side[j],hsm_fseq[j],
									(int) hsm_blockid[j][0],
									(int) hsm_blockid[j][1],
									(int) hsm_blockid[j][2],
									(int) hsm_blockid[j][3]);
							RESTORE_EID;
#endif
							/* But we definitely cannot continue with another stcp if by... */
							/* bad luck this time, current file have another segment and this */
							/* segment would (should, in theory, with a well configured pool) be on another tape... */
							/* e.g. we do not add another logic if next segment would be on the same tape */
							/* we break stream right now */
							if (hsm_oksegment[j] != hsm_endsegment[j]) {
#ifdef STAGER_DEBUG
								/* Search for castor a-la-unix path in the file to stagein */
								SAVE_EID;
								sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : Not in a single segment. Forcing limitation of the stream\n",
										castor_hsm);
								RESTORE_EID;
#endif
								break;
							}


							last_found = j;
						} else {
							/* Bad luck : we are not allowed to stagein regardless of order and the next segment */
							/* in the list that shares same tape is NOT the next one just after i */
#ifdef STAGER_DEBUG
							/* Search for castor a-la-unix path in the file to stagein */
							SAVE_EID;
							sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : use_subreqid = 0 => Cannot add vid/side.fseq=%s/%d.%d\n",
									castor_hsm,
									hsm_segments[j][hsm_oksegment[j]].vid,
									hsm_segments[j][hsm_oksegment[j]].side,
									hsm_segments[j][hsm_oksegment[j]].fseq);
							RESTORE_EID;
#endif
							/* No point going further */
							break;
						}
					}
                }
			}
		}
		/* We check if we flagged that we can exit this loop on i */
		if (new_tape != 0) {
			break;
		}
	}

	if (! something_to_do) {
		RETURN(0);
	}

	if ((new_tape == 0) || (last_vid[0] == '\0') || (last_side < 0)) {
		/* If we are there this is because we run there the first time or we reached a */
		/* goto getseg. In any case new_tape must not be zero */
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "%s : Internal error : new_tape=%d, last_vid=%s, last_side=%d\n",
				(castor_hsm != NULL) ? castor_hsm : "???",
				new_tape,
				last_vid,
				last_side);
		RESTORE_EID;
		/* The RETURN macro will superimpose globals status code 0 if necessary */
		RETURN (SYERR);
	}

	SETTAPEEID(stcs->uid,stcs->gid);

	while (1) {
		/* Wait for the corresponding tape to be available */
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : Calling vmgr_querytape(vid=\"%s\",side=%d,&vmgr_tapeinfo,dgn)\n",castor_hsm,last_vid,last_side);
		RESTORE_EID;
#endif
		vid[0] = '\0';
		side = -1;
		if (vmgr_querytape (last_vid, last_side, &vmgr_tapeinfo, dgn) == 0) {
			if (((vmgr_tapeinfo.status & DISABLED) == DISABLED) ||
				((vmgr_tapeinfo.status & EXPORTED) == EXPORTED) ||
				((vmgr_tapeinfo.status & ARCHIVED) == ARCHIVED)) {
				SAVE_EID;
				sendrep(&rpfd, MSG_ERR, STG134, last_vid,
						((vmgr_tapeinfo.status & DISABLED) == DISABLED) ? "DISABLED" :
						(((vmgr_tapeinfo.status & EXPORTED) == EXPORTED) ? "EXPORTED" : "ARCHIVED"));
				RESTORE_EID;
				vid[0] = '\0';
				side = -1;
				RETURN (((vmgr_tapeinfo.status & DISABLED) == DISABLED) ? ETHELDERR : USERR);
			}
			strcpy(vid,last_vid);
			side = last_side;
			break;
		}
		vid[0] = '\0';
		side = -1;
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "vmgr_querytape", sstrerror (serrno));
		sendrep (&rpfd, MSG_ERR, "STG47 - %s : Retrying request in %d seconds\n", castor_hsm, RETRYI);
		RESTORE_EID;
		sleep(RETRYI);
	}

	/* We grab the optimal blocksize */
	devinfo = Ctape_devinfo(vmgr_tapeinfo.model);

	/* Build the request from where we found vid/side (included) up to our next (excluded) */
	nbcat_ent_current = 0;
	istart = -1;
	iend = -1;
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		if ((hsm_vid[i] != NULL) && (hsm_side[i] >= 0)) {
			++nbcat_ent_current;
			if (istart < 0) {
				istart = i;
			} else {
				iend = i;
			}
		}
	}

	if (istart >= 0 && iend < 0) {
		/* This means that there was only one entry */
		if (nbcat_ent_current != 1) {
			serrno = SEINTERNAL;
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Internal error : istart >= 0 && iend < 0 && nbcat_ent_current != 1",
					 sstrerror (serrno));
			RESTORE_EID;
			RETURN (USERR);
		}
		iend = istart;
	}

	/* nbcat_ent_current will be the number of entries that will use vid */
	if ((nbcat_ent_current == 0) || (istart < 0) || (iend < 0)) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Internal error : nbcat_ent_current == 0 || istart < 0 || iend < 0",
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
		sendrep (&rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "malloc error",strerror(errno));
		RESTORE_EID;
		free(stcp_start);
		RETURN (USERR);
	}
	stcp_end += nbcat_ent_current;

	/* we fill temporary window [stcp_start,stcp_end] */
	fseq_for_log[0] = '\0';
	for (stcp = stcs, i = 0, stcp_tmp = stcp_start; stcp < stce; stcp++, i++) {
		if ((hsm_vid[i] != NULL) && (hsm_side[i] >= 0)) {
			*stcp_tmp = *stcp;
			/* We also force the blocksize while recalling in some cases (s/w pbs with default values) */
			if (devinfo->defblksize > 99999) {
				stcp_tmp->blksize = devinfo->defblksize;
			}
			stcp_tmp++;

			/* Can we add something into fseq_for_log ? */
			if (fseq_for_log[0] == '\0') {
				/* First time */
				this_cont = start_fseq;
				p_fseq = fseq_for_log;
			} else if (last_fseq_for_log == hsm_fseq[i] - 1) {
				/* Continuation in tape sequence */
				if ((this_cont == start_fseq) || (this_cont == new_fseq)) {
					/* If previous time was first time or a new entry */
					/* we want to point exactly to the null byte, e.g. at the end */
					p_fseq += strlen(p_fseq);
				}
				this_cont = cont_fseq;
			} else {
				/* Not a continuation : we go exactly to the null byte */
				p_fseq += strlen(p_fseq);
				this_cont = new_fseq;
			}
			last_fseq_for_log = hsm_fseq[i];

#if (defined(__osf__) && defined(__alpha))
			sprintf (this_string, "%s%d", this_cont, hsm_fseq[i]);
#else
#if defined(_WIN32)
			_snprintf (this_string, CA_MAXFSEQLEN, "%s%d", this_cont, hsm_fseq[i]);
#else
			snprintf (this_string, CA_MAXFSEQLEN, "%s%d", this_cont, hsm_fseq[i]);
#endif
#endif
			this_string[CA_MAXFSEQLEN] = '\0';
			/* p_fseq points somewhere in fseq string */
			if ((strlen(fseq_for_log) - strlen(p_fseq) + strlen(this_string)) <= 1024) {
				strcpy(p_fseq, this_string);
			}
		}
	}
	/* Makes sure fseq_for_log ends with '\n\0' */
	fseq_for_log[1024] = '\0';
	if (fseq_for_log[1023] != '\0') { /* Does not end here - logging too long */
		fseq_for_log[1023] = '\n';    /* Forces a newline */
		fseq_for_log[1022] = '.';     /* and log that */
		fseq_for_log[1021] = '.';     /* there is something after */
		fseq_for_log[1020] = '.';     /* that we do not log */
	}

#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] Use [vid,side,vsn,dgn,aden,lbltype]=[%s,%d,%s,%s,%s,%s], fseqs=%s\n",
			vid,side,vmgr_tapeinfo.vsn,dgn,vmgr_tapeinfo.density,vmgr_tapeinfo.lbltype,fseq_for_log);
	RESTORE_EID;
#else
	SAVE_EID;
	stglogit (func, "Use [vid,side,vsn,dgn,aden,lbltype]=[%s,%d,%s,%s,%s,%s], fseqs=%s\n",
			  vid,side,vmgr_tapeinfo.vsn,dgn,vmgr_tapeinfo.density,vmgr_tapeinfo.lbltype,fseq_for_log);
	if (ismig_log) {
		sendrep (&rpfd, MSG_OUT, "Use [vid,side,vsn,dgn,aden,lbltype]=[%s,%d,%s,%s,%s,%s], fseqs=%s\n",
				 vid,side,vmgr_tapeinfo.vsn,dgn,vmgr_tapeinfo.density,vmgr_tapeinfo.lbltype,fseq_for_log);
	}
	RESTORE_EID;
#endif


	/* We "interrogate" for the total number of structures */
	if (build_rtcpcreq(&nrtcpcreqs, NULL, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
		RETURN (USERR);
	}
	if (nrtcpcreqs <= 0) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "Cannot determine number of tapes",sstrerror(serrno));
		RESTORE_EID;
		RETURN (USERR);
	}

	/* We reset all the hsm_flag[] values that are candidates for search */
	for (i = 0; i < nbcat_ent; i++) {
		if ((hsm_totalsize[i] > hsm_transferedsize[i]) && ((stcs + i) -> t_or_d != 'd')) {
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
		sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "build_rtcpcreq",sstrerror (serrno));
		RESTORE_EID;
		free(stcp_start);
		RETURN ((serrno == EINVAL) ? USERR : SYERR);
	}

	/* By construction, rtcpcreqs has only one indice: 0 */
  reselect:
#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling rtcpc()\n");
	STAGER_RTCP_DUMP(rtcpcreqs[0]);
	sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN] sleep(%d)\n",SLEEP_DEBUG);
	sleep(SLEEP_DEBUG);
	RESTORE_EID;
#endif
	Flags = 0;
	rtcpc_kill_cleanup = 1;
	error_already_processed = 0;

	/* We reset all the hsm_flag[] values that are candidates for callback search */
	for (i = 0; i < nbcat_ent; i++) {
		if ((hsm_totalsize[i] > hsm_transferedsize[i]) && ((stcs + i) -> t_or_d != 'd')) {
			hsm_flag[i] = 0;            /* This one will be a candidate while searching */
		} else {
			hsm_flag[i] = 1;            /* We will certainly NOT return again this file while searching */
		}
	}

	fatal_callback_error = callback_error = callback_forced_noretry = 0;
	rtcp_rc = rtcpc(rtcpcreqs[0]);
	save_serrno = serrno;
	rtcpc_kill_cleanup = 0;
	if (rtcp_rc < 0) {

		if ((api_flags & STAGE_NORETRY) == STAGE_NORETRY) {
			/* Error and user said to never retry */
			SAVE_EID;
			if (rtcpcreqs[0]->tapereq.side > 0) {
				sendrep (&rpfd, MSG_ERR, STG202, rtcpcreqs[0]->tapereq.vid, rtcpcreqs[0]->tapereq.side, "rtcpc",sstrerror(save_serrno));
			} else {
				sendrep (&rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc",sstrerror(save_serrno));
			}
			RESTORE_EID;
			free(stcp_start);
			RETURN (SYERR);
		}

		if (callback_error != 0) {
			/* This is a callback error - considered as fatal */
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "callback", save_serrno ? sstrerror(save_serrno) : "");
			RESTORE_EID;
			free(stcp_start);
			RETURN (SYERR);
		}
		if ((rtcpc_CheckRetry(rtcpcreqs[0]) == TRUE) && (callback_forced_noretry != 0)) {
			/* Callback wants to exit immediately in contrary to what to the mover's API says */
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "forced exit", save_serrno ? sstrerror(save_serrno) : "");
			RESTORE_EID;
			free(stcp_start);
			RETURN (USERR);
		}
		/* Because if serrno == EVQDGNINVL rtcpc_CheckRetry() would return also TRUE */
		if ((rtcpc_CheckRetry(rtcpcreqs[0]) == TRUE)
#ifdef TMS
			&& (save_serrno != EVQDGNINVL)
#endif
			) {
			tape_list_t *tl;
			/* Rtcopy bits suggest to retry */
			CLIST_ITERATE_BEGIN(rtcpcreqs[0],tl) {
		   		*tl->tapereq.unit = '\0';
		   		if ( dont_change_srv == 0 ) *tl->tapereq.server = '\0'; 
			} CLIST_ITERATE_END(rtcpcreqs[0],tl);
			SAVE_EID;
			if (rtcpcreqs[0]->tapereq.side > 0) {
				sendrep (&rpfd, MSG_ERR, STG202, rtcpcreqs[0]->tapereq.vid, rtcpcreqs[0]->tapereq.side, "rtcpc",sstrerror(save_serrno));
			} else {
				sendrep (&rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc",sstrerror(save_serrno));
			}
			if (save_serrno == ETVBSY) {
				sendrep (&rpfd, MSG_ERR, "STG47 - Re-selecting another tape server in %d seconds\n", RETRYI);
				sleep(RETRYI);
			} else {
				sendrep (&rpfd, MSG_ERR, "STG47 - Re-selecting another tape server\n");
			}
			RESTORE_EID;
			goto reselect;
		} else {
			int forced_exit = 0;

			/* Rtcopy bits suggest to stop */
			SAVE_EID;
			if (rtcpcreqs[0]->tapereq.side > 0) {
				sendrep (&rpfd, MSG_ERR, STG202, rtcpcreqs[0]->tapereq.vid, rtcpcreqs[0]->tapereq.side, "rtcpc",sstrerror(save_serrno));
			} else {
				sendrep (&rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc",sstrerror(save_serrno));
			}
			if (
				(error_already_processed != 0) ||
				/* Catch cases when TAPE error not yet handle by stager_process_error() */
				ISTAPESERRNO(save_serrno) ||
				/* Catch cases when VDQM error not yet handle by stager_process_error() */
				ISVDQMSERRNO(save_serrno)
				) {
				if (error_already_processed == 0) stager_process_error(save_serrno,&(rtcpcreqs[0]->tapereq),NULL,NULL);
				forced_exit = 1;
			} else {
				for (stcp_tmp = stcp_start, i = 0; stcp_tmp < stcp_end; stcp_tmp++, i++) {
					stager_process_error(save_serrno,&(rtcpcreqs[0]->tapereq),&(rtcpcreqs[0]->file[i].filereq),stcp_tmp->u1.h.xfile);
					if (error_already_processed != 0) {
						forced_exit = 1;
						break;
					}
					/* No medium error was processed */
					switch (rtcpcreqs[0]->file[i].filereq.err.errorcode) {
					case ENOENT:
						/* We check if the failure was because of a missing file ? */
					case EINVAL:
						/* We check if the failure was because of a invalid value somewhere in the chain ? */
					case EIO:
						/* We check if the failure was because of an input/output error somewhere in the chain ? */
						serrno = rtcpcreqs[0]->file[i].filereq.err.errorcode;
						sendrep (&rpfd, MSG_ERR, STG02, stcp_tmp->u1.h.xfile, "",sstrerror(serrno));
						forced_exit = 1;
						break;
					}
				}
			}
			RESTORE_EID;
			if (forced_exit != 0) {
				free(stcp_start);
				RETURN (((serrno == ENOENT) || (serrno == EINVAL)) ? USERR : SYERR);
			}
		}
	} else if (rtcp_rc > 0) {
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "rtcpc() : Unknown error code (returned %d)\n", rtcp_rc);
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
	struct stat64 statbuf;
	int rtcp_rc;
	struct stgcat_entry *stcp = stcs;
	char *castor_hsm = NULL;
#ifdef STAGER_DEBUG
	char tmpbuf[21];
#endif
	extern char* poolname2tapepool _PROTO((char *));
	struct devinfo *devinfo;
	int save_serrno;
	int fseq_start;
	int fseq_end;
	int max_tape_fseq;

	ALLOCHSM;

	/* Set CallbackClient */
	rtcpc_ClientCallback = &stager_hsm_callback;

	/* We initialize those size arrays */
	i = 0;
	for (stcp = stcs; stcp < stce; stcp++, i++) {

		SETEID(stcp->uid,stcp->gid);
		statbuf.st_size = stcp->actual_size;

		/* Search for castor a-la-unix path in the file to stagewrt */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
			RESTORE_EID;
			RETURN (USERR);
		}

		hsm_totalsize[i] = (u_signed64) statbuf.st_size;

#ifndef FULL_STAGEWRT_HSM
		if (ISSTAGEWRT(stcs)) {
			/* Here we support limitation of number of bytes in write-to-tape */
			if (stcp->size > 0) {
				u_signed64 new_totalsize;

				new_totalsize = stcp->size;

				/* If the amount of bytes to transfer is asked to be lower than the physical */
				/* size of the file, we reflect this in the corresponding field.             */
				if (new_totalsize < hsm_totalsize[i]) {
					hsm_totalsize[i] = new_totalsize;
				}
			}
		}
#endif /* FULL_STAGEWRT_HSM */
		hsm_transferedsize[i] = 0;
		hsm_fseq[i] = -1;
		hsm_vid[i] = NULL;
		hsm_side[i] = -1;
	}

	while (1) {
		u_signed64 totalsize_to_transfer;
		u_signed64 estimated_free_space;
		int vmgr_gettape_nretry;
		int vmgr_gettape_iretry;

	  gettape:
		totalsize_to_transfer = 0;

		for (i = 0; i < nbcat_ent; i++) {
			if (hsm_ignore[i] != 0) continue;
			totalsize_to_transfer += (hsm_totalsize[i] - hsm_transferedsize[i]);
		}

		if (totalsize_to_transfer <= 0) {
			/* Finished */
			break;
		}

		/* We loop until everything is staged */
		Flags = 0;

		SETTAPEEID(rtcp_uid ? rtcp_uid : stcs->uid,rtcp_gid ? rtcp_gid : stcs->gid);

		vmgr_gettape_nretry = 3;
		vmgr_gettape_iretry = 0;

		while (1) {
			stagewrt_nomoreupdatetape = 0;

#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] "
					"Calling vmgr_gettape(tape_pool=\"%s\",size=%s,NULL,vid,vsn,dgn,aden,lbltype,model,&side,&fseq,&estimated_free_space)\n",
					tape_pool, u64tostr((u_signed64) totalsize_to_transfer, tmpbuf, 0));
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
				&side,                  /* side      (output)              */
				&fseq,                  /* fseq      (output)              */
				&estimated_free_space   /* freespace (output)              */
				) == 0) {
				max_tape_fseq = stage_util_maxtapefseq(lbltype);
				if ((max_tape_fseq > 0) && (fseq > max_tape_fseq)) {
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, "STG02 - %s/%d : vmgr_gettape returns fseq > max_tape_fseq=%d (label type \"%s\")\n", vid, side, max_tape_fseq, lbltype);
					sendrep (&rpfd, MSG_ERR, "STG02 - %s/%d : Flagging tape RDONLY\n", vid, side);
					RESTORE_EID;
					if (vmgr_updatetape(vid,
										side,
										(u_signed64) 0,
										0,
										0,
										TAPE_RDONLY
						) != 0) {
						SAVE_EID;
						if (side > 0) {
							sendrep (&rpfd, MSG_ERR, STG202, vid, side, "vmgr_updatetape", sstrerror(serrno));
						} else {
							sendrep (&rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape", sstrerror(serrno));
						}
						RESTORE_EID;
					}
					vid[0] = '\0';
					side = -1;
					continue;
				}
				break;
			}

			/* Makes sure vid variable has not been overwritten */
			vid[0] = '\0';
			side = -1;
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, "", "vmgr_gettape",sstrerror (serrno));
			RESTORE_EID;
			if (serrno != ENOSPC) {
				/* So we reset the ENOSPC special counter... */
				retryi_vmgr_gettape_enospc = 0;
				if (++vmgr_gettape_iretry > vmgr_gettape_nretry) {
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, "", "Number of retries exceeded on vmgr_gettape. Exit.",sstrerror (serrno));
					RESTORE_EID;
					RETURN (SYERR);
				}
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, "Re-trying in %d seconds [Retry No %d/%d]\n",
						 RETRYI, vmgr_gettape_iretry, vmgr_gettape_nretry);
				RESTORE_EID;
				sleep(RETRYI);
			} else {
				if (retryi_vmgr_gettape_enospc <= 0) {
					retryi_vmgr_gettape_enospc = 5 * 60; /* Start with a sleep of 5 minutes */
				} else if (retryi_vmgr_gettape_enospc < RETRYI_VMGR_GETTAPE_ENOSPC_MAX) {
					retryi_vmgr_gettape_enospc *= 2; /* Then double the sleep */
				}
				if (retryi_vmgr_gettape_enospc > RETRYI_VMGR_GETTAPE_ENOSPC_MAX) {
					/* But not more than the maximum */
					retryi_vmgr_gettape_enospc = RETRYI_VMGR_GETTAPE_ENOSPC_MAX;
				}
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, "Re-trying in %d seconds\n",
						 retryi_vmgr_gettape_enospc);
				RESTORE_EID;
				sleep(retryi_vmgr_gettape_enospc);
			}
		}
		/* From now on, "vid/side" is in status TAPE_BUSY */

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
			if (hsm_ignore[i] != 0) {
				/* To be skipped */
				continue;
			}
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
					/* We always force the blocksize while migrating */
					stcp->blksize = devinfo->defblksize;
					if (hsm_ignore[i] != 0) {
						/* To be skipped */
						continue;
					}
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
			sendrep (&rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "Cannot find where to begin",sstrerror(serrno));
			RESTORE_EID;
			Flags = 0;
			RETURN (USERR);
		}

#ifdef MAX_RTCPC_FILEREQ
		if ((iend - istart + 1) > MAX_RTCPC_FILEREQ) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, "STG02 - stagewrt_castor_hsm_file : MAX_RTCPC_FILEREQ=%d reached. Migration will be sequentially splitted\n",MAX_RTCPC_FILEREQ);
			RESTORE_EID;
			iend = MAX_RTCPC_FILEREQ + istart - 1;
			stcp_end = stcp_start + MAX_RTCPC_FILEREQ;
		}
#endif
		/* From now on we know that stcp[istart,iend] fulfill a-priori the requirement */
		nbcat_ent_current = iend - istart + 1;
		fseq_start = fseq_end = -1;
		for (i = 0; i < nbcat_ent; i++) {
			if ((i >= istart) && (i <= iend) && (hsm_ignore[i] == 0)) {
				hsm_vid[i] = vid;
				hsm_side[i] = side;
				hsm_fseq[i] = fseq++;
				fseq_end = hsm_fseq[i];
				if (fseq_start == -1) fseq_start = fseq_end;
				if ((max_tape_fseq > 0) && (fseq_end >= max_tape_fseq)) {
					/* We reached the last allowed tape sequence number */
					if (i < iend) {
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, "STG02 - stagewrt_castor_hsm_file : max_tape_fseq=%d reached (label type \"%s\"). Migration will be sequentially splitted\n",max_tape_fseq, lbltype);
						RESTORE_EID;
						/* By changing iend now we make sure next iteration will not go in there */
						iend = i;
						stcp_end = stcp_start + (iend - istart + 1);
					}
				}
			} else {
				hsm_vid[i] = NULL;
				hsm_side[i] = -1;
				hsm_fseq[i] = -1;
			}
		}

#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Use [vid,side,vsn,dgn,aden,lbltype,fseqs]=[%s,%d,%s,%s,%s,%s,%d to %d]\n",
				vid,side,vsn,dgn,aden,lbltype,hsm_fseq[istart],hsm_fseq[iend]);
		RESTORE_EID;
#else
		SAVE_EID;
		stglogit (func, "Use [vid,side,vsn,dgn,aden,lbltype,fseqs]=[%s,%d,%s,%s,%s,%s,%d to %d]\n",
				  vid,side,vsn,dgn,aden,lbltype,fseq_start,fseq_end);
		if (ismig_log) {
			sendrep (&rpfd, MSG_OUT, "Use [vid,side,vsn,dgn,aden,lbltype,fseqs]=[%s,%d,%s,%s,%s,%s,%d to %d]\n",
					 vid,side,vsn,dgn,aden,lbltype,fseq_start,fseq_end);
		}
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
			sendrep (&rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "Number of rtcpc structures different vs. deterministic value of 1",sstrerror(serrno));
			RESTORE_EID;
			Flags = 0;
			RETURN (USERR);
		}

		/* We reset all the hsm_flags entries */
		for (i = 0; i < nbcat_ent; i++) {
			if ((hsm_totalsize[i] > hsm_transferedsize[i]) && (hsm_ignore[i] == 0)) {
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
			sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "build_rtcpcreq",sstrerror (serrno));
			RESTORE_EID;
			Flags = 0;
			RETURN ((serrno == EINVAL) ? USERR : SYERR);
		}

#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling rtcpc()\n");
		STAGER_RTCP_DUMP(rtcpcreqs[0]);
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] sleep(%d)\n",SLEEP_DEBUG);
		sleep(SLEEP_DEBUG);
		RESTORE_EID;
#endif

	  reselect:
		Flags = 0;
		rtcpc_kill_cleanup = 1;
		error_already_processed = 0;

		/* We reset all the hsm_flag[] values that are candidates for callback search */
		for (i = 0; i < nbcat_ent; i++) {
			if (hsm_totalsize[i] > hsm_transferedsize[i]) {
				hsm_flag[i] = 0;            /* This one will be a candidate while searching */
			} else {
				hsm_flag[i] = 1;            /* We will certainly NOT return again this file while searching */
			}
		}

		fatal_callback_error = callback_error = callback_forced_noretry = 0;
		rtcp_rc = rtcpc(rtcpcreqs[0]);
		save_serrno = serrno;
		rtcpc_kill_cleanup = 0;

		if (rtcp_rc < 0) {
			int j;

			/* rtcpc failed */
			SAVE_EID;
			if (side > 0) {
				sendrep (&rpfd, MSG_ERR, STG202, vid, side, "rtcpc",sstrerror (save_serrno));
			} else {
				sendrep (&rpfd, MSG_ERR, STG02, vid, "rtcpc",sstrerror (save_serrno));
			}
			RESTORE_EID;

			if (! error_already_processed) {
				stager_process_error(save_serrno,&(rtcpcreqs[0]->tapereq),NULL,NULL);
			}

			if ((api_flags & STAGE_NORETRY) == STAGE_NORETRY) {
				/* Error and user said to never retry */
				free(stcp_start);
				RETURN (SYERR);
			}

			if (callback_error != 0) {
				/* This is a callback error - considered as fatal only if we run in non-asynchroneous mode */
				if ((use_subreqid != 0) && (fatal_callback_error == 0)) {
					/* We continue as an acceptable error */
					if ((Flags != TAPE_FULL) && (stagewrt_nomoreupdatetape == 0)) {
						/* This will remove the BUSY flag in vmgr */
						Flags = 0;
						if ((vid[0] != '\0') && (side >= 0)) {
#ifdef STAGER_DEBUG
							SAVE_EID;
							sendrep (&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=\"%s\",side=%d,BytesWriten=0,CompressionFactor=0,FilesWriten=0,Flags=%d)\n",vid,side,Flags);
							RESTORE_EID;
#endif
							if (vmgr_updatetape (vid, side, (u_signed64) 0, 0, 0, Flags) != 0) {
								SAVE_EID;
								if (side > 0) {
									sendrep (&rpfd, MSG_ERR, STG202, vid, side, "vmgr_updatetape",sstrerror (serrno));
								} else {
									sendrep (&rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",sstrerror (serrno));
								}
								RESTORE_EID;
							}
							vid[0] = '\0';
							side = -1;
						}
					}
					/* We precede a bit the calculation that is done at the gettape: label */
					{
						u_signed64 next_totalsize_to_transfer = 0;

						for (i = 0; i < nbcat_ent; i++) {
							if (hsm_ignore[i] != 0) continue;
							next_totalsize_to_transfer += (hsm_totalsize[i] - hsm_transferedsize[i]);
						}

						if (next_totalsize_to_transfer > 0) {
							/* Not yet finished */
							SAVE_EID;
							sendrep (&rpfd, MSG_ERR, "Retrying request immediately\n");
							RESTORE_EID;
						}
					}
					goto gettape;
				}
				RETURN (SYERR);
			}

			if ((rtcpc_CheckRetry(rtcpcreqs[0]) == TRUE) && (callback_forced_noretry != 0)) {
				/* Callback wants to exit immediately in contrary to what mover's API says */
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "callback asks for exit", serrno ? sstrerror(serrno) : "");
				RESTORE_EID;
				free(stcp_start);
				RETURN (USERR);
			}

			/* Because if serrno == EVQDGNINVL rtcpc_CheckRetry() would return also TRUE */
			if ((rtcpc_CheckRetry(rtcpcreqs[0]) == TRUE)
#ifdef TMS
				&& (save_serrno != EVQDGNINVL)
#endif
				) {
				tape_list_t *tl;
				/* Rtcopy bits suggest to retry */
				CLIST_ITERATE_BEGIN(rtcpcreqs[0],tl) {
					*tl->tapereq.unit = '\0';
					if ( dont_change_srv == 0 ) *tl->tapereq.server = '\0'; 
				} CLIST_ITERATE_END(rtcpcreqs[0],tl);
				SAVE_EID;
				if (rtcpcreqs[0]->tapereq.side > 0) {
					sendrep (&rpfd, MSG_ERR, STG202, rtcpcreqs[0]->tapereq.vid, rtcpcreqs[0]->tapereq.side, "rtcpc",sstrerror(save_serrno));
				} else {
					sendrep (&rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc",sstrerror(save_serrno));
				}
				if (save_serrno == ETVBSY) {
					sendrep (&rpfd, MSG_ERR, "STG47 - Re-selecting another tape server in %d seconds\n", RETRYI);
					sleep(RETRYI);
				} else {
					sendrep (&rpfd, MSG_ERR, "STG47 - Re-selecting another tape server\n");
				}
				RESTORE_EID;
				goto reselect;
			}

			i = 0;
			for (j = istart; j <= iend; j++, i++) {
				if (error_already_processed != 0) break;
				stager_process_error(save_serrno,&(rtcpcreqs[0]->tapereq),&(rtcpcreqs[0]->file[i].filereq),NULL);
				if (error_already_processed == 0) {
					if (rtcpcreqs[0]->file[i].filereq.err.errorcode == ENOENT) {
						/* Tape info very probably inconsistency with, for ex., TMS */
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, rtcpcreqs[0]->file[i].filereq.file_path, "rtcpc",sstrerror(serrno));
						RESTORE_EID;
						Flags = 0;
						RETURN(USERR);
					}
				}
			}
			if ((Flags != TAPE_FULL) && (stagewrt_nomoreupdatetape == 0)) {
				/* If Flags is TAPE_FULL then it has already been set by the callback which called vmgr_updatetape */
				/* We remove the busy flag if any */
				Flags &= ~TAPE_BUSY;
				if ((vid[0] != '\0') && (side >= 0)) {
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=\"%s\",side=%d,BytesWriten=0,CompressionFactor=0,FilesWriten=0,Flags=%d)\n",vid,side,Flags);
					RESTORE_EID;
#endif
					if (vmgr_updatetape (vid, side, (u_signed64) 0, 0, 0, Flags) != 0) {
						SAVE_EID;
						if (side > 0) {
							sendrep (&rpfd, MSG_ERR, STG202, vid, side, "vmgr_updatetape",sstrerror (serrno));
						} else {
							sendrep (&rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",sstrerror (serrno));
						}
						RESTORE_EID;
					}
					vid[0] = '\0';
					side = -1;
				}
			}
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, "Retrying request in %d seconds\n", RETRYI);
			RESTORE_EID;
			sleep(RETRYI);
			goto gettape;
		} else {
			if (Flags != TAPE_FULL && stagewrt_nomoreupdatetape == 0) {
				/* If Flags is TAPE_FULL then it has already been set by the callback */
				if ((vid[0] != '\0') && (side >= 0)) {
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=\"%s\",side=%d,ByteWriten=0,CompressionFactor=0,filesWriten0,Flags=0)\n",vid,side);
					RESTORE_EID;
#endif
					if (vmgr_updatetape (vid, side, (u_signed64) 0, 0, 0, 0) != 0) {
						SAVE_EID;
						if (side > 0) {
							sendrep (&rpfd, MSG_ERR, STG202, vid, side, "vmgr_updatetape",sstrerror (serrno));
						} else {
							sendrep (&rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",sstrerror (serrno));
						}
						RESTORE_EID;
					}
					vid[0] = '\0';
					side = -1;
				}
			}
		}
	}
	Flags = 0;
	RETURN(0);
}

void cleanup() {
	/* Safety cleanup - executed ONLY in case of write-to-tape */
	/* Note that the callback_forced_noretry flag is set only in... read-from-tape mode */
	if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
		if ((vid[0] != '\0') && (side >= 0)) {
#ifdef STAGER_DEBUG
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, "[DEBUG-CLEANUP] Calling vmgr_updatetape(vid=\"%s\",side=%d,BytesWriten=0,CompressionFactor=0,Files0,Flags=0)\n",vid,side);
			RESTORE_EID;
#endif
			if (vmgr_updatetape (vid, side, (u_signed64) 0, 0, 0, 0) != 0) {
				SAVE_EID;
				if (side > 0) {
					sendrep (&rpfd, MSG_ERR, STG202, vid, side, "vmgr_updatetape", sstrerror (serrno));
				} else {
					sendrep (&rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape", sstrerror (serrno));
				}
				RESTORE_EID;
			}
			vid[0] = '\0';
			side = -1;
		}
	}
	if (rtcpc_kill_cleanup != 0) {
		/* rtcpc() API cleanup */
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "[DEBUG-CLEANUP] Calling rtcpc_kill()\n");
		RESTORE_EID;
#endif
		rtcpc_kill();
	}
}

void stagekilled() {
	cleanup();
	exit (REQKILD);
}

/* Conversion routine between catalog entries list to a single tapereq rtcpc() structure                */
/* Keep in mind that, several catalog entries as input or not, it required that all entries             */
/* have the same "type", e.g. they are ALL tape ('t') request, hsm stagein requests or hsm              */
/* stagewrt requests. The following routine will give unknown result if catalog entries in              */
/* input do mix this flavor.                                                                            */
/* In case of a tape ('t') request : everything has to be build in one go                               */
/* In case of a stagewrt   request : vid, side, hsm_totalsize[] and hsm_transferedsize are used[]       */
/* In case of a stagein    request : vid, side, hsm_totalsize[] and hsm_transferedsize[] are used       */

/* You can "interrogate" this routine by calling it with a NULL second argument : you                   */
/* will then get in return the number of rtcpc requests.                                                */

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
	int i;                                   /* Loop counter on rtcpc requests */
	char *castor_hsm = NULL;
	int ihsm;                                /* Correct hsm index */
	int nfile_list = 0;                      /* Number of file lists in case of hsm */
	int max_tape_fseq;

	max_tape_fseq = stage_util_maxtapefseq(lbltype);

	if (nrtcpcreqs_in == NULL || fixed_stcs == NULL || fixed_stce == NULL) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Bad arguments to build_rtcpcreq",sstrerror(serrno));
		RESTORE_EID;
		return(-1);
	}

	/* We calculate how many rtcpc requests we need. */
	*nrtcpcreqs_in = 1;

	if (rtcpcreqs_in == NULL) {
		/* We were running in the "interrogate" mode */
		return(0);
	}
	
	/* We allocate the array */
	if ((*rtcpcreqs_in = (tape_list_t **) calloc(*nrtcpcreqs_in,sizeof(tape_list_t *))) == NULL) {
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
		RESTORE_EID;
		serrno = SEINTERNAL;
		return(-1);
	}
	
	/* We allocate content of the array */
	for (i = 0; i < *nrtcpcreqs_in; i++) {
		if (((*rtcpcreqs_in)[i] = calloc(1,sizeof(tape_list_t))) == NULL) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
			RESTORE_EID;
			serrno = SEINTERNAL;
			return(-1);
		}
	}
	
	/* We initialize the mapping */
	if (hsm_map != NULL) {
		/* This is for sure an HSM request and hsm_map[] has already been malloced */
		for (i = 0; i < nbcat_ent; i++) {
			hsm_map[i] = -1;
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

	/* We fill the content of the array */
	for (stcp = stcs; stcp < stce; stcp++) {
		/* This is an hsm request */
		if ((hsm_totalsize == NULL) || (hsm_transferedsize == NULL) || (hsm_ignore == NULL)) {
			serrno = SEINTERNAL;
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "No hsm_totalsize or hsm_transferedsize array (should be != NULL)",sstrerror(serrno));
			RESTORE_EID;
			return(-1);
		}
		if ((ihsm = hsmidx_vs_ipath(stcp->ipath)) < 0) {
			if ((ihsm = hsmidx(stcp)) < 0) {
				if ((ihsm = hsmidx_is_ignored(stcp)) < 0) {
					serrno = SEINTERNAL;
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Cannot find internal hsm index",sstrerror(serrno));
					RESTORE_EID;
					return(-1);
				}
			}
		}
		if (hsm_ignore[ihsm] != 0) continue;
		if (hsm_totalsize[ihsm] == 0) {
			serrno = SEINTERNAL;
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Bad size parameter (should be > 0)",sstrerror(serrno));
			RESTORE_EID;
			return(-1);
		}
		if (hsm_transferedsize[ihsm] > hsm_totalsize[ihsm]) {
			serrno = SEINTERNAL;
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Yet transfered size claimed to be > Hsm total size...",sstrerror(serrno));
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
		(*rtcpcreqs_in)[i]->tapereq.side = side;
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
#endif /* TAPESRVR */
		if ((*rtcpcreqs_in)[i]->file == NULL) {
			/* First file for this VID */
			if (((*rtcpcreqs_in)[i]->file = (file_list_t *) calloc(1,sizeof(file_list_t))) == NULL) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
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
			int i2;

			if ((dummy = (file_list_t *) realloc((*rtcpcreqs_in)[i]->file,(nfile_list + 1) * sizeof(file_list_t))) == NULL) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, "build_rtcpcreq", "realloc",strerror(errno));
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

		/* We keep in memory the mapping diskfseq <-> ihsm */
		hsm_map[nfile_list-1] = ihsm;

		/*
		 * For HSM files - if the file is renamed, then the check on fid will NOT work.
		 * We set fid on write, but not on read, to prevent recall failure on renamed files.
		 */

		switch (stcp->status & 0xF) {
		case STAGEWRT:
		case STAGEPUT:
			if ((castor_hsm = hsmpath(stcp)) == NULL) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
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
		if ((max_tape_fseq > 0) && ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.tape_fseq > max_tape_fseq)) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, "STG02 - %s : %s : Tape sequence must be <= %d (label type \"%s\")\n", stcp->u1.m.xfile, (*rtcpcreqs_in)[i]->tapereq.vid, max_tape_fseq, lbltype);
			RESTORE_EID;
			serrno = EINVAL;
			RETURN (USERR);
		}
		/* We set the hsm_flag[ihsm] so that this entry cannot be returned twice */
		hsm_flag[ihsm] = 1;
#ifndef SKIP_FILEREQ_MAXSIZE
		/* Here we support maxsize in the rtcopy structure only if explicit STAGEWRT */
		if (ISSTAGEWRT(stcs)) {
#ifndef FULL_STAGEWRT_HSM
			/* Here we support limitation of number of bytes in write-to-tape */
			if (stcp->size > 0) {
				u_signed64 dummysize;
				dummysize = stcp->size;
				dummysize -= (u_signed64) hsm_transferedsize[ihsm];
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.maxsize = dummysize;
			}
#endif /* FULL_STAGEWRT_HSM */
		} else {
			/* Here we do NOT support limitation of number of bytes */
			if (stcp->size > 0) {
				/* But user (unfortunately) specified such a number... We overwrite it if necessasry */
				u_signed64 dummysize;
				dummysize = stcp->size;
				/* If stcp->size (in bytes) in lower than totalsize, we change maxsize value */
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
	}
	return(0);
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

int stager_hsm_callback(tapereq,filereq)
	rtcpTapeRequest_t *tapereq;
	rtcpFileRequest_t *filereq;
{
	char tmpbuf1[21];
#ifdef STAGER_DEBUG
	char tmpbuf2[21];
	char tmpbuf3[21];
#endif
#ifdef SETSEGATTRS_WITH_OWNER_UID_GID
	struct Cns_filestat statbuf;
#endif
	int compression_factor = 0;		/* times 100 */
	int stager_client_callback_i = -1;
	int stager_client_true_i = -1;
	struct stgcat_entry *stcp;
	char *castor_hsm;
	struct Cns_fileid Cnsfileid;
	int max_tape_fseq;

	if (tapereq == NULL || filereq == NULL) {
		serrno = EINVAL;
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "### Received invalid callback (tapereq=0x%lx,filereq=0x%lx)\n", (unsigned long) tapereq, (unsigned long) filereq);
		RESTORE_EID;
		fatal_callback_error = callback_error = 1;
		return(-1);
	}

	stager_hsm_log_callback(tapereq,filereq);

	stager_client_callback_i = filereq->disk_fseq - 1;

	if ((stager_client_callback_i < 0) || (stager_client_callback_i >= nbcat_ent)) {
		/* This cannot be in HSM mode */
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, filereq->file_path, "rtcpc() ClientCallback [(stager_client_callback_i < 0) || (stager_client_callback_i >= nbcat_ent)]",sstrerror(serrno));
		RESTORE_EID;
		fatal_callback_error = callback_error = 1;
		return(-1);
	}

	stager_client_true_i = hsm_map[stager_client_callback_i];

	if ((stager_client_callback_i < 0) || (stager_client_callback_i > iend) || (stager_client_true_i >= nbcat_ent) || (stager_client_true_i < 0)) {
		serrno = SEINTERNAL;
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, filereq->file_path, "rtcpc() ClientCallback [(stager_client_callback_i < 0) || (stager_client_callback_i > iend) || (stager_client_true_i >= nbcat_ent) || (stager_client_true_i < 0)]",sstrerror(serrno));
		RESTORE_EID;
		fatal_callback_error = callback_error = 1;
		return(-1);
	}

	/* stcp = &(stcp_start[stager_client_callback_i]); */
	stcp = stcs + stager_client_true_i;

	SETEID(stcp->uid,stcp->gid);

	/* Search for castor a-la-unix path in the file to stagewrt */
	if ((castor_hsm = hsmpath(stcp)) == NULL) {
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "hsmpath", sstrerror(serrno));
		RESTORE_EID;
		fatal_callback_error = callback_error = 1;
		return(-1);
	}

#ifdef STAGE_CSETPROCNAME
	if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_CASTOR,
					 sav_argv0,
					 (filereq->cprc == 0 && filereq->proc_status == RTCP_FINISHED) ? "COPIED" : "COPYING",
					 tapereq->vid,
					 tapereq->side,
					 filereq->tape_fseq,
					 castor_hsm
		) != 0) {
		stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif

	/* Successful or end of tape */
	/* - 1st line :     OK and flagged as finished (consistency check) */
	/* - 2nd line : NOT OK and flagged as failed   (consistency check) */
	/* - 3rd line :      + and EndOfVolume in write mode               */
	/* PS: in read mode, the EndOFVolume would be flagged ETEOV        */ 
	if (((filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED)) ||
		((filereq->cprc <  0) && ((filereq->err.severity & RTCP_FAILED) == RTCP_FAILED) &&
		 (
			 ((filereq->err.errorcode == ENOSPC) && (ISSTAGEWRT(stcs) ||
													 ISSTAGEPUT(stcs))) ||
			 ISSTAGEIN(stcs)
			 )
			)
		) {

#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN/WRT/PUT-CALLBACK] bytes_in = %s, bytes_out = %s, host_bytes = %s\n",
				u64tostr(filereq->bytes_in, tmpbuf1, 0),
				u64tostr(filereq->bytes_out, tmpbuf2, 0),
				u64tostr(filereq->host_bytes, tmpbuf3, 0));
		RESTORE_EID;
#endif

 		if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
			/* This is the tpwrite callback */
			int tape_flag = filereq->cprc < 0 ? TAPE_FULL : (stager_client_callback_i == iend ? 0 : TAPE_BUSY);

			if (stager_client_callback_i == iend) {
				stagewrt_nomoreupdatetape = 1;
			}
			if (filereq->bytes_out > 0) {
				compression_factor = filereq->host_bytes * 100 / filereq->bytes_out;
			} else {
				compression_factor = 0;
			}

			Flags = tape_flag;

			if (filereq->bytes_in > 0) {
				/* gcc compiler bug - fixed or forbidden register was spilled. */
				/* This may be due to a compiler bug or to impossible asm      */
				/* statements or clauses.                                      */
				u_signed64 dummyvalue;

				dummyvalue = filereq->bytes_in;
				hsm_transferedsize[stager_client_true_i] += dummyvalue;

#ifdef STAGER_DEBUG
				SAVE_EID;
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling vmgr_updatetape(vid=\"%s\",side=%d,BytesWriten=%s,CompressionFactor=%d,FilesWriten=%d,Flags=%d)\n",
						tapereq->vid,
						tapereq->side,
						u64tostr(filereq->bytes_in, tmpbuf1, 0),
						compression_factor,
						1,
						Flags);
				RESTORE_EID;
#endif
				if (vmgr_updatetape(tapereq->vid,
									tapereq->side,
									filereq->bytes_in,
									compression_factor,
									1,
									Flags
					) != 0) {
					SAVE_EID;
					if (tapereq->side > 0) {
						sendrep (&rpfd, MSG_ERR, STG202, tapereq->vid, tapereq->side, "vmgr_updatetape", sstrerror(serrno));
					} else {
						sendrep (&rpfd, MSG_ERR, STG02, tapereq->vid, "vmgr_updatetape", sstrerror(serrno));
					}
					RESTORE_EID;
					fatal_callback_error = callback_error = 1;
					return(-1);
				}

#ifdef STAGER_DEBUG
				SAVE_EID;
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] istart = %d , iend = %d, stager_client_callback_i = %d -> stager_client_true_i = %d\n",istart, iend, stager_client_callback_i, stager_client_true_i);
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_nsegments[%d] = %d\n",stager_client_true_i,hsm_nsegments[stager_client_true_i]);
				RESTORE_EID;
#endif
				if (hsm_nsegments[stager_client_true_i] == 0) {
					/* And this is the first of the segments */
					if ((hsm_segments[stager_client_true_i] = (struct Cns_segattrs *) malloc(sizeof(struct Cns_segattrs))) == NULL) {
						SAVE_EID;
						if (tapereq->side > 0) {
 							sendrep (&rpfd, MSG_ERR, STG202, tapereq->vid, tapereq->side, "malloc", strerror(errno));
						} else {
 							sendrep (&rpfd, MSG_ERR, STG02, tapereq->vid, "malloc", strerror(errno));
						}
						RESTORE_EID;
						serrno = SEINTERNAL;
						fatal_callback_error = callback_error = 1;
						return(-1);
					}
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].fsec = %d\n",stager_client_true_i,hsm_nsegments[stager_client_true_i],1);
					RESTORE_EID;
#endif
					hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].fsec = 1;
				} else {
					struct Cns_segattrs *dummy;

					if ((dummy = (struct Cns_segattrs *) realloc(hsm_segments[stager_client_true_i],
																 (hsm_nsegments[stager_client_true_i] + 1) *
																 sizeof(struct Cns_segattrs))) == NULL) {
						SAVE_EID;
						if (tapereq->side > 0) {
							sendrep (&rpfd, MSG_ERR, STG202, tapereq->vid, tapereq->side, "realloc", strerror(errno));
						} else {
							sendrep (&rpfd, MSG_ERR, STG02, tapereq->vid, "realloc", strerror(errno));
						}
						RESTORE_EID;
						serrno = SEINTERNAL;
						fatal_callback_error = callback_error = 1;
						return(-1);
					}
					hsm_segments[stager_client_true_i] = dummy;
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].fsec = %d\n",
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
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].copyno = %d\n",
						stager_client_true_i,hsm_nsegments[stager_client_true_i],0);
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].segsize = %d\n",
						stager_client_true_i,hsm_nsegments[stager_client_true_i],filereq->bytes_in);
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].compression = %d\n",
						stager_client_true_i,hsm_nsegments[stager_client_true_i],compression_factor);
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].s_status = '-'\n",
						stager_client_true_i,hsm_nsegments[stager_client_true_i]);
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].vid = \"%s\"\n",
						stager_client_true_i,hsm_nsegments[stager_client_true_i],tapereq->vid);
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].side = %d\n",
						stager_client_true_i,hsm_nsegments[stager_client_true_i],tapereq->side);
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] hsm_segments[%d][%d].fseq = %d\n",
						stager_client_true_i,hsm_nsegments[stager_client_true_i],filereq->tape_fseq);
				RESTORE_EID;
#endif
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].copyno = 0;
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].segsize = filereq->bytes_in;
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].compression = compression_factor;
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].s_status = '-';
				strcpy(hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].vid,tapereq->vid);
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].side = tapereq->side;
				memcpy(hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].blockid,filereq->blockid,sizeof(blockid_t));
				hsm_segments[stager_client_true_i][hsm_nsegments[stager_client_true_i]].fseq = filereq->tape_fseq;
				hsm_nsegments[stager_client_true_i]++;
				if (hsm_transferedsize[stager_client_true_i] >= hsm_totalsize[stager_client_true_i]) {
					/* tpwrite of this file is over */
					strcpy(Cnsfileid.server,stcp->u1.h.server);
					Cnsfileid.fileid = stcp->u1.h.fileid;
#ifdef SETSEGATTRS_WITH_OWNER_UID_GID
					/* Grab current euid/egid */
#ifndef SETSEGATTRS_WITH_OWNER_UID_GID_FROM_CATALOG
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling Cns_statx(path=\"%s\",&Cnsfileid={server=\"%s\",fileid=%s},&statbuf)\n",
			                castor_hsm,
			                Cnsfileid.server,
			                u64tostr(Cnsfileid.fileid, tmpbuf1, 0));
					RESTORE_EID;
#endif
					SAVE_EID;
					if (Cns_statx (castor_hsm, &Cnsfileid, &statbuf) < 0) {
						int save_serrno = serrno;
						sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx", sstrerror (save_serrno));
						RESTORE_EID;
						if (((api_flags & STAGE_HSM_ENOENT_OK) == STAGE_HSM_ENOENT_OK) && (save_serrno == ENOENT)) {
							/* This is OK for have an ENOENT here */
							sendrep (&rpfd, MSG_ERR, STG175,
									 castor_hsm,
									 u64tostr((u_signed64) Cnsfileid.fileid, tmpbuf1, 0),
									 Cnsfileid.server,
									 "skipped : STAGE_HSM_ENOENT_OK in action");
						} else {
							callback_error = 1;
							if (use_subreqid) {
								SET_HSM_IGNORE(stager_client_true_i,serrno);
							}
							return(-1);
						}
					}
#else
					statbuf.uid = stcp->uid;
					statbuf.gid = stcp->gid;
#endif
#ifdef STAGER_DEBUG
					sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling Cns_setsegattrs(\"%s\",&Cnsfileid={server=\"%s\",fileid=%s},nbseg=%d,segments)\n",
							castor_hsm,
							Cnsfileid.server,
							u64tostr(Cnsfileid.fileid,tmpbuf1,0),
							hsm_nsegments[stager_client_true_i]);
#endif
					/* Move to this user uid/gid */
					SETEID(statbuf.uid,statbuf.gid);
#else /* SETSEGATTRS_WITH_OWNER_UID_GID */
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling Cns_setsegattrs(\"%s\",&Cnsfileid={server=\"%s\",fileid=%s},nbseg=%d,segments)\n",
							castor_hsm,
							Cnsfileid.server,
							u64tostr(Cnsfileid.fileid,tmpbuf1,0),
							hsm_nsegments[stager_client_true_i]);
					RESTORE_EID;
#endif
#endif /* SETSEGATTRS_WITH_OWNER_UID_GID */
					if (Cns_setsegattrs(castor_hsm,
										&Cnsfileid,
										hsm_nsegments[stager_client_true_i],
										hsm_segments[stager_client_true_i]) < 0) {
						int save_serrno = serrno;
#ifdef SETSEGATTRS_WITH_OWNER_UID_GID
						SETEID(start_passwd.pw_gid,start_passwd.pw_uid);
#else
						SAVE_EID;
#endif
						sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setsegattrs", sstrerror(serrno));
						RESTORE_EID;
						if (((api_flags & STAGE_HSM_ENOENT_OK) == STAGE_HSM_ENOENT_OK) && (save_serrno == ENOENT)) {
							/* This is OK for have an ENOENT here */
							sendrep (&rpfd, MSG_ERR, STG175,
									 castor_hsm,
									 u64tostr((u_signed64) Cnsfileid.fileid, tmpbuf1, 0),
									 Cnsfileid.server,
									 "skipped : STAGE_HSM_ENOENT_OK in action");
						} else {
							callback_error = 1;
							if (use_subreqid) {
								SET_HSM_IGNORE(stager_client_true_i,serrno);
							}
							return(-1);
						}
					}
#ifdef SETSEGATTRS_WITH_OWNER_UID_GID
					/* Go back to uid/gid before our change */
					RESTORE_EID;
#endif
					/* If we reach this part of the code then we know undoubtly */
					/* that the transfer of this HSM file IS ok                 */
					hsm_status[stager_client_true_i] = 1;
				}
				max_tape_fseq = stage_util_maxtapefseq(lbltype);
				if ((max_tape_fseq > 0) && (filereq->tape_fseq >= max_tape_fseq)) {
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, "STG02 - %s : Reached max_tape_fseq=%d (label type \"%s\")\n", tapereq->vid, max_tape_fseq, lbltype);
					sendrep (&rpfd, MSG_ERR, "STG02 - %s : Flagging tape RDONLY\n", tapereq->vid);
					RESTORE_EID;
					if (vmgr_updatetape(tapereq->vid,
										tapereq->side,
										(u_signed64) 0,
										0,
										0,
										TAPE_RDONLY
						) != 0) {
						SAVE_EID;
						if (tapereq->side > 0) {
							sendrep (&rpfd, MSG_ERR, STG202, tapereq->vid, tapereq->side, "vmgr_updatetape", sstrerror(serrno));
						} else {
							sendrep (&rpfd, MSG_ERR, STG02, tapereq->vid, "vmgr_updatetape", sstrerror(serrno));
						}
						RESTORE_EID;
					}
				}
			} else {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG123,
						 tapereq->vid,
						 filereq->tape_fseq,
						 filereq->cprc < 0 ? "full" : (stager_client_callback_i == iend ? "free" : "busy")
					);
				RESTORE_EID;

#ifdef STAGER_DEBUG
				SAVE_EID;
				sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling vmgr_updatetape(vid=\"%s\",side=%d,BytesWriten=0,CompressionFactor=0,FilesWriten=0,Flags=%d)\n",
						tapereq->vid,
						tapereq->side,
						Flags);
				RESTORE_EID;
#endif
				if (vmgr_updatetape(tapereq->vid,
									tapereq->side,
									(u_signed64) 0,
									0,
									0,
									Flags
					) != 0) {
					SAVE_EID;
					if (tapereq->side > 0) {
						sendrep (&rpfd, MSG_ERR, STG202, tapereq->vid, tapereq->side, "vmgr_updatetape", sstrerror(serrno));
					} else {
						sendrep (&rpfd, MSG_ERR, STG02, tapereq->vid, "vmgr_updatetape", sstrerror(serrno));
					}
					RESTORE_EID;
					fatal_callback_error = callback_error = 1;
					return(-1);
				}
			}

		} else {
			if (filereq->cprc != 0) {
				/* STAGEIN callback with cprc error */
				stager_process_error(serrno,tapereq,filereq,castor_hsm);
			} else {
				/* gcc compiler bug - fixed or forbidden register was spilled. */
				/* This may be due to a compiler bug or to impossible asm      */
				/* statements or clauses.                                      */
				u_signed64 dummyvalue;

				dummyvalue = filereq->bytes_out;
				hsm_transferedsize[stager_client_true_i] += dummyvalue;

				/* This is the tpread callback */
				if (hsm_transferedsize[stager_client_true_i] >= hsm_totalsize[stager_client_true_i]) {
					strcpy(Cnsfileid.server,stcp->u1.h.server);
					Cnsfileid.fileid = stcp->u1.h.fileid;
#ifdef STAGER_DEBUG
					SAVE_EID;
					sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN/CALLBACK] Calling Cns_setatime(%s,&Cnsfileid={server=\"%s\",fileid=%s})\n",
							castor_hsm,
							Cnsfileid.server,
							u64tostr(Cnsfileid.fileid,tmpbuf1,0));
					RESTORE_EID;
#endif
					if (Cns_setatime (castor_hsm, &Cnsfileid) < 0) {
						int save_serrno = serrno;
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setatime", sstrerror(serrno));
						RESTORE_EID;
						if (((api_flags & STAGE_HSM_ENOENT_OK) == STAGE_HSM_ENOENT_OK) && (save_serrno == ENOENT)) {
							/* This is OK for have an ENOENT here */
							sendrep (&rpfd, MSG_ERR, STG175,
									 castor_hsm,
									 u64tostr((u_signed64) Cnsfileid.fileid, tmpbuf1, 0),
									 Cnsfileid.server,
									 "skipped : STAGE_HSM_ENOENT_OK in action");
						} else {
							callback_error = 1;
							return(-1);
						}
					}
					/* If we reach this part of the code then we know undoubly */
					/* that the transfer of this HSM file IS ok                */
					hsm_status[stager_client_true_i] = 1;
				}
			}
		}
	} else if (filereq->cprc != 0) {
		stager_process_error(serrno,tapereq,filereq,castor_hsm);
		if ((use_subreqid != 0) && ((filereq->err.severity & RTCP_FAILED) == RTCP_FAILED) && (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs))) {
			/* In the specific case of STAGEWRT or STAGEPUT we can exceptionnaly force a callback error */
			/* that will prevent RTCOPY to send another callback to the stgdaemon itself, allowing us to */
			/* continue with another rtcpc() request */
			callback_error = 1;
			if (error_already_processed == 0) {
				/* We did not recognized the error as a tape error */
				/* so we say that we will not retry transfer of this file */
				SET_HSM_IGNORE(stager_client_true_i,filereq->err.errorcode);
			}
			serrno = filereq->err.errorcode; /* Can be unset at this step */
			return(-1);
		}
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


void stager_hsm_log_callback(tapereq,filereq)
	rtcpTapeRequest_t *tapereq;
	rtcpFileRequest_t *filereq;
{
	char tmpbuf1[21];
	char tmpbuf2[21];
	char tmpbuf3[21];

	SAVE_EID;
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG-CALLBACK] VID/SIDE.FSEQ=%s/%d.%d, File No %d (%s), filereq->cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s\n",
			tapereq->vid,
			tapereq->side,
			(int) filereq->tape_fseq,
			(int) filereq->disk_fseq,
			filereq->file_path,
			(int) filereq->cprc,
			u64tostr((u_signed64) filereq->bytes_in, tmpbuf1, 0),
			u64tostr((u_signed64) filereq->bytes_out, tmpbuf2, 0),
			u64tostr((u_signed64) filereq->host_bytes, tmpbuf3, 0));
	sendrep(&rpfd, MSG_ERR, "[DEBUG-CALLBACK] VID/SIDE.FSEQ=%s/%d.%d, filereq->proc_status=%d (0x%lx), filereq->err.severity=%d (0x%lx), filereq->err.errorcode=%d (0x%lx), tapereq->err.severity=%d (0x%lx), tapereq->err.errorcode=%d (0x%lx)\n",
			tapereq->vid,
			tapereq->side,
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
	sendrep(&rpfd, MSG_ERR, "[DEBUG-CALLBACK] VID/SIDE.FSEQ=%s/%d.%d, errno=%d (%s), serrno=%d (%s)\n",
			tapereq->vid,
			tapereq->side,
			(int) filereq->tape_fseq,
			errno,
			strerror(errno),
			serrno,
			sstrerror(serrno));
#else
	/* We log every callback for log-survey and administration reason */
	if (filereq->cprc == 0) {
		stglogit(func, "%s/%d.%d, File No %d (%s), cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s\n",
				 tapereq->vid,
				 tapereq->side,
				 (int) filereq->tape_fseq,
				 (int) filereq->disk_fseq,
				 filereq->file_path,
				 (int) filereq->cprc,
				 u64tostr((u_signed64) filereq->bytes_in, tmpbuf1, 0),
				 u64tostr((u_signed64) filereq->bytes_out, tmpbuf2, 0),
				 u64tostr((u_signed64) filereq->host_bytes, tmpbuf3, 0));
		if (ismig_log) {
			sendrep(&rpfd, MSG_OUT, "%s/%d.%d, File No %d (%s), cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s\n",
					tapereq->vid,
					tapereq->side,
					(int) filereq->tape_fseq,
					(int) filereq->disk_fseq,
					filereq->file_path,
					(int) filereq->cprc,
					u64tostr((u_signed64) filereq->bytes_in, tmpbuf1, 0),
					u64tostr((u_signed64) filereq->bytes_out, tmpbuf2, 0),
					u64tostr((u_signed64) filereq->host_bytes, tmpbuf3, 0));
		}
	} else {
		stglogit(func, "%s/%d.%d, File No %d (%s), cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s, proc_status=%d (0x%lx), filereq->err.severity=%d (0x%lx), filereq->err.errorcode=%d (0x%lx), tapereq->err.severity=%d (0x%lx), tapereq->err.errorcode=%d (0x%lx), errno=%d (%s), serrno=%d (%s)\n",
				 tapereq->vid,
				 tapereq->side,
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
		if (ismig_log) {
			sendrep(&rpfd, MSG_OUT, "%s/%d.%d, File No %d (%s), cprc=%d, bytes_in=%s, bytes_out=%s, host_bytes=%s, proc_status=%d (0x%lx), filereq->err.severity=%d (0x%lx), filereq->err.errorcode=%d (0x%lx), tapereq->err.severity=%d (0x%lx), tapereq->err.errorcode=%d (0x%lx), errno=%d (%s), serrno=%d (%s)\n",
					tapereq->vid,
					tapereq->side,
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
	}
#endif
	RESTORE_EID;
}

void stager_process_error(save_serrno,tapereq,filereq,castor_hsm)
	int save_serrno;
	rtcpTapeRequest_t *tapereq;
	rtcpFileRequest_t *filereq;
	char *castor_hsm;
{
	char *THIS_DISABLED = "DISABLED";
	char *THIS_EXPORTED = "EXPORTED";
	char *THIS_TAPE_RDONLY = "TAPE_RDONLY";
	char *THIS_ARCHIVED = "ARCHIVED";
	int this_flag = 0;
	int is_this_flag = 0;
	char *this_string = NULL;
	int segments_to_soft_delete = 0;
	
	/* We require at least tapereq to not be NULL */
	if (tapereq == NULL) return;

	switch (save_serrno) {
	case ETWLBL:                          /* Wrong label type */
	case ETWVSN:                          /* Wrong vsn */
	case ETHELD:                          /* Volume held (TMS) */
	case ETVUNKN:                         /* Volume unknown or absent (TMS) */
	case ETOPAB:                          /* Operator cancel */
	case ETNOSNS:                         /* No sense */
#ifdef TMS
	case EVQDGNINVL:                      /* Request for non-existing DGN */
#endif
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
	case ETBLANK:                         /* Blank tape */
		/* In this case, and only if it is a recall, we decide to delete the copy number that correspond to */
		/* the segment in the Name Server, that itself is refering this tape. */
		/* This happens with 'residus' of a buggy version of stager 1.3.1.x that was putting a */
		/* segment in the Name Server when the write-to-tape failed with an EOT, even not enough space */
		/* to write the data block, sometimes not enough even to finish the header */
		/* and for sure not enough space to write anything relevant */

		/* We could do the API equivalent of nslisttape here */
		/* But then we don't really know which name server to contact !? */
		/* On the other hand if we have castor_hsm variable != NULL */
		/* We know which server to contact (castor name are self explanatory) */

		/* And we need to know exactly which tape fseq did fail. This information is in the filereq */
		if ((castor_hsm != NULL) && (filereq != NULL)) {
			segments_to_soft_delete = 1;
			is_this_flag = 1;
		}
		break;
	default:
		/* what shall we do here ? */
		break;
	}
	if (is_this_flag == 0) {
		/* Beware in the following if: ETBLANK is to be considered for action ONLY if it is a recall with a valid HSM name */
		if (((filereq != NULL) && ((filereq->err.errorcode == ETPARIT) || (filereq->err.errorcode == ETUNREC) || (filereq->err.errorcode == ETLBL) || ((castor_hsm != NULL) && (! ((ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)))) && (filereq->err.errorcode == ETBLANK))))
			||
			((tapereq->err.errorcode == ETPARIT) || (tapereq->err.errorcode == ETUNREC) || (tapereq->err.errorcode == ETLBL) || ((castor_hsm != NULL) && (filereq != NULL) && (! ((ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)))) && (tapereq->err.errorcode == ETBLANK)))
			) {
			/* save_serrno was not of any help but we can anyway detect there was something serious with this tape */
			SAVE_EID;
 			if (ISSTAGEWRT(stcs) || ISSTAGEPUT(stcs)) {
				/* Will not be executed if the match is with ETBLANK */
				if (filereq != NULL) {
					sendrep(&rpfd, MSG_ERR, "### [%s] For tape/side.fseq %s/%d.%d, global error code %d not enough, but tapereq->err.errorcode=%d (0x%lx) and filereq->err.errorcode=%d (0x%lx) - Safety action is to try to flag the tape as read-only\n",
							(castor_hsm != NULL) ? castor_hsm : "...",
							tapereq->vid,
							tapereq->side,
							filereq->tape_fseq,
							save_serrno,
							(int) tapereq->err.errorcode,
							(unsigned long) tapereq->err.errorcode,
							(int) filereq->err.errorcode,
							(unsigned long) filereq->err.errorcode);
				} else {
					sendrep(&rpfd, MSG_ERR, "### [%s] For tape/side %s/%d, global error code %d not enough, but tapereq->err.errorcode=%d (0x%lx) - Safety action is to try to flag the tape as read-only\n",
							(castor_hsm != NULL) ? castor_hsm : "...",
							tapereq->vid,
							tapereq->side,
							save_serrno,
							(int) tapereq->err.errorcode,
							(unsigned long) tapereq->err.errorcode);
				}
				this_flag = TAPE_RDONLY;
				this_string = THIS_TAPE_RDONLY;
			} else {
				/* Can be be executed if the match is with ETBLANK */
				if (filereq != NULL) {
					if ((castor_hsm != NULL) && ((filereq->err.errorcode == ETBLANK) || (tapereq->err.errorcode == ETBLANK))) {
						segments_to_soft_delete = 1;
					}
					sendrep(&rpfd, MSG_ERR, "### [%s] For tape/side.fseq %s/%d.%d, global error code %d not enough, but tapereq->err.errorcode=%d (0x%lx) and filereq->err.errorcode=%d (0x%lx) - Safety action is to try to %s\n",
							(castor_hsm != NULL) ? castor_hsm : "...",
							tapereq->vid,
							tapereq->side,
							filereq->tape_fseq,
							save_serrno,
							(int) tapereq->err.errorcode,
							(unsigned long) tapereq->err.errorcode,
							(int) filereq->err.errorcode,
							(unsigned long) filereq->err.errorcode,
							segments_to_soft_delete ? "soft-delete the segments" : "disable the tape"
						);
				} else {
					sendrep(&rpfd, MSG_ERR, "### [%s] For tape/side %s/%d, global error code %d not enough, but tapereq->err.errorcode=%d (0x%lx) - Safety action is to try to disable the tape\n",
							(castor_hsm != NULL) ? castor_hsm : "...",
							tapereq->vid,
							tapereq->side,
							save_serrno,
							(int) tapereq->err.errorcode,
							(unsigned long) tapereq->err.errorcode
							);
				}
				if (! segments_to_soft_delete) {
					/* Except segments to soft delete, this is an action on VMGR */
					this_flag = DISABLED;
					this_string = THIS_DISABLED;
				}
			}
			RESTORE_EID;
			is_this_flag = 1;
		}
	}
	if (is_this_flag != 0) {
		/* Two cases: segments to soft-delete (action in NS) or not (action in VMGR) */
		if ((castor_hsm != NULL) && (filereq != NULL) && (segments_to_soft_delete != 0)) {
			/* We need find again which copyno/fsec exactly refers to this tape/side.fseq */
			/* This must be in the list of segments that are known in advance: */
			if (hsm_segments != NULL && hsm_nsegments != NULL) {
				int i;
				for (i = 0; i < nbcat_ent; i++) {
					if (hsm_segments[i] != NULL) {
						int isegments, jsegments;
						int nsegments_soft_delete = 0;
						int jsegments_soft_delete_start = 0;
						struct Cns_segattrs *hsm_segments_soft_delete = NULL;

						for (isegments = 0; isegments < hsm_nsegments[i]; isegments++) {
							if ((strcmp(hsm_segments[i][isegments].vid,tapereq->vid) == 0) && /* Same VID */
								hsm_segments[i][isegments].side == tapereq->side &&           /* Same side */
								hsm_segments[i][isegments].fseq == filereq->tape_fseq         /* Same fseq on tape */
								) {
								/* So we found the offending triplet [vid,side,fseq] in this segment */
								
								/* By definition all segments are grouped by copy number */
								/* So we will re-use the yet existing hsm_segments[i] in memory, and make sure */
								/* that all segments of copy number hsm_segments[i][isegments].copyno are soft */
								/* deleted */
								for (jsegments = 0; jsegments < hsm_nsegments[i]; jsegments++) {
									if (hsm_segments[i][isegments].copyno == hsm_segments[i][jsegments].copyno) {
										/* Found the copy number. We initialize start address if needed */
										if (hsm_segments_soft_delete == NULL) {
											hsm_segments_soft_delete = &(hsm_segments[i][isegments]);
											jsegments_soft_delete_start = jsegments;
										}
										/* We explicitely soft-delete the segment */
										hsm_segments[i][isegments].s_status = 'D';										
										/* And we increment the number of segments to delete */
										nsegments_soft_delete++;
									}
								}
								if (nsegments_soft_delete > 0) {
									struct Cns_fileid Cnsfileid;
									char tmpbuf[21];

									/* And we do it in the name server */
									SAVE_EID;
									sendrep(&rpfd, MSG_ERR, "### [%s] Doing a soft-delete of %d segment%s in the Name Server:\n", castor_hsm, nsegments_soft_delete, (nsegments_soft_delete > 1) ? "s" : "");
									for (jsegments = jsegments_soft_delete_start; jsegments < jsegments_soft_delete_start + nsegments_soft_delete; jsegments++) {
										sendrep(&rpfd, MSG_ERR, "### [%s] ... Copy Number %d, tape/side.fseq = %s/%d.%d\n", castor_hsm, hsm_segments[i][jsegments].copyno, hsm_segments[i][jsegments].vid, hsm_segments[i][jsegments].side, hsm_segments[i][jsegments].fseq);
									}
									sendrep(&rpfd, MSG_ERR, "### [%s] ... Modifying segments (invariants of file are %s@%s)\n", castor_hsm, u64tostr((u_signed64) stcs[i].u1.h.fileid, tmpbuf, 0), stcs[i].u1.h.server);
									RESTORE_EID;
									strcpy(Cnsfileid.server,stcs[i].u1.h.server);
									Cnsfileid.fileid = stcs[i].u1.h.fileid;
									if (Cns_setsegattrs(castor_hsm,
														&Cnsfileid,
														nsegments_soft_delete,
														hsm_segments_soft_delete) < 0) {
										SAVE_EID;
										sendrep (&rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setsegattrs", sstrerror(serrno));
										RESTORE_EID;
									}
								} else {
									SAVE_EID;
									sendrep(&rpfd, MSG_ERR, "### [%s] Could not find offending segment to soft-delete for tape/side.fseq = %s/%d.%d !\n", (castor_hsm != NULL) ? castor_hsm : "...", tapereq->vid, tapereq->side, filereq->tape_fseq);
									sendrep(&rpfd, MSG_ERR, "### [%s] Giving up when trying to repair segments in the Name Server\n", (castor_hsm != NULL) ? castor_hsm : "...");
									RESTORE_EID;
								}
							}
						}
					}
				}
			}
		} else {
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "### [%s] Trying to set tape/side %s/%d to %s state\n", (castor_hsm != NULL) ? castor_hsm : "...", tapereq->vid, tapereq->side, this_string);
			if (vmgr_updatetape(tapereq->vid, tapereq->side, 0, 0, 0, this_flag) != 0) {
				if (tapereq->side > 0) {
					sendrep (&rpfd, MSG_ERR, STG202, tapereq->vid, tapereq->side, "vmgr_updatetape", sstrerror(serrno));
				} else {
					sendrep (&rpfd, MSG_ERR, STG02, tapereq->vid, "vmgr_updatetape", sstrerror(serrno));
				}
				RESTORE_EID;
				fatal_callback_error = callback_error = 1;
				return;
			}
			RESTORE_EID;
		}
		/* By resetting this variable we makes sure that our decision is not overwriten by the cleanup() */
		vid[0] = '\0';
		side = -1;
		/* We say to the remaining process that we already took action for error processing */
		error_already_processed = 1;
	}
	if (this_flag == DISABLED) {
		/* We have decided to DISABLE a tape - in such a case can NEVER admit to do a retry even if the mover says so */
		/* - Apply to recall mode */
		if (ISSTAGEIN(stcs)) {
			callback_forced_noretry = 1;
		}
	}
}

int stage_copyfile(inpfile,outfile,st_mode,subreqid,totalsize,size)
	char *inpfile;
	char *outfile;
	mode_t st_mode;
	int subreqid;
	u_signed64 totalsize;           /* How many to transfer */
	u_signed64 *size;               /* How many transferred */
{
	char buf[256];
	char command[2*(CA_MAXHOSTNAMELEN+1+MAXPATH)+CA_MAXHOSTNAMELEN+1+196];
	char *filename;
	char *host;
	FILE *rf;
	int c;
	int fd1, fd2;
	int v;
	char *ifce;
	char ifce1[8] ;
	char ifce2[8] ;
	time_t starttime, endtime;
	EXTERN_C char DLL_DECL *getifnam _PROTO(());
	int l1 = -1;
	char stageid[CA_MAXSTGRIDLEN+1];
	int rc;
	/*
	 * @@@ TO BE MOVED TO cpdskdsk.sh @@@
	 */

	sprintf (stageid, "%d.%d@%s", reqid, key, hostname);

	if (! *outfile) {
		/* Deferred allocation (Aflag) */
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "Calling stage_updc_tppos(stageid=%s,...)\n",stageid);
		RESTORE_EID;
#endif
		if (stage_updc_tppos (
			stageid,                 /* Stage ID      */
			use_subreqid != 0 ? subreqid : -1, /* subreqid      */
			-1,                      /* Copy rc       */
			-1,                      /* Blocksize     */
			NULL,                    /* drive         */
			NULL,                    /* fid           */
			0,                       /* fseq          */
			0,                       /* lrecl         */
			NULL,                    /* recfm         */
			outfile                  /* path          */
			) != 0) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, inpfile, "stage_updc_tppos", sstrerror (serrno));
			RESTORE_EID;
			return(serrno);
		}
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "[DEBUG] Internal path setted to %s\n", outfile);
		RESTORE_EID;
#endif
	}

	(void) rfio_parseln (inpfile, &host, &filename, NORDLINKS);

	if (host != NULL) {
		/* Source file is remote: we will do copy from there */
		c = RFIO_NONET;
		rfiosetopt (RFIO_NETOPT, &c, 4);
		sprintf (command, "%s:%s/cpdskdsk", host, BIN);
		sprintf (command+strlen(command), " -Z %d.%d@%s", reqid, key, hostname);

		if (totalsize > 0) {
			char tmpbuf[21];
			u64tostr(totalsize, tmpbuf, 0);
			sprintf (command+strlen(command), " -s %s", tmpbuf);
		}
		sprintf (command+strlen(command), " '%s'", inpfile);
		sprintf (command+strlen(command), " '%s'", outfile);
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "[DEBUG-FILECOPY] Execing %s\n",command);
		RESTORE_EID;
#else
		SAVE_EID;
		stglogit (func, "execing command : %s\n", command);
		RESTORE_EID;
#endif
		
		/* Note: look just before call to stage_copyfile(), SETID() is called there */
		PRE_RFIO;
		rf = rfio_popen (command, "r");
		if (rf == NULL) {
			/* This way we will sure that it will be logged... */
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "STG02 - %s : %s\n", command, rfio_serror());
			RESTORE_EID;
			return(rfio_serrno());
		}
		
		while (1) {
			PRE_RFIO;
			if ((c = rfio_pread (buf, 1, sizeof(buf)-1, rf)) <= 0) break;
			buf[c] = '\0';
			SAVE_EID;
			sendrep (&rpfd, RTCOPY_OUT, "%s", buf);
			RESTORE_EID;
		}
		
		PRE_RFIO;
		c = rfio_pclose (rf);
		if (c != 0) {
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "STG02 - %s : error reported at %s time : status 0x%x (%s)\n", "filecopy", "rfio_pclose", c, sstrerror((c >> 8) & 0xFF));
			RESTORE_EID;
			return((c >> 8) & 0xFF); /* Status is in higher byte */
		}
		return(c);
	} else {
		/* Source file is local : we will do ourself the copy */
		/* because rfio_popen() on localhost would translate to */
		/* popen() and we will LOOSE the euid, e.g. popen() would */
		/* have executed something under root (we do not want to */
		/* play with setuid and/or options of the shell) */

		/* Active V3 RFIO protocol */
		v = RFIO_STREAM;
		rfiosetopt(RFIO_READOPT,&v,4); 
		
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "[DEBUG] rfio_open64(\"%s\",O_RDONLY,0644)\n", inpfile);
		RESTORE_EID;
#endif
		
		PRE_RFIO;
		fd1 = rfio_open64(inpfile,O_RDONLY ,0644);
		
		if (fd1 < 0) {
			if (serrno) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, inpfile, "rfio_open64", rfio_serror());
				RESTORE_EID;
				rc = serrno;
			} else {
				switch (rfio_errno) {
				case EACCES:
				case EISDIR:
				case ENOENT:
				case EPERM :
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, inpfile, "rfio_open64", rfio_serror());
					RESTORE_EID;
					rc = rfio_errno;
					break ;
				case 0:
					switch(errno) {
					case EACCES:
					case EISDIR:
					case ENOENT:
					case EPERM :
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, inpfile, "rfio_open64", rfio_serror());
						RESTORE_EID;
						rc = errno;
						break;
					default:
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, inpfile, "rfio_open64", rfio_serror());
						RESTORE_EID;
						rc = SYERR;
					}
					break;
				default:
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, inpfile, "rfio_open64", rfio_serror());
					RESTORE_EID;
					rc = SYERR;
				}
			}
			return(rc);
		}

		if ((ifce=getifnam(fd1)) == NULL) {
			strcpy(ifce1,"local");
		} else {
			strncpy(ifce1,ifce,7);
			ifce1[7] = '\0';
		}

		/* Active V3 RFIO protocol */
		v = RFIO_STREAM;
		rfiosetopt(RFIO_READOPT,&v,4); 

#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "[DEBUG] rfio_open64(\"%s\",O_WRONLY|O_CREAT|O_TRUNC,st_mode & 0777)\n", outfile);
		RESTORE_EID;
#endif

		PRE_RFIO;
		fd2 = rfio_open64(outfile, O_WRONLY|O_CREAT|O_TRUNC, st_mode & 0777);
		if (fd2 < 0) {
			if (serrno) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_open64", rfio_serror());
				RESTORE_EID;
				rc = serrno;
			} else {
				switch (rfio_errno) {
				case EACCES:
				case EISDIR:
				case ENOENT:
				case EPERM :
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_open64", rfio_serror());
					RESTORE_EID;
					rc = rfio_errno;
					break;
				case 0:
					switch(errno) {
					case EACCES:
					case EISDIR:
					case ENOENT:
					case EPERM :
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_open64", rfio_serror());
						RESTORE_EID;
						rc = errno;
						break;
					default:
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_open64", rfio_serror());
						RESTORE_EID;
						rc = SYERR;
					}
					break;
				default:
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_open64", rfio_serror());
					RESTORE_EID;
					rc = SYERR;
				}
			}
			return(rc);
		}

		if ((ifce=getifnam(fd2)) == NULL) {
			strcpy(ifce2,"local");
		} else {
			strncpy(ifce2,ifce,7) ;
			ifce2[7] = '\0';
		}

		time(&starttime);
		rc = copyfile(fd1, fd2, inpfile, outfile, totalsize, size);
		time(&endtime);
		if (rc == 0) {
			if (*size > 0)  {
				char tmpbuf1[21];

				l1 = (int) (endtime-starttime);
				if ( l1 > 0) {
					SAVE_EID;
					sendrep (&rpfd, RTCOPY_OUT, "%s bytes in %d seconds through %s (in) and %s (out) (%d KB/sec)\n", u64tostr(*size, tmpbuf1, 0), (int) (endtime-starttime), ifce1, ifce2, (int) ((*size)/(1024*l1)));
					RESTORE_EID;
				} else {
					SAVE_EID;
					sendrep (&rpfd, RTCOPY_OUT, "%s bytes in %d seconds through %s (in) and %s (out)\n", u64tostr(*size, tmpbuf1, 0), (int) (endtime-starttime), ifce1, ifce2);
					RESTORE_EID;
					l1 = 0;
				}
				rc = (totalsize == *size ? 0 : SYERR);
			} else {
				SAVE_EID;
				sendrep (&rpfd, RTCOPY_OUT, "%d bytes transferred !!\n",(int) 0);
				RESTORE_EID;
				rc = (totalsize == 0 ? 0 : SYERR);
			}
		}
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "Calling stage_updc_filcp(stageid=%s,...,rc=%d,...)\n",stageid,rc);
		RESTORE_EID;
#endif
		if (stage_updc_filcp (
				stageid,                 /* Stage ID      */
				use_subreqid != 0 ? subreqid : -1, /* subreqid      */
				rc,                      /* Copy rc       */
				NULL,                    /* Interface     */
				rc == 0 ? (*size) : 0,   /* Size          */
				0,                       /* Waiting time  */
				rc == 0 ? l1 : 0,        /* Transfer time */
				0,                       /* block size    */
				NULL,                    /* drive         */
				NULL,                    /* fid           */
				0,                       /* fseq          */
				0,                       /* lrecl         */
				NULL,                    /* recfm         */
				NULL                     /* path          */
				) != 0) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, outfile, "stage_updc_filcp", sstrerror (serrno));
			RESTORE_EID;
			return(serrno);
		}
		return(rc);
	}
}

#ifndef TRANSFER_UNIT
#define TRANSFER_UNIT   131072 
#endif

int copyfile(fd1, fd2, inpfile, outfile, totalsize, effsize)
	int fd1;
	int fd2;
	char *inpfile;
	char *outfile;
	u_signed64 totalsize;
	u_signed64 *effsize;
{
	int n, m = 0, mode;
	struct stat64 sbuf;
	char *p;
	int bufsize = TRANSFER_UNIT;
	char *cpbuf;
	extern char *getenv();		/* External declaration */
	u_signed64 total_bytes = 0;
	EXTERN_C char DLL_DECL *getconfent _PROTO(());

	*effsize = 0;

	if ((p = getenv("RFCPBUFSIZ")) == NULL) {
		if ((p = getconfent("RFCP","BUFFERSIZE",0)) == NULL) {
			bufsize=TRANSFER_UNIT;
		} else {
			bufsize=atoi(p);
		}
	} else {
		bufsize=atoi(p);
	}
	if (bufsize<=0) bufsize=TRANSFER_UNIT;

	if ( ( cpbuf = malloc(bufsize) ) == NULL ) {
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, "", "malloc", strerror(errno));
		RESTORE_EID;
		return(SYERR);
	}

	do {
		/* By definition totalsize is >= 0 */
		if ((totalsize - total_bytes) < bufsize)
			bufsize = totalsize - total_bytes;
		PRE_RFIO;
		n = rfio_read(fd1, cpbuf, bufsize);
		if (n > 0) {
			int count = 0;
      
			total_bytes += n;
			*effsize += n;
			PRE_RFIO;
			while (count != n && (m = rfio_write(fd2, cpbuf+count, n-count)) > 0) {
				count += m;
			}
			if (m < 0) {
				int save_error = rfio_serrno();
				/* Write failed.  Don't keep truncated regular file. */
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_write", rfio_serror());
				RESTORE_EID;
				PRE_RFIO;
				if (rfio_close(fd2) != 0) {
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_close", rfio_serror());
					RESTORE_EID;
				} else {
					PRE_RFIO;
					if (rfio_stat64(outfile, &sbuf) != 0) {
						SAVE_EID;
						sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_stat64", rfio_serror());
						RESTORE_EID;
					} else {
						mode = sbuf.st_mode & S_IFMT;
						if (mode == S_IFREG) {
							PRE_RFIO;
							if (rfio_unlink(outfile) != 0) {
								SAVE_EID;
								sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_unlink", rfio_serror());
								RESTORE_EID;
							}
						}
					}
				}
				free(cpbuf);
				return(save_error);
			}
		} else {
			if (n < 0) {
				SAVE_EID;
				sendrep (&rpfd, MSG_ERR, STG02, inpfile, "rfio_read", rfio_serror());
				RESTORE_EID;
				break;
			}
		}
	} while ((total_bytes != totalsize) && (n > 0));

    
	PRE_RFIO;
	if (rfio_close(fd1) < 0) {
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, inpfile, "rfio_close", rfio_serror());
		RESTORE_EID;
		free(cpbuf);
		return(rfio_serrno());
	}

	PRE_RFIO;
	if (rfio_close(fd2) < 0) {
		int save_error = rfio_serrno();
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_close", rfio_serror());
		RESTORE_EID;
		PRE_RFIO;
		if (rfio_stat64(outfile, &sbuf) != 0) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_stat64", rfio_serror());
			RESTORE_EID;
		} else {
			mode = sbuf.st_mode & S_IFMT;
			if (mode == S_IFREG) {
				PRE_RFIO;
				if (rfio_unlink(outfile) != 0) {
					SAVE_EID;
					sendrep (&rpfd, MSG_ERR, STG02, outfile, "rfio_unlink", rfio_serror());
					RESTORE_EID;
				}
			}
		}
		free(cpbuf);
		return(save_error);
	}

	free(cpbuf);
	return(0);
}

void stcplog(level,msg)
	int level;
	char *msg;
{
	return;
}
