/*
 * $Id: stager.c,v 1.72 2000/05/29 07:56:27 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* When filereq.maxsize will work removed, remove the define below and in the code below again */
/* #define SKIP_FILEREQ_MAXSIZE */

/* If you don't want a turnaround on tape pools, via stage_migpool() call, remove the define below */
/* #define SKIP_TAPE_POOL_TURNAROUND */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager.c,v $ $Revision: 1.72 $ $Date: 2000/05/29 07:56:27 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#ifndef _WIN32
#include <unistd.h>                 /* For getcwd() etc... */
#endif
#include <grp.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include "Cns_api.h"
#include "log.h"
#define RFIO_KERNEL
#include "rfio.h"
#include "rtcp_api.h"
#include "serrno.h"
#include "stage.h"
#include "stage_api.h"
#include "vmgr_api.h"
#include "Castor_limits.h"
#include "u64subr.h"
#include "osdep.h"

#if !defined(IRIX5) && !defined(__Lynx__) && !defined(_WIN32)
EXTERN_C void DLL_DECL stager_usrmsg _PROTO(());
#else
EXTERN_C void DLL_DECL stager_usrmsg _PROTO((int, ...));
#endif
EXTERN_C int sendrep _PROTO(());
EXTERN_C int stglogit _PROTO(());
EXTERN_C int DLL_DECL stage_migpool _PROTO((char *, char *, char *));

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
int stager_client_callback _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
extern int (*rtcpc_ClientCallback) _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
int isuserlevel _PROTO((char *));
int getnamespace _PROTO((char *, char *));
void init_nfsroot_and_hostname _PROTO(());

int Aflag;                          /* Allocation flag */
int Migrationflag;                  /* Migration flag */
int nbcat_ent = -1;                 /* Number of catalog entries in stdin */
char hostname[CA_MAXHOSTNAMELEN+1]; /* Current hostname */
int reqid = 0;                      /* Request id */
int key;                            /* Request id associated key */
int rpfd = -1;                      /* Reply socket file descriptor */
struct stgcat_entry *stce = NULL;   /* End of stage catalog */
struct stgcat_entry *stcs = NULL;   /* Start of stage catalog */
char last_vid[CA_MAXVIDLEN+1];      /* Last vid returned by vmgr_gettape or Cns_getsegattrs */
char vid[CA_MAXVIDLEN+1];           /* Vid returned by vmgr_gettape or Cns_getsegattrs */
char vsn[CA_MAXVIDLEN+1];           /* Vsn returned by vmgr_gettape or vmgr_querytape */
char dgn[CA_MAXDENLEN+1];           /* Dgn returned by vmgr_gettape or vmgr_querytape */
char aden[CA_MAXDENLEN+1];          /* Aden returned by vmgr_gettape or vmgr_querytape */
char lbltype[3];                    /* Lbltype returned by vmgr_gettape or vmgr_querytape */
int fseq = -1;                      /* Fseq returned by vmgr_gettape or vmgr_querytape */
char tmpbuf[21];                    /* Printout of u_signed64 quantity temp. buffer */
char *fid = NULL;                   /* File ID */
int nrtcpcreqs;                     /* Number of rtcpcreq structures in circular list */
tape_list_t **rtcpcreqs = NULL;     /* rtcp request itself (circular list) */

u_signed64 *hsm_totalsize = NULL;   /* Total size per hsm file */
u_signed64 *hsm_transferedsize = NULL; /* Yet transfered size per hsm file */
int *hsm_fseg = NULL;               /* Current segment */
u_signed64 *hsm_fsegsize = NULL;    /* Current segment size */
int *hsm_fseq = NULL;               /* Current file sequence on current tape (vid) */
char **hsm_vid = NULL;              /* Current vid pointer or NULL if not in current rtcpc request */
char cns_error_buffer[512];         /* Cns error buffer */
char vmgr_error_buffer[512];        /* Vmgr error buffer */
int Flags = 0;                      /* Tape flag for vmgr_updatetape */
int stagewrt_nomoreupdatetape = 0;  /* Tell if the last updatetape from callback already did the job */
struct passwd *stpasswd;            /* Generic uid/gid stage:st */
int nuserlevel = 0;                 /* Number of user-level files */
int nexplevel = 0;                  /* Number of experiment-level files */
char nfsroot[MAXPATH];              /* Current RFIO's NFSROOT for shift-like path parsing */
int callback_error = 0;             /* Flag to tell us that there was an error in the callback */

#ifdef STAGER_DEBUG
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
	setegid(0);                                  \
	seteuid(0);                                  \
	if (Migrationflag) {                         \
		setegid(stpasswd->pw_gid);               \
		seteuid(stpasswd->pw_uid);               \
	} else {                                     \
		setegid(thisegid);                       \
		seteuid(thiseuid);                       \
	}                                            \
}

#define SETTAPEID(thisuid,thisgid) {             \
	setegid(0);                                  \
	seteuid(0);                                  \
	if (Migrationflag) {                         \
		setgid(stpasswd->pw_gid);                \
		setuid(stpasswd->pw_uid);                \
	} else {                                     \
		setgid(thisgid);                         \
		setuid(thisuid);                         \
	}                                            \
}

/* The following macros will set the correct uid/gid or euid/egid for nameserver operations */

#define SETID(thisuid,thisgid) {                 \
	setegid(0);                                  \
	seteuid(0);                                  \
	setgid(thisgid);                             \
	setuid(thisuid);                             \
}

#define SETEID(thiseuid,thisegid) {              \
	setegid(0);                                  \
	seteuid(0);                                  \
	setegid(thisegid);                           \
	seteuid(thiseuid);                           \
}

/* This macro call the cleanup at exit if necessary */
#define RETURN(exit_code) {                      \
	if (exit_code != 0) {                        \
		cleanup();                               \
	}                                            \
	if (rtcpcreqs != NULL) {                     \
		free_rtcpcreq(&rtcpcreqs);               \
		rtcpcreqs = NULL;                        \
	}                                            \
	return(exit_code);                           \
}

int main(argc, argv)
		 int argc;
		 char **argv;
{
	int c;
	int l;
	int nretry;
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
#ifdef __INSURE__
	char *tmpfile;
	FILE *f;
#endif
	char func[16];

	strcpy (func, "stager");
	stglogit (func, "function entered\n");

	reqid = atoi (argv[1]);
	key = atoi (argv[2]);
	rpfd = atoi (argv[3]);
	nbcat_ent = atoi (argv[4]);
	nretry = atoi (argv[5]);
	Aflag = atoi (argv[6]);
	Migrationflag = atoi (argv[7]);
#ifdef __INSURE__
	tmpfile = argv[7];
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
	stce++ = &(stcp[nbcat_ent - 1]);
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
    sendrep(rpfd, MSG_ERR, "[DEBUG] GO ON WITH gdb /usr/local/bin/stager %d, then break %d\n",getpid(),__LINE__ + 6);
    sendrep(rpfd, MSG_ERR, "[DEBUG] sleep(10)\n");
    sleep(10);
#endif

	/* Initialize RFIO's NFSROOT and hostname */
	init_nfsroot_and_hostname();

	/* Add hostname to all ipath'es if needed */
	{
		char *host;
		char *filename;
		int parseln_status;
		char	ipath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];

		for (stcp = stcs; stcp < stce; stcp++) {
			switch ((parseln_status = rfio_parseln(stcp->ipath, &host, &filename, NORDLINKS))) {
			case 0:
				/* It is localhost */
				host = hostname;
			case 1:
				if (filename == NULL) {
					parseln_status = -1;
					sendrep(rpfd, MSG_ERR, "rfio_parseln returns 1 (remote file) but filename == NULL. Internal error.\n");
					break;
				}
				if (strlen(host) + 1 + strlen(filename) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
					parseln_status = -1;
					sendrep(rpfd, MSG_ERR, "length of %s:%s too big (max %d characters)\n",host,filename,CA_MAXHOSTNAMELEN+MAXPATH);
					break;
				}
				strcpy(ipath,host);
				strcat(ipath,":");
				strcat(ipath,filename);
				strcpy(stcp->ipath,ipath);
				parseln_status = 0;
				break;
			default:
				sendrep(rpfd, MSG_ERR, "rfio_parseln returns -1 (%s - %s)\n",strerror(errno),sstrerror(serrno));
				break;
			}
			if (parseln_status != 0) {
				sendrep(rpfd, MSG_ERR, "### Global status after rfio_parseln failure. Internal error.\n");
			}
		}
	}

    if (stcs->t_or_d == 'm' || stcs->t_or_d == 'h') {
      /* We cannot mix HPSS and CASTOR files... */
      int nhpss = 0;
      int ncastor = 0;

      for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->t_or_d == 'm') {
          nhpss++;
        } else if (stcp->t_or_d == 'h') {
          ncastor++;
        } else {
          sendrep(rpfd, MSG_ERR, "### HSM file %s is of unknown type\n",stcp->u1.m.xfile);
          exit(SYERR);
        }
      }
      if ((nhpss == 0 && ncastor == 0) ||
          (nhpss  > 0 && ncastor  > 0)) {
        sendrep(rpfd, MSG_ERR, "### Recognized %d HPSS files, %d CASTOR files\n",nhpss,ncastor);
		free(stcs);
        exit(SYERR);
      }
    }

	if (Migrationflag) {
      struct stgcat_entry *stcp;
      char currentnamespace[CA_MAXPATHLEN+1];
      char previousnamespace[CA_MAXPATHLEN+1];

      previousnamespace[0] = '\0';

      /* We decide if this migration is a user-level one of an experiment-level one */
      for (stcp = stcs; stcp < stce; stcp++) {
		if (isuserlevel(stcp->u1.m.xfile)) {
          nuserlevel++;
        } else {
          nexplevel++;
        }
        if (stcp == stcs) {
          if (getnamespace(previousnamespace,stcp->u1.m.xfile) != 0) {
            sendrep(rpfd, MSG_ERR, "### Cannot get namespace (3rd component) of %s\n",stcp->u1.m.xfile);
			free(stcs);
            exit(SYERR);
          }
        } else {
          if (getnamespace(currentnamespace,stcp->u1.m.xfile) != 0) {
            sendrep(rpfd, MSG_ERR, "### Cannot get namespace (3rd component) of %s\n",stcp->u1.m.xfile);
			free(stcs);
            exit(SYERR);
          }
          if (strcmp(previousnamespace,currentnamespace) != 0) {
            sendrep(rpfd, MSG_ERR, "### Cannot mix namespaces (/%s/ and /%s/)\n",previousnamespace,currentnamespace);
			free(stcs);
            exit(SYERR);
          }
          strcpy(previousnamespace,currentnamespace);
        }
      }
      if ((nuserlevel == 0 && nexplevel == 0) ||
          (nuserlevel  > 0 && nexplevel  > 0)) {
        sendrep(rpfd, MSG_ERR, "### Found %d user-level files, %d experiment-level files\n",
                nuserlevel,nexplevel);
		free(stcs);
        exit(SYERR);
      }
      /* We get information on generic stage:st uid/gid */
      if ((stpasswd = getpwnam("stage")) == NULL) {
        sendrep(rpfd, MSG_ERR, "### Cannot getpwnam(\"%s\") (%s)\n","stage",strerror(errno));
		free(stcs);
        exit(SYERR);
      }
      /* For exp level, we will use the last of the entries */
      if (nexplevel > 0) {
        stpasswd->pw_gid = (stce - 1)->gid;
        stpasswd->pw_uid = (stce - 1)->uid;
      }
	}

	(void) umask (stcs->mask);

	signal (SIGINT, stagekilled);        /* If client died */
	signal (SIGTERM, stagekilled);       /* If killed from administrator */
	if (nretry) sleep (RETRYI);

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
	rtcp_log = (void (*) _PROTO((int, CONST char *, ...))) &stager_usrmsg;

	if (stcs->t_or_d == 't') {

      /* -------- TAPE TO TAPE ----------- */

#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] GO ON WITH gdb /usr/local/bin/stager %d, then break stage_tape\n",getpid());
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] sleep(10)\n");
		sleep(10);
#endif
		c = stage_tape ();
	} else {

      /* -------- CASTOR MIGRATION ----------- */

#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Setting Cns_errbuf and vmgr_errbuf\n");
		if (Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0 ||
			vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0 ||
			stage_setlog((void (*) _PROTO((int, char *))) &stager_log_callback) != 0) {
			sendrep(rpfd, MSG_ERR, "### Cannot set Cns or Vmgr API error buffer(s) or stager API callback function (%s)\n",sstrerror(serrno));
			free(stcs);
			exit(SYERR);
		}
#endif
		if (((stcs->status & STAGEWRT) == STAGEWRT) || ((stcs->status & STAGEPUT) == STAGEPUT)) {
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] GO ON WITH gdb /usr/local/bin/stager %d, then break stagewrt_castor_hsm_file\n",getpid());
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] sleep(10)\n");
				sleep(10);
#endif
			c = stagewrt_castor_hsm_file ();
		} else {
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] GO ON WITH gdb /usr/local/bin/stager %d, then break stagein_castor_hsm_file\n",getpid());
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] sleep(10)\n");
				sleep(10);
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


int hsmidx(stcp)
     struct stgcat_entry *stcp;
{
	struct stgcat_entry *stcx;
	int i;

	for (stcx = stcs, i = 0; stcx < stce; stcx++, i++) {
		if (strcmp(stcx->u1.m.xfile,stcp->u1.m.xfile) == 0) {
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
	if ((castor_hsm = CASTORFILE(stcp->u1.m.xfile)) == NULL) {
		serrno = EINVAL;
		return(NULL);
	}
	if (castor_hsm != stcp->u1.m.xfile) {
		/* We extract the host information from the hsm file */
		if ((end_host_hsm = strchr(stcp->u1.m.xfile,':')) == NULL) {
			serrno = EINVAL;
			return(NULL);
		}
		if (end_host_hsm <= stcp->u1.m.xfile) {
			serrno = EINVAL;
			return(NULL);
		}
		/* We replace the fist ':' with a null character */
		save_char = end_host_hsm[0];
		end_host_hsm[0] = '\0';
		host_hsm = stcp->u1.m.xfile;
		/* If the hostname begins with castor... then the user explicitely gave */
		/* a nameserver host - otherwise he might have get the HSM_HOST of hpss */
		/* or nothing or something wrong. In those three last cases, we will let */
		/* the nameserver API get CNS_HOST from shift.conf                       */
		if (strstr(host_hsm,"hpss") != host_hsm) {
			/* It is an explicit and valid castor nameserver : the API will be able */
			/* to connect directly to this host. No need to do a putenv ourself. */
			end_host_hsm[0] = save_char;
			return(stcp->u1.m.xfile);
		} else {
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_ERR, "[DEBUG-XXX] Hsm Host %s is incompatible with a /castor file. Default CNS_HOST (from shift.conf) will apply\n",host_hsm);
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
	int transfer_time = 0;
	int waiting_time = 0;
	char *castor_hsm;
	int i;
	int new_tape;
	struct stgcat_entry *stcs_tmp = NULL;
	struct stgcat_entry *stce_tmp = NULL;
	struct stgcat_entry *stcp_tmp = NULL;
	int nbcat_ent_tmp;
#ifdef STAGER_DEBUG
	char tmpbuf1[21];
	char tmpbuf2[21];
#endif
	unsigned char blockid[4];
	char fseg_status;

	/* We allocate as many size arrays */
	if ((hsm_totalsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))        == NULL ||
		(hsm_transferedsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))   == NULL ||
		(hsm_fseg = (int *) calloc(nbcat_ent,sizeof(int)))                           == NULL ||
		(hsm_fsegsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))         == NULL ||
		(hsm_fseq = (int *) calloc(nbcat_ent,sizeof(int)))                           == NULL ||
		(hsm_vid = (char **) calloc(nbcat_ent,sizeof(char *)))                       == NULL) {
		sendrep (rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "malloc error",strerror(errno));
		RETURN (USERR);
	}

	/* We initialize those size arrays */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		struct Cns_fileid Cnsfileid;

		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
			RETURN (SYERR);
		}
		/* check permissions in parent directory, get file size */
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling Cns_statx(%s,&statbuf,&Cnsfileid)\n",castor_hsm);
#endif
        SETEID(stcp->uid,stcp->gid);
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
		if (Cns_statx (castor_hsm, &statbuf, &Cnsfileid) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx (with invariants)",
							 sstrerror (serrno));
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
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx (with invariants)",
							 sstrerror (EACCES));
			RETURN (USERR);
		}
		hsm_totalsize[i]      = statbuf.filesize;
		hsm_transferedsize[i] = 0;
		hsm_fseg[i]         = 0;
			
	}

 getseg:
	/* We initalize current vid */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		hsm_vid[i] = NULL;
	}

	/* We initialize the latest got vid */
	strcpy(last_vid,"");

	/* We loop on all the requests, choosing those that requires the same tape */
	new_tape = 0;
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		struct Cns_fileid Cnsfileid;

		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
			RETURN (SYERR);
		}
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] %s : totalsize=%s, transferedsize=%s\n",
						castor_hsm,u64tostr(hsm_totalsize[i], tmpbuf1, 0),u64tostr(hsm_transferedsize[i], tmpbuf2, 0));
#endif
		if (hsm_totalsize[i] >  hsm_transferedsize[i]) {
#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling Cns_getsegattrs(%s,&Cnsfileid,copyrc=1,1+hsm_fseg[%d]=%d,&(hsm_fsegsize[%d]),vid,&(hsm_fseq[%d]),blockid)\n",
							castor_hsm,i,1+hsm_fseg[i],i,i);
#endif
				
			SETEID(stcp->uid,stcp->gid);
			strcpy(Cnsfileid.server,stcp->u1.h.server);
			Cnsfileid.fileid = stcp->u1.h.fileid;
			if (Cns_getsegattrs (castor_hsm,        /* hsm path */
								&Cnsfileid,         /* Cns invariants */
								1,                  /* copy number */
								++(hsm_fseg[i]),    /* file segment */
								&(hsm_fsegsize[i]), /* segment size */
								vid,                /* segment vid */
								&(hsm_fseq[i]),     /* segment fseq */
								blockid,            /* segment blockid */
								&fseg_status        /* segment status */
								)) {
				strcpy(vid,"");
				sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_getsegattrs (with invariants)",
								 sstrerror (serrno));
				RETURN (SYERR);
			}
			
			hsm_vid[i] = vid;

#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] [%s] Got vid.fseq=%s.%d, last_vid=%s\n",
							castor_hsm,
							hsm_vid[i],hsm_fseq[i],last_vid);
#endif
			
			/* We check previous vid returned by Cns_getsegattrs if any */
			if (last_vid[0] != '\0') {
				if (strcmp(last_vid,vid) != 0) {
					/* We are moving from one tape to another : we will run current and partial rtcp request */
					new_tape = 1;
					/* We remember this vid */
					/* strcpy(last_vid,vid); */
					/* And we reset the last found one : we will have to redo it */
					hsm_vid[i] = NULL;
					--(hsm_fseg[i]);
				}
			} else {
				/* We initialize this first vid */
				strcpy(last_vid,vid);
				if (stcp == stce - 1) {
					/* And this will be the only one */
					new_tape = 1;
				}
			}

			if (new_tape != 0) {
				break;
			}
		}
	}

	SETTAPEEID(stcs->uid,stcs->gid);

	if (new_tape != 0) {
		while (1) {
			/* Wait for the corresponding tape to be available */
#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] [%s] Calling vmgr_querytape(vid,vdn,dgn,aden,lbltype,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)\n",castor_hsm);
#endif
			if (vmgr_querytape (last_vid, vsn, dgn, aden, lbltype, NULL, NULL, NULL,
								NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) == 0) {
				strcpy(vid,last_vid);
				break;
			}
			strcpy(vid,"");
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "vmgr_querytape",
							 sstrerror (serrno));
			sendrep (rpfd, MSG_ERR, "%s : Retrying Volume Manager request in %d seconds\n", castor_hsm, RETRYI);
			sleep(RETRYI);
		}
		/* In any case we reinit vid variable to last_vid */
		strcpy(last_vid,vid);
	}

	/* Build the request from where we found vid (included) up to our next (excluded) */
	nbcat_ent_tmp = 0;
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		if (hsm_vid[i] != NULL) {
			++nbcat_ent_tmp;
		}
	}

	/* nbcat_ent_tmp will be the number of entries that will use vid */
	if (nbcat_ent_tmp == 0) {
		serrno = SEINTERNAL;
		sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "nbcat_ent_tmp == 0...",
						 sstrerror (serrno));
		RETURN (USERR);
	}

	/* window [stcs_tmp,stce_tmp(excluded)] will be a temporary set of catalog entries */
	if (stcs_tmp != NULL) {
		free(stcs_tmp);
		stcs_tmp = NULL;
	}
	if ((stce_tmp = stcs_tmp = (struct stgcat_entry *) calloc(nbcat_ent_tmp,sizeof(struct stgcat_entry))) == NULL) {
		sendrep (rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "malloc error",strerror(errno));
		free(stcs_tmp);
		RETURN (USERR);
	}
	stce_tmp += nbcat_ent_tmp;

	/* we fill temporary window [stcs_tmp,stce_tmp] */
	for (stcp = stcs, i = 0, stcp_tmp = stcs_tmp; stcp < stce; stcp++, i++) {
		if (hsm_vid[i] != NULL) {
			*stcp_tmp = *stcp;
			stcp_tmp++;
		}
	}
		
	/* We "interrogate" for the total number of structures */
	if (build_rtcpcreq(&nrtcpcreqs, NULL, stcs_tmp, stce_tmp, stcs_tmp, stce_tmp) != 0) {
		RETURN (USERR);
	}
	if (nrtcpcreqs <= 0) {
		serrno = SEINTERNAL;
		sendrep (rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "cannot determine number of hsm files",sstrerror(serrno));
		RETURN (USERR);
	}

	/* We build the request */
	if (rtcpcreqs != NULL) {
		free_rtcpcreq(&rtcpcreqs);
		rtcpcreqs = NULL;
	}
	if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcs_tmp, stce_tmp, stcs_tmp, stce_tmp) != 0) {
		sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "build_rtcpcreq",sstrerror (serrno));
		free(stcs_tmp);
		RETURN (SYERR);
	}

	/* By construction, rtcpcreqs has only one indice: 0 */

	Flags = 0;
	while (1) {
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling rtcpc()\n");
		STAGER_RTCP_DUMP(rtcpcreqs[0]);
#endif
		rtcp_rc = rtcpc(rtcpcreqs[0]);
		if (rtcp_rc < 0) {
			int stagein_castor_hsm_file_retry = 0;

            if (callback_error != 0) {
              /* This is a callback error - considered as fatal */
              sendrep (rpfd, MSG_ERR, STG02, "", "Callback Error - Fatal error",
                       sstrerror (serrno));
              free(stcs_tmp);
              RETURN (USERR);
            }

			for (stcp_tmp = stcs_tmp, i = 0; stcp_tmp < stce_tmp; stcp_tmp++, i++) {
				if (rtcpcreqs[0]->file[i].filereq.err.errorcode == ETPARIT ||
					rtcpcreqs[0]->file[i].filereq.err.errorcode == ETUNREC ||
					rtcpcreqs[0]->tapereq.err.errorcode == ETPARIT ||
					rtcpcreqs[0]->tapereq.err.errorcode == ETUNREC) {
					Flags |= DISABLED;
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc",sstrerror (serrno));
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Flaging tape to DISABLED");
					free(stcs_tmp);
					RETURN (SYERR);
				} else if ((rtcpcreqs[0]->file[i].filereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV ||
							(rtcpcreqs[0]->tapereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV) {
					/* Reselect a server - we retry, though */
					stagein_castor_hsm_file_retry = 1;
				} else if (rtcpcreqs[0]->tapereq.err.errorcode == ETVBSY) {
					/* Reselect a server - we retry, though */
					stagein_castor_hsm_file_retry = 1;
				} else if (rtcpcreqs[0]->file[i].filereq.err.errorcode != ENOENT) {
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc",sstrerror(serrno));
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Exit");
					free(stcs_tmp);
					RETURN (SYERR);
				}
				if (stagein_castor_hsm_file_retry > 0) {
					break;
				}
			}
			if (stagein_castor_hsm_file_retry > 0) {
				continue;
			}
		} else if (rtcp_rc > 0) {
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Unknown error code (>0)");
			free(stcs_tmp);
			RETURN (SYERR);
		}
		free(stcs_tmp);
		stcs_tmp = NULL;
		/* We checked if there is pending things to stagein */
		for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
			struct Cns_fileid Cnsfileid;

			if (hsm_vid[i] != NULL) {
				if (rtcpcreqs[0]->file[i].filereq.err.errorcode == ENOENT) {
					/* Entry did not exist - We make the stager believe that this request was */
					/* successful neverthless...                                              */
					hsm_transferedsize[i] = hsm_totalsize[i];
					sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", strerror(ENOENT));
				} else {
					{
                      /* gcc compiler bug - fixed or forbidden register was spilled. */
                      /* This may be due to a compiler bug or to impossible asm      */
                      /* statements or clauses.                                      */
                      u_signed64 dummyvalue;

                      dummyvalue = rtcpcreqs[0]->file[i].filereq.bytes_out;
                      hsm_transferedsize[i] += dummyvalue;

					}
					waiting_time += rtcpcreqs[0]->file[i].filereq.TEndPosition -
									rtcpcreqs[0]->tapereq.TStartRequest;
					transfer_time += rtcpcreqs[0]->file[i].filereq.TEndTransferDisk -
									rtcpcreqs[0]->file[i].filereq.TEndPosition;
				}
				if ((castor_hsm = hsmpath(stcp)) == NULL) {
					sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
					RETURN (USERR);
				}
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEIN] Calling Cns_setatime(%s)\n",castor_hsm);
#endif
				SETEID(stcp->uid,stcp->gid);
				strcpy(Cnsfileid.server,stcp->u1.h.server);
				Cnsfileid.fileid = stcp->u1.h.fileid;
				if (Cns_setatime (castor_hsm, &Cnsfileid) != 0) {
                  sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setatime (with invariants)",
                           sstrerror (serrno));
                }
			}
		}
		for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
			if (stcp->size > 0) {
				if (hsm_transferedsize[i] < hsm_totalsize[i] &&
					hsm_transferedsize[i] < stcp->size * ONE_MB) {
					/* Not finished */
					goto getseg;
				}
			} else {
				if (hsm_transferedsize[i] < hsm_totalsize[i]) {
					/* Not finished */
					goto getseg;
				}
			}
		}
		break;
	}
	RETURN(0);
}

int stagewrt_castor_hsm_file() {
	int fseq;
	int fseq_rtcp;
	int i;
	struct stat statbuf;
	struct Cns_filestat statbuf_check;
	int rtcp_rc;
	struct stgcat_entry *stcp = stcs;
	int transfer_time = 0;
	int waiting_time = 0;
	char *castor_hsm;
	char tmpbuf[21];
	extern char* poolname2tapepool _PROTO((char *));
	unsigned char blockid[4];
#ifdef SKIP_TAPE_POOL_TURNAROUND
    char tape_pool_first[CA_MAXPOOLNAMELEN + 1];
#endif

#ifdef SKIP_TAPE_POOL_TURNAROUND
    tape_pool_first[0] = '\0';
#endif

	/* We allocate as many size arrays */
	if ((hsm_totalsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))            == NULL ||
		(hsm_transferedsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))   == NULL ||
		(hsm_fseg = (int *) calloc(nbcat_ent,sizeof(int)))                           == NULL ||
		(hsm_fsegsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))         == NULL ||
		(hsm_fseq = (int *) calloc(nbcat_ent,sizeof(int)))                           == NULL ||
		(hsm_vid = (char **) calloc(nbcat_ent,sizeof(char *)))                       == NULL) {
      sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "malloc error",strerror(errno));
      RETURN (USERR);
	}
    
    /* Set CallbackClient */
    rtcpc_ClientCallback = &stager_client_callback;

	/* We initialize those size arrays */
	i = 0;
	for (stcp = stcs; stcp < stce; stcp++, i++) {
      SETEID(stcp->uid,stcp->gid);
      if (rfio_stat (stcp->ipath, &statbuf) < 0) {
        sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_stat",
                 rfio_serror());
        RETURN (USERR);
      }
      if (statbuf.st_size == 0) {
        sendrep (rpfd, MSG_ERR, "STG02 - %s is empty\n", stcp->ipath);
        RETURN (USERR);
      }

      /* Search for castor a-la-unix path in the file to stagewrt */
      if ((castor_hsm = hsmpath(stcp)) == NULL) {
        sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
        RETURN (USERR);
      }
      
      /* We do not want to overwrite an existing file, except it its size is zero */
      if (((stcp->status & STAGEWRT) == STAGEWRT) || ((stcp->status & STAGEPUT) == STAGEPUT)) {
		struct Cns_fileid Cnsfileid;

#ifdef STAGER_DEBUG
        sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling Cns_statx(%s,&statbuf_check,&Cnsfileid)\n",castor_hsm);
#endif
        SETEID(stcp->uid,stcp->gid);
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
        if (Cns_statx(castor_hsm, &statbuf_check, &Cnsfileid) != 0) {
            sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx (with invariants)",
                     "file already exists and is non-zero size");
            RETURN (USERR);
        } else {
          if (statbuf_check.filesize > 0) {
            sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_statx (with invariants)",
                     "file already exists and is non-zero size");
            RETURN (USERR);
          }
        }
      }
      
      hsm_totalsize[i] = statbuf.st_size;
      hsm_transferedsize[i] = 0;
      hsm_fseg[i] = 1;
      hsm_fsegsize[i] = 0;
      hsm_fseq[i] = -1;
      hsm_vid[i] = NULL;
    }

    while (1) {
      u_signed64 totalsize_to_transfer;
      u_signed64 estimated_free_space;
      int istart, iend;
      struct stgcat_entry *stcp_start, *stcp_end;
      char tape_pool[CA_MAXPOOLNAMELEN + 1];
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

        /* We select a tape (migration) pool */
        if (stage_migpool(NULL,stcs->poolname,tape_pool) != 0) {
          sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "stage_migpool",
                   sstrerror (serrno));
          RETURN (USERR);
        }
        
#ifdef SKIP_TAPE_POOL_TURNAROUND
        if (tape_pool_first[0] == '\0') {
          /* First selected tape pool - We remember it */
          strcpy(tape_pool_first,tape_pool);
        } else {
          /* Not the fisrt one - did we make a turnaround ? */
          if (strcmp(tape_pool_first,tape_pool) == 0) {
            /* Yes... Bad... Let's sleep a long while then */
            sendrep (rpfd, MSG_ERR, "No migration pool currently available. Sleeping 1 hour\n");
            sleep(3600);
          }
        }
#endif

#ifdef STAGER_DEBUG
        sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] "
                "Calling vmgr_gettape(%s,%s,NULL,vid,vsn,dgn,aden,lbltype,&fseq,&estimated_free_space)\n",
                tape_pool != NULL ? tape_pool : "NULL", u64tostr((u_signed64) statbuf.st_size, tmpbuf, 0));
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
                          &fseq,                  /* fseq      (output)              */
                          &estimated_free_space   /* freespace (output)              */
                          ) == 0) {
          break;
        }

        /* Makes sure vid variable has not been overwritten... (to be checked with Jean-Philippe) */
        strcpy(vid,"");
        sendrep (rpfd, MSG_ERR, STG02, "", "vmgr_gettape",
                 sstrerror (serrno));
        if (++vmgr_gettape_iretry > vmgr_gettape_nretry) {
          sendrep (rpfd, MSG_ERR, STG02, "", "Number of retries exceeded on vmgr_gettape. Exit.",
                   sstrerror (serrno));
          RETURN (SYERR);
        }
        sendrep (rpfd, MSG_ERR, "Trying on another migration pool (if any). Retry No %d/%d\n",
                 vmgr_gettape_iretry, vmgr_gettape_nretry);
        sleep(10);
      }
      /* From now on, "vid" is in status TAPE_BUSY */
      
      /* We check which hsm files are concerned */
      i = 0;
      istart = -1;
      iend = -1;
      stcp_start = stcp_end = NULL;

      for (stcp = stcs; stcp < stce; stcp++, i++) {
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
        sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "Cannot find where to begin",sstrerror(serrno));
        Flags = 0;
        RETURN (USERR);
      }

      /* From now on we know that stcp[istart,iend] fulfill a-priori the requirement */
      for (i = istart; i <= iend; i++) {
        hsm_vid[i] = vid;
        hsm_fseq[i] = fseq++;
      }

#ifdef STAGER_DEBUG
      sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Use [vid,vsn,dgn,aden,lbltype,fseqs]=[%s,%s,%s,%s,%s,%d to %d]\n",
              vid,vsn,dgn,aden,lbltype,hsm_fseq[istart],hsm_fseq[iend]);
#endif
      
      /* We "interrogate" for the number of structures */
      if (build_rtcpcreq(&nrtcpcreqs, NULL, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
        Flags = 0;
        RETURN (USERR);
      }
      
      /* It has to be one exactly (one vid exactly !) */
      if (nrtcpcreqs != 1) {
        serrno = SEINTERNAL;
        sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "Number of rtcpc structures different vs. deterministic value of 1",sstrerror(serrno));
        Flags = 0;
        RETURN (USERR);
      }

      /* Build the request from where we started (included) up to our next (excluded) */
      if (rtcpcreqs != NULL) {
        free_rtcpcreq(&rtcpcreqs);
        rtcpcreqs = NULL;
      }

      if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcp_start, stcp_end, stcp_start, stcp_end) != 0) {
        sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "build_rtcpcreq",sstrerror (serrno));
        Flags = 0;
        RETURN (SYERR);
      }
      
#ifdef STAGER_DEBUG
      sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling rtcpc()\n");
      STAGER_RTCP_DUMP(rtcpcreqs[0]);
#endif
      
      Flags = 0;
      rtcp_rc = rtcpc(rtcpcreqs[0]);
      
      if (rtcp_rc < 0) {
        int j;

        /* rtcpc failed */
        sendrep (rpfd, MSG_ERR, STG02, vid, "rtcpc",
                 sstrerror (serrno));

        if (callback_error != 0) {
          /* This is a callback error - considered as fatal */
          sendrep (rpfd, MSG_ERR, STG02, "", "Callback Error - Fatal error",
                   sstrerror (serrno));
          RETURN (USERR);
        }


        i = 0;
        for (j = istart; j <= iend; j++, i++) {
          if (rtcpcreqs[0]->file[i].filereq.err.errorcode == ETPARIT ||
              rtcpcreqs[0]->file[i].filereq.err.errorcode == ETUNREC ||
              rtcpcreqs[0]->file[i].filereq.err.errorcode == ETLBL ||
              rtcpcreqs[0]->tapereq.err.errorcode == ETPARIT ||
              rtcpcreqs[0]->tapereq.err.errorcode == ETUNREC ||
              rtcpcreqs[0]->tapereq.err.errorcode == ETLBL) {
            sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Flaging tape to DISABLED");
            Flags |= DISABLED;
            break;
          } else if ((rtcpcreqs[0]->file[i].filereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV ||
                     (rtcpcreqs[0]->tapereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV) {
            /* Reselect a server - we retry, though */
            sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Retrying (Another Tape Server Required)");
            break;
          } else if (rtcpcreqs[0]->tapereq.err.errorcode == ETVBSY) {
            /* Reselect also server */
            sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Retrying (ETVBSY)");
            break;
          } else if (rtcpcreqs[0]->file->filereq.err.errorcode == ENOENT) {
            /* Tape info very probably inconsistency with, for ex., TMS */
            sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc",sstrerror(serrno));
            sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Exit");
            Flags = 0;
            RETURN(USERR);
          }
		}
        if (Flags != TAPE_FULL && stagewrt_nomoreupdatetape == 0) {
          /* If Flags is TAPE_FULL then it been set by the callback which already called vms_updatetape */
#ifdef STAGER_DEBUG
          sendrep (rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(%s,0,0,0,Flags=%d)\n",vid,Flags);
#endif
          if (vmgr_updatetape (vid, (u_signed64) 0, 0, 0, Flags) != 0) {
            sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
                     sstrerror (serrno));
          }
        }
        sendrep (rpfd, MSG_ERR, "Retrying Volume Manager request in %d seconds\n", RETRYI);
        sleep(RETRYI);
        goto gettape;
      } else {
        if (Flags != TAPE_FULL && stagewrt_nomoreupdatetape == 0) {
          /* If Flags is TAPE_FULL then it has already been set by the callback */
#ifdef STAGER_DEBUG
          sendrep (rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(%s,0,0,0,0)\n",vid);
#endif
          if (vmgr_updatetape (vid, (u_signed64) 0, 0, 0, 0) != 0) {
            sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
                     sstrerror (serrno));
          }
        }
      }
	}
    Flags = 0;
	RETURN (0);
}

int stage_tape() {
	int rtcp_rc;

    SETTAPEEID(stcs->uid,stcs->gid);

 stage_tape_retry:
	/* We "interrogate" for the number of structures */
	if (build_rtcpcreq(&nrtcpcreqs, NULL, stcs, stce, stcs, stce) != 0) {
		RETURN (USERR);
	}
	if (nrtcpcreqs <= 0) {
		serrno = SEINTERNAL;
		sendrep (rpfd, MSG_ERR, STG02, "stage_tape", "cannot determine number of hsm files",sstrerror(serrno));
		RETURN (USERR);
	}

	/* Build the request */
	if (rtcpcreqs != NULL) {
		free_rtcpcreq(&rtcpcreqs);
		rtcpcreqs = NULL;
	}
	if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcs, stce, stcs, stce) != 0) {
		sendrep (rpfd, MSG_ERR, STG02, "", "build_rtcpcreq",sstrerror (serrno));
		RETURN (SYERR);
	}
			
	
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG-STAGETAPE] Calling rtcpc()\n");
	STAGER_RTCP_DUMP(rtcpcreqs[0]);
#endif

	rtcp_rc = rtcpc(rtcpcreqs[0]);

    if (callback_error != 0) {
      /* This is a callback error - considered as fatal */
      sendrep (rpfd, MSG_ERR, STG02, "", "Callback Error - Fatal error",
               sstrerror (serrno));
      RETURN (USERR);
    }

	if (rtcp_rc < 0) {
      int stage_tape_retry_flag = 0;
      int i;

      sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc", sstrerror (serrno));
      switch (rtcpcreqs[0]->tapereq.err.errorcode) {
      case ETPARIT:
      case ETUNREC:
      case ETLBL:
        sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Tape should be disabled - Contact responsibles");
        RETURN (SYERR);
      case ETVBSY:
        sendrep(rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc", "Tape information inconsistency - Contact responsibles");
        RETURN (SYERR);
      default:
        break;
      }

      for (i = 0; i < nbcat_ent; i++) {
        if (rtcpcreqs[0]->file[i].filereq.err.errorcode == ETPARIT || 
            rtcpcreqs[0]->file[i].filereq.err.errorcode == ETUNREC ||
            rtcpcreqs[0]->file[i].filereq.err.errorcode == ETLBL) {
          sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Tape should be disabled - Contact responsibles");
          RETURN (SYERR);
        } else if ((rtcpcreqs[0]->file[i].filereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV ||
                   (rtcpcreqs[0]->tapereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV) {
          stage_tape_retry_flag = 1;
          break;
        }
      }
      if (stage_tape_retry_flag != 0) {
        sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Internal retry as rtcpc suggests");
        goto stage_tape_retry;
      }

      RETURN (USERR);

    } else if (rtcp_rc > 0) {
      sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Unknown error code (>0)");
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
	RFILE *rf;
	char stageid[CA_MAXSTGRIDLEN+1];
	EXTERN_C int rfiosetopt _PROTO((int, int *, int));
	EXTERN_C int rfio_pread _PROTO((char *, int, int, RFILE *));

    SETTAPEEID(stcs->uid,stcs->gid);

	if (stcp->t_or_d == 'm') {
		/* filecopy of an HPSS file */
		struct stat filemig_stat;

		if (! (((stcp->status & STAGEWRT) == STAGEWRT) || ((stcp->status & STAGEPUT) == STAGEPUT))) {
			/* This is a Reading op. */
			if (! stcp->size) {
				/* Without -s option */
				if (rfio_stat (stcp->u1.m.xfile, &filemig_stat) < 0) {
					sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile,
							 "rfio_stat", rfio_serror());
					RETURN(USERR);
				}
				stcp->size = (int) ((filemig_stat.st_size/(ONE_MB)) + 1);
			}
		} else {
			if (rfio_stat (stcp->u1.m.xfile, &filemig_stat) == 0) {
				sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile,
						 "rfio_stat", "file already exists");
				RETURN(USERR);
			}
		}
	}

	/*
	 * @@@ TO BE MOVED TO cpdskdsk.sh @@@
	 */

	if (! stcp->ipath[0]) {
		/* Deferred allocation (Aflag) */
		sprintf (stageid, "%d.%d@%s", reqid, key, hostname);
		if (stage_updc_filcp (
								stageid,                 /* Stage ID      */
								0,                       /* Copy rc       */
								NULL,                    /* Interface     */
								(u_signed64) stcp->size, /* Size          */
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
			sendrep (rpfd, MSG_ERR, STG02, stageid, "stage_updc_filcp",
							 sstrerror (serrno));
			RETURN (USERR);
		}
		/* sendrep (rpfd, MSG_ERR, "filecopy : internal path setted to %s\n", stcp->ipath); */
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
							 stcp->size * ONE_MB);
		if (stcp->t_or_d == 'm')
			sprintf (command+strlen(command), " %s", stcp->u1.m.xfile);
		else
			sprintf (command+strlen(command), " %s", stcp->u1.d.xfile);	    
		sprintf (command+strlen(command), " %s", stcp->ipath);
	}
	setgid (stcp->gid);
	setuid (stcp->uid);
	rf = rfio_popen (command, "r");
	if (rf == NULL) {
		RETURN (SYERR);
	}

	while ((c = rfio_pread (buf, 1, sizeof(buf)-1, rf)) > 0) {
		buf[c] = '\0';
		sendrep (rpfd, RTCOPY_OUT, "%s", buf);
	}

	c = rfio_pclose (rf);
	RETURN (c);
}

void cleanup() {
	/* Safety cleanup */
	if (vid[0] != '\0') {
#ifdef STAGER_DEBUG
		sendrep (rpfd, MSG_ERR, "[DEBUG-CLEANUP] Calling vmgr_updatetape(%s,0,0,0,0)\n",vid);
#endif
		if (vmgr_updatetape (vid, (u_signed64) 0, 0, 0, 0) != 0) {
			sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
							 sstrerror (serrno));
		}
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
		sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Bad arguments to build_rtcpcreq",sstrerror(serrno));
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
		sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
		serrno = SEINTERNAL;
		return(-1);
	}
	
	/* We allocate content of the array */
	for (i = 0; i < *nrtcpcreqs_in; i++) {
		if (((*rtcpcreqs_in)[i] = calloc(1,sizeof(tape_list_t))) == NULL) {
			sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
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
				strcpy((*rtcpcreqs_in)[i]->tapereq.server   , stcp->u1.t.tapesrvr );
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
				char *p, *q;
				char trailing;
				char trailing2;
				int n1, n2, n;
				int nbfiles;
				file_list_t *fl;
				int last_disk_fseq;
				fseq_elem *fseq_list = NULL;
				int stcp_nbtpf = 0;
				int stcp_inbtpf = 0;
				struct stgcat_entry save_stcp;
				struct stgcat_entry *save_stcp_p;

				n = -1;

				/* We compute the number of tape files */
				save_stcp_p = stcp;
                save_stcp = *stcp;
				switch (stcp->u1.t.fseq[0]) {
				case 'n':
				case 'u':
					nbtpf = 1;
					if (fixed_stce > (fixed_stcs+1)) {
						for (stcp = fixed_stcs+1; stcp < fixed_stce; stcp++) {
							if (stcp->reqid != (stcp-1)->reqid) nbtpf++;
						}
					}
					break;
				default:
					q = fixed_stcs->u1.t.fseq + strlen (fixed_stcs->u1.t.fseq) - 1;
					if ((trailing = *q) == '-')
						*q = '\0';
					if (fixed_stce > (fixed_stcs+1)) {
						nbtpf = fixed_stce - fixed_stcs - 1;
					} else {
						nbtpf = 0;
					}
					p = strtok ((fixed_stce-1)->u1.t.fseq, ",");
					while (p != NULL) {
						if ((q = strchr (p, '-')) != NULL) {
							if (q[1] != '\0') {
								*q = '\0';
								n2 = atoi (q + 1);
								n1 = atoi (p);
								*q = '-';
								nbtpf += n2 - n1 + 1;
							}
						} else {
							n1 = atoi (p);
							n2 = n1;
							nbtpf += n2 - n1 + 1;
						}
						if ((p = strtok (NULL, ",")) != NULL)
							*(p - 1) = ',';
					}
				}
				*save_stcp_p = save_stcp;
				nbfiles = (nbcat_ent > nbtpf) ? nbcat_ent : nbtpf;
				if ((fl = (file_list_t *) calloc (nbfiles, sizeof(file_list_t))) == NULL) {
					sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
					serrno = SEINTERNAL;
					return(-1);
				}
				/* We makes sure it is a circular list and correctly initialized */
				for (j = 0; j < nbfiles; j++) {
					switch (j) {
					case 0:
						fl[j].prev = &(fl[nbfiles - 1]);
						fl[j].next = &(fl[nbfiles > 1 ? j + 1 : 0]);
						break;
					default:
						if (j == nbfiles - 1) {
							fl[j].prev = &(fl[j - 1]); /* Nota : we are there only if nbfiles > 1 */
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

				(*rtcpcreqs_in)[0]->file = fl;
				last_disk_fseq = 0;
				for (stcp = fixed_stcs, j = 0; j < nbfiles; j++) {
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
						strcpy (fl[j].filereq.file_path, stcp->ipath);
					} else {
						strcpy (fl[j].filereq.file_path, ".");
					}
					if (last_disk_fseq < nbcat_ent) {
						fl[j].filereq.disk_fseq = ++last_disk_fseq;
					} else {
						fl[j].filereq.disk_fseq = nbcat_ent;
					}
					if (strcmp (stcp->recfm, "U,b") == 0) {
						strcpy (fl[j].filereq.recfm, "U");
						fl[j].filereq.convert = NOF77CW;
					} else if (strcmp (stcp->recfm, "U,f") == 0) {
						strcpy (fl[j].filereq.recfm, "U");
					} else if (strcmp (stcp->recfm, "F,-") == 0) {
						strcpy (fl[j].filereq.recfm, "F");
						fl[j].filereq.convert = NOF77CW;
					} else {
						strcpy (fl[j].filereq.recfm, stcp->recfm);
					}
					if (strlen(stcp->u1.t.fid) > CA_MAXFIDLEN) {
						/* Take the LAST 17 characters of fid */
						strncpy (fl[j].filereq.fid, &(stcp->u1.t.fid[strlen(stcp->u1.t.fid) - CA_MAXFIDLEN]), CA_MAXFIDLEN);
					} else {
						strcpy (fl[j].filereq.fid, stcp->u1.t.fid);
					}
					sprintf (fl[j].filereq.stageID, "%d.%d@%s", reqid, key,
									 hostname);
					switch (stcp->u1.t.fseq[0]) {
					case 'n':
						if (j >= nbtpf - 1 && j > 0) {
							fl[j].filereq.concat = CONCAT;
						}
						fl[j].filereq.position_method = TPPOSIT_EOI;
						break;
					case 'u':
						fl[j].filereq.position_method = TPPOSIT_FID;
						break;
					default:
						fl[j].filereq.position_method = TPPOSIT_FSEQ;
						fl[j].filereq.tape_fseq = atoi (fseq_list[stcp_inbtpf++]);
						break;
					}
					if (stcp->blksize > 0) {
						fl[j].filereq.blocksize = stcp->blksize;
					}
					if (stcp->lrecl > 0) {
						fl[j].filereq.recordlength = stcp->lrecl;
					}
					if (stcp->u1.t.retentd > 0) {
						fl[j].filereq.retention = stcp->u1.t.retentd;
					}
					fl[j].filereq.def_alloc = Aflag;
					if ((stcp->u1.t.E_Tflags & SKIPBAD) == SKIPBAD) {
						fl[j].filereq.rtcp_err_action |= SKIPBAD;
					}
					if ((stcp->u1.t.E_Tflags & KEEPFILE) == KEEPFILE) {
						fl[j].filereq.rtcp_err_action |= KEEPFILE;
					}
					if ((stcp->u1.t.E_Tflags & IGNOREEOI) == IGNOREEOI) {
						fl[j].filereq.tp_err_action |= IGNOREEOI;
					}
					if ((stcp->u1.t.E_Tflags & NOTRLCHK) == NOTRLCHK) {
						fl[j].filereq.tp_err_action |= NOTRLCHK;
					}
					if (stcp->charconv > 0) {
						fl[j].filereq.convert = stcp->charconv;
					}
					switch (stcp->status & 0xF) {
					case STAGEWRT:
					case STAGEPUT:
						if (stcp->u1.t.filstat == 'o') {
							fl[j].filereq.check_fid = CHECK_FILE;
						}
						break;
					default:
						fl[j].filereq.check_fid = CHECK_FILE;
						break;
					}
					if (stcp->u1.t.fseq[0] != 'n') {
						if (trailing2 == '-') {
							if (j >= nbtpf) {
								fl[j].filereq.concat = (nbcat_ent >= nbtpf ? CONCAT_TO_EOD : NOCONCAT_TO_EOD);
							} else {
								fl[j].filereq.concat = (nbcat_ent >= nbtpf ? CONCAT        : NOCONCAT       );
							}
						} else {
							if (((fixed_stcs->status & STAGEWRT) == STAGEWRT) || ((fixed_stcs->status & STAGEPUT) == STAGEPUT)) {
								fl[j].filereq.concat = (j >= nbtpf ? CONCAT : NOCONCAT);
							} else {
								fl[j].filereq.concat = (j >= nbcat_ent ? CONCAT : NOCONCAT);
							}
						}
					}
					if (last_disk_fseq > nbcat_ent) {
						fl[j].filereq.concat |= VOLUME_SPANNING;
					}
					fl[j].filereq.maxnbrec = stcp->nread;

					if (stcp->size > 0) {
											/* filereq.maxsize is in bytes */
#ifndef SKIP_FILEREQ_MAXSIZE
						fl[j].filereq.maxsize = 1 + (u_signed64) ((u_signed64) stcp->size * ONE_MB);
#endif
					}
					if (stcp + 1 < fixed_stce) stcp++;
				}

				/* Say what we will exit the loop */
				loop_break = 1;

				if (fseq_list != NULL) {
					free(fseq_list);
					fseq_list = NULL;
				}
			}
			break;
		default:
			/* This is an hsm request */
			if (hsm_totalsize == NULL || hsm_transferedsize == NULL) {
				serrno = SEINTERNAL;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "No hsm_totalsize or hsm_transferedsize array (should be != NULL)",sstrerror(serrno));
				return(-1);
			}
			if ((ihsm = hsmidx(stcp)) < 0) {
				serrno = SEINTERNAL;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Cannot find valid hsm !\n",sstrerror(serrno));
				return(-1);
			}
			if (hsm_totalsize[ihsm] == 0) {
				serrno = SEINTERNAL;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Bad size parameter (should be > 0)",sstrerror(serrno));
				return(-1);
			}
			if (hsm_transferedsize[ihsm] > hsm_totalsize[ihsm]) {
				serrno = SEINTERNAL;
				sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "Yet transfered size claimed to be > Hsm total size...",sstrerror(serrno));
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
			if ((*rtcpcreqs_in)[i]->file == NULL) {
				/* First file for this VID */
				if (((*rtcpcreqs_in)[i]->file = (file_list_t *) calloc(1,sizeof(file_list_t))) == NULL) {
					sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "calloc",strerror(errno));
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
					sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "realloc",strerror(errno));
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
			if ((castor_hsm = hsmpath(stcp)) == NULL) {
				sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
				RETURN (USERR);
			}
			if ((fid = strrchr (castor_hsm, '/')) == NULL) {
				sendrep (rpfd, MSG_ERR, STG33, stcp->u1.m.xfile, "Invalid path");
				return(-1);
			}
			fid++;
			if (strlen(fid) > CA_MAXFIDLEN) {
				/* Take the LAST 17 characters of fid */
				strncpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.fid, &(fid[strlen(fid) - CA_MAXFIDLEN]), CA_MAXFIDLEN);
			} else {
				strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.fid, fid);
			}
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.position_method = TPPOSIT_FSEQ;
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.tape_fseq = hsm_fseq[ihsm];
#ifndef SKIP_FILEREQ_MAXSIZE
			if (stcp->size > 0) {
				u_signed64 dummysize;

				dummysize = (u_signed64) stcp->size;
				dummysize *= (u_signed64) ONE_MB;
				dummysize -= (u_signed64) hsm_transferedsize[ihsm];
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.maxsize = 1 + dummysize;
				/* Default is NOT to specify maxsize if user did not */
				/*
			} else {
				u_signed64 dummysize;

				dummysize = (u_signed64) hsm_totalsize[ihsm];
				dummysize -= (u_signed64) hsm_transferedsize[ihsm];
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.maxsize = 1 + dummysize;
				*/
			}
#endif
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.offset = hsm_transferedsize[ihsm];
			(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.def_alloc = Aflag;
			if (! Aflag) {
				strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.file_path, stcp->ipath);
			} else {
				strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.file_path, ".");
			}
			(*rtcpcreqs_in)[i]->file[nfile_list-1].tape = (*rtcpcreqs_in)[i];
			switch (stcp->status & 0xF) {
			case STAGEWRT:
			case STAGEPUT:
				/* This is an hsm write request */
				if (stcp->u1.t.filstat == 'o') {
					(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.check_fid = CHECK_FILE;
				}
				(*rtcpcreqs_in)[i]->tapereq.mode            = WRITE_ENABLE;
#ifdef ALICETEST
				/* ALICE TEST - FORCE BLOCKSIZE */
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.blocksize = 256 * ONE_KB;
#endif
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

	*trailing = *(fseq + strlen (fseq) - 1);
	if (*trailing == '-') {
		if ((req_type & 0xF) != STAGEIN) {
			sendrep (rpfd, MSG_ERR, STG18);
			return (0);
		}
		*(fseq + strlen (fseq) - 1) = '\0';
	}
	switch (*fseq) {
	case 'n':
		if ((req_type & 0xF) == STAGEIN) {
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
	
	tape_list_t *first_rtcpcreq, *current_rtcpcreq, *dummy_rtcpcreq;
	
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
				*rtcpcreq = NULL;
				break;
			}
		}
	}
}

void stager_log_callback(level,message)
     int level;
     char *message;
{
#ifdef STAGER_DEBUG
  /* In debug mode we always want to have all the messages in the stager log-file */
  sendrep(rpfd,MSG_ERR,"%s",message);
#else
  sendrep(rpfd,level == LOG_INFO ? MSG_OUT : MSG_ERR,"%s",message);
#endif
}

int stager_client_callback(tapereq,filereq)
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  char tmpbuf1[21];
  char tmpbuf2[21];
  char tmpbuf3[21];
  int compression_factor = 0;		/* times 100 */
  int stager_client_callback_i = -1;
  int i;
  struct stgcat_entry *stcp;
  char *castor_hsm;
  unsigned char blockid[4];
  struct Cns_fileid Cnsfileid;

  if (tapereq == NULL || filereq == NULL) {
    serrno = EINVAL;
    callback_error = 1;
    return(-1);
  }

#ifdef STAGER_DEBUG
  sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Received Callback for VID.FSEQ=%s.%d, Disk File No %d, filereq->cprc = %d, filereq->proc_status = %d, filereq->err.severity = %d, serrno = %d\n",
          tapereq->vid,
          filereq->tape_fseq,
          filereq->disk_fseq,
          filereq->cprc,
          filereq->proc_status,
          filereq->err.severity,
          serrno);
#endif
  /* We search the catalog entry corresponding to this filereq */
  for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
    if (strcmp(filereq->file_path,stcp->ipath) == 0) {
      stager_client_callback_i = i;
      break;
    }
  }

  if (stager_client_callback_i < 0 || stager_client_callback_i >= nbcat_ent) {
    serrno = SEINTERNAL;
    sendrep (rpfd, MSG_ERR, STG02, filereq->file_path, "rtcpc() ClientCallback",strerror(errno));
    callback_error = 1;
    return(-1);
  }

  SETEID(stcp->uid,stcp->gid);

  /* Search for castor a-la-unix path in the file to stagewrt */
  if ((castor_hsm = hsmpath(stcp)) == NULL) {
    sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
    callback_error = 1;
    return(-1);
  }
      
  /* Successful or end of tape */
  if ((filereq->cprc == 0 && filereq->proc_status == RTCP_FINISHED) ||
      (filereq->cprc <  0 && (filereq->err.severity & (RTCP_FAILED | RTCP_USERR)) == (RTCP_FAILED | RTCP_USERR) && filereq->err.errorcode == ETEOV)) {
    int tape_flag = filereq->cprc < 0 ? TAPE_FULL : (stager_client_callback_i == nbcat_ent - 1 ? 0 : TAPE_BUSY);

#ifdef STAGER_DEBUG
    sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] bytes_in = %s, bytes_out = %s, host_bytes = %s\n",
            u64tostr(filereq->bytes_in, tmpbuf1, 0),
            u64tostr(filereq->bytes_out, tmpbuf2, 0),
            u64tostr(filereq->host_bytes, tmpbuf3, 0));
#endif

    if (stager_client_callback_i == nbcat_ent - 1) {
      stagewrt_nomoreupdatetape = 1;
    }

    hsm_fsegsize[stager_client_callback_i] = filereq->bytes_in;
    {
      /* gcc compiler bug - fixed or forbidden register was spilled. */
      /* This may be due to a compiler bug or to impossible asm      */
      /* statements or clauses.                                      */
      u_signed64 dummyvalue;
      
      dummyvalue = hsm_fsegsize[stager_client_callback_i];
      hsm_transferedsize[stager_client_callback_i] += dummyvalue;
    }
    if (filereq->host_bytes > 0 && filereq->bytes_out > 0) {
      compression_factor = filereq->host_bytes * 100 / filereq->bytes_out;
    }
    Flags = tape_flag;
#ifdef STAGER_DEBUG
    sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling vmgr_updatetape(vid=%s,hsm_fsegsize[%d]=%s,compression_factor=%d,fileswriten=%d,Flags=%d)\n",
            tapereq->vid,
            stager_client_callback_i,
            u64tostr(hsm_fsegsize[stager_client_callback_i], tmpbuf1, 0),
            compression_factor,
            1,
            Flags);
#endif
    if (vmgr_updatetape(tapereq->vid,
                        hsm_fsegsize[stager_client_callback_i],
                        compression_factor,
                        1,
                        Flags
                        ) != 0) {
      sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
               sstrerror (serrno));
      callback_error = 1;
      return(-1);
    }
#ifdef STAGER_DEBUG
    sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling Cns_setsegattrs(%s,&Cnsfileid,copyrc=1,hsm_fseg[%d]=%d,hsm_fsegsize[%d]=%s,vid=%s,fseq=%d,blockid)\n",
            castor_hsm,
            stager_client_callback_i,
            hsm_fseg[stager_client_callback_i],
            stager_client_callback_i,
            u64tostr(hsm_fsegsize[stager_client_callback_i], tmpbuf1, 0),
            hsm_vid[stager_client_callback_i],
            hsm_fseq[stager_client_callback_i]);
#endif
    /* For the moment blockid is NOT used */
    memset(blockid,0,sizeof(blockid));
	strcpy(Cnsfileid.server,stcp->u1.h.server);
	Cnsfileid.fileid = stcp->u1.h.fileid;
    if (Cns_setsegattrs (castor_hsm,
                         &Cnsfileid,         /* Cns invariants */
                         1,
                         hsm_fseg[stager_client_callback_i],
                         hsm_fsegsize[stager_client_callback_i],
                         hsm_vid[stager_client_callback_i],
                         hsm_fseq[stager_client_callback_i],
                         blockid) != 0) {
      sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setsegattrs (with invariants)",
               sstrerror (serrno));
      callback_error = 1;
      return(-1);
    }
    if (hsm_transferedsize[stager_client_callback_i] >= hsm_totalsize[stager_client_callback_i]) {
#ifdef STAGER_DEBUG
      sendrep(rpfd, MSG_ERR, "[DEBUG-STAGEWRT/PUT-CALLBACK] Calling Cns_setfsize(%s,&Cnsfileid,size=%s)\n",
              castor_hsm,
              u64tostr(hsm_totalsize[stager_client_callback_i], tmpbuf1, 0));
#endif
      if (Cns_setfsize(castor_hsm,
                       &Cnsfileid,         /* Cns invariants */
                       hsm_totalsize[stager_client_callback_i]) != 0) {
        sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setfsize (with invariants)",
                 sstrerror (serrno));
        callback_error = 1;
        return(-1);
      }
    } else {
      hsm_fseg[stager_client_callback_i]++;
      hsm_fsegsize[stager_client_callback_i] = 0;
    }
  }

  return(0);
}

void init_nfsroot_and_hostname()
{
	extern char *getconfent _PROTO((char *, char *, int));
	int n = 0;
	char *p;

	if (p = getconfent ("RFIO", "NFS_ROOT", 0))
		strcpy (nfsroot, p);
	else
		nfsroot[0] = '\0';
	gethostname (hostname, CA_MAXHOSTNAMELEN + 1);
}


