/*
 * $Id: stager.c,v 1.28 2000/03/31 08:32:01 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* When filereq.maxsize will work removed, remove the define below and in the code below again */
/* #define SKIP_FILEREQ_MAXSIZE */

#ifndef lint
static char sccsid[] = "$RCSfile: stager.c,v $ $Revision: 1.28 $ $Date: 2000/03/31 08:32:01 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <sys/types.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
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

int Aflag;                          /* Allocation flag */
int nbcat_ent = -1;                 /* Number of catalog entries in stdin */
char hostname[CA_MAXHOSTNAMELEN+1]; /* Current hostname */
int reqid = 0;                      /* Request id */
int key;                            /* Request id associated key */
int rpfd = -1;                      /* Reply socket file descriptor */
struct stgcat_entry *stce = NULL;   /* End of stage catalog */
struct stgcat_entry *stcs = NULL;   /* Start of stage catalog */
char last_vid[CA_MAXVIDLEN+1];      /* Last vid returbed by vmgr_gettape or Cns_getsegattrs */
char vid[CA_MAXVIDLEN+1];           /* Vid returbed by vmgr_gettape or Cns_getsegattrs */
char vsn[CA_MAXVIDLEN+1];           /* Vsn returbed by vmgr_gettape or vmgr_querytape */
char dgn[CA_MAXDENLEN+1];           /* Dgn returbed by vmgr_gettape or vmgr_querytape */
char aden[CA_MAXDENLEN+1];          /* Aden returbed by vmgr_gettape or vmgr_querytape */
char lbltype[3];                    /* Lbltype returbed by vmgr_gettape or vmgr_querytape */
int fseq = -1;                      /* Fseq returbed by vmgr_gettape or vmgr_querytape */
unsigned int blockid = 0;           /* Blockid returbed by vmgr_gettape or vmgr_querytape */
char tmpbuf[21];                    /* Printout of u_signed64 quantity temp. buffer */
char *fid = NULL;                   /* File ID */
int nrtcpcreqs;                     /* Number of rtcpcreq structures in circular list */
tape_list_t **rtcpcreqs = NULL;     /* rtcp request itself (circular list) */

char castor_hsm_tokill[CA_MAXPATHLEN+1]; /* For cleanup (see RETURN macro) */
u_signed64 *hsm_totalsize = NULL;        /* Total size per hsm file */
u_signed64 *hsm_transferedsize = NULL;   /* Yet transfered size per hsm file */
int *hsm_fseg = NULL;                    /* Current segment */
u_signed64 *hsm_fsegsize = NULL;         /* Current segment size */
int *hsm_fseq = NULL;                    /* Current file sequence on current tape (vid) */
unsigned int *hsm_blockid = NULL;        /* Current blockid on current tape (vid) */
char **hsm_vid = NULL;                   /* Current vid pointer or NULL if not in current rtcpc request */
char putenv_string[CA_MAXHOSTNAMELEN + 10]; /* "CNS_HOST=%s" where %s max length is CA_MAXHOSTNAMELEN */
char cns_error_buffer[512];
char vmgr_error_buffer[512];
char stager_error_buffer[512];
char stager_output_buffer[512];

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

/* This macro returns TRUE is the file is an hpss one */
#define ISHPSS(xfile)   ( strncmp (xfile, "/hpss/"  , 6) == 0 || strstr (xfile, ":/hpss/"  ) != NULL)

/* This macro returns TRUE is the file is an castor one */
#define ISCASTOR(xfile) ( strncmp (xfile, "/castor/", 8) == 0 || strstr (xfile, ":/castor/") != NULL)

/* This macro returns the hpss file pointer, without hostname */
#define HPSSFILE(xfile)   strstr(xfile,"/hpss/")

/* This macro returns the castor file pointer, without hostname */
#define CASTORFILE(xfile) strstr(xfile,"/castor/")

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
	int ovl_status = -1;
	char func[16];

	strcpy (func, "stager");
	stglogit (func, "function entered\n");

	reqid = atoi (argv[1]);
	key = atoi (argv[2]);
	rpfd = atoi (argv[3]);
	nbcat_ent = atoi (argv[4]);
	nretry = atoi (argv[5]);
	Aflag = atoi (argv[6]);
#ifdef __INSURE__
	tmpfile = argv[7];
#endif

	/* Init if of crucial variables for the signal handler */
	vid[0] = '\0';
	castor_hsm_tokill[0] = '\0';

	if ((stcs = (struct stgcat_entry *) malloc (nbcat_ent * sizeof(struct stgcat_entry))) == NULL) {
		exit (SYERR);
	}
	stcp = stcs;

#ifdef __INSURE__
	if ((f = fopen(tmpfile,"r")) == NULL) {
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

	setgid (stcs->gid);
	setuid (stcs->uid);

	(void) umask (stcs->mask);

	signal (SIGINT, stagekilled);
	if (nretry) sleep (RETRYI);

	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

	if (stcs->t_or_d == 'd' ||
			(stcs->t_or_d == 'm' && ISHPSS(stcs->u1.m.xfile))) {
		int exit_code;

		exit_code = filecopy (stcs, key, hostname);
		/* stglogit (func, "filecopy exiting with status %x\n", exit_code & 0xFFFF); */
		ovl_status = (exit_code & 0xFF) ? SYERR : ((exit_code >> 8) & 0xFF);
		exit (ovl_status);
	}

	/* Redirect RTCOPY log message directly to user's console */
	rtcp_log = (void (*) _PROTO((int, CONST char *, ...))) &stager_usrmsg;

	if (stcs->t_or_d == 't') {
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGETAPE] GO ON WITH gdb /usr/local/bin/stager %d, then break stage_tape\n",getpid());
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGETAPE] sleep(10)\n");
		sleep(10);
#endif
		c = stage_tape ();
	} else {
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Setting Cns_errbuf and vmgr_errbuf\n");
		if (Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0 ||
			vmgr_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0 ||
			stage_setlog((void (*) _PROTO((int, char *))) &stager_log_callback) != 0) {
			sendrep(rpfd, MSG_ERR, "### Cannot set Cns or Vmgr API error buffer(s) or stager API callback function (%s)\n",sstrerror(serrno));
			exit(SYERR);
		}
#endif
		if (stcs->status == STAGEWRT || stcs->status == STAGEPUT) {
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] GO ON WITH gdb /usr/local/bin/stager %d, then break stagewrt_castor_hsm_file\n",getpid());
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] sleep(10)\n");
				sleep(10);
#endif
			c = stagewrt_castor_hsm_file ();
		} else {
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] GO ON WITH gdb /usr/local/bin/stager %d, then break stagein_castor_hsm_file\n",getpid());
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] sleep(10)\n");
				sleep(10);
#endif
			c = stagein_castor_hsm_file ();
		}
	}

	exit (c);
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
			/*
#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_OUT, "[DEBUG-XXX] Will set environment variable CNS_HOST=%s\n",host_hsm);
#endif
			strcpy(putenv_string,"CNS_HOST=");
			strcat(putenv_string,host_hsm);
			if (putenv(putenv_string) != 0) {
				sendrep(rpfd, MSG_ERR, "### putenv(%s) error in stager (%s)\n",putenv_string,strerror(errno));
				exit(SYERR);
			}
			*/
		} else {
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-XXX] Hsm Host %s is incompatible with a /castor file. Default CNS_HOST (from shift.conf) will apply\n",host_hsm);
#endif
		}
		end_host_hsm[0] = save_char;
	}
	return(castor_hsm);
}

int stagein_castor_hsm_file() {
	int Flags;
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

	/* We allocate as many size arrays */
	if ((hsm_totalsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))      == NULL ||
		(hsm_transferedsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64))) == NULL ||
		(hsm_fseg = (int *) calloc(nbcat_ent,sizeof(int)))                         == NULL ||
		(hsm_fsegsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64)))       == NULL ||
		(hsm_fseq = (int *) calloc(nbcat_ent,sizeof(int)))                         == NULL ||
		(hsm_blockid = (unsigned int *) calloc(nbcat_ent,sizeof(unsigned int)))    == NULL ||
		(hsm_vid = (char **) calloc(nbcat_ent,sizeof(char *)))                     == NULL) {
		sendrep (rpfd, MSG_ERR, STG02, "stagein_castor_hsm_file", "malloc error",strerror(errno));
		RETURN (USERR);
	}

	/* We initialize those size arrays */
	for (stcp = stcs, i = 0; stcp < stce; stcp++, i++) {
		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
			RETURN (USERR);
		}
		/* check permissions in parent directory, get file size */
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] Calling Cns_stat(%s,&statbuf)\n",castor_hsm);
#endif
		if (Cns_stat (castor_hsm, &statbuf) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_stat",
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
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_stat",
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
		/* Search for castor a-la-unix path in the file to stagein */
		if ((castor_hsm = hsmpath(stcp)) == NULL) {
			sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
			RETURN (USERR);
		}
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] %s : totalsize=%s, transferedsize=%s\n",
						castor_hsm,u64tostr(hsm_totalsize[i], tmpbuf1, 0),u64tostr(hsm_transferedsize[i], tmpbuf2, 0));
#endif
		if (hsm_totalsize[i] >  hsm_transferedsize[i]) {
#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] Calling Cns_getsegattrs(%s,copyrc=1,hsm_fseg[%d]=%d,&(hsm_fsegsize[%d]),vid,&(hsm_fseq[%d]),&(hsm_blockid[%d]))\n",
							castor_hsm,i,hsm_fseg[i],i,i,i);
#endif
				
			if (Cns_getsegattrs (castor_hsm,        /* hsm path */
								1,                  /* copy number */
								++(hsm_fseg[i]),    /* file segment */
								&(hsm_fsegsize[i]), /* segment size */
								vid,                /* segment vid */
								&(hsm_fseq[i]),     /* segment fseq */
								&(hsm_blockid[i])   /* segment blockid */
								)) {
				strcpy(vid,"");
				sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_getsegattrs",
								 sstrerror (serrno));
				RETURN (USERR);
			}
			
			hsm_vid[i] = vid;

#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] [%s] Got vid.fseq=%s.%d, last_vid=%s\n",
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

	if (new_tape != 0) {
		while (1) {
			/* Wait for the corresponding tape to be available */
#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] [%s] Calling vmgr_querytape(vid,vdn,dgn,aden,lbltype,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)\n",castor_hsm);
#endif
			if (vmgr_querytape (last_vid, vsn, dgn, aden, lbltype, NULL, NULL, NULL,
								NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) == 0) {
				strcpy(vid,last_vid);
				break;
			}
			strcpy(vid,"");
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "vmgr_querytape",
							 sstrerror (serrno));
			sendrep (rpfd, MSG_OUT, "%s : Retrying Volume Manager request in %d seconds\n", castor_hsm, RETRYI);
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
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] Calling rtcpc()\n");
		STAGER_RTCP_DUMP(rtcpcreqs[0]);
#endif
		rtcp_rc = rtcpc(rtcpcreqs[0]);
		if (rtcp_rc < 0) {
			int stagein_castor_hsm_file_retry = 0;
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
				} else if (rtcpcreqs[0]->tapereq.err.errorcode == ETVBSY) {
					/* This is *serious* error, most probably tape info inconsistency with, for ex., TMS */
					Flags |= DISABLED;
					sendrep(rpfd, MSG_ERR, STG02, vid, "rtcpc", "Tape information inconsistency - Contact responsibles");
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Flaging tape to DISABLED");
					free(stcs_tmp);
					RETURN (SYERR);
				} else if ((rtcpcreqs[0]->file[i].filereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV) {
					/* Reselect a server - we retry, though */
					stagein_castor_hsm_file_retry = 1;
				} else if (rtcpcreqs[0]->file[i].filereq.err.errorcode != ENOENT) {
					Flags |= DISABLED;
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc",sstrerror (serrno));
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Flaging tape to DISABLED");
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
					if ((castor_hsm = hsmpath(stcp)) == NULL) {
						sendrep (rpfd, MSG_ERR, STG02, stcp->u1.m.xfile, "hsmpath", sstrerror(serrno));
						RETURN (USERR);
					}
				}
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEIN] Calling Cns_setatime(%s)\n",castor_hsm);
#endif
				Cns_setatime (castor_hsm);
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
	unsigned int blockid;
	int compression_factor = 0;		/* times 100 */
	int Flags;
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

	/* We allocate as many size arrays */
	if ((hsm_totalsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64))) == NULL ||
			(hsm_transferedsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64))) == NULL ||
			(hsm_fseg = (int *) calloc(nbcat_ent,sizeof(int))) == NULL ||
			(hsm_fsegsize = (u_signed64 *) calloc(nbcat_ent,sizeof(u_signed64))) == NULL ||
			(hsm_fseq = (int *) calloc(nbcat_ent,sizeof(int))) == NULL ||
			(hsm_blockid = (unsigned int *) calloc(nbcat_ent,sizeof(unsigned int))) == NULL ||
			(hsm_vid = (char **) calloc(nbcat_ent,sizeof(char *))) == NULL) {
		sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "malloc error",strerror(errno));
		RETURN (USERR);
	}
		
	/* We initialize those size arrays */
	i = 0;
	for (stcp = stcs; stcp < stce; stcp++, i++) {
		if (rfio_stat (stcp->ipath, &statbuf) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_stat",
							 sstrerror (serrno));
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
				if (stcp->status == STAGEWRT || stcp->status == STAGEPUT) {
#ifdef STAGER_DEBUG
					sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling Cns_stat(%s,&statbuf_check)\n",castor_hsm);
#endif
					if (Cns_stat (castor_hsm, &statbuf_check) == 0) {
						if (statbuf_check.filesize > 0) {
							sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_stat",
											 "file already exists and is non-zero size");
							RETURN (USERR);
						}
					} else {
#ifdef STAGER_DEBUG
						sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling Cns_creat(%s,mode=0x%x)\n",
										castor_hsm,(int) statbuf.st_mode);
#endif
						/* Create the file */
						if (Cns_creat (castor_hsm, statbuf.st_mode) < 0) {
							sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_creat",
											 sstrerror (serrno));
							RETURN (USERR);
						}
					}
				}

		strncpy(castor_hsm_tokill,castor_hsm,CA_MAXPATHLEN);
		/* Makes sure it will in any case terminate with a null byte */
		castor_hsm_tokill[CA_MAXPATHLEN] = '\0';
			
		hsm_totalsize[i] = statbuf.st_size;
		hsm_transferedsize[i] = 0;
		hsm_fseg[i] = 1;
		hsm_fsegsize[i] = 0;
		hsm_fseq[i] = -1;
		hsm_blockid[i] = 0;
		hsm_vid[i] = NULL;

	gettape:
		Flags = 0;
		while (1) {
			char *tape_pool = NULL;

			if (stcp->poolname[0] != '\0') {
				/* Check the corresponding tape poolname */
				tape_pool = poolname2tapepool(stcp->poolname);
			}

			/* Wait for a free tape */
#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling vmgr_gettape(%s,%s,NULL,vid,vsn,dgn,aden,lbltype,&fseq,&blockid)\n",
							tape_pool != NULL ? tape_pool : "NULL", u64tostr((u_signed64) statbuf.st_size, tmpbuf, 0));
#endif
			if (vmgr_gettape (
								tape_pool,              /* poolname  (input - can be NULL) */
								(u_signed64) statbuf.st_size,  /* size      (input)        */
								NULL,                   /* condition (input - can be NULL) */
								vid,                    /* vid       (output)              */
								vsn,                    /* vsn       (output)              */
								dgn,                    /* dgn       (output)              */
								aden,                   /* density   (output)              */
								lbltype,                /* lbltype   (output)              */
								&fseq,                  /* fseq      (output)              */
								&blockid                /* blockid   (output)              */
								) == 0) {
				break;
			}
			/* Makes sure vid variable has not been overwritten... (to be checked with Jean-Philippe) */
			strcpy(vid,"");
			sendrep (rpfd, MSG_ERR, STG02, "", "vmgr_gettape",
							 sstrerror (serrno));
			sendrep (rpfd, MSG_OUT, "Retrying Volume Manager request in %d seconds\n", RETRYI);
			sleep(RETRYI);
		}
		/* From now on, "vid" is in status TAPE_BUSY */
			
		hsm_vid[i] = vid;
		hsm_fseq[i] = fseq;
		hsm_blockid[i] = blockid;

#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Use [vid,vsn,dgn,aden,lbltype,fseq,blockid]=[%s,%s,%s,%s,%s,%d,%d]\n",
						vid,vsn,dgn,aden,lbltype,fseq,(int) blockid);
#endif

		/* We "interrogate" for the number of structures */
		if (build_rtcpcreq(&nrtcpcreqs, NULL, stcs, stce, stcs, stce) != 0) {
			RETURN (USERR);
		}
		if (nrtcpcreqs <= 0) {
			serrno = SEINTERNAL;
			sendrep (rpfd, MSG_ERR, STG02, "stagewrt_castor_hsm_file", "cannot determine number of hsm files",sstrerror(serrno));
			RETURN (USERR);
		}

		/* By construction the number of rtcpcreqs is one => index 0 */

		/* Build the request from where we started (included) up to our next (excluded) */
		if (rtcpcreqs != NULL) {
			free_rtcpcreq(&rtcpcreqs);
			rtcpcreqs = NULL;
		}
		if (build_rtcpcreq(&nrtcpcreqs, &rtcpcreqs, stcp, stcp + 1, stcp, stcp + 1) != 0) {
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "build_rtcpcreq",sstrerror (serrno));
			Flags = 0;
			RETURN (SYERR);
		}
			
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling rtcpc()\n");
		STAGER_RTCP_DUMP(rtcpcreqs[0]);
#endif
			

		rtcp_rc = rtcpc(rtcpcreqs[0]);
		fseq_rtcp = rtcpcreqs[0]->file->filereq.tape_fseq;
			
#ifdef STAGER_DEBUG
		sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] rtcpc() status = %d, returned VID.FSEQ=%s.%d (asked for VID.FSEQ=%s.%d)\n",rtcp_rc,vid,fseq_rtcp,vid,fseq);
#endif
			
		if (rtcp_rc < 0) {
			/* rtcpc failed */
			sendrep (rpfd, MSG_ERR, STG02, vid, "rtcpc",
							 sstrerror (serrno));
			if (rtcpcreqs[0]->file->filereq.err.errorcode == ENOSPC) {
							sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Flaging tape to TAPE_FULL");
							Flags |= TAPE_FULL;
			} else if (rtcpcreqs[0]->file->filereq.err.errorcode == ETPARIT ||
											 rtcpcreqs[0]->file->filereq.err.errorcode == ETUNREC ||
											 rtcpcreqs[0]->tapereq.err.errorcode == ETPARIT ||
											 rtcpcreqs[0]->tapereq.err.errorcode == ETUNREC) {
							sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Flaging tape to DISABLED");
							Flags |= DISABLED;
						} else if (rtcpcreqs[0]->tapereq.err.errorcode == ETVBSY ||
											 rtcpcreqs[0]->file->filereq.err.errorcode == ENOENT) {
							/* Tape info very probably inconsistency with, for ex., TMS */
							sendrep(rpfd, MSG_ERR, STG02, vid, "rtcpc", "Tape information inconsistency - Contact responsibles");
							sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "rtcpc","Flaging tape to TAPE_FULL");
							Flags |= DISABLED;
						}
		}
			
		if (rtcp_rc == 0 || (Flags & TAPE_FULL) == TAPE_FULL) {
			/* OK or Accepted error */
			if (rtcpcreqs[0]->file->filereq.TEndPosition - rtcpcreqs[0]->tapereq.TStartRequest >= 0) {
				waiting_time += rtcpcreqs[0]->file->filereq.TEndPosition -
					rtcpcreqs[0]->tapereq.TStartRequest;
			}
			if (rtcpcreqs[0]->file->filereq.TEndTransferTape - rtcpcreqs[0]->file->filereq.TEndPosition >= 0) {
				transfer_time += rtcpcreqs[0]->file->filereq.TEndTransferTape -
					rtcpcreqs[0]->file->filereq.TEndPosition;
			}
			if (rtcpcreqs[0]->file->filereq.bytes_in > 0) {
				hsm_fsegsize[i] = rtcpcreqs[0]->file->filereq.bytes_in;
			}
			if (rtcpcreqs[0]->file->filereq.bytes_in > 0 && rtcpcreqs[0]->file->filereq.bytes_out > 0) {
				compression_factor = rtcpcreqs[0]->file->filereq.bytes_in * 100 /
					rtcpcreqs[0]->file->filereq.bytes_out;
			}
		}

	if (rtcp_rc == 0) {
			/* We are done */
#ifdef STAGER_DEBUG
			sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=%s,hsm_fsegsize[%d]=%s,compression_factor=%d,fileswriten=%d,Flags=%d)\n",
							vid,
							i,
							u64tostr(hsm_fsegsize[i], tmpbuf, 0),
							compression_factor,
							1,
							Flags);
#endif
			if (vmgr_updatetape (vid,hsm_fsegsize[i],compression_factor, 1, Flags) != 0) {
				sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
								 sstrerror (serrno));
			}
	} else {
			if ((Flags & TAPE_FULL) == TAPE_FULL) {
				/* Accepted error - we want another vid */
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=%s,hsm_fsegsize[%d]=%s,compression_factor=%d,fileswriten=%d,Flags=%d)\n",
								vid,
								i,
								u64tostr(hsm_fsegsize[i], tmpbuf, 0),
								compression_factor,
								1,
								Flags);
#endif
				if (vmgr_updatetape (vid,hsm_fsegsize[i],compression_factor,1, Flags) != 0) {
					sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
									 sstrerror (serrno));
				}
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling Cns_setsegattrs(%s,copyrc=1,hsm_fseg[%d]=%d,hsm_fsegsize[%d]=%s,vid=%s,fseq=%d,blockid=%d)\n",
								castor_hsm,i,hsm_fseg[i],i,u64tostr(hsm_fsegsize[i], tmpbuf, 0), vid, fseq_rtcp, blockid);
#endif
				if (Cns_setsegattrs (castor_hsm, 1, hsm_fseg[i], hsm_fsegsize[i], vid, fseq_rtcp, blockid) != 0) {
					sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setsegattrs",
									 sstrerror (serrno));
				}
				hsm_fseg[i]++;
			} else {
				/* Unrecoverable error - we try again */
#ifdef STAGER_DEBUG
				sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling vmgr_updatetape(vid=%s,hsm_fsegsize[%d]=0,compression_factor=0,fileswriten=%d,Flags=%d)\n",
						vid,
						i,
						0,
						Flags);
#endif
				hsm_fsegsize[i] = 0;
				if (vmgr_updatetape (vid,
														 hsm_fsegsize[i], 0, 0, Flags) != 0) {
					sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
									 sstrerror (serrno));
				}
			}
			goto gettape;
	}

#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling Cns_setsegattrs(%s,copyrc=1,hsm_fseg[%d]=%d,hsm_fsegsize[%d]=%s,vid=%s,fseq=%d,blockid=%d)\n",
						castor_hsm,i,hsm_fseg[i],i,u64tostr(hsm_fsegsize[i], tmpbuf, 0), vid, fseq_rtcp, blockid);
#endif
	if (Cns_setsegattrs (castor_hsm, 1, hsm_fseg[i], hsm_fsegsize[i], vid, fseq_rtcp, blockid) != 0) {
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setsegattrs",
							 sstrerror (serrno));
			RETURN (USERR);
	}
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling Cns_setfsize(%s,size=%d)\n",
						castor_hsm,(int) statbuf.st_size);
#endif
	if (Cns_setfsize (castor_hsm, statbuf.st_size) != 0) {
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm, "Cns_setfsize",
							 sstrerror (serrno));
			RETURN (USERR);
	}
	
	castor_hsm_tokill[0] = '\0';
	}
	RETURN (0);
}

int stage_tape() {
	int rtcp_rc;

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
	sendrep(rpfd, MSG_OUT, "[DEBUG-STAGEWRT/PUT] Calling rtcpc()\n");
	STAGER_RTCP_DUMP(rtcpcreqs[0]);
#endif

	rtcp_rc = rtcpc(rtcpcreqs[0]);

	if (rtcp_rc < 0) {
      int stage_tape_retry_flag = 0;
      int i;

      sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc", sstrerror (serrno));
      switch (rtcpcreqs[0]->tapereq.err.errorcode) {
      case ETPARIT:
      case ETUNREC:
        sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Tape should be disabled - Contact responsibles");
        RETURN (SYERR);
        break;
      case ETVBSY:
        sendrep(rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc", "Tape information inconsistency - Contact responsibles");
        RETURN (SYERR);
        break;
      default:
        break;
      }

      for (i = 0; i < nbcat_ent; i++) {
        if (rtcpcreqs[0]->file[i].filereq.err.errorcode == ETPARIT || 
            rtcpcreqs[0]->file[i].filereq.err.errorcode == ETUNREC) {
          sendrep (rpfd, MSG_ERR, STG02, rtcpcreqs[0]->tapereq.vid, "rtcpc","Tape should be disabled - Contact responsibles");
          RETURN (SYERR);
          break;
        } else if ((rtcpcreqs[0]->file[i].filereq.err.severity & RTCP_RESELECT_SERV) == RTCP_RESELECT_SERV) {
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

	if (stcp->t_or_d == 'm' && ISHPSS(stcp->u1.m.xfile)) {
			/* filecopy of an HPSS file */
			struct stat filemig_stat;
			
			if (! ((stcp->status == STAGEWRT) || (stcp->status == STAGEPUT))) {
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
		/* sendrep (rpfd, MSG_OUT, "filecopy : internal path setted to %s\n", stcp->ipath); */
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
	if ((stcp->status == STAGEWRT) || (stcp->status == STAGEPUT)) {
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
	/* stglogit (func, "execing command : %s\n", command); */
	setgid (stcp->gid);
	setuid (stcp->uid);
	rf = rfio_popen (command, "r");
	if (rf == NULL) {
		/* stglogit (func, "%s : %s\n", command, rfio_serror()); */
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
		sendrep (rpfd, MSG_OUT, "[DEBUG-CLEANUP] Calling vmgr_updatetape(%s,0,0,0,0)\n",vid);
#endif
		if (vmgr_updatetape (vid, (u_signed64) 0, 0, 0, 0) != 0) {
			sendrep (rpfd, MSG_ERR, STG02, vid, "vmgr_updatetape",
							 sstrerror (serrno));
		}
	}
	if (castor_hsm_tokill[0] != '\0' && stcs->status == STAGEWRT) {
#ifdef STAGER_DEBUG
		sendrep (rpfd, MSG_OUT, "[DEBUG-CLEANUP] Calling Cns_unlink(%s)\n",castor_hsm_tokill);
#endif
		if (Cns_unlink(castor_hsm_tokill) != 0) {
			sendrep (rpfd, MSG_ERR, STG02, castor_hsm_tokill, "Cns_unlink",
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
				strcpy((*rtcpcreqs_in)[i]->tapereq.vsn      , stcp->u1.t.vsn[ivid]);
				strcpy((*rtcpcreqs_in)[i]->tapereq.label    , stcp->u1.t.lbl      );
				strcpy((*rtcpcreqs_in)[i]->tapereq.dgn      , stcp->u1.t.dgn      );
				strcpy((*rtcpcreqs_in)[i]->tapereq.density  , stcp->u1.t.den      );
				strcpy((*rtcpcreqs_in)[i]->tapereq.server   , stcp->u1.t.tapesrvr );
				switch (stcp->status) {
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
				int isconcat = 0;

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
					if ((fixed_stce - 1)->u1.t.fseq[strlen((fixed_stce - 1)->u1.t.fseq) - 1] == '-') {
						isconcat = 1;
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
					strcpy (fl[j].filereq.fid, stcp->u1.t.fid);
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
					if (stcp->u1.t.E_Tflags & SKIPBAD) {
						fl[j].filereq.rtcp_err_action |= SKIPBAD;
					}
					if (stcp->u1.t.E_Tflags & KEEPFILE) {
						fl[j].filereq.rtcp_err_action |= KEEPFILE;
					}
					if (stcp->u1.t.E_Tflags & IGNOREEOI) {
						fl[j].filereq.tp_err_action |= IGNOREEOI;
					}
					if (stcp->u1.t.E_Tflags & NOTRLCHK) {
						fl[j].filereq.tp_err_action |= NOTRLCHK;
					}
					if (stcp->charconv > 0) {
						fl[j].filereq.convert = stcp->charconv;
					}
					switch (stcp->status) {
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
					if (trailing2 == '-') {
						if ((nbcat_ent >= nbtpf) && (j == nbcat_ent - 1)) {
							fl[j].filereq.concat = CONCAT_TO_EOD;
						} else {
							fl[j].filereq.concat = NOCONCAT_TO_EOD;
						}
					} else {
						if (fixed_stcs->status == STAGEWRT || fixed_stcs->status == STAGEPUT) {
							if (j >= nbtpf) {
								fl[j].filereq.concat = (isconcat != 0 ? CONCAT_TO_EOD : CONCAT);
							} else {
								fl[j].filereq.concat = (isconcat != 0 ? NOCONCAT_TO_EOD : NOCONCAT);
							}
						} else {
							if (j >= nbcat_ent) {
								fl[j].filereq.concat = (isconcat != 0 ? CONCAT_TO_EOD : CONCAT);
							} else {
								fl[j].filereq.concat = (isconcat != 0 ? NOCONCAT_TO_EOD : NOCONCAT);
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
			strcpy((*rtcpcreqs_in)[i]->tapereq.vsn     ,vsn    );
			strcpy((*rtcpcreqs_in)[i]->tapereq.label   ,lbltype);
			strcpy((*rtcpcreqs_in)[i]->tapereq.dgn     ,dgn    );
			strcpy((*rtcpcreqs_in)[i]->tapereq.density ,aden   );
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

				if ((dummy = (file_list_t *) realloc((*rtcpcreqs_in)[i]->file,(nfile_list + 1) * sizeof(file_list_t))) == NULL) {
					sendrep (rpfd, MSG_ERR, STG02, "build_rtcpcreq", "realloc",strerror(errno));
					serrno = SEINTERNAL;
					return(-1);
				}
				(*rtcpcreqs_in)[i]->file = dummy;
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
			strcpy ((*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.fid, fid);
			if (blockid) {
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.position_method = TPPOSIT_BLKID;
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.blockid = hsm_blockid[ihsm];
			} else {
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.position_method = TPPOSIT_FSEQ;
				(*rtcpcreqs_in)[i]->file[nfile_list-1].filereq.tape_fseq = hsm_fseq[ihsm];
			}
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
			switch (stcp->status) {
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
  sendrep(rpfd,level,"%s",message);
}
