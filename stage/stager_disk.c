/*
 * $Id: stager_disk.c,v 1.11 2002/09/30 16:55:25 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#define USE_SUBREQID

#ifdef STAGE_CSETPROCNAME
#define STAGE_CSETPROCNAME_FORMAT_DISK "%s %s %s"
#include "Csetprocname.h"
#endif

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_disk.c,v $ $Revision: 1.11 $ $Date: 2002/09/30 16:55:25 $ CERN IT-PDP/DM Jean-Damien Durand";
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
#include "rfio_api.h"
#include "serrno.h"
#include "stage_api.h"
#include "Castor_limits.h"
#include "u64subr.h"
#include "osdep.h"
#include "Cpwd.h"
#include "Cgrp.h"

EXTERN_C int DLL_DECL rfio_parseln _PROTO((char *, char **, char **, int));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int *, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());
extern char *stglogflags _PROTO((char *, char *, u_signed64));

int filecopy _PROTO((struct stgcat_entry *, int, char *));
void cleanup _PROTO(());
void stagekilled _PROTO(());
void init_hostname _PROTO(());
int get_subreqid _PROTO((struct stgcat_entry *));
void stager_log_callback _PROTO((int, char *));

char func[16];                      /* This executable name in logging */
int Aflag;                          /* Allocation flag */
int api_flag;                       /* Api flag, .e.g we will NOT re-arrange the file sequences in case of a tape request */
u_signed64 api_flags;               /* Api flags themselves */
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
struct passwd start_passwd;
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
	int l;
	int nretry;
#ifdef STAGER_DEBUG
	int thisrpfd;
#endif
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
	struct passwd *this_passwd;
	struct passwd root_passwd;
#ifdef __INSURE__
	char *tmpfile;
	FILE *f;
#endif
	int exit_code = 0;

	strcpy (func, "stager_disk");
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
#ifdef __INSURE__
	tmpfile = argv[12];
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] tmpfile = %s\n", tmpfile);
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
			dump_stcp(&rpfd, stcs + i, &sendrep);
        }
	}
#endif

#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(&rpfd, MSG_ERR, "[DEBUG] GO ON WITH gdb /usr/local/bin/stager_disk %d, then break %d\n",getpid(),__LINE__ + 6);
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

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcs->t_or_d != 'd' && stcs->t_or_d != 'm') {
			SAVE_EID;
			sendrep(&rpfd, MSG_ERR, "### HSM file is of unvalid type ('%c')\n",stcp->t_or_d);
			RESTORE_EID;
			free(stcs);
			exit(USERR);
		}
	}

	(void) umask (stcs->mask);

	signal (SIGINT, stagekilled);        /* If client died */
	signal (SIGTERM, stagekilled);       /* If killed from administrator */
	if (nretry) sleep (RETRYI);

	/* We always set stager logger callback because we will use stage API in filecopy() */
#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(&rpfd, MSG_ERR, "[DEBUG-STAGEIN/WRT/PUT] Setting stage_setlog()\n");
	RESTORE_EID;
#endif
	if (stage_setlog((void (*) _PROTO((int, char *))) &stager_log_callback) != 0) {
		SAVE_EID;
		sendrep(&rpfd, MSG_ERR, "### Cannot set stager API callback function (%s)\n",sstrerror(serrno));
		RESTORE_EID;
		free(stcs);
		exit(SYERR);
	}

    /* -------- DISK TO DISK OR HPSS MIGRATION ----------- */

	for (stcp = stcs; stcp < stce; stcp++) {
#ifdef STAGE_CSETPROCNAME
		if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_DISK,
						sav_argv0,
						"STARTING",
						stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.d.xfile
						) != 0) {
			stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
		}
#endif
		if ((exit_code = filecopy(stcp, key, hostname)) != 0) {
			free(stcs);
			exit((exit_code >> 8) & 0xFF);
		}
	}
	free(stcs);
	exit(0);
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

	SETTAPEEID(stcs->uid,stcs->gid);

	/*
	 * @@@ TO BE MOVED TO cpdskdsk.sh @@@
	 */

	if (! stcp->ipath[0]) {
		/* Deferred allocation (Aflag) */
		sprintf (stageid, "%d.%d@%s", reqid, key, hostname);
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "Calling stage_updc_tppos(stageid=%s,...)\n",stageid);
		RESTORE_EID;
#endif
#ifdef STAGE_CSETPROCNAME
		if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_DISK,
						sav_argv0,
						"ASKING SPACE FOR",
						stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.d.xfile
						) != 0) {
			stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
		}
#endif
		if (stage_updc_tppos (
								stageid,                 /* Stage ID      */
								use_subreqid != 0 ? get_subreqid(stcp) : -1, /* subreqid      */
								-1,                      /* Copy rc       */
								-1,                      /* Blocksize     */
								NULL,                    /* drive         */
								NULL,                    /* fid           */
								0,                       /* fseq          */
								0,                       /* lrecl         */
								NULL,                    /* recfm         */
								stcp->ipath              /* path          */
								) != 0) {
			SAVE_EID;
			sendrep (&rpfd, MSG_ERR, STG02, stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.d.xfile,
					"stage_updc_tppos", sstrerror (serrno));
			RESTORE_EID;
			return(serrno << 8); /* We simulate the output from rfio_pclose() */
		}
#ifdef STAGER_DEBUG
		SAVE_EID;
		sendrep (&rpfd, MSG_ERR, "[DEBUG-FILECOPY] Internal path setted to %s\n", stcp->ipath);
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
	if (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)) {
		char tmpbuf[21];
		u64tostr((u_signed64) stcp->actual_size, tmpbuf, 0);
		sprintf (command+strlen(command), " -s %s",tmpbuf);
		sprintf (command+strlen(command), " '%s'", stcp->ipath);
		if (stcp->t_or_d == 'm')
			sprintf (command+strlen(command), " '%s'", stcp->u1.m.xfile);
		else
			sprintf (command+strlen(command), " '%s'", stcp->u1.d.xfile);
	} else {
		/* Reading file */
		if (stcp->size) {
			char tmpbuf[21];
			u64tostr(stcp->size, tmpbuf, 0);
			sprintf (command+strlen(command), " -s %s", tmpbuf);
		}
		if (stcp->t_or_d == 'm')
			sprintf (command+strlen(command), " '%s'", stcp->u1.m.xfile);
		else
			sprintf (command+strlen(command), " '%s'", stcp->u1.d.xfile);	    
		sprintf (command+strlen(command), " '%s'", stcp->ipath);
	}
#ifdef STAGER_DEBUG
	SAVE_EID;
	sendrep(&rpfd, MSG_ERR, "[DEBUG-FILECOPY] Execing %s\n",command);
	RESTORE_EID;
#else
	SAVE_EID;
	stglogit (func, "execing command : %s\n", command);
	RESTORE_EID;
#endif

#ifdef STAGE_CSETPROCNAME
	if (Csetprocname(STAGE_CSETPROCNAME_FORMAT_DISK,
					sav_argv0,
					"COPYING",
					stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.d.xfile
					) != 0) {
		stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif

	SETID(stcp->uid,stcp->gid);
	PRE_RFIO;
	rf = rfio_popen (command, "r");
	if (rf == NULL) {
		/* This way we will sure that it will be logged... */
		sendrep(&rpfd, MSG_ERR, "STG02 - %s : %s\n", command, rfio_serror());
		return(rfio_serrno() << 8); /* We simulate the output from rfio_pclose() */
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
		sendrep(&rpfd, MSG_ERR, "STG02 - %s : error reported at %s time : status 0x%x (%s)\n", "filecopy", "rfio_pclose", c, sstrerror((c >> 8) & 0xFF));
	}
	return(c); /* This is the output of rfio_pclose, e.g. status in the higher byte */
}

void cleanup() {
}

void stagekilled() {
	cleanup();
	exit (REQKILD);
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

void stager_log_callback(level,message)
     int level;
     char *message;
{
	SAVE_EID;
#ifdef STAGER_DEBUG
	/* In debug mode we always want to have all the messages in the stager log-file */
	sendrep(&rpfd,MSG_ERR,"%s",message);
#else
	/* In migration mode we want to make sure that everything is logged */
	sendrep(&rpfd, (level == LOG_INFO) ? MSG_OUT : MSG_ERR,"%s",message);
#endif
	RESTORE_EID;
}

