/*
 * $Id: stage_api.c,v 1.4 2007/02/21 11:28:01 sponcec3 Exp $
 */

#include <stdlib.h>            /* For malloc(), etc... */
#include <stddef.h>            /* For NULL definition */
#include <string.h>
#include <errno.h>             /* For EINVAL */
#if defined(_WIN32)
#define W_OK 2
#else
#include <unistd.h>
#endif
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif

#include "osdep.h"             /* For OS dependencies */
#include "serrno.h"            /* For CASTOR's errno */
#include "Castor_limits.h"     /* Get all hardcoded CASTOR constants */
#include "marshall.h"          /* For marshalling macros */
#include "Cpwd.h"              /* For CASTOR password functions */
#include "Cgrp.h"              /* For CASTOR group functions */
#include "rfio_api.h"          /* RFIO API */
#include "u64subr.h"           /* u_signed 64 conversion routines */
#include "Cglobals.h"          /* To get CthreadId */
#include "stage_api.h"
#include "rtcp_constants.h"    /* For EBCCONV, FIXVAR, SKIPBAD, KEEPFILE */
#include "Ctape_constants.h"   /* For IGNOREEOI, WRITE_ENABLE and WRITE_DISABLE */
#if VMGR
#include "vmgr_api.h"          /* For vmgrcheck() */
#endif
#include "net.h"

#ifdef hpux
static int DLL_DECL stageinit _PROTO(());
static int DLL_DECL stageshutdown _PROTO(());
#else
static int DLL_DECL stageinit _PROTO((u_signed64, char *));
static int DLL_DECL stageshutdown _PROTO((u_signed64, char *));
#endif

extern char *getenv();         /* To get environment variables */
extern char *getconfent();     /* To get configuration entries */
extern int rfio_access _PROTO((const char *, int));

#if TMS
static int stage_api_tmscheck _PROTO((char *, char *, char *, char *, char *));
EXTERN_C int DLL_DECL sysreq _PROTO((char *, char *, int *, char *, int *));
#endif
#if VMGR
static char vmgr_error_buffer[512];        /* Vmgr error buffer */
static int stage_api_vmgrcheck _PROTO((char *, char *, char *, char *, char *, int));
#endif

#define STVAL2BUF(stcp,member,option,buf,bufsize) {                                       \
  char tmpbuf1[21 + 4];                                                                   \
  char tmpbuf2[21];                                                                       \
  if ((stcp)->member != 0) {                                                              \
    sprintf(tmpbuf1," -%c %s",option,u64tostr((u_signed64) (stcp)->member, tmpbuf2, 0));  \
    if ((strlen(buf) + strlen(tmpbuf1) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    strcat(buf,tmpbuf1);                                                                  \
  }                                                                                       \
}

#define STVAL2BUF_V2(stcp,member,option,buf,bufsize) {                                    \
  char tmpbuf1[21+1];                                                                     \
  char tmpbuf2[21];                                                                       \
  if ((stcp)->member != 0) {                                                              \
    sprintf(tmpbuf1," %s",u64tostr((u_signed64) (stcp)->member, tmpbuf2, 0));             \
    if ((strlen(buf) + strlen(option) + 1 + strlen(tmpbuf1) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    strcat(buf,option);                                                                   \
    strcat(buf,tmpbuf1);                                                                  \
  }                                                                                       \
}

/* V3 macro == V2 macro except that V3 logs arguments >= 0 while V2 logs when it is != 0 */
#define STVAL2BUF_V3(stcp,member,option,buf,bufsize) {                                    \
  char tmpbuf1[21+1];                                                                     \
  char tmpbuf2[21];                                                                       \
  if ((stcp)->member >= 0) {                                                              \
    sprintf(tmpbuf1," %s",u64tostr((u_signed64) (stcp)->member, tmpbuf2, 0));             \
    if ((strlen(buf) + strlen(option) + 1 + strlen(tmpbuf1) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    strcat(buf,option);                                                                   \
    strcat(buf,tmpbuf1);                                                                  \
  }                                                                                       \
}

#define STSTR2BUF(stcp,member,option,buf,bufsize) {                                       \
  if ((stcp)->member[0] != '\0') {                                                        \
    if ((strlen(buf) + 4 + strlen((stcp)->member) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    sprintf(buf + strlen(buf)," -%c %s",option,(stcp)->member);                           \
  }                                                                                       \
}

#define STSTR2BUF_V2(stcp,member,option,buf,bufsize) {                                    \
  if ((stcp)->member[0] != '\0') {                                                        \
    if ((strlen(buf) + strlen(option) + 1 + strlen((stcp)->member) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    strcat(buf,option);                                                                   \
    strcat(buf, " ");                                                                     \
    strcat(buf,(stcp)->member);                                                           \
  }                                                                                       \
}

#define STR2BUF(value,option,buf,bufsize) {                                               \
  if (value[0] != '\0') {                                                                 \
    if ((strlen(buf) + 4 + strlen(value) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    sprintf(buf + strlen(buf)," -%c %s",option,value);                                    \
  }                                                                                       \
}

#define STCHR2BUF(stcp,member,option,buf,bufsize) {                                       \
  if ((stcp)->member != 0) {                                                              \
    if ((strlen(buf) + 4) > bufsize) { serrno = SEUMSG2LONG; return(-1); }                \
    sprintf(buf + strlen(buf)," -%c",option);                                             \
  }                                                                                       \
}

#define STFLAG2BUF(flags,flag,option,buf,bufsize) {                                       \
  if (((flags) & (flag)) == (flag)) {                                                     \
    if ((strlen(buf) + 4) > bufsize) { serrno = SEUMSG2LONG; return(-1); }                \
    sprintf(buf + strlen(buf)," -%c",option);                                             \
  }                                                                                       \
}

#define STFLAG2BUF_V2(flags,flag,option,buf,bufsize) {                                    \
  if (((flags) & (flag)) == (flag)) {                                                     \
    if ((strlen(buf) + strlen(option) + 2) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    sprintf(buf + strlen(buf)," %s",option);                                              \
  }                                                                                       \
}

#define STFLAGVAL2BUF(flags,flag,option,value,buf,bufsize) {                              \
  if (((flags) & (flag)) == (flag)) {                                                     \
    if ((strlen(buf) + 4 + strlen(value) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    sprintf(buf + strlen(buf)," -%c %s",option,value);                                    \
  }                                                                                       \
}

#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif /* _WIN32 */

int DLL_DECL rc_castor2shift(rc)
	int rc;
{
	int arc; /* Absolute rc */

	/* Input  is a CASTOR return code (routine error code) */
	/* Output is a SHIFT  return code (process exit code) */

	/* If input is < 0 then the mapping is done on -rc, and -rc is */
	/* is returned if no match */

	if (rc < 0) {
		arc = -rc;
	} else {
		arc = rc;
	}

	switch (arc) {
	case 0:
		return(0);
	case ETHELD:
		return(ETHELDERR);
	case ERTBLKSKPD:
		return(BLKSKPD);
	case ERTTPE_LSZ:
		return(TPE_LSZ);
	case ERTMNYPARY:
	case ETPARIT:
		return(MNYPARI);
	case ERTLIMBYSZ:
		return(LIMBYSZ);
	case EACCES:
	case EINVAL:
	case ENOENT:
	case EPERM:
	case SENAMETOOLONG:
	case SENOSHOST:
		return(USERR);
	case SESYSERR:
	case SEOPNOTSUP:
	case SECONNDROP:
	case SECOMERR:
	case SETIMEDOUT:
		return(SYERR);
	case ESTNACT:
		return(SHIFT_ESTNACT);
	case ESTKILLED:
		return(REQKILD);
	case ESTCLEARED:
		return(CLEARED);
	case ESTCONF:
		return(CONFERR);
	case ESTLNKNSUP:
		return(LNKNSUP);
	case ECUPVNACT:
		return(SHIFT_ECUPVNACT);
	case SECHECKSUM:
		return(CHECKSUMERR);
	case ENOUGHF:
		return(ENOUGHF);
	case ENOSPC:
		return(ENOSPC);
	default:
		if (rc < 0) {
			return(arc);
		} else {
			return(UNERR);
		}
	}
}

int DLL_DECL stage_iowc (int req_type,
                         char t_or_d,
                         u_signed64 flags,
                         int openflags,
                         mode_t openmode,
                         char *hostname,
                         char *pooluser,
                         int nstcp_input,
                         struct stgcat_entry *stcp_input,
                         int *nstcp_output,
                         struct stgcat_entry **stcp_output,
                         int nstpp_input,
                         struct stgpath_entry *stpp_input)
{
	struct stgcat_entry *thiscat; /* Catalog current pointer */
	struct stgpath_entry *thispath; /* Path current pointer */
	int istcp;                    /* Counter on catalog structures passed in the protocol */
	int istpp;                    /* Counter on path structures passed in the protocol */
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	struct group *gr;             /* Group entry */

	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	uid_t Geuid;                  /* Forced effective uid (-G option) */
	char Gname[CA_MAXUSRNAMELEN+1]; /* Forced effective name (-G option) */
	char User[CA_MAXUSRNAMELEN+1];  /* Forced internal path with user (-u options) */
	gid_t egid;                   /* Current effective gid */
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int mask;                     /* User mask */
#else
	mode_t mask;                  /* User mask */
#endif
	int pid;                      /* Current pid */
	int status;                   /* Variable overwritten by macros in header */
	int build_linkname_status;    /* Status of build_linkname() call */
	char user_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	int c;                        /* Output of build_linkname() */
	static char cmdnames_api[4][10] = {"stage_cat", "stage_in", "stage_out", "stage_wrt"};
	char *func;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char stgpool_forced[CA_MAXPOOLNAMELEN + 1];
	int Tid;
	u_signed64 uniqueid;
	int maxretry = MAXRETRY;
	int magic_client;
	int magic_server;
	int nm;
	int nh;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_client = magic_server = stage_stgmagic();
  
  _stage_iowc_retry:
	ntries = nstg161 = Tid = 0;
	uniqueid = 0;
	nm = nh = 0;

	/* It is not allowed to have no input */
	if (nstcp_input <= 0) {
		serrno = EINVAL;
		return(-1);
	}

	if (stcp_input == NULL) {
		serrno = EFAULT;
		return(-1);
	}

	/* We check existence of an STG POOL from environment variable or configuration */
	if ((p = getenv("STAGE_POOL")) != NULL) {
		strncpy(stgpool_forced,p,CA_MAXPOOLNAMELEN);
		stgpool_forced[CA_MAXPOOLNAMELEN] = '\0';
	} else if ((p = getconfent("STG","POOL",0)) != NULL) {
		strncpy(stgpool_forced,p,CA_MAXPOOLNAMELEN);
		stgpool_forced[CA_MAXPOOLNAMELEN] = '\0';
	} else {
		stgpool_forced[0] = '\0';
	}

	switch (req_type) {                   /* This method works only for: */
	case STAGE_IN:                        /* - stagein */
		func = cmdnames_api[1];
		break;
	case STAGE_OUT:                       /* - stageout */
		func = cmdnames_api[2];
		break;
	case STAGE_WRT:                       /* - stagewrt */
		func = cmdnames_api[3];
		break;
	case STAGE_CAT:                       /* - stagecat */
		func = cmdnames_api[0];
		break;
	default:
		serrno = EINVAL;                    /* Otherwise req_type is an invalid parameter */
		return(-1);
	}

	euid = Geuid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	/* It is not allowed to have stcp_output != NULL and nstcp_output == NULL */
	if ((stcp_output != NULL) && (nstcp_output == NULL)) {
		serrno = EFAULT;
		return(-1);
	}

	/* It is not allowed to have stpp_input == NULL and nstpp_input > 0 */
	if ((stpp_input == NULL) && (nstpp_input > 0)) {
		serrno = EFAULT;
		return(-1);
	}

	/* It is not allowed to have stpp_input != NULL and nstpp_input <= 0 */
	if ((stpp_input != NULL) && (nstpp_input <= 0)) {
		serrno = EFAULT;
		return(-1);
	}

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {  /* User want to overwrite euid under which request is processed by stgdaemon */
		if ((gr = Cgetgrgid(egid)) == NULL) { /* This is allowed only if its group exist */
			if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetgrgid", strerror(errno));
			stage_errmsg(func, STG36, egid);
			serrno = ESTGROUP;
			return(-1);
		}
		if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) { /* And if GRPUSER is configured */
			stage_errmsg(func, STG10, gr->gr_name);
			serrno = ESTGRPUSER;
			return(-1);
		} else {
			strcpy (Gname, p);
			if ((pw = Cgetpwnam(p)) == NULL) { /* And if GRPUSER content is a valid user name */
				if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwnam", strerror(errno));
				stage_errmsg(func, STG11, p);
				serrno = SEUSERUNKN;
				return(-1);
			} else
				Geuid = pw->pw_uid;              /* In such a case we grab its uid */
		}
	}

	/* If no user specified we check environment variable STAGE_USER */
	if (((pooluser == NULL) || (pooluser[0] == '\0')) && (p = getenv ("STAGE_USER")) != NULL) {
		strncpy(User,p,CA_MAXUSRNAMELEN);
		User[CA_MAXUSRNAMELEN] = '\0';
		/* We verify this user login is defined */
		if (((pw = Cgetpwnam(User)) == NULL) || (pw->pw_gid != egid)) {
			if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwnam", strerror(errno));
			stage_errmsg(func, STG11, User);
			serrno = SEUSERUNKN;
			return(-1);
		}
	} else if ((pooluser != NULL) && (pooluser[0] != '\0')) {
		strncpy(User,pooluser,CA_MAXUSRNAMELEN);
		User[CA_MAXUSRNAMELEN] = '\0';
	} else {
		User[0] = '\0';
	}


	switch (t_or_d) {                     /* This method supports only */
	case 't':                             /* - tape files */
	case 'd':                             /* - disk files */
	case 'a':                             /* - allocated files */
	case 'm':                             /* - non-CASTOR HSM files */
	case 'h':                             /* - CASTOR HSM files */
		break;
	default:
		serrno = EINVAL;                    /* Otherwise t_or_d is an invalid parameter */
		return(-1);
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {
		sendbuf_size += strlen(Gname) + 1;     /* Name under which request is to be done */
	} else {
		sendbuf_size += strlen(pw->pw_name) + 1;
	}
	sendbuf_size += LONGSIZE;                /* Uid */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += LONGSIZE;                /* Mask */
	sendbuf_size += LONGSIZE;                /* Pid */
	sendbuf_size += HYPERSIZE;               /* Our uniqueid  */
	sendbuf_size += strlen(User) + 1;        /* User for internal path (default to STAGERSUPERUSER in stgdaemon) */
	sendbuf_size += HYPERSIZE;               /* Flags */
	sendbuf_size += LONGSIZE;                /* openflags */
	sendbuf_size += LONGSIZE;                /* openmode */
	sendbuf_size += BYTESIZE;                /* t_or_d */
	sendbuf_size += LONGSIZE;                /* Number of catalog structures */
	sendbuf_size += LONGSIZE;                /* Number of path structures */
	for (istcp = 0; istcp < nstcp_input; istcp++) {
		int i, numvid, numvsn;

		thiscat = &(stcp_input[istcp]);              /* Current catalog structure */

		/* If this entry has no poolname we copy it the forced one */
		if ((thiscat->poolname[0] == '\0') && (stgpool_forced[0] != '\0')) {
			strcpy(thiscat->poolname, stgpool_forced);
		}

		switch (t_or_d) {
		case 't':                              /* For tape request we do some internal check */
			/* tape request */
			numvid = numvsn = 0;
			for (i = 0; i < MAXVSN; i++) {
				if (thiscat->u1.t.vid[i][0] != '\0') numvid++;
				if (thiscat->u1.t.vsn[i][0] != '\0') numvsn++;
			}
			if (numvid || numvsn) {
				if (numvid == 0) {
					for (i = 0; i < numvsn; i++) {
						strcpy(thiscat->u1.t.vid[i],thiscat->u1.t.vsn[i]);
					}
				}
				if (strcmp(thiscat->u1.t.dgn,"CT1") == 0) strcpy(thiscat->u1.t.dgn,"CART");
				if (strcmp(thiscat->u1.t.den,"38K") == 0) strcpy(thiscat->u1.t.dgn,"38000");
				/* setting defaults (from TMS if installed) */
				for (i = 0; i < numvid; i++) {
#if VMGR
					/* Call VMGR */
					if (stage_api_vmgrcheck(thiscat->u1.t.vid[i], thiscat->u1.t.vsn[i], thiscat->u1.t.dgn, thiscat->u1.t.den, thiscat->u1.t.lbl, ((req_type == STAGE_OUT) || (req_type == STAGE_WRT)) ? WRITE_ENABLE : WRITE_DISABLE) != 0) {
#endif
#if TMS
#if VMGR
						if (serrno == ETVUNKN) {
							/* VMGR fails with acceptable serrno and TMS is available */
#endif
							if (stage_api_tmscheck (thiscat->u1.t.vid[i], thiscat->u1.t.vsn[i], thiscat->u1.t.dgn, thiscat->u1.t.den, thiscat->u1.t.lbl) != 0) {
								serrno = ESTTMSCHECK;
#if defined(_WIN32)
								WSACleanup();
#endif
								return(-1);
							}
#if VMGR
						} else {
							/* VMGR fails with not acceptable serrno */
							stage_errmsg(func, STG02, thiscat->u1.t.vid[i][0] != '\0' ? thiscat->u1.t.vid[i] : thiscat->u1.t.vsn[i], "vmgrcheck", sstrerror(serrno));
#if defined(_WIN32)
							WSACleanup();
#endif
							return(-1);
						}
#endif
#elif (defined(VMGR))
						/* VMGR fails and there is not TMS */
						stage_errmsg(func, STG02, thiscat->u1.t.vid[i][0] != '\0' ? thiscat->u1.t.vid[i] : thiscat->u1.t.vsn[i], "vmgrcheck", sstrerror(serrno));
#if defined(_WIN32)
						WSACleanup();
#endif
						return(-1);
#endif /* TMS */
#if VMGR
					}
#endif
#if (! (defined(VMGR) || defined(TMS)))
					/* No VMGR not TMS */
					if (thiscat->u1.t.vsn[i][0] == '\0') {
						strcpy(thiscat->u1.t.vsn[i],thiscat->u1.t.vid[i]);
					}
					if (thiscat->u1.t.dgn[0] == '\0')
						strcpy (thiscat->u1.t.dgn, DEFDGN);
					thiscat->u1.t.den[0] = '\0';      /* No default density */
					if (thiscat->u1.t.lbl[0] == '\0')
						strcpy (thiscat->u1.t.lbl, "sl");
#endif
				}
			}
			break;
		case 'm':
		case 'h':
			/* The stager daemon will take care to verify if it is a valid HSM file... */
			if (ISHPSS(thiscat->u1.m.xfile)) {
				char *dummy;
				char *optarg = thiscat->u1.m.xfile;
				char *hsm_host;
				char hsm_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];

				/* We prepend HSM_HOST only for non CASTOR-like files */
				if (((dummy = strchr(optarg,':')) == NULL) || (dummy != optarg && strrchr(dummy,'/') == NULL)) {
					if ((hsm_host = getenv("HSM_HOST")) != NULL) {
						if (hsm_host[0] != '\0') {
							strcpy (hsm_path, hsm_host);
							strcat (hsm_path, ":");
						}
						strcat (hsm_path, optarg);
					} else if ((hsm_host = getconfent("STG", "HSM_HOST",0)) != NULL) {
						if (hsm_host[0] != '\0') {
							strcpy (hsm_path, hsm_host);
							strcat (hsm_path, ":");
						}
						strcat (hsm_path, optarg);
					} else {
						stage_errmsg(func, STG54);
						serrno = ESTHSMHOST;
#if defined(_WIN32)
						WSACleanup();
#endif
						return(-1);
					}
				}
				++nm;
			} else {
				if (thiscat->u1.h.xfile[0] != '/') {
					char *thiscwd;
					char dummy[STAGE_MAX_HSMLENGTH+1];
			  
					/* This is a non-absolute non-HPSS (e.g. CASTOR) HSM file */
			  
					if ((thiscwd = rfio_getcwd(NULL,CA_MAXPATHLEN+1)) != NULL) {
						/* We check that we have enough room for such a CASTOR full pathname */
						if ((strlen(thiscwd) + 1 + strlen(thiscat->u1.h.xfile)) > STAGE_MAX_HSMLENGTH) {
							free(thiscwd);
							serrno = EFAULT;
							return(-1);
						}
						strcpy(dummy, thiscwd);
						strcat(dummy, "/");
						strcat(dummy, thiscat->u1.h.xfile);
						strcpy(thiscat->u1.h.xfile, dummy);
						free(thiscwd);
					}
				}
				++nh;
			}
			break;
		default:
			break;
		}
		thiscat->t_or_d = t_or_d;
	}
	if ((nm > 0) || (nh > 0)) {
		/* There were HSM files in the request */
		if (nm == nstcp_input) {
			/* And they are all non-CASTOR files */
			t_or_d = 'm';
		} else if (nh == nstcp_input) {
			/* And they are all CASTOR files */
			t_or_d = 'h';
		} else {
			stage_errmsg(func, "Non-CASTOR and CASTOR files on the same api call is not allowed\n");
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		/* We make sure all t_or_d's are correct */
		for (istcp = 0; istcp < nstcp_input; istcp++) {
			stcp_input[istcp].t_or_d = (nm > 0) ? 'm' : 'h';
		}
	}

	sendbuf_size += (nstcp_input * sizeof(struct stgcat_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */

	for (istpp = 0; istpp < nstpp_input; istpp++) {
		thispath = &(stpp_input[istpp]);                           /* Current path structure */
		if (thispath->upath[0] == '\0') {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		if ((flags & STAGE_NOLINKCHECK) != STAGE_NOLINKCHECK) {
			user_path[0] = '\0';
			if ((build_linkname_status = build_linkname(thispath->upath, user_path, sizeof(user_path), req_type)) == SESYSERR) {
				serrno = ESTLINKNAME;
#if defined(_WIN32)
				WSACleanup();
#endif
				return(-1);
			}
			if (build_linkname_status) {
				serrno = ESTLINKNAME;
#if defined(_WIN32)
				WSACleanup();
#endif
				return(-1);
			}
			strcpy(thispath->upath,user_path);
		}
	}
	sendbuf_size += (nstpp_input * sizeof(struct stgpath_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */


	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}
	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {
		marshall_STRING (sbp, Gname);            /* Name under which is is to be done */
		marshall_LONG (sbp, Geuid);              /* Uid under which it is to be done */
	} else {
		marshall_STRING (sbp, pw->pw_name);
		marshall_LONG (sbp, euid);
	}
	marshall_LONG (sbp, egid);                 /* gid */
	umask (mask = umask(0));
	marshall_LONG (sbp, mask);                 /* umask */
	marshall_LONG (sbp, pid);                  /* pid */
	marshall_HYPER (sbp, uniqueid);            /* Our uniqueid */
	marshall_STRING (sbp, User);               /* User internal path (default to STAGERSUPERUSER in stgdaemon) */
	marshall_HYPER (sbp, flags);               /* Flags */
	marshall_LONG (sbp, openflags);            /* openflags */
	marshall_LONG (sbp, openmode);             /* openmode */
	marshall_BYTE (sbp, t_or_d);               /* Type of request (you cannot merge Tape/Disk/Allocation/Hsm) */
	marshall_LONG (sbp, nstcp_input);          /* Number of input stgcat_entry structures */
	marshall_LONG (sbp, nstpp_input);          /* Number of input stgpath_entry structures */
	for (istcp = 0; istcp < nstcp_input; istcp++) {
		thiscat = &(stcp_input[istcp]);               /* Current catalog structure */
		status = 0;
		marshall_STAGE_CAT(magic_client,magic_server,STAGE_INPUT_MODE,status,sbp,thiscat); /* Structures */
		if (status != 0) {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
	}
	for (istpp = 0; istpp < nstpp_input; istpp++) {
		thispath = &(stpp_input[istpp]);                           /* Current path structure */
		status = 0;
		marshall_STAGE_PATH(magic_client,magic_server,STAGE_INPUT_MODE,status,sbp,thispath); /* Structures */
		if (status != 0) {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
	}

	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		int original_serrno = -1;
		c = send2stgd(hostname, req_type, flags, sendbuf, msglen, 1, NULL, (size_t) 0, nstcp_input, stcp_input, nstcp_output, stcp_output, NULL, NULL);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			original_serrno = ((serrno != SECONNDROP) && (serrno != SETIMEDOUT)) ? errno : serrno;
			/* Server do not support this protocol */
			/* We keep client magic number as it is now but we downgrade magic server */
			switch (magic_server) {
			case STGMAGIC4:
				magic_server = STGMAGIC3;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_iowc_retry;
			case STGMAGIC3:
				magic_server = STGMAGIC2;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_iowc_retry;
			default:
				stage_errmsg(func, "%s\n", (original_serrno >= 0) ? sstrerror(original_serrno) : neterror());
				serrno = SEOPNOTSUP;
				break;
			}
			break;
		}
		if ((c == 0) ||
			(serrno == EINVAL)     || (serrno == ERTBLKSKPD) || (serrno == ERTTPE_LSZ) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) || (serrno == SENAMETOOLONG) || (serrno == SECHECKSUM) ||
			(serrno == EISDIR) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) ||
			(serrno == ERTMNYPARY) || (serrno == ERTLIMBYSZ) || (serrno == ESTCLEARED) ||
			(serrno == ESTKILLED)  || (serrno == ENOSPC) || (serrno == EBUSY) || (serrno == ESTLNKNSUP)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif
	return (c == 0 ? 0 : -1);
}

#if TMS
static int stage_api_tmscheck(vid, vsn, dgn, den, lbl)
	char *vid;
	char *vsn;
	char *dgn;
	char *den;
	char *lbl;
{
	int c, j;
	int errflg = 0;
	char *p;
	int repsize;
	int reqlen;
	char tmrepbuf[132];
	static char tmsden[6] = "     ";
	static char tmsdgn[7] = "      ";
	static char tmslbl[4] = "   ";
	char tmsreq[80];
	static char tmsvsn[7] = "      ";
	char *func = "tmscheck";

	sprintf (tmsreq, "VIDMAP %s QUERY (GENERIC SHIFT MESSAGE", vid);
	reqlen = strlen (tmsreq);
	while (1) {
		repsize = sizeof(tmrepbuf);
		c = sysreq ("TMS", tmsreq, &reqlen, tmrepbuf, &repsize);
		switch (c) {
		case 0:
			break;
		case 8:
		case 100:
		case 312:
		case 315:
			stage_errmsg(func, "%s\n", tmrepbuf);
			errflg++;
			break;
		default:
			stage_sleep (60);
			continue;
		}
		break;
	}
	if (errflg) return (1);
	strncpy (tmsvsn, tmrepbuf, 6);
	for  (j = 0; tmsvsn[j]; j++)
		if (tmsvsn[j] == ' ') break;
	tmsvsn[j] = '\0';
	if (*vsn) {
		if (strcmp (vsn, tmsvsn)) {
			stage_errmsg(func, STG15, vsn, tmsvsn);
			errflg++;
		}
	} else {
		strcpy (vsn, tmsvsn);
	}
	
	strncpy (tmsdgn, tmrepbuf+ 25, 6);
	for  (j = 0; tmsdgn[j]; j++)
		if (tmsdgn[j] == ' ') break;
	tmsdgn[j] = '\0';
	if (strcmp (tmsdgn, "CT1") == 0) strcpy (tmsdgn, "CART");
	if (*dgn) {
		if (strcmp (dgn, tmsdgn) != 0) {
			stage_errmsg(func, STG15, dgn, tmsdgn);
			errflg++;
		}
	} else {
		strcpy (dgn, tmsdgn);
	}
	
	p = tmrepbuf + 32;
	while (*p == ' ') p++;
	j = tmrepbuf + 40 - p;
	strncpy (tmsden, p, j);
	tmsden[j] = '\0';
	if (*den) {
		if (strcmp (den, tmsden) != 0) {
			stage_errmsg(func, STG15, den, tmsden);
			errflg++;
		}
	} else {
		strcpy (den, tmsden);
	}

	/* Indexes 74..76 contains the label type (padded with a blank if necessary at index 76) */
	tmslbl[0] = tmrepbuf[74] - 'A' + 'a';
	tmslbl[1] = tmrepbuf[75] - 'A' + 'a';
	tmslbl[2] = (tmrepbuf[76] == ' ') ? '\0' : tmrepbuf[76] - 'A' + 'a';
	if (*lbl) {
		if (strcmp (lbl, "blp") && strcmp (lbl, tmslbl)) {
			stage_errmsg(func, STG15, lbl, tmslbl);
			errflg++;
		}
	} else {
		strcpy (lbl, tmslbl);
	}
	return (errflg);
}
#endif

#if VMGR
static int stage_api_vmgrcheck(vid, vsn, dgn, den, lbl, mode)
	char *vid;
	char *vsn;
	char *dgn;
	char *den;
	char *lbl;
	int mode;
{
    uid_t uid;
    gid_t gid;
	int rc;

    /* Make sure error buffer of VMGR do not go to stderr */
    vmgr_error_buffer[0] = '\0';
    if (vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0) {
		return(serrno);
    }

    /* We need to know exact current uid/gid */
    uid = geteuid();
    gid = getegid();

#if defined(_WIN32)
	if ((uid < 0) || (uid >= CA_MAXUID) || (gid < 0) || (gid >= CA_MAXGID)) {
		return(SEUSERUNKN);
    }
#endif

	if ((rc = vmgrcheck(vid,vsn,dgn,den,lbl,mode,uid,gid)) != 0) {
		serrno = rc;
		return(1);
	} else {
		return(0);
	}
}
#endif

int DLL_DECL stage_stcp2buf(buf,bufsize,stcp)
	char *buf;
	size_t bufsize;
	struct stgcat_entry *stcp;
{
	int i;

	if ((buf == NULL) || (bufsize <= 0)) {
		serrno = EINVAL;
		return(-1);
	}

	switch (stcp->t_or_d) {
	case 't':
		/* Special case of VID and VSN */
		STVAL2BUF(stcp,blksize,'b',buf,bufsize);
		STVAL2BUF(stcp,lrecl,'L',buf,bufsize);
		STVAL2BUF(stcp,nread,'N',buf,bufsize);
		STSTR2BUF(stcp,recfm,'F',buf,bufsize);
		/* Special case of charconv */
		if ((stcp->charconv & (EBCCONV|FIXVAR)) == (EBCCONV|FIXVAR)) {
			if (strlen(buf) + strlen(" -C ebcdic,block") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -C ebcdic,block");
		} else if ((stcp->charconv & EBCCONV) == EBCCONV) {
			if (strlen(buf) + strlen(" -C ebcdic") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -C ebcdic");
		} else if ((stcp->charconv & FIXVAR) == FIXVAR) {
			if (strlen(buf) + strlen(" -C block") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -C block");
		}
		{
			char bigvid[MAXVSN * (CA_MAXVIDLEN + 1)];
			char bigvsn[MAXVSN * (CA_MAXVIDLEN + 1)];
			bigvid[0] = '\0';
			bigvsn[0] = '\0';
			for (i = 0; i < MAXVSN; i++) {
				strcat(bigvid,stcp->u1.t.vid[i]);
				strcat(bigvsn,stcp->u1.t.vsn[i]);
			}
			STR2BUF(bigvid,'V',buf,bufsize);
			STR2BUF(bigvsn,'v',buf,bufsize);
		}
		STSTR2BUF(stcp,u1.t.lbl,'l',buf,bufsize);
		STSTR2BUF(stcp,u1.t.den,'d',buf,bufsize);
		STSTR2BUF(stcp,u1.t.dgn,'g',buf,bufsize);
		STSTR2BUF(stcp,u1.t.fid,'f',buf,bufsize);
		STVAL2BUF(stcp,u1.t.retentd,'t',buf,bufsize);
		STVAL2BUF_V3(stcp,u1.t.side," --side",buf,bufsize);
		STSTR2BUF(stcp,u1.t.fseq,'q',buf,bufsize);
		/* Special case of filstat */
		if (stcp->u1.t.filstat == 'o') {
			if (strlen(buf) + strlen(" -o") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -o");
		} else if (stcp->u1.t.filstat == 'n') {
			if (strlen(buf) + strlen(" -n") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -n");
		}
		/* Special case of E_Tflags */
		if (stcp->u1.t.E_Tflags == SKIPBAD) {
			if (strlen(buf) + strlen(" -E skip") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -E skip");
		} else if (stcp->u1.t.E_Tflags == KEEPFILE) {
			if (strlen(buf) + strlen(" -E keep") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -E keep");
		} else if (stcp->u1.t.E_Tflags == IGNOREEOI) {
			if (strlen(buf) + strlen(" -E ignoreeoi") + 1 > bufsize) { serrno = SEUMSG2LONG; return(-1); }
			strcat(buf," -E ignoreeoi");
		}
		STSTR2BUF_V2(stcp,u1.t.tapesrvr," --tapesrvr",buf,bufsize);
		break;
	case 'd':
	case 'a':
		STSTR2BUF(stcp,u1.d.xfile,'I',buf,bufsize);
		STSTR2BUF_V2(stcp,u1.d.Xparm," --Xparm",buf,bufsize);
		break;
	case 'm':
		STSTR2BUF(stcp,u1.m.xfile,'M',buf,bufsize);
		break;
	case 'h':
		STSTR2BUF(stcp,u1.h.xfile,'M',buf,bufsize);
		STSTR2BUF_V2(stcp,u1.h.server," --server",buf,bufsize);
		STVAL2BUF_V2(stcp,u1.h.fileid," --fileid",buf,bufsize);
		STVAL2BUF_V2(stcp,u1.h.fileclass," --fileclass",buf,bufsize);
		/* STSTR2BUF_V2(stcp,u1.h.tppool," --tppool",buf,bufsize); */
		/* V3 macro == V2 macro except that V3 logs arguments >= 0 while V2 logs when it is != 0 */
		STVAL2BUF_V3(stcp,u1.h.retenp_on_disk," --retenp_on_disk",buf,bufsize);
		STVAL2BUF_V3(stcp,u1.h.mintime_beforemigr," --mintime_beforemigr",buf,bufsize);
		STVAL2BUF_V2(stcp,u1.h.flag," --flag",buf,bufsize);
		break;
	default:
		serrno = EFAULT;
		return(-1);
	}
	STVAL2BUF_V2(stcp,reqid," --reqid",buf,bufsize);
	STSTR2BUF_V2(stcp,ipath," --ipath",buf,bufsize);
	STCHR2BUF(stcp,keep,'K',buf,bufsize);
	STSTR2BUF(stcp,poolname,'p',buf,bufsize);
	STVAL2BUF(stcp,size,'s',buf,bufsize);
	STSTR2BUF_V2(stcp,group," --group",buf,bufsize);
	STSTR2BUF_V2(stcp,user," --user",buf,bufsize);
	STVAL2BUF_V2(stcp,uid," --uid",buf,bufsize);
	STVAL2BUF_V2(stcp,gid," --gid",buf,bufsize);
	STVAL2BUF_V2(stcp,mask," --mask",buf,bufsize);
	STVAL2BUF_V2(stcp,status," --status",buf,bufsize);
	STVAL2BUF_V2(stcp,actual_size," --actual_size",buf,bufsize);
	STVAL2BUF_V2(stcp,c_time," --c_time",buf,bufsize);
	STVAL2BUF_V2(stcp,a_time," --a_time",buf,bufsize);
	STVAL2BUF_V2(stcp,nbaccesses," --nbaccesses",buf,bufsize);
	return(0);
}

int DLL_DECL stage_kill(sig)
	int sig;
{
	char *laststghost;

	/* We check the latest, if any, stager host conctacted in this thread */
	if (stage_getlaststghost(&laststghost) == 0) {
		return(stage_admin_kill(laststghost,NULL));
	} else {
		return(stage_admin_kill(NULL,NULL));
	}
}

int DLL_DECL stage_admin_kill(hostname,uniqueid)
	char *hostname;
	u_signed64 *uniqueid;
{
	int c;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[74];
	u_signed64 this_uniqueid = 0;
	struct passwd *pw;            /* Password entry */
	uid_t euid;                   /* Current effective uid */
	gid_t egid;                   /* Current effective gid */
	int magic = STGMAGIC3;        /* Force a level that all stagers understand - no need for STGMAGIC >= 3 here */

	euid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
	pw = Cgetpwuid(euid);

	if ((uniqueid == NULL) || (*uniqueid == 0)) {
		/* If the parameter is NULL or a dummy value, we use the thread-specific variable */
		if (stage_getuniqueid(&this_uniqueid) != 0) {
			/* Error */
			return(-1);
		}
		if (this_uniqueid == 0) {
			/* Dummy value - nothing to kill */
			return(0);
		}
	} else {
		/* Force unique ID value (admin action) */
		this_uniqueid = *uniqueid;
	}
	sbp = sendbuf;
	marshall_LONG (sbp, magic);
	marshall_LONG (sbp, STAGE_KILL);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
	marshall_STRING (sbp, ((pw != NULL) ? pw->pw_name : "<unknown>"));	/* login name */
	marshall_LONG (sbp, euid);
	marshall_LONG (sbp, egid);
	marshall_HYPER (sbp, this_uniqueid);
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */
	c = send2stgd(hostname, STAGE_KILL, (u_signed64) 0, sendbuf, msglen, 0, NULL, (size_t) 0, 0, NULL, NULL, NULL, NULL, NULL);
#if defined(_WIN32)
	WSACleanup();
#endif
	return(0);
}

int DLL_DECL stage_qry (char t_or_d,
                        u_signed64 flags,
                        char *hostname,
                        int nstcp_input,
                        struct stgcat_entry *stcp_input,
                        int *nstcp_output,
                        struct stgcat_entry **stcp_output,
                        int *nstpp_output,
                        struct stgpath_entry **stpp_output) {
	int req_type = STAGE_QRY;
	struct stgcat_entry *thiscat; /* Catalog current pointer */
	int istcp;                    /* Counter on catalog structures passed in the protocol */
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	gid_t egid;                   /* Current effective gid */
	int status;                   /* Variable overwritten by macros in header */
	int c;                        /* Output of send2stgd() */
	int nstcp_output_internal;
	struct stgcat_entry *stcp_output_internal;
	int nstpp_output_internal;
	struct stgpath_entry *stpp_output_internal;
	int rc;
	char stgpool_forced[CA_MAXPOOLNAMELEN + 1];
	int pid;
	int Tid;
	u_signed64 uniqueid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *func = "stage_qry";
	int maxretry = MAXRETRY;
	struct stgcat_entry stcp_dummy;
	int magic_client;
	int magic_server;
	int nm;
	int nh;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_client = magic_server = stage_stgmagic();
  
  _stage_qry_retry:
	ntries = nstg161 = Tid = 0;
	nstcp_output_internal = nstpp_output_internal = 0;
	stcp_output_internal = NULL;
	stpp_output_internal = NULL;
	uniqueid = 0;
	nm = nh = 0;

	/* It is not allowed to have anything else but one single entry in input, or none only if t_or_d == '\0' */
	if (nstcp_input > 1) {
		/* 1 or 0 only */
		serrno = EINVAL;
		return(-1);
	}
	if (((nstcp_input == 1) && (t_or_d == '\0')) || ((nstcp_input == 0) && (t_or_d != '\0'))) {
		serrno = EINVAL;
		return(-1);
	}
	if ((stcp_input != NULL) && (nstcp_input == 0)) {
		serrno = EFAULT;
		return(-1);
	}
	if ((nstcp_input != 0) && (stcp_input == NULL)) {
		serrno = EFAULT;
		return(-1);
	}

	euid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	/* It is not allowed to have stcp_output != NULL and nstcp_output == NULL */
	if ((stcp_output != NULL) && (nstcp_output == NULL)) {
		serrno = EFAULT;
		return(-1);
	}

	/* It is not allowed to have stpp_output != NULL and nstpp_output == NULL */
	if ((stpp_output != NULL) && (nstpp_output == NULL)) {
		serrno = EFAULT;
		return(-1);
	}

	/* We check existence of an STG POOL from environment variable or configuration */
	if ((p = getenv("STAGE_POOL")) != NULL) {
		strncpy(stgpool_forced,p,CA_MAXPOOLNAMELEN);
		stgpool_forced[CA_MAXPOOLNAMELEN] = '\0';
	} else if ((p = getconfent("STG","POOL",0)) != NULL) {
		strncpy(stgpool_forced,p,CA_MAXPOOLNAMELEN);
		stgpool_forced[CA_MAXPOOLNAMELEN] = '\0';
	} else {
		stgpool_forced[0] = '\0';
	}

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	switch (t_or_d) {                     /* This method supports only */
	case '\0':                            /* - Dummy flag */
		memset((void *) &stcp_dummy,0,sizeof(struct stgcat_entry));
		stcp_input = &stcp_dummy;
		nstcp_input = 1;
		t_or_d = 't';                     /* - Dummy value */
	case 't':                             /* - tape files */
	case 'd':                             /* - disk files */
	case 'a':                             /* - allocated files */
	case 'm':                             /* - non-CASTOR HSM files */
	case 'h':                             /* - CASTOR HSM files */
		break;
	default:
		serrno = EINVAL;                    /* Otherwise t_or_d is an invalid parameter */
		return(-1);
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += HYPERSIZE;               /* Flags */
	sendbuf_size += BYTESIZE;                /* t_or_d */
	sendbuf_size += LONGSIZE;                /* Number of catalog structures */
	sendbuf_size += HYPERSIZE;               /* Our uniqueid */
	for (istcp = 0; istcp < nstcp_input; istcp++) {

		thiscat = &(stcp_input[istcp]);              /* Current catalog structure */

		/* If this entry has no poolname we copy it the forced one */
		if ((thiscat->poolname[0] == '\0') && (stgpool_forced[0] != '\0')) {
			strcpy(thiscat->poolname, stgpool_forced);
		}

		switch (t_or_d) {
		case 't':                              /* For tape request we do some internal check */
			/* tape request */
			break;
		case 'm':
		case 'h':
			/* The stager daemon will take care to verify if it is a valid HSM file... */
			if (ISHPSS(thiscat->u1.m.xfile)) {
				++nm;
			} else {
				++nh;
			}
			break;
		default:
			break;
		}
		thiscat->t_or_d = t_or_d;
	}
	if ((nm > 0) || (nh > 0)) {
		/* There were HSM files in the request */
		if (nm == nstcp_input) {
			/* And they are all non-CASTOR files */
			t_or_d = 'm';
		} else if (nh == nstcp_input) {
			/* And they are all CASTOR files */
			t_or_d = 'h';
		} else {
			stage_errmsg(func, "Non-CASTOR and CASTOR files on the same api call is not allowed\n");
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		/* We make sure all t_or_d's are correct */
		for (istcp = 0; istcp < nstcp_input; istcp++) {
			stcp_input[istcp].t_or_d = (nm > 0) ? 'm' : 'h';
		}
	}
	sendbuf_size += (nstcp_input * sizeof(struct stgcat_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */

	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}
	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	marshall_LONG (sbp, egid);                 /* gid */
	marshall_HYPER (sbp, flags);               /* Flags */
	marshall_BYTE (sbp, t_or_d);               /* Type of request (you cannot merge Tape/Disk/Allocation/Hsm) */
	marshall_LONG (sbp, nstcp_input);          /* Number of input stgcat_entry structures */
	marshall_HYPER (sbp, uniqueid);            /* Our uniqueid */
	for (istcp = 0; istcp < nstcp_input; istcp++) {
		thiscat = &(stcp_input[istcp]);               /* Current catalog structure */
		status = 0;
		marshall_STAGE_CAT(magic_client,magic_server,STAGE_INPUT_MODE,status,sbp,thiscat); /* Structures */
		if (status != 0) {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
	}
	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		int original_serrno = -1;
		c = send2stgd(hostname, req_type, flags, sendbuf, msglen, 1, NULL, (size_t) 0, nstcp_input, stcp_input, &nstcp_output_internal, &stcp_output_internal, &nstpp_output_internal, &stpp_output_internal);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			original_serrno = ((serrno != SECONNDROP) && (serrno != SETIMEDOUT)) ? errno : serrno;
			/* Server do not support this protocol */
			/* We keep client magic number as it is now but we downgrade magic server */
			switch (magic_server) {
			case STGMAGIC4:
				magic_server = STGMAGIC3;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_qry_retry;
			case STGMAGIC3:
				magic_server = STGMAGIC2;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_qry_retry;
			default:
				stage_errmsg(func, "%s\n", (original_serrno >= 0) ? sstrerror(original_serrno) : neterror());
				serrno = SEOPNOTSUP;
				break;
			}
			break;
		}
		if ((c == 0) || (serrno == EINVAL) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) ||  (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SECHECKSUM) || (serrno == SENAMETOOLONG)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif

	if ((rc = c) == 0) {
		/* If command was processed successfully */
		if (((nstcp_output_internal > 0) && (stcp_output_internal == NULL)) ||
			((nstpp_output_internal > 0) && (stpp_output_internal == NULL))) {
			/* Inconsistency */
			serrno = SEINTERNAL;
			rc = -1;
		} else if ((nstcp_output_internal == 0) && (nstpp_output_internal == 0)) {
			/* Nothing found */
			if ((flags & STAGE_STATPOOL) != STAGE_STATPOOL) {
				/* This is an error only if querying the catalog */
				serrno = ENOENT;
				rc = -1;
			}
		}
	}

	if ((rc == 0) && (nstcp_output != NULL)) *nstcp_output = nstcp_output_internal;
	if ((rc == 0) && (nstpp_output != NULL)) *nstpp_output = nstpp_output_internal;

	if ((rc == 0) && (stcp_output != NULL)) {
		/* We will not freeze internally allocated memory if it has been ever allocated */
		*stcp_output =  stcp_output_internal;
	} else {
		/* User is not interested by this output regardless of rc - freeze it if necessary */
		if (stcp_output_internal != NULL) free(stcp_output_internal);
	}
	if ((rc == 0) && (stpp_output != NULL)) {
		/* We will not freeze internally allocated memory if it has been ever allocated */
		*stpp_output =  stpp_output_internal;
	} else {
		/* User is not interested by this output regardless of rc - freeze it if necessary */
		if (stpp_output_internal != NULL) free(stpp_output_internal);
	}

	return (rc);
}

int DLL_DECL stage_qry_Tape(flags,hostname,poolname,tape,fseq,nstcp_output,stcp_output,nstpp_output,stpp_output)
	u_signed64 flags;
	char *hostname;
	char *poolname;
	char *tape;
	char *fseq;
	int *nstcp_output;
	struct stgcat_entry **stcp_output;
	int *nstpp_output;
	struct stgpath_entry **stpp_output;
{
	struct stgcat_entry stcp_input;

	/* Check tape in input */
	if (tape == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check tape length and poolname validity */
	if (((*tape == '\0') || (strlen(tape)        > CA_MAXVIDLEN      )) ||
		((fseq  != NULL) && (strlen(fseq)        > CA_MAXFSEQLEN     )) ||
		((poolname != NULL) && (strlen(poolname) > CA_MAXPOOLNAMELEN))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageqry API request */
	memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	if (poolname != NULL) strcpy(stcp_input.poolname,poolname); /* Can be zero-length */
	strcpy(stcp_input.u1.t.vid[0],tape); /* Cannot be zero-length */
	if (fseq != NULL) strcpy(stcp_input.u1.t.fseq,fseq); /* Can be zero-length */
	return(stage_qry_tape(flags,hostname,1,&stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output));
}

int DLL_DECL stage_qry_Hsm(flags,hostname,poolname,hsmname,nstcp_output,stcp_output,nstpp_output,stpp_output)
	u_signed64 flags;
	char *hostname;
	char *poolname;
	char *hsmname;
	int *nstcp_output;
	struct stgcat_entry **stcp_output;
	int *nstpp_output;
	struct stgpath_entry **stpp_output;
{
	struct stgcat_entry stcp_input;

	/* Check hsmname in input */
	if (hsmname == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check hsmname length and poolname validity */
	if (((*hsmname == '\0') || (strlen(hsmname)  > STAGE_MAX_HSMLENGTH)) ||
		((poolname != NULL) && (strlen(poolname) > CA_MAXPOOLNAMELEN))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageqry API request */
	memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	if (poolname != NULL) strcpy(stcp_input.poolname,poolname); /* Can be zero-length */
	strcpy(stcp_input.u1.h.xfile,hsmname); /* Cannot be zero-length */
	return(stage_qry_hsm(flags,hostname,1,&stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output));
}


int DLL_DECL stage_qry_Disk(flags,hostname,poolname,diskname,nstcp_output,stcp_output,nstpp_output,stpp_output)
	u_signed64 flags;
	char *hostname;
	char *poolname;
	char *diskname;
	int *nstcp_output;
	struct stgcat_entry **stcp_output;
	int *nstpp_output;
	struct stgpath_entry **stpp_output;
{
	struct stgcat_entry stcp_input;

	/* Check diskname in input */
	if (diskname == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check diskname length and poolname validity */
	if (((*diskname == '\0') || (strlen(diskname)  > (CA_MAXHOSTNAMELEN+MAXPATH))) ||
		((poolname  != NULL) && (strlen(poolname)  > CA_MAXPOOLNAMELEN         ))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageqry API request */
	memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	if (poolname != NULL) strcpy(stcp_input.poolname,poolname); /* Can be zero-length */
	strcpy(stcp_input.u1.d.xfile,diskname); /* Cannot be zero-length */
	return(stage_qry_disk(flags,hostname,1,&stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output));
}


int DLL_DECL stage_updc(flags,hostname,pooluser,rcstatus,nstcp_output,stcp_output,nstpp_input,stpp_input)
	u_signed64 flags;
	char *hostname;
	char *pooluser;
	int rcstatus;
	int *nstcp_output;
	struct stgcat_entry **stcp_output;
	int nstpp_input;
	struct stgpath_entry *stpp_input;     
{
	int req_type = STAGE_UPDC;
	struct stgpath_entry *thispath; /* Path current pointer */
	int istpp;                    /* Counter on path structures passed in the protocol */
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	struct group *gr;             /* Group entry */

	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	uid_t Geuid;                  /* Forced effective uid (-G option) */
	char Gname[CA_MAXUSRNAMELEN+1]; /* Forced effective name (-G option) */
	char User[CA_MAXUSRNAMELEN+1];  /* Forced internal path with user (-u options) */
	gid_t egid;                   /* Current effective gid */
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int mask;                     /* User mask */
#else
	mode_t mask;                  /* User mask */
#endif
	int pid;                      /* Current pid */
	int status;                   /* Variable overwritten by macros in header */
	int build_linkname_status;    /* Status of build_linkname() call */
	char user_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	int c;                        /* Output of build_linkname() */
	char *func =  "stage_updc";
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	int Tid;
	u_signed64 uniqueid;
	char tmpbuf[21];
	int maxretry = MAXRETRY;
	int magic_client;
	int magic_server;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_client = magic_server = stage_stgmagic();
  
  _stage_updc_retry:
	ntries = nstg161 = Tid = 0;
	uniqueid = 0;

	/* It is not allowed to have no input */
	if (nstpp_input <= 0) {
		serrno = EINVAL;
		return(-1);
	}

	if (stpp_input == NULL) {
		serrno = EFAULT;
		return(-1);
	}

	euid = Geuid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	/* It is not allowed to have stcp_output != NULL and nstcp_output == NULL */
	if ((stcp_output != NULL) && (nstcp_output == NULL)) {
		serrno = EFAULT;
		return(-1);
	}

	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {  /* User want to overwrite euid under which request is processed by stgdaemon */
		if ((gr = Cgetgrgid(egid)) == NULL) { /* This is allowed only if its group exist */
			if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetgrgid", strerror(errno));
			stage_errmsg(func, STG36, egid);
			serrno = ESTGROUP;
			return(-1);
		}
		if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) { /* And if GRPUSER is configured */
			stage_errmsg(func, STG10, gr->gr_name);
			serrno = ESTGRPUSER;
			return(-1);
		} else {
			strcpy (Gname, p);
			if ((pw = Cgetpwnam(p)) == NULL) { /* And if GRPUSER content is a valid user name */
				if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwnam", strerror(errno));
				stage_errmsg(func, STG11, p);
				serrno = SEUSERUNKN;
				return(-1);
			} else
				Geuid = pw->pw_uid;              /* In such a case we grab its uid */
		}
	}

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	/* If no user specified we check environment variable STAGE_USER */
	if (((pooluser == NULL) || (pooluser[0] == '\0')) && (p = getenv ("STAGE_USER")) != NULL) {
		strncpy(User,p,CA_MAXUSRNAMELEN);
		User[CA_MAXUSRNAMELEN] = '\0';
		/* We verify this user login is defined */
		if (((pw = Cgetpwnam(User)) == NULL) || (pw->pw_gid != egid)) {
			if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwnam", strerror(errno));
			stage_errmsg(func, STG11, User);
			serrno = SEUSERUNKN;
			return(-1);
		}
	} else if ((pooluser != NULL) && (pooluser[0] != '\0')) {
		strncpy(User,pooluser,CA_MAXUSRNAMELEN);
		User[CA_MAXUSRNAMELEN] = '\0';
	} else {
		User[0] = '\0';
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {
		sendbuf_size += strlen(Gname) + 1;     /* Name under which request is to be done */
	} else {
		sendbuf_size += strlen(pw->pw_name) + 1;
	}
	sendbuf_size += LONGSIZE;                /* Uid */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += LONGSIZE;                /* Mask */
	sendbuf_size += LONGSIZE;                /* Pid */
	sendbuf_size += HYPERSIZE;               /* Our uniqueid  */
	sendbuf_size += strlen(User) + 1;        /* User for internal path (default to STAGERSUPERUSER in stgdaemon) */
	sendbuf_size += HYPERSIZE;               /* Flags */
	if (rcstatus >= 0) {
		sprintf (tmpbuf, "%d", rcstatus);
		sendbuf_size += strlen(tmpbuf) + 1; /* rc status */
	} else {
		sprintf (tmpbuf, "%d", (int) -1);
		sendbuf_size += strlen(tmpbuf) + 1; /* rc status (disabled) */
	}
	sendbuf_size += LONGSIZE;                /* Number of catalog structures */
	sendbuf_size += LONGSIZE;                /* Number of path structures */

	for (istpp = 0; istpp < nstpp_input; istpp++) {
		thispath = &(stpp_input[istpp]);                           /* Current path structure */
		if (thispath->upath[0] == '\0') {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		if ((flags & STAGE_NOLINKCHECK) != STAGE_NOLINKCHECK) {
			user_path[0] = '\0';
			if ((build_linkname_status = build_linkname(thispath->upath, user_path, sizeof(user_path), req_type)) == SESYSERR) {
				serrno = ESTLINKNAME;
#if defined(_WIN32)
				WSACleanup();
#endif
				return(-1);
			}
			if (build_linkname_status) {
				serrno = ESTLINKNAME;
#if defined(_WIN32)
				WSACleanup();
#endif
				return(-1);
			}
			strcpy(thispath->upath,user_path);
		}
	}
	sendbuf_size += (nstpp_input * sizeof(struct stgpath_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */

	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}
	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {
		marshall_STRING (sbp, Gname);            /* Name under which is is to be done */
		marshall_LONG (sbp, Geuid);              /* Uid under which it is to be done */
	} else {
		marshall_STRING (sbp, pw->pw_name);
		marshall_LONG (sbp, euid);
	}
	marshall_LONG (sbp, egid);                 /* gid */
	umask (mask = umask(0));
	marshall_LONG (sbp, mask);                 /* umask */
	marshall_LONG (sbp, pid);                  /* pid */
	marshall_HYPER (sbp, uniqueid);            /* Our uniqueid */
	marshall_STRING (sbp, User);               /* User internal path (default to STAGERSUPERUSER in stgdaemon) */
	marshall_HYPER (sbp, flags);               /* Flags */
	if (rcstatus >= 0) {
		sprintf (tmpbuf, "%d", rcstatus);
		marshall_STRING(sbp, tmpbuf);            /* rc status */
	} else {
		sprintf (tmpbuf, "%d", (int) -1);
		marshall_STRING(sbp, tmpbuf);            /* rc status (disabled) */
	}
	marshall_LONG (sbp, nstpp_input);          /* Number of input stgpath_entry structures */
	for (istpp = 0; istpp < nstpp_input; istpp++) {
		thispath = &(stpp_input[istpp]);                           /* Current path structure */
		status = 0;
		marshall_STAGE_PATH(magic_client,magic_server,STAGE_INPUT_MODE,status,sbp,thispath); /* Structures */
		if (status != 0) {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
	}

	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		int original_serrno = -1;
		c = send2stgd(hostname, req_type, flags, sendbuf, msglen, 1, NULL, (size_t) 0, 0, NULL, nstcp_output, stcp_output, NULL, NULL);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			original_serrno = ((serrno != SECONNDROP) && (serrno != SETIMEDOUT)) ? errno : serrno;
			/* Server do not support this protocol */
			/* We keep client magic number as it is now but we downgrade magic server */
			switch (magic_server) {
			case STGMAGIC4:
				magic_server = STGMAGIC3;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_updc_retry;
			case STGMAGIC3:
				magic_server = STGMAGIC2;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_updc_retry;
			default:
				stage_errmsg(func, "%s\n", (original_serrno >= 0) ? sstrerror(original_serrno) : neterror());
				serrno = SEOPNOTSUP;
				break;
			}
			break;
		}
		if ((c == 0) ||
			(serrno == EINVAL) || (serrno == ENOSPC) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SECHECKSUM) || (serrno == SENAMETOOLONG)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif
	return (c == 0 ? 0 : -1);
}

int DLL_DECL stage_clr(char t_or_d,
                       u_signed64 flags,
                       char *hostname,
                       int nstcp_input,
                       struct stgcat_entry *stcp_input,
                       int nstpp_input,
                       struct stgpath_entry *stpp_input) {
	int req_type = STAGE_CLR;
	struct stgcat_entry *thiscat; /* Catalog current pointer */
	struct stgpath_entry *thispath; /* Path current pointer */
	int istcp;                    /* Counter on catalog structures passed in the protocol */
	int istpp;                    /* Counter on path structures passed in the protocol */
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	struct group *gr;             /* Group entry */

	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	uid_t Geuid;                  /* Forced effective uid (-G option) */
	char Gname[CA_MAXUSRNAMELEN+1]; /* Forced effective name (-G option) */
	gid_t egid;                   /* Current effective gid */
	int pid;                      /* Current pid */
	int status;                   /* Variable overwritten by macros in header */
	int build_linkname_status;    /* Status of build_linkname() call */
	char user_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	int c;                        /* Output of build_linkname() */
	char *func = "stage_clr";
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	int Tid;
	u_signed64 uniqueid;
	u_signed64 flagsok;
	int maxretry = MAXRETRY;
	int magic_client;
	int magic_server;
	int nm;
	int nh;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_client = magic_server = stage_stgmagic();
  
  _stage_clr_retry:
	ntries = nstg161 = Tid = 0;
	uniqueid = 0;
	flagsok = flags;
	nm = nh = 0;

	/* It is not allowed to have no input */
	if ((nstcp_input <= 0) && (nstpp_input <= 0)) {
		serrno = EINVAL;
		return(-1);
	}

	/* It is not allowed to have both stcp_input and stpp_input */
	if ((nstcp_input > 0) && (nstpp_input > 0)) {
		serrno = EINVAL;
		return(-1);
	}

	/* It is not allowed to have nstcp_input > 0 and != 1 */
	if ((nstcp_input > 0) && (nstcp_input != 1)) {
		serrno = EINVAL;
		return(-1);
	}

	/* It is not allowed to have nstpp_input > 0 and != 1 */
	if ((nstpp_input > 0) && (nstpp_input != 1)) {
		serrno = EINVAL;
		return(-1);
	}


	if ((nstcp_input == 1) && (stcp_input == NULL)) {
		serrno = EFAULT;
		return(-1);
	}

	if ((nstpp_input == 1) && (stpp_input == NULL)) {
		serrno = EFAULT;
		return(-1);
	}

	euid = Geuid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	if ((flagsok & STAGE_GRPUSER) == STAGE_GRPUSER) {  /* User want to overwrite euid under which request is processed by stgdaemon */
		if ((gr = Cgetgrgid(egid)) == NULL) { /* This is allowed only if its group exist */
			if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetgrgid", strerror(errno));
			stage_errmsg(func, STG36, egid);
			serrno = ESTGROUP;
			return(-1);
		}
		if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) { /* And if GRPUSER is configured */
			stage_errmsg(func, STG10, gr->gr_name);
			serrno = ESTGRPUSER;
			return(-1);
		} else {
			strcpy (Gname, p);
			if ((pw = Cgetpwnam(p)) == NULL) { /* And if GRPUSER content is a valid user name */
				if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwnam", strerror(errno));
				stage_errmsg(func, STG11, p);
				serrno = SEUSERUNKN;
				return(-1);
			} else
				Geuid = pw->pw_uid;              /* In such a case we grab its uid */
		}
	}

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flagsok & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flagsok so that the server will log it */
		flagsok |= STAGE_NORETRY;
		maxretry = 0;
	}

	switch (t_or_d) {                     /* This method supports only */
	case 't':                             /* - tape files */
	case 'd':                             /* - disk files */
	case 'a':                             /* - allocated files */
	case 'm':                             /* - non-CASTOR HSM files */
	case 'h':                             /* - CASTOR HSM files */
		flagsok &= ~STAGE_LINKNAME;
		flagsok &= ~STAGE_PATHNAME;
		break;
	case 'p':                             /* - For path - ignored */
		flagsok &= ~STAGE_LINKNAME;
		flagsok |= STAGE_PATHNAME;
		break;
	case 'l':                             /* - For path - ignored */
		flagsok &= ~STAGE_PATHNAME;
		flagsok |= STAGE_LINKNAME;
		break;
	default:
		serrno = EINVAL;                    /* Otherwise t_or_d is an invalid parameter */
		return(-1);
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	sendbuf_size += LONGSIZE;                /* Uid */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += HYPERSIZE;               /* Our uniqueid  */
	sendbuf_size += HYPERSIZE;               /* Flags */
	sendbuf_size += BYTESIZE;                /* t_or_d */
	sendbuf_size += LONGSIZE;                /* Number of catalog structures */
	sendbuf_size += LONGSIZE;                /* Number of path structures */
	for (istcp = 0; istcp < nstcp_input; istcp++) {

		thiscat = &(stcp_input[istcp]);              /* Current catalog structure */

		switch (t_or_d) {
		case 'm':
		case 'h':
			/* The stager daemon will take care to verify if it is a valid HSM file... */
			if (ISHPSS(thiscat->u1.m.xfile)) {
				char *dummy;
				char *optarg = thiscat->u1.m.xfile;
				char *hsm_host;
				char hsm_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];

				/* We prepend HSM_HOST only for non CASTOR-like files */
				if (((dummy = strchr(optarg,':')) == NULL) || (dummy != optarg && strrchr(dummy,'/') == NULL)) {
					if ((hsm_host = getenv("HSM_HOST")) != NULL) {
						if (hsm_host[0] != '\0') {
							strcpy (hsm_path, hsm_host);
							strcat (hsm_path, ":");
						}
						strcat (hsm_path, optarg);
					} else if ((hsm_host = getconfent("STG", "HSM_HOST",0)) != NULL) {
						if (hsm_host[0] != '\0') {
							strcpy (hsm_path, hsm_host);
							strcat (hsm_path, ":");
						}
						strcat (hsm_path, optarg);
					} else {
						stage_errmsg(func, STG54);
						serrno = ESTHSMHOST;
#if defined(_WIN32)
						WSACleanup();
#endif
						return(-1);
					}
				}
				++nm;
			} else {
				if (thiscat->u1.h.xfile[0] != '/') {
					char *thiscwd;
					char dummy[STAGE_MAX_HSMLENGTH+1];
			  
					/* This is a non-asbolute non-HPSS (e.g. CASTOR) HSM file */
			  
					if ((thiscwd = rfio_getcwd(NULL,CA_MAXPATHLEN+1)) != NULL) {
						/* We check that we have enough room for such a CASTOR full pathname */
						if ((strlen(thiscwd) + 1 + strlen(thiscat->u1.h.xfile)) > STAGE_MAX_HSMLENGTH) {
							free(thiscwd);
							serrno = EFAULT;
							return(-1);
						}
						strcpy(dummy, thiscwd);
						strcat(dummy, "/");
						strcat(dummy, thiscat->u1.h.xfile);
						strcpy(thiscat->u1.h.xfile, dummy);
						free(thiscwd);
					}
				}
				++nh;
			}
			break;
		default:
			break;
		}
		thiscat->t_or_d = t_or_d;
	}
	if ((nm > 0) || (nh > 0)) {
		/* There were HSM files in the request */
		if (nm == nstcp_input) {
			/* And they are all non-CASTOR files */
			t_or_d = 'm';
		} else if (nh == nstcp_input) {
			/* And they are all CASTOR files */
			t_or_d = 'h';
		} else {
			stage_errmsg(func, "Non-CASTOR and CASTOR files on the same api call is not allowed\n");
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		/* We make sure all t_or_d's are correct */
		for (istcp = 0; istcp < nstcp_input; istcp++) {
			stcp_input[istcp].t_or_d = (nm > 0) ? 'm' : 'h';
		}
	}
	sendbuf_size += (nstcp_input * sizeof(struct stgcat_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */

	for (istpp = 0; istpp < nstpp_input; istpp++) {
		thispath = &(stpp_input[istpp]);                           /* Current path structure */
		if (thispath->upath[0] == '\0') {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		if ((flags & STAGE_NOLINKCHECK) != STAGE_NOLINKCHECK) {
			user_path[0] = '\0';
			if ((build_linkname_status = build_linkname(thispath->upath, user_path, sizeof(user_path), req_type)) == SESYSERR) {
				serrno = ESTLINKNAME;
#if defined(_WIN32)
				WSACleanup();
#endif
				return(-1);
			}
			if (build_linkname_status) {
				serrno = ESTLINKNAME;
#if defined(_WIN32)
				WSACleanup();
#endif
				return(-1);
			}
			strcpy(thispath->upath,user_path);
		}
	}
	sendbuf_size += (nstpp_input * sizeof(struct stgpath_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */


	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}
	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	if ((flagsok & STAGE_GRPUSER) == STAGE_GRPUSER) {
		marshall_LONG (sbp, Geuid);              /* Uid under which it is to be done */
	} else {
		marshall_LONG (sbp, euid);
	}
	marshall_LONG (sbp, egid);                 /* gid */
	marshall_HYPER (sbp, uniqueid);            /* Our uniqueid */
	marshall_HYPER (sbp, flagsok);             /* Flags */
	marshall_BYTE (sbp, t_or_d);               /* Type of request (you cannot merge Tape/Disk/Allocation/Hsm) */
	marshall_LONG (sbp, nstcp_input);          /* Number of input stgcat_entry structures */
	marshall_LONG (sbp, nstpp_input);          /* Number of input stgpath_entry structures */
	for (istcp = 0; istcp < nstcp_input; istcp++) {
		thiscat = &(stcp_input[istcp]);               /* Current catalog structure */
		status = 0;
		marshall_STAGE_CAT(magic_client,magic_server,STAGE_INPUT_MODE,status,sbp,thiscat); /* Structures */
		if (status != 0) {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
	}
	for (istpp = 0; istpp < nstpp_input; istpp++) {
		thispath = &(stpp_input[istpp]);                           /* Current path structure */
		status = 0;
		marshall_STAGE_PATH(magic_client,magic_server,STAGE_INPUT_MODE,status,sbp,thispath); /* Structures */
		if (status != 0) {
			serrno = EINVAL;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
	}

	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		int original_serrno = -1;
		c = send2stgd(hostname, req_type, flagsok, sendbuf, msglen, 1, NULL, (size_t) 0, 0, NULL, 0, NULL, NULL, NULL);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			original_serrno = ((serrno != SECONNDROP) && (serrno != SETIMEDOUT)) ? errno : serrno;
			/* Server do not support this protocol */
			/* We keep client magic number as it is now but we downgrade magic server */
			switch (magic_server) {
			case STGMAGIC4:
				magic_server = STGMAGIC3;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_clr_retry;
			case STGMAGIC3:
				magic_server = STGMAGIC2;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_clr_retry;
			default:
				stage_errmsg(func, "%s\n", (original_serrno >= 0) ? sstrerror(original_serrno) : neterror());
				serrno = SEOPNOTSUP;
				break;
			}
			break;
		}
		if ((c == 0) ||
			(serrno == EINVAL) || (serrno == EBUSY) || (serrno == ENOUGHF) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == ENOENT) || (serrno == SECHECKSUM) || (serrno == SENAMETOOLONG)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif
	return (c == 0 ? 0 : -1);
}

int DLL_DECL stage_clr_Tape(flags,hostname,tape,fseq)
	u_signed64 flags;
	char *hostname;
	char *tape;
	char *fseq;
{
	struct stgcat_entry stcp_input;

	/* Check tape in input */
	if (tape == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check tape length and poolname validity */
	if (((*tape == '\0') || (strlen(tape)        > CA_MAXVIDLEN      )) ||
		((fseq  != NULL) && (strlen(fseq)        > CA_MAXFSEQLEN     ))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageclr API request */
	memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	strcpy(stcp_input.u1.t.vid[0],tape); /* Cannot be zero-length */
	if (fseq != NULL) strcpy(stcp_input.u1.t.fseq,fseq); /* Can be zero-length */
	return(stage_clr_tape(flags,hostname,1,&stcp_input));
}

int DLL_DECL stage_clr_Hsm(flags,hostname,hsmname)
	u_signed64 flags;
	char *hostname;
	char *hsmname;
{
	struct stgcat_entry stcp_input;

	/* Check hsmname in input */
	if (hsmname == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check hsmname length and poolname validity */
	if (((*hsmname == '\0') || (strlen(hsmname)  > STAGE_MAX_HSMLENGTH))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageclr API request */
	memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	strcpy(stcp_input.u1.h.xfile,hsmname); /* Cannot be zero-length */
	return(stage_clr_hsm(flags,hostname,1,&stcp_input));
}


int DLL_DECL stage_clr_Disk(flags,hostname,diskname)
	u_signed64 flags;
	char *hostname;
	char *diskname;
{
	struct stgcat_entry stcp_input;

	/* Check diskname in input */
	if (diskname == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check diskname length and poolname validity */
	if (((*diskname == '\0') || (strlen(diskname)  > (CA_MAXHOSTNAMELEN+MAXPATH)))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageclr API request */
	memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	strcpy(stcp_input.u1.d.xfile,diskname); /* Cannot be zero-length */
	return(stage_clr_disk(flags,hostname,1,&stcp_input));
}

int DLL_DECL stage_clr_Path(flags,hostname,pathname)
	u_signed64 flags;
	char *hostname;
	char *pathname;
{
	struct stgpath_entry stpp_input;

	/* Check pathname in input */
	if (pathname == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check pathname length and poolname validity */
	if (((*pathname == '\0') || (strlen(pathname)  > (CA_MAXHOSTNAMELEN+MAXPATH)))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageclr API request */
	memset(&stpp_input, 0, sizeof(struct stgpath_entry));
	strcpy(stpp_input.upath,pathname); /* Cannot be zero-length */
	return(stage_clr_path(flags,hostname,1,&stpp_input));
}

int DLL_DECL stage_clr_Link(flags,hostname,linkname)
	u_signed64 flags;
	char *hostname;
	char *linkname;
{
	struct stgpath_entry stpp_input;

	/* Check linkname in input */
	if (linkname == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	/* Check linkname length and poolname validity */
	if (((*linkname == '\0') || (strlen(linkname)  > (CA_MAXHOSTNAMELEN+MAXPATH)))) {
		serrno = EINVAL;
		return(-1);
	}

	/* We build the full stageclr API request */
	memset(&stpp_input, 0, sizeof(struct stgpath_entry));
	strcpy(stpp_input.upath,linkname); /* Cannot be zero-length */
	return(stage_clr_link(flags,hostname,1,&stpp_input));
}

int DLL_DECL stage_ping(flags,hostname)
	u_signed64 flags;
	char *hostname;
{
	int req_type = STAGE_PING;
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	gid_t egid;                   /* Current effective gid */
	int c;                        /* Output of send2stgd() */
	int pid;
	int Tid;
	u_signed64 uniqueid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *func = "stage_ping";
	int maxretry = MAXRETRY;
	int magic_client;
	int magic_server;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_client = magic_server = stage_stgmagic();
  
  _stage_ping_retry:
	ntries = nstg161 = Tid = 0;
	uniqueid = 0;

	euid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += HYPERSIZE;               /* Flags */

	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	marshall_LONG (sbp, egid);                 /* gid */
	marshall_HYPER (sbp, flags);               /* Flags */
	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		int original_serrno = -1;
		c = send2stgd(hostname, req_type, (u_signed64) 0, sendbuf, msglen, 1, NULL, (size_t) 0, 0, NULL, NULL, NULL, NULL, NULL);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			original_serrno = ((serrno != SECONNDROP) && (serrno != SETIMEDOUT)) ? errno : serrno;
			/* Server do not support this protocol */
			/* We keep client magic number as it is now but we downgrade magic server */
			switch (magic_server) {
			case STGMAGIC4:
				magic_server = STGMAGIC3;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_ping_retry;
			case STGMAGIC3:
				magic_server = STGMAGIC2;
#if defined(_WIN32)
				WSACleanup();
#endif
				goto _stage_ping_retry;
			default:
				stage_errmsg(func, "%s\n", (original_serrno >= 0) ? sstrerror(original_serrno) : neterror());
				serrno = SEOPNOTSUP;
				break;
			}
			break;
		}
		if ((c == 0) || (serrno == EINVAL) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SECHECKSUM) || (serrno == SENAMETOOLONG)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif

	return (c);
}

int DLL_DECL stage_init(flags,hostname)
	u_signed64 flags;
	char *hostname;
{
	int req_type = STAGE_INIT;
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	gid_t egid;                   /* Current effective gid */
	int c;                        /* Output of send2stgd() */
	int pid;
	int Tid;
	u_signed64 uniqueid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *func = "stage_init";
	int maxretry = MAXRETRY;
	int magic_server;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_server = stage_stgmagic();
  
	ntries = nstg161 = Tid = 0;
	uniqueid = 0;

	euid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	sendbuf_size += LONGSIZE;                /* Uid */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += HYPERSIZE;               /* Flags */

	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	marshall_LONG (sbp, euid);                 /* uid */
	marshall_LONG (sbp, egid);                 /* gid */
	marshall_HYPER (sbp, flags);               /* Flags */
	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		c = send2stgd(hostname, req_type, (u_signed64) 0, sendbuf, msglen, 1, NULL, (size_t) 0, 0, NULL, NULL, NULL, NULL, NULL);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			/* Server do not support this protocol - STAGE_INIT exist only since level STGMAGIC4 */
			return(stageinit(flags,hostname));
		}
		if ((c == 0) || (serrno == EINVAL) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SENAMETOOLONG) || (serrno == ESTCONF) || (serrno == SECHECKSUM) || (serrno == ECUPVNACT)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif

	return (c);
}

/* Version of stage_init that is compatible with the command-line protocol */
static int DLL_DECL stageinit(flags,hostname)
	u_signed64 flags;
	char *hostname;
{
	int req_type = STAGEINIT;
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	gid_t egid;                   /* Current effective gid */
	int c;                        /* Output of send2stgd() */
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *func = "stageinit";
	int maxretry = MAXRETRY;
	int magic_server;
	int argc;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_server = STGMAGIC;
  
	ntries = nstg161 = 0;

	euid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	sendbuf_size += WORDSIZE;                /* Gid */
	sendbuf_size += WORDSIZE;                /* argc */
	sendbuf_size += (strlen(func)+1);        /* argv[0] */
	argc = 1;
	if (hostname != NULL && hostname[0] != '\0') {
		sendbuf_size += 3;                   /* "-h" */
		sendbuf_size += (strlen(hostname)+1); /* hostname */
		argc += 2;
	}
	if ((flags & STAGE_FORCE) == STAGE_FORCE) {
		sendbuf_size += 3;                   /* "-F" */
		argc++;
	}
	if ((flags & STAGE_MIGRINIT) == STAGE_MIGRINIT) {
		sendbuf_size += 3;                   /* "-X" */
		argc++;
	}

	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	marshall_WORD (sbp, egid);                 /* gid */
	marshall_WORD (sbp, argc);                 /* argc */
	marshall_STRING(sbp, func);                /* argv[0] */
	if (hostname != NULL && hostname[0] != '\0') {
		marshall_STRING(sbp, "-h");
		marshall_STRING(sbp, hostname);
	}
	if ((flags & STAGE_FORCE) == STAGE_FORCE) {
		marshall_STRING(sbp, "-F");
	}
	if ((flags & STAGE_MIGRINIT) == STAGE_MIGRINIT) {
		marshall_STRING(sbp, "-X");
	}
	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		c = send2stgd(hostname, req_type, (u_signed64) 0, sendbuf, msglen, 1, NULL, (size_t) 0, 0, NULL, NULL, NULL, NULL, NULL);
		if ((c == 0) || (serrno == EINVAL) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SENAMETOOLONG) || (serrno == ESTCONF) || (serrno == SECHECKSUM) || (serrno == ECUPVNACT)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif

	return (c);
}

int DLL_DECL stage_shutdown(flags,hostname)
	u_signed64 flags;
	char *hostname;
{
	int req_type = STAGE_SHUTDOWN;
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	gid_t egid;                   /* Current effective gid */
	int c;                        /* Output of send2stgd() */
	int pid;
	int Tid;
	u_signed64 uniqueid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *func = "stage_shutdown";
	int maxretry = MAXRETRY;
	int magic_server;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_server = stage_stgmagic();
  
	ntries = nstg161 = Tid = 0;
	uniqueid = 0;

	euid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	sendbuf_size += LONGSIZE;                /* Uid */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += HYPERSIZE;               /* Flags */

	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	marshall_LONG (sbp, euid);                 /* uid */
	marshall_LONG (sbp, egid);                 /* gid */
	marshall_HYPER (sbp, flags);               /* Flags */
	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		c = send2stgd(hostname, req_type, (u_signed64) 0, sendbuf, msglen, 1, NULL, (size_t) 0, 0, NULL, NULL, NULL, NULL, NULL);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			/* Server do not support this protocol - STAGE_SHUTDOWN exist only since level STGMAGIC4 */
			return(stageshutdown(flags,hostname));
		}
		if ((c == 0) || (serrno == EINVAL) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SENAMETOOLONG) || (serrno == ESTCONF) || (serrno == SECHECKSUM) || (serrno == ECUPVNACT)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif

	return (c);
}

/* Version of stage_shutdown that is compatible with the command-line protocol */
static int DLL_DECL stageshutdown(flags,hostname)
	u_signed64 flags;
	char *hostname;
{
	int req_type = STAGESHUTDOWN;
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	gid_t egid;                   /* Current effective gid */
	int c;                        /* Output of send2stgd() */
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *func = "stageshutdown";
	int maxretry = MAXRETRY;
	int magic_server;
	int argc;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_server = STGMAGIC;
  
	ntries = nstg161 = 0;

	euid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */

	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
	sendbuf_size += WORDSIZE;                /* Gid */
	sendbuf_size += WORDSIZE;                /* argc */
	sendbuf_size += (strlen(func)+1);        /* argv[0] */
	argc = 1;
	if (hostname != NULL && hostname[0] != '\0') {
		sendbuf_size += 3;                   /* "-h" */
		sendbuf_size += (strlen(hostname)+1); /* hostname */
		argc += 2;
	}
	if ((flags & STAGE_FORCE) == STAGE_FORCE) {
		sendbuf_size += 3;                   /* "-F" */
		argc++;
	}

	/* Will we go over the maximum socket size (we say -1000 just to be safe v.s. header size) */
	/* And anyway MAX_NETDATA_SIZE is alreayd 1MB by default which is high! */
	if (sendbuf_size > MAX_NETDATA_SIZE) {
		serrno = ESTMEM;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING(sbp, pw->pw_name);         /* Login name */
	marshall_WORD (sbp, egid);                 /* gid */
	marshall_WORD (sbp, argc);                 /* argc */
	marshall_STRING(sbp, func);                /* argv[0] */
	if (hostname != NULL && hostname[0] != '\0') {
		marshall_STRING(sbp, "-h");
		marshall_STRING(sbp, hostname);
	}
	if ((flags & STAGE_FORCE) == STAGE_FORCE) {
		marshall_STRING(sbp, "-F");
	}

	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		c = send2stgd(hostname, req_type, (u_signed64) 0, sendbuf, msglen, 1, NULL, (size_t) 0, 0, NULL, NULL, NULL, NULL, NULL);
		if ((c == 0) || (serrno == EINVAL) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ENOENT) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SENAMETOOLONG) || (serrno == ESTCONF) || (serrno == SECHECKSUM) || (serrno == ECUPVNACT)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);

#if defined(_WIN32)
	WSACleanup();
#endif

	return (c);
}

int DLL_DECL stage_alloc_or_get(req_type,flags,openmode,hostname,pooluser,filename,filesize,nstcp_output,stcp_output)
	int req_type;
	u_signed64 flags;
	mode_t openmode;
	char *hostname;
	char *pooluser;
	char *filename;
	u_signed64 filesize;
	int *nstcp_output;
	struct stgcat_entry **stcp_output;
{
	int msglen;                   /* Buffer length (incremental) */
	int ntries;                   /* Number of retries */
	int nstg161;                  /* Number of STG161 messages */
	struct passwd *pw;            /* Password entry */
	struct group *gr;             /* Group entry */

	char *sbp, *p, *q;            /* Internal pointers */
	char *sendbuf;                /* Socket send buffer pointer */
	size_t sendbuf_size;          /* Total socket send buffer length */
	uid_t euid;                   /* Current effective uid */
	uid_t Geuid;                  /* Forced effective uid (-G option) */
	char Gname[CA_MAXUSRNAMELEN+1]; /* Forced effective name (-G option) */
	char User[CA_MAXUSRNAMELEN+1];  /* Forced internal path with user (-u options) */
	char user_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	gid_t egid;                   /* Current effective gid */
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int mask;                     /* User mask */
#else
	mode_t mask;                  /* User mask */
#endif
	int pid;                      /* Current pid */
	int status;                   /* Variable overwritten by macros in header */
	int c;                        /* Output of build_linkname() */
	char *func = "stage_alloc";

#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char stgpool_forced[CA_MAXPOOLNAMELEN + 1];
	int Tid;
	u_signed64 uniqueid;
	int maxretry = MAXRETRY;
	int magic_client;
	int magic_server;
	struct stgcat_entry stcp_input;
	struct stgcat_entry *thiscat;
	int build_linkname_status;    /* Status of build_linkname() call */
	char t_or_d;

	/* We suppose first that server is at the same level as the client from protocol point of view */
	magic_client = magic_server = stage_stgmagic();
  
	ntries = nstg161 = Tid = 0;
	uniqueid = 0;

	/* Only valid to STAGE_ALLOC or STAGE_GET */
	if ((req_type != STAGE_ALLOC) && (req_type != STAGE_GET)) {
		serrno = EINVAL;
		return(-1);
	}

	/* It is not allowed to have no input */
	if (filename == NULL) {
		serrno = EFAULT;
		return(-1);
	}
	if (*filename == '\0') {
		serrno = EINVAL;
		return(-1);
	}
	/* Or a too big input */
	if (strlen(filename) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
		serrno = SENAMETOOLONG;
		return(-1);
	}

	/* We check existence of an STG POOL from environment variable or configuration */
	if ((p = getenv("STAGE_POOL")) != NULL) {
		strncpy(stgpool_forced,p,CA_MAXPOOLNAMELEN);
		stgpool_forced[CA_MAXPOOLNAMELEN] = '\0';
	} else if ((p = getconfent("STG","POOL",0)) != NULL) {
		strncpy(stgpool_forced,p,CA_MAXPOOLNAMELEN);
		stgpool_forced[CA_MAXPOOLNAMELEN] = '\0';
	} else {
		stgpool_forced[0] = '\0';
	}

	euid = Geuid = geteuid();             /* Get current effective uid */
	egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	/* It is not allowed to have stcp_output != NULL and nstcp_output == NULL */
	if ((stcp_output != NULL) && (nstcp_output == NULL)) {
		serrno = EFAULT;
		return(-1);
	}
	/* We check existence of an STG NORETRY from environment variable or configuration or flags */
	if (
		(((p = getenv("STAGE_NORETRY")) != NULL) && (atoi(p) != 0)) ||
		(((p = getconfent("STG","NORETRY",0)) != NULL) && (atoi(p) != 0)) ||
		((flags & STAGE_NORETRY) == STAGE_NORETRY)
		) {
		/* Makes sure STAGE_NORETRY is anyway in the flags so that the server will log it */
		flags |= STAGE_NORETRY;
		maxretry = 0;
	}

	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {  /* User want to overwrite euid under which request is processed by stgdaemon */
		if ((gr = Cgetgrgid(egid)) == NULL) { /* This is allowed only if its group exist */
			if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetgrgid", strerror(errno));
			stage_errmsg(func, STG36, egid);
			serrno = ESTGROUP;
			return(-1);
		}
		if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) { /* And if GRPUSER is configured */
			stage_errmsg(func, STG10, gr->gr_name);
			serrno = ESTGRPUSER;
			return(-1);
		} else {
			strcpy (Gname, p);
			if ((pw = Cgetpwnam(p)) == NULL) { /* And if GRPUSER content is a valid user name */
				if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwnam", strerror(errno));
				stage_errmsg(func, STG11, p);
				serrno = SEUSERUNKN;
				return(-1);
			} else
				Geuid = pw->pw_uid;              /* In such a case we grab its uid */
		}
	}

	/* If no user specified we check environment variable STAGE_USER */
	if (((pooluser == NULL) || (pooluser[0] == '\0')) && (p = getenv ("STAGE_USER")) != NULL) {
		strncpy(User,p,CA_MAXUSRNAMELEN);
		User[CA_MAXUSRNAMELEN] = '\0';
		/* We verify this user login is defined */
		if (((pw = Cgetpwnam(User)) == NULL) || (pw->pw_gid != egid)) {
			if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwnam", strerror(errno));
			stage_errmsg(func, STG11, User);
			serrno = SEUSERUNKN;
			return(-1);
		}
	} else if ((pooluser != NULL) && (pooluser[0] != '\0')) {
		strncpy(User,pooluser,CA_MAXUSRNAMELEN);
		User[CA_MAXUSRNAMELEN] = '\0';
	} else {
		User[0] = '\0';
	}

	t_or_d = 'a';

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		serrno = SEINTERNAL;
		return(-1);
	}
#endif
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	if ((pw = Cgetpwuid(euid)) == NULL) { /* We check validity of current effective uid */
		if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* Our uniqueid is a combinaison of our pid and our threadid */
	pid = getpid();
	Cglobals_getTid(&Tid); /* Get CthreadId  - can be -1 */
	Tid++;
	uniqueid = (((u_signed64) pid) * ((u_signed64) 0xFFFFFFFF)) + Tid;

	if (stage_setuniqueid((u_signed64) uniqueid) != 0) {
#if defined(_WIN32)
		WSACleanup();
#endif
		return (-1);
	}

	/* How many bytes do we need ? */
	sendbuf_size = 3 * LONGSIZE;             /* Request header (magic number + req_type + msg length) */
	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {
		sendbuf_size += strlen(Gname) + 1;     /* Name under which request is to be done */
	} else {
		sendbuf_size += strlen(pw->pw_name) + 1;
	}
	sendbuf_size += LONGSIZE;                /* Uid */
	sendbuf_size += LONGSIZE;                /* Gid */
	sendbuf_size += LONGSIZE;                /* Mask */
	sendbuf_size += LONGSIZE;                /* Pid */
	sendbuf_size += HYPERSIZE;               /* Our uniqueid  */
	sendbuf_size += strlen(User) + 1;        /* User for internal path (default to STAGERSUPERUSER in stgdaemon) */
	sendbuf_size += HYPERSIZE;               /* Flags */
	if (req_type == STAGE_ALLOC) {
		sendbuf_size += LONGSIZE;            /* openmode */
	}
	sendbuf_size += BYTESIZE;                /* t_or_d */
	sendbuf_size += LONGSIZE;                /* Number of catalog structures */

	memset ((char *)&stcp_input, 0, sizeof(stcp_input));
	thiscat = &stcp_input;                   /* Current catalog structure */
	/* If this entry has no poolname we copy it the forced one */
	if ((thiscat->poolname[0] == '\0') && (stgpool_forced[0] != '\0')) {
		strcpy(thiscat->poolname, stgpool_forced);
	}
	thiscat->t_or_d = t_or_d;
	if (req_type == STAGE_ALLOC) {
		thiscat->size = filesize;            /* Can be zero */
	}
	strcpy(thiscat->u1.d.xfile,filename);
	if ((flags & STAGE_NOLINKCHECK) != STAGE_NOLINKCHECK) {
		user_path[0] = '\0';
		if ((build_linkname_status = build_linkname(thiscat->u1.d.xfile, user_path, sizeof(user_path), req_type)) == SESYSERR) {
			serrno = ESTLINKNAME;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		if (build_linkname_status) {
			serrno = ESTLINKNAME;
#if defined(_WIN32)
			WSACleanup();
#endif
			return(-1);
		}
		strcpy(thiscat->u1.d.xfile,user_path);
	}

	sendbuf_size += sizeof(struct stgcat_entry); /* We overestimate by few bytes (gaps and strings zeroes) */

	/* Allocate memory */
	if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, magic_server);
	marshall_LONG (sbp, req_type);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {
		marshall_STRING (sbp, Gname);            /* Name under which is is to be done */
		marshall_LONG (sbp, Geuid);              /* Uid under which it is to be done */
	} else {
		marshall_STRING (sbp, pw->pw_name);
		marshall_LONG (sbp, euid);
	}
	marshall_LONG (sbp, egid);                 /* gid */
	umask (mask = umask(0));
	marshall_LONG (sbp, mask);                 /* umask */
	marshall_LONG (sbp, pid);                  /* pid */
	marshall_HYPER (sbp, uniqueid);            /* Our uniqueid */
	marshall_STRING (sbp, User);               /* User internal path (default to STAGERSUPERUSER in stgdaemon) */
	marshall_HYPER (sbp, flags);               /* Flags */
	if (req_type == STAGE_ALLOC) {
		marshall_LONG (sbp, openmode);         /* openmode */
	}
	marshall_BYTE (sbp, t_or_d);               /* Type of request (you cannot merge Tape/Disk/Allocation/Hsm) */
	marshall_LONG (sbp, 1);                    /* Number of input stgcat_entry structures */
	thiscat = &stcp_input;                     /* Current catalog structure */
	status = 0;
	marshall_STAGE_CAT(magic_client,magic_server,STAGE_INPUT_MODE,status,sbp,thiscat); /* Structures */
	if (status != 0) {
		serrno = EINVAL;
#if defined(_WIN32)
		WSACleanup();
#endif
		return(-1);
	}

	/* Update request header */
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Dial with the daemon */
	while (1) {
		int original_serrno = -1;
		c = send2stgd(hostname, req_type, flags, sendbuf, msglen, 1, NULL, (size_t) 0, 1, &stcp_input, nstcp_output, stcp_output, NULL, NULL);
		if ((c != 0) &&
#if !defined(_WIN32)
			(serrno == SECONNDROP || errno == ECONNRESET)
#else
			(serrno == SECONNDROP || serrno == SETIMEDOUT)
#endif
			) {
			original_serrno = ((serrno != SECONNDROP) && (serrno != SETIMEDOUT)) ? errno : serrno;
			/* Server do not support this protocol */
			/* We keep client magic number as it is now but we downgrade magic server */
			stage_errmsg(func, "%s\n", (original_serrno >= 0) ? sstrerror(original_serrno) : neterror());
			serrno = SEOPNOTSUP;
			break;
		}
		if ((c == 0) ||
			(serrno == EINVAL) || (serrno == EACCES) || (serrno == EPERM) ||
			(serrno == ENOENT) || (serrno == SENAMETOOLONG) ||
			(serrno == EISDIR) ||
			(serrno == ERTMNYPARY) || (serrno == ERTLIMBYSZ) || (serrno == ESTCLEARED) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SECHECKSUM) || (serrno == SEGROUPUNKN) ||
			(serrno == ESTKILLED)  || (serrno == ENOSPC) || (serrno == EBUSY) || (serrno == ESTLNKNSUP)) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
		if (serrno == ESTNACT && nstg161++ == 0 && ((flags & STAGE_NORETRY) != STAGE_NORETRY)) stage_errmsg(NULL, STG161);
		if (serrno != ESTNACT && ntries++ > maxretry) break;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) break;  /* To be sure we always break if --noretry is in action */
		stage_sleep (RETRYI);
	}
	free(sendbuf);
  
#if defined(_WIN32)
	WSACleanup();
#endif
	return (c == 0 ? 0 : -1);
}
