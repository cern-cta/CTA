/*
 * $Id: stage_api.c,v 1.8 2001/02/02 08:56:52 jdurand Exp $
 */

#include <stdlib.h>            /* For malloc(), etc... */
#include <stddef.h>            /* For NULL definition */
#include <string.h>
#include "osdep.h"             /* For OS dependencies */
#include "serrno.h"            /* For CASTOR's errno */
#include "Castor_limits.h"     /* Get all hardcoded CASTOR constants */
#include "stage.h"             /* To get the API req_type constants - bijection with stgdaemon */
#include "marshall.h"          /* For marshalling macros */
#include "stage_api.h"         /* For definitions */
#include "Cpwd.h"              /* For CASTOR password functions */
#include "Cgrp.h"              /* For CASTOR group functions */
#include "rfio_api.h"          /* RFIO API */
#include "u64subr.h"           /* u_signed64 conversion routines */

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

extern char *getenv();         /* To get environment variables */
extern char *getconfent();     /* To get configuration entries */
EXTERN_C int DLL_DECL sysreq _PROTO((char *, char *, int *, char *, int *));
extern int rfio_access _PROTO((char *, int));

int stage_api_chkdirw _PROTO((char *));
#if TMS
int stage_api_tmscheck _PROTO((char *, char *, char *, char *, char *));
#endif
int copyrc_shift2castor _PROTO((int));

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

#define STSTR2BUF(stcp,member,option,buf,bufsize) {                                       \
  if ((stcp)->member[0] != '\0') {                                                        \
    if ((strlen(buf) + 4 + strlen((stcp)->member) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    sprintf(buf + strlen(buf)," -%c %s",option,(stcp)->member);                           \
  }                                                                                       \
}

#define STSTR2BUF_V2(stcp,member,option,buf,bufsize) {                                    \
  if ((stcp)->member[0] != '\0') {                                                        \
    if ((strlen(buf) + strlen(option) + 2 + strlen((stcp)->member) + 1) > bufsize) { serrno = SEUMSG2LONG; return(-1); } \
    sprintf(buf + strlen(buf)," %s %s",option,(stcp)->member);                            \
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

int DLL_DECL copyrc_castor2shift(copyrc)
     int copyrc;
{
  /* Input  is a CASTOR return code */
  /* Output is a SHIFT  return code */
  switch (copyrc) {
  case ERTBLKSKPD:
    return(BLKSKPD);
  case ERTTPE_LSZ:
    return(TPE_LSZ);
  case ERTMNYPARY:
  case ETPARIT:
    return(MNYPARI);
  case ERTLIMBYSZ:
    return(LIMBYSZ);
  default:
    return(copyrc);
  }
}

int DLL_DECL stage_iowc(req_type,t_or_d,flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
     int req_type;
     char t_or_d;
     u_signed64 flags;
     int openflags;
     mode_t openmode;
     char *hostname;
     char *pooluser;
     int nstcp_input;
     struct stgcat_entry *stcp_input;
     int *nstcp_output;
     struct stgcat_entry **stcp_output;
     int nstpp_input;
     struct stgpath_entry *stpp_input;     
{
  struct stgcat_entry *thiscat; /* Catalog current pointer */
  struct stgpath_entry *thispath; /* Path current pointer */
  int istcp;                    /* Counter on catalog structures passed in the protocol */
  int istpp;                    /* Counter on path structures passed in the protocol */
  int msglen;                   /* Buffer length (incremental) */
  int ntries;                   /* Number of retries */
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
  int errflg = 0;               /* Error flag */
  int build_linkname_status;    /* Status of build_linkname() call */
  char user_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
  int c;                        /* Output of build_linkname() */
  static char cmdnames_api[4][9] = {"stage_cat", "stage_in", "stage_out", "stage_wrt"};
  char *func;
#if defined(_WIN32)
  WSADATA wsadata;
#endif
  int enospc_retry = -1;
  int enospc_retryint = -1;
  int enospc_ntries = 0;

  /* It is not allowed to have no input */
  if ((nstcp_input <= 0) || (stcp_input == NULL)) {
    serrno = EINVAL;
    return(-1);
  }

  switch (req_type) {                   /* This method works only for: */
  case STAGE_IN:                        /* - stagein */
    func = cmdnames_api[1];
    break;
  case STAGE_OUT:                       /* - stageout */
    func = cmdnames_api[2];
    /* Grab the ENOSPC_RETRY configuration */
    if (((p = getenv("STAGE_ENOSPC_RETRY")) != NULL) || ((p = getconfent("STG","ENOSPC_RETRY")) != NULL)) {
      enospc_retry = atoi(p);
    }
    if (enospc_retry < 0) {
      /* Invalid value */
      enospc_retry = MAXRETRY;
      stage_errmsg(func, "ENOSPC_RETRY value set to %d\n", enospc_retry);
    }
    /* Grab the ENOSPC_RETRYINT configuration */
    if (((p = getenv("STAGE_ENOSPC_RETRYINT")) != NULL) || ((p = getconfent("STG","ENOSPC_RETRYINT")) != NULL)) {
      enospc_retryint = atoi(p);
    }
    if (enospc_retryint < 0) {
      /* Invalid value */
      enospc_retryint = RETRYI;
      stage_errmsg(func, "ENOSPC_RETRYINT value set to %d\n", enospc_retryint);
    }
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

  euid = geteuid();             /* Get current effective uid */
  egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
  if (euid < 0 || egid < 0) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* It is not allowed to have stcp_output != NULL and nstcp_output == NULL */
  if ((stcp_output != NULL) && (nstcp_output == NULL)) {
    serrno = EINVAL;
    return(-1);
  }

  if ((flags & STAGE_GRPUSER) == STAGE_GRPUSER) {  /* User want to overwrite euid under which request is processed by stgdaemon */
    if ((gr = Cgetgrgid(egid)) == NULL) { /* This is allowed only if its group exist */
      stage_errmsg(func, STG36, egid);
      serrno = ESTGROUP;
      return(-1);
    }
    if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) { /* And if GRPUSER is configured */
      stage_errmsg(func, STG10, gr->gr_name);
      serrno = ESTGRPUSER;
      errflg++;
    } else {
      strcpy (Gname, p);
      if ((pw = Cgetpwnam(p)) == NULL) { /* And if GRPUSER content is a valid user name */
        stage_errmsg(func, STG11, p);
        serrno = ESTUSER;
        errflg++;
      } else
        Geuid = pw->pw_uid;              /* In such a case we grab its uid */
    }
  }

  /* If no user specified we check environment variable STAGE_USER */
  if (((pooluser == NULL) || (pooluser[0] == '\0')) && (p = getenv ("STAGE_USER")) != NULL) {
    strcpy(User,p);
    /* We verify this user login is defined */
    if (((pw = Cgetpwnam(User)) == NULL) || (pw->pw_gid != egid)) {
      stage_errmsg(func, STG11, User);
      serrno = ESTUSER;
      return(-1);
    }
  } else {
    User[0] = '\0';
  }

  switch (t_or_d) {                     /* This method supports only */
  case 't':                             /* - tape files */
  case 'd':                             /* - disk files */
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
    serrno = SENOMAPFND;
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
  sendbuf_size += strlen(User) + 1;        /* User for internal path (default to "stage" in stgdaemon) */
  sendbuf_size += HYPERSIZE;               /* Flags */
  sendbuf_size += LONGSIZE;                /* openflags */
  sendbuf_size += LONGSIZE;                /* openmode */
  sendbuf_size += BYTESIZE;                /* t_or_d */
  sendbuf_size += LONGSIZE;                /* Number of catalog structures */
  sendbuf_size += LONGSIZE;                /* Number of path structures */
  for (istcp = 0; istcp < nstcp_input; istcp++) {
    int i, numvid, numvsn;

    thiscat = &(stcp_input[istcp]);              /* Current catalog structure */
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
#if TMS
          if (stage_api_tmscheck (thiscat->u1.t.vid[i], thiscat->u1.t.vsn[i], thiscat->u1.t.dgn, thiscat->u1.t.den, thiscat->u1.t.lbl)) {
            stage_errmsg(func, "STG02 - TMSCHECK(%s,%s,%s,%s,%s) error\n",
                         thiscat->u1.t.vid[i], thiscat->u1.t.vsn[i], thiscat->u1.t.dgn, thiscat->u1.t.den, thiscat->u1.t.lbl);
            serrno = ESTTMSCHECK;
#if defined(_WIN32)
            WSACleanup();
#endif
            return(-1);
          }
#else
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
      if (! ISCASTOR(thiscat->u1.m.xfile)) {
        char *dummy;
        char *optarg = thiscat->u1.m.xfile;
        char *hsm_host;
        char hsm_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];

        /* We prepend HSM_HOST only for non CASTOR-like files */
        if (((dummy = strchr(optarg,':')) == NULL) || (dummy != optarg && strrchr(dummy,'/') == NULL)) {
          if ((hsm_host = getenv("HSM_HOST")) != NULL) {
            strcpy (hsm_path, hsm_host);
            strcat (hsm_path, ":");
            strcat (hsm_path, optarg);
          } else if ((hsm_host = getconfent("STG", "HSM_HOST",0)) != NULL) {
            strcpy (hsm_path, hsm_host);
            strcat (hsm_path, ":");
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
      }
      break;
    default:
      break;
    }
    thiscat->t_or_d = t_or_d;
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
    if ((build_linkname_status = build_linkname(thispath->upath, user_path, sizeof(user_path), req_type)) == SYERR) {
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
  }
  sendbuf_size += (nstpp_input * sizeof(struct stgpath_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */


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
  marshall_LONG (sbp, STGMAGIC);
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
  pid = getpid();
  marshall_LONG (sbp, pid);                  /* pid */
  marshall_STRING (sbp, User);               /* User internal path (default to "stage" in stgdaemon) */
  marshall_HYPER (sbp, flags);               /* Flags */
  marshall_LONG (sbp, openflags);            /* openflags */
  marshall_LONG (sbp, openmode);             /* openmode */
  marshall_BYTE (sbp, t_or_d);               /* Type of request (you cannot merge Tape/Disk/Allocation/Hsm) */
  marshall_LONG (sbp, nstcp_input);          /* Number of input stgcat_entry structures */
  marshall_LONG (sbp, nstpp_input);          /* Number of input stgpath_entry structures */
  for (istcp = 0; istcp < nstcp_input; istcp++) {
    thiscat = &(stcp_input[istcp]);               /* Current catalog structure */
    status = 0;
    marshall_STAGE_CAT(STAGE_INPUT_MODE,status,sbp,thiscat); /* Structures */
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
    marshall_STAGE_PATH(STAGE_INPUT_MODE,status,sbp,thispath); /* Structures */
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
    c = send2stgd(hostname, req_type, flags, sendbuf, msglen, 1, NULL, (size_t) 0, nstcp_input, stcp_input, nstcp_output, stcp_output, NULL, NULL);
    if (c == 0) break;
    if ((req_type == STAGE_OUT) && (serrno == ENOSPC)) {
      if (enospc_ntries++ > enospc_retry) break;
      stage_sleep(enospc_retryint);
      continue;
    }
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
    stage_sleep (RETRYI);
  }
  free(sendbuf);

  /* Unpack for the output */

#if defined(_WIN32)
  WSACleanup();
#endif
  return (c == 0 ? 0 : -1);
}

#if TMS
int stage_api_tmscheck(vid, vsn, dgn, den, lbl)
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
  static char tmslbl[3] = "  ";
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
	
  tmslbl[0] = tmrepbuf[74] - 'A' + 'a';
  tmslbl[1] = tmrepbuf[75] - 'A' + 'a';
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

/*	stage_api_chkdirw - extract directory name from full pathname
 *		and check if writable.
 *
 *	return	-1	in case of error
 *		0	if writable
 */
int stage_api_chkdirw(path)
     char *path;
{
  char *p, *q;
  int rc;
  char sav;
  char *func = "chkdirw";
  
  if ((q = strchr (path, ':')) != NULL)
    q++;
  else
    q = path;
  p = strrchr (path, '/');
  if (p == q)
    p++;
  if (strncmp (path, "/afs/", 5) == 0) {
    stage_errmsg(func, STG44);
    rc = -1;
  } else {
    sav = *p;
    *p = '\0';
    rc = rfio_access (path, W_OK);
    *p = sav;
    if (rc < 0)
      stage_errmsg(func, STG33, path, rfio_serror());
  }
  return (rc);
}

int DLL_DECL stage_stcp2buf(buf,bufsize,flags,stcp)
     char *buf;
     size_t bufsize;
     u_signed64 flags;
     struct stgcat_entry *stcp;
{
  int i;

  if ((buf == NULL) || (bufsize <= 0)) {
    serrno = EINVAL;
    return(-1);
  }

  STFLAG2BUF_V2(flags,STAGE_SILENT,"--silent",buf,bufsize);
  STFLAG2BUF_V2(flags,STAGE_NOWAIT,"--nowait",buf,bufsize);
  STFLAG2BUF(flags,STAGE_GRPUSER,'G',buf,bufsize);
  STFLAGVAL2BUF(flags,STAGE_DEFERRED,'A',"deferred",buf,bufsize);
  STFLAGVAL2BUF(flags,STAGE_COFF,'c',"off",buf,bufsize);
  STFLAG2BUF(flags,STAGE_UFUN,'U',buf,bufsize);
  STFLAG2BUF(flags,STAGE_INFO,'z',buf,bufsize);
  STCHR2BUF(stcp,keep,'K',buf,bufsize);
  STSTR2BUF(stcp,poolname,'p',buf,bufsize);
  STVAL2BUF(stcp,size,'s',buf,bufsize);
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
    break;
  case 'd':
  case 'a':
    STSTR2BUF(stcp,u1.d.xfile,'I',buf,bufsize);
    break;
  case 'm':
    STSTR2BUF(stcp,u1.m.xfile,'M',buf,bufsize);
    break;
  case 'h':
    STSTR2BUF(stcp,u1.h.xfile,'M',buf,bufsize);
    STVAL2BUF_V2(stcp,u1.h.fileid,"--fileid",buf,bufsize)
    STVAL2BUF_V2(stcp,u1.h.fileclass,"--fileclass",buf,bufsize)
    STSTR2BUF_V2(stcp,u1.h.tppool,"--tppool",buf,bufsize);
    break;
  default:
    serrno = EFAULT;
    return(-1);
  }
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
  euid = geteuid();             /* Get current effective uid */
  egid = getegid();             /* Get current effective gid */
  pw = Cgetpwuid(euid);

#if defined(_WIN32)
  /* Well - in principle the real information is the uniqueid - we continue */
  /*
  if (euid < 0 || egid < 0) {
    exit (USERR);
  }
  */
#endif
  
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
  marshall_LONG (sbp, STGMAGIC);
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

int DLL_DECL stage_qry(t_or_d,flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output)
     char t_or_d;
     u_signed64 flags;
     char *hostname;
     int nstcp_input;
     struct stgcat_entry *stcp_input;
     int *nstcp_output;
     struct stgcat_entry **stcp_output;
     int *nstpp_output;
     struct stgpath_entry **stpp_output;
{
  int req_type = STAGE_QRY;
  struct stgcat_entry *thiscat; /* Catalog current pointer */
  int istcp;                    /* Counter on catalog structures passed in the protocol */
  int msglen;                   /* Buffer length (incremental) */
  int ntries;                   /* Number of retries */
  struct passwd *pw;            /* Password entry */
  char *sbp, *q;                /* Internal pointers */
  char *sendbuf;                /* Socket send buffer pointer */
  size_t sendbuf_size;          /* Total socket send buffer length */
  uid_t euid;                   /* Current effective uid */
  gid_t egid;                   /* Current effective gid */
  int status;                   /* Variable overwritten by macros in header */
  int c;                        /* Output of build_linkname() */
  char *func = "stage_qry";
  int nstcp_output_internal = 0;
  struct stgcat_entry *stcp_output_internal = NULL;
  int nstpp_output_internal = 0;
  struct stgpath_entry *stpp_output_internal = NULL;
  int rc;
#if defined(_WIN32)
  WSADATA wsadata;
#endif

  /* It is not allowed to have anything else but one single entry in input */
  if ((nstcp_input != 1) || (stcp_input == NULL)) {
    serrno = EINVAL;
    return(-1);
  }

  euid = geteuid();             /* Get current effective uid */
  egid = getegid();             /* Get current effective gid */
#if defined(_WIN32)
  if (euid < 0 || egid < 0) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* It is not allowed to have stcp_output != NULL and nstcp_output == NULL */
  if ((stcp_output != NULL) && (nstcp_output == NULL)) {
    serrno = EINVAL;
    return(-1);
  }

  /* It is not allowed to have stpp_output != NULL and nstpp_output == NULL */
  if ((stpp_output != NULL) && (nstpp_output == NULL)) {
    serrno = EINVAL;
    return(-1);
  }

  switch (t_or_d) {                     /* This method supports only */
  case 't':                             /* - tape files */
  case 'd':                             /* - disk files */
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
    serrno = SENOMAPFND;
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
  for (istcp = 0; istcp < nstcp_input; istcp++) {
    int i, numvid, numvsn;

    thiscat = &(stcp_input[istcp]);              /* Current catalog structure */
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
#if TMS
          if (stage_api_tmscheck (thiscat->u1.t.vid[i], thiscat->u1.t.vsn[i], thiscat->u1.t.dgn, thiscat->u1.t.den, thiscat->u1.t.lbl)) {
            stage_errmsg(func, "STG02 - TMSCHECK(%s,%s,%s,%s,%s) error\n",
                         thiscat->u1.t.vid[i], thiscat->u1.t.vsn[i], thiscat->u1.t.dgn, thiscat->u1.t.den, thiscat->u1.t.lbl);
            serrno = ESTTMSCHECK;
#if defined(_WIN32)
            WSACleanup();
#endif
            return(-1);
          }
#else
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
      if (! ISCASTOR(thiscat->u1.m.xfile)) {
        char *dummy;
        char *optarg = thiscat->u1.m.xfile;
        char *hsm_host;
        char hsm_path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];

        /* We prepend HSM_HOST only for non CASTOR-like files */
        if (((dummy = strchr(optarg,':')) == NULL) || (dummy != optarg && strrchr(dummy,'/') == NULL)) {
          if ((hsm_host = getenv("HSM_HOST")) != NULL) {
            strcpy (hsm_path, hsm_host);
            strcat (hsm_path, ":");
            strcat (hsm_path, optarg);
          } else if ((hsm_host = getconfent("STG", "HSM_HOST",0)) != NULL) {
            strcpy (hsm_path, hsm_host);
            strcat (hsm_path, ":");
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
      }
      break;
    default:
      break;
    }
    thiscat->t_or_d = t_or_d;
  }
  sendbuf_size += (nstcp_input * sizeof(struct stgcat_entry)); /* We overestimate by few bytes (gaps and strings zeroes) */

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
  marshall_LONG (sbp, STGMAGIC);
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
  for (istcp = 0; istcp < nstcp_input; istcp++) {
    thiscat = &(stcp_input[istcp]);               /* Current catalog structure */
    status = 0;
    marshall_STAGE_CAT(STAGE_INPUT_MODE,status,sbp,thiscat); /* Structures */
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
    c = send2stgd(hostname, req_type, flags, sendbuf, msglen, 1, NULL, (size_t) 0, nstcp_input, stcp_input, &nstcp_output_internal, &stcp_output_internal, &nstpp_output_internal, &stpp_output_internal);
    if (c == 0) break;
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
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
    } else if ((nstcp_output_internal == 0) || (nstpp_output_internal == 0)) {
      /* Nothing found */
      serrno = ENOENT;
      rc = -1;
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

int DLL_DECL stageqry_Tape(flags,hostname,poolname,tape,fseq,nstcp_output,stcp_output,nstpp_output,stpp_output)
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
    serrno = EINVAL;
    return(-1);
  }
  /* Check tape length and poolname validity */
  if (((*tape == '\0') || (strlen(tape)        > CA_MAXVIDLEN      )) ||
      ((fseq  != NULL) && (strlen(fseq)        > CA_MAXFSEQLEN     )) ||
      ((poolname != NULL) && (strlen(poolname) > CA_MAXPOOLNAMELEN))) {
    serrno = ENAMETOOLONG;
    return(-1);
  }

  /* We build the full stageqry API request */
  memset(&stcp_input, 0, sizeof(struct stgcat_entry));
  strcpy(stcp_input.poolname,poolname); /* Can be zero-length */
  strcpy(stcp_input.u1.t.vid[0],tape); /* Cannot be zero-length */
  strcpy(stcp_input.u1.t.fseq,fseq); /* Can be zero-length */
  return(stageqry_tape(flags,hostname,1,&stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output));
}

int DLL_DECL stageqry_Hsm(flags,hostname,poolname,hsmname,nstcp_output,stcp_output,nstpp_output,stpp_output)
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
    serrno = EINVAL;
    return(-1);
  }
  /* Check hsmname length and poolname validity */
  if (((*hsmname == '\0') || (strlen(hsmname)  > 166              )) ||
      ((poolname != NULL) && (strlen(poolname) > CA_MAXPOOLNAMELEN))) {
    serrno = ENAMETOOLONG;
    return(-1);
  }

  /* We build the full stageqry API request */
  memset(&stcp_input, 0, sizeof(struct stgcat_entry));
  strcpy(stcp_input.poolname,poolname); /* Can be zero-length */
  strcpy(stcp_input.u1.h.xfile,hsmname); /* Cannot be zero-length */
  return(stageqry_hsm(flags,hostname,1,&stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output));
}


int DLL_DECL stageqry_Disk(flags,hostname,poolname,diskname,nstcp_output,stcp_output,nstpp_output,stpp_output)
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
    serrno = EINVAL;
    return(-1);
  }
  /* Check diskname length and poolname validity */
  if (((*diskname == '\0') || (strlen(diskname)  > (CA_MAXHOSTNAMELEN+MAXPATH))) ||
      ((poolname  != NULL) && (strlen(poolname)  > CA_MAXPOOLNAMELEN         ))) {
    serrno = ENAMETOOLONG;
    return(-1);
  }

  /* We build the full stageqry API request */
  memset(&stcp_input, 0, sizeof(struct stgcat_entry));
  strcpy(stcp_input.poolname,poolname); /* Can be zero-length */
  strcpy(stcp_input.u1.d.xfile,diskname); /* Cannot be zero-length */
  return(stageqry_disk(flags,hostname,1,&stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output));
}


