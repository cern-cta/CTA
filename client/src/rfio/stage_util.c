/*
 * $Id: stage_util.c,v 1.2 2007/02/21 09:46:22 sponcec3 Exp $
 */

#include <sys/types.h>
#include <stdlib.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <winsock2.h>
#endif
#include "osdep.h"
#include "u64subr.h"
#include "serrno.h"
#include "stage_api.h"
#include "rtcp_constants.h"    /* For EBCCONV, FIXVAR, SKIPBAD, KEEPFILE */
#include "Ctape_constants.h"   /* For NOTRLCHK */
#include "Csnprintf.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cmutex.h"
#include "getacctent.h"

#ifdef __STDC__
#define NAMEOFVAR(x) #x
#else
#define NAMEOFVAR(x) "x"
#endif

#ifdef SIXMONTHS
#undef SIXMONTHS
#endif
#define SIXMONTHS (6*30*24*60*60)

#if defined(_WIN32)
static char strftime_format_sixmonthsold[] = "%b %d %Y";
static char strftime_format[] = "%b %d %H:%M:%S";
#else /* _WIN32 */
static char strftime_format_sixmonthsold[] = "%b %e %Y";
static char strftime_format[] = "%b %e %H:%M:%S";
#endif /* _WIN32 */

#define DUMP_VAL(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define PRINT_VAL(st,member) fprintf(stdout, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define DUMP_VALHEX(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define PRINT_VALHEX(st,member) fprintf(stdout, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define DUMP_VALOCT(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20lo (oct)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define PRINT_VALOCT(st,member) fprintf(stdout, "%-23s : %20lo (oct)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define DUMP_VALSTATUS(rpfd,st,member) { \
	char statusstring[1024]; \
	funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, stage_util_status2string((char *) statusstring, (int) 1024, (int) st->member) == 0 ? statusstring : "<?>"); \
}
#define PRINT_VALSTATUS(st,member) { \
	char statusstring[1024]; \
	fprintf(stdout, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, stage_util_status2string((char *) statusstring, (int) 1024, (int) st->member) == 0 ? statusstring : "<?>"); \
}
#define DUMP_VALCHARCONV(rpfd,st,member) { \
	char charconvstring[1024]; \
	funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, stage_util_charconv2string((char *) charconvstring, (int) 1024, (int) st->member) == 0 ? charconvstring : "<?>"); \
}
#define PRINT_VALCHARCONV(st,member) { \
	char charconvstring[1024]; \
	fprintf(stdout, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, stage_util_charconv2string((char *) charconvstring, (int) 1024, (int) st->member) == 0 ? charconvstring : "<?>"); \
}
#define DUMP_VALE_TFLAGS(rpfd,st,member) { \
	char E_Tflagsstring[1024]; \
	funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, stage_util_E_Tflags2string((char *) E_Tflagsstring, (int) 1024, (int) st->member) == 0 ? E_Tflagsstring : "<?>"); \
}
#define PRINT_VALE_TFLAGS(st,member) { \
	char E_Tflagsstring[1024]; \
	fprintf(stdout, "%-23s : %20lx (hex) == %s\n", NAMEOFVAR(member) , (unsigned long) st->member, stage_util_E_Tflags2string((char *) E_Tflagsstring, (int) 1024, (int) st->member) == 0 ? E_Tflagsstring : "<?>"); \
}
#define DUMP_TIME(rpfd,st,member) {                                         \
    char tmpbuf[21];                                                        \
	char timestr[64] ;                                                      \
    stage_util_time((time_t) st->member, timestr);                          \
	funcrep(rpfd, MSG_OUT, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	    \
				 u64tostr((u_signed64) st->member, tmpbuf,0), timestr);	    \
}
#define PRINT_TIME(st,member) {                                             \
    char tmpbuf[21];                                                        \
	char timestr[64] ;                                                      \
    stage_util_time((time_t) st->member, timestr);                          \
	fprintf(stdout, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	            \
				 u64tostr((u_signed64) st->member, tmpbuf,0), timestr);	    \
}
#define DUMP_U64(rpfd,st,member) {                                          \
    char tmpbuf[21];                                                        \
	funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) ,	        \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}
#define DUMP_U64_WITH_COMMENT(rpfd,st,member,comment) {                     \
    char tmpbuf[21];                                                        \
	funcrep(rpfd, MSG_OUT, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	        \
				 u64tostr((u_signed64) st->member, tmpbuf,0), (comment != NULL) ? comment : ""); \
}
#define PRINT_U64(st,member) {                                              \
    char tmpbuf[21];                                                        \
	fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) ,	                \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}
#define PRINT_U64_WITH_COMMENT(st,member,comment) {                         \
    char tmpbuf[21];                                                        \
	fprintf(stdout, "%-23s : %20s (%s)\n", NAMEOFVAR(member) ,	            \
				 u64tostr((u_signed64) st->member, tmpbuf,0), (comment != NULL) ? comment : ""); \
}

#define DUMP_CHAR(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define PRINT_CHAR(st,member) fprintf(stdout, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define DUMP_STRING(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)
#define PRINT_STRING(st,member) fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

extern char *getenv();         /* To get environment variables */
extern char *getconfent();     /* To get configuration entries */

char *forced_endptr_error = "Z";

static int stage_util_validuser_mutex = -1;

struct flag2name {
	u_signed64 flag;
	char *name;
};

static int stage_util_newacct _PROTO((struct passwd *, gid_t));

void DLL_DECL stage_sleep(nsec)
     int nsec;
{
  struct timeval ts;

  if (nsec <= 0) return;
  ts.tv_sec = nsec;
  ts.tv_usec = 0;
  select(0,NULL,NULL,NULL,&ts);
}

/* This function will return the preferred magic number used by the client */

#define STGMAGIC_DEFAULT STGMAGIC4
#define STGMAGIC_DEFAULT_STRING "STGMAGIC4"

int DLL_DECL stage_stgmagic()
{
  char *p;
  char *func = "stage_stgmagic";
  long int stgmagic;
  char *dp;

  /* We check existence of an STG STGMAGIC from environment variable or configuration */
  if (((p = getenv("STAGE_STGMAGIC")) != NULL) || ((p = getconfent("STG","STGMAGIC",0)) != NULL)) {
    errno = 0;
    stgmagic = strtol(p, &dp, 0);
    if ((*dp != '\0') || (((stgmagic == LONG_MIN) || (stgmagic == LONG_MAX)) && (errno == ERANGE))) {
      stage_errmsg(func, STG02, "Magic Number", "Configuration", "Using default magic number " STGMAGIC_DEFAULT_STRING);
      return(STGMAGIC_DEFAULT);
    }
    switch (stgmagic) {
    case STGMAGIC2:
      return(STGMAGIC2);
    case STGMAGIC3:
      return(STGMAGIC3);
    case STGMAGIC4:
      return(STGMAGIC4);
    default:
      stage_errmsg(func, STG02, "Magic Number", "Configuration", "Using default magic number " STGMAGIC_DEFAULT_STRING);
      return(STGMAGIC_DEFAULT);
    }
  } else {
    return(STGMAGIC_DEFAULT);
  }
}

void DLL_DECL dump_stpp(rpfd, stpp, funcrep)
	int *rpfd;
	struct stgpath_entry *stpp;
	int (*funcrep) _PROTO((int *, int, ...));
{
	int save_serrno;

	if ((funcrep == NULL) || (stpp == NULL)) return;

	save_serrno = serrno;
	funcrep(rpfd, MSG_OUT, "----------------------------------\n");
	funcrep(rpfd, MSG_OUT, "Path entry  -   dump of reqid %d\n", stpp->reqid);
	funcrep(rpfd, MSG_OUT, "----------------------------------\n");
	DUMP_VAL(rpfd,stpp,reqid);
	DUMP_STRING(rpfd,stpp,upath);
	serrno = save_serrno;
}

void DLL_DECL dump_stcp(rpfd, stcp, funcrep)
	int *rpfd;
	struct stgcat_entry *stcp;
	int (*funcrep) _PROTO((int *, int, ...));
{
	int i, save_serrno;

	if ((funcrep == NULL) || (stcp == NULL)) return;

	save_serrno = serrno;
	funcrep(rpfd, MSG_OUT, "-------------------------------------\n");
	funcrep(rpfd, MSG_OUT, "Catalog entry - dump of reqid %d\n", stcp->reqid);
	funcrep(rpfd, MSG_OUT, "-------------------------------------\n");
	DUMP_VAL(rpfd,stcp,reqid);
	DUMP_VAL(rpfd,stcp,blksize);
	DUMP_STRING(rpfd,stcp,filler);
	DUMP_VALCHARCONV(rpfd,stcp,charconv);
	DUMP_CHAR(rpfd,stcp,keep);
	DUMP_VAL(rpfd,stcp,lrecl);
	DUMP_VAL(rpfd,stcp,nread);
	DUMP_STRING(rpfd,stcp,poolname);
	DUMP_STRING(rpfd,stcp,recfm);
	DUMP_U64_WITH_COMMENT(rpfd,stcp,size,"bytes");
	DUMP_STRING(rpfd,stcp,ipath);
	DUMP_CHAR(rpfd,stcp,t_or_d);
	DUMP_STRING(rpfd,stcp,group);
	DUMP_STRING(rpfd,stcp,user);
	DUMP_VAL(rpfd,stcp,uid);
	DUMP_VAL(rpfd,stcp,gid);
	DUMP_VALOCT(rpfd,stcp,mask);
	DUMP_VALSTATUS(rpfd,stcp,status);
	DUMP_U64(rpfd,stcp,actual_size);
	DUMP_TIME(rpfd,stcp,c_time);
	DUMP_TIME(rpfd,stcp,a_time);
	DUMP_VAL(rpfd,stcp,nbaccesses);
	switch (stcp->t_or_d) {
	case 't':
		DUMP_STRING(rpfd,stcp,u1.t.den);
		DUMP_STRING(rpfd,stcp,u1.t.dgn);
		DUMP_STRING(rpfd,stcp,u1.t.fid);
		DUMP_CHAR(rpfd,stcp,u1.t.filstat);
		DUMP_STRING(rpfd,stcp,u1.t.fseq);
		DUMP_STRING(rpfd,stcp,u1.t.lbl);
		DUMP_VAL(rpfd,stcp,u1.t.retentd);
		DUMP_VAL(rpfd,stcp,u1.t.side);
		DUMP_STRING(rpfd,stcp,u1.t.tapesrvr);
        DUMP_VALE_TFLAGS(rpfd,stcp,u1.t.E_Tflags);
		for (i = 0; i < MAXVSN; i++) {
			switch (i) {
			case 0:
				DUMP_STRING(rpfd,stcp,u1.t.vid[0]);
				DUMP_STRING(rpfd,stcp,u1.t.vsn[0]);
				break;
			case 1:
				DUMP_STRING(rpfd,stcp,u1.t.vid[1]);
				DUMP_STRING(rpfd,stcp,u1.t.vsn[1]);
				break;
			case 2:
				DUMP_STRING(rpfd,stcp,u1.t.vid[2]);
				DUMP_STRING(rpfd,stcp,u1.t.vsn[2]);
				break;
			default:
				DUMP_STRING(rpfd,stcp,u1.t.vid[i]);
				DUMP_STRING(rpfd,stcp,u1.t.vsn[i]);
				break;
			}
		}
		break;
	case 'd':
	case 'a':
		DUMP_STRING(rpfd,stcp,u1.d.xfile);
		DUMP_STRING(rpfd,stcp,u1.d.Xparm);
		break;
	case 'm':
		DUMP_STRING(rpfd,stcp,u1.m.xfile);
		break;
	case 'h':
		DUMP_STRING(rpfd,stcp,u1.h.xfile);
		DUMP_STRING(rpfd,stcp,u1.h.server);
		DUMP_U64(rpfd,stcp,u1.h.fileid);
		DUMP_VAL(rpfd,stcp,u1.h.fileclass);
		DUMP_STRING(rpfd,stcp,u1.h.tppool);
		DUMP_VAL(rpfd,stcp,u1.h.retenp_on_disk);
		DUMP_VAL(rpfd,stcp,u1.h.mintime_beforemigr);
		DUMP_VAL(rpfd,stcp,u1.h.flag);
		break;
	}
	serrno = save_serrno;
}


void DLL_DECL print_stpp(stpp)
	struct stgpath_entry *stpp;
{
	int save_serrno;

	if (stpp == NULL) return;

	save_serrno = serrno;
	fprintf(stdout, "----------------------------------\n");
	fprintf(stdout, "Path entry  -   dump of reqid %d\n", stpp->reqid);
	fprintf(stdout, "----------------------------------\n");
	PRINT_VAL(stpp,reqid);
	PRINT_STRING(stpp,upath);
	serrno = save_serrno;
}

void DLL_DECL print_stcp(stcp)
	struct stgcat_entry *stcp;
{
	int i, save_serrno;

	if (stcp == NULL) return;

	save_serrno = serrno;
	fprintf(stdout, "-------------------------------------\n");
	fprintf(stdout, "Catalog entry - dump of reqid %d\n", stcp->reqid);
	fprintf(stdout, "-------------------------------------\n");
	PRINT_VAL(stcp,reqid);
	PRINT_VAL(stcp,blksize);
	PRINT_STRING(stcp,filler);
	PRINT_VALCHARCONV(stcp,charconv);
	PRINT_CHAR(stcp,keep);
	PRINT_VAL(stcp,lrecl);
	PRINT_VAL(stcp,nread);
	PRINT_STRING(stcp,poolname);
	PRINT_STRING(stcp,recfm);
	PRINT_U64_WITH_COMMENT(stcp,size,"bytes");
	PRINT_STRING(stcp,ipath);
	PRINT_CHAR(stcp,t_or_d);
	PRINT_STRING(stcp,group);
	PRINT_STRING(stcp,user);
	PRINT_VAL(stcp,uid);
	PRINT_VAL(stcp,gid);
	PRINT_VALOCT(stcp,mask);
	PRINT_VALSTATUS(stcp,status);
	PRINT_U64(stcp,actual_size);
	PRINT_TIME(stcp,c_time);
	PRINT_TIME(stcp,a_time);
	PRINT_VAL(stcp,nbaccesses);
	switch (stcp->t_or_d) {
	case 't':
		PRINT_STRING(stcp,u1.t.den);
		PRINT_STRING(stcp,u1.t.dgn);
		PRINT_STRING(stcp,u1.t.fid);
		PRINT_CHAR(stcp,u1.t.filstat);
		PRINT_STRING(stcp,u1.t.fseq);
		PRINT_STRING(stcp,u1.t.lbl);
		PRINT_VAL(stcp,u1.t.retentd);
		PRINT_VAL(stcp,u1.t.side);
		PRINT_STRING(stcp,u1.t.tapesrvr);
        PRINT_VALE_TFLAGS(stcp,u1.t.E_Tflags);
		for (i = 0; i < MAXVSN; i++) {
			switch (i) {
			case 0:
				PRINT_STRING(stcp,u1.t.vid[0]);
				PRINT_STRING(stcp,u1.t.vsn[0]);
				break;
			case 1:
				PRINT_STRING(stcp,u1.t.vid[1]);
				PRINT_STRING(stcp,u1.t.vsn[1]);
				break;
			case 2:
				PRINT_STRING(stcp,u1.t.vid[2]);
				PRINT_STRING(stcp,u1.t.vsn[2]);
				break;
			default:
				PRINT_STRING(stcp,u1.t.vid[i]);
				PRINT_STRING(stcp,u1.t.vsn[i]);
				break;
			}
		}
		break;
	case 'd':
	case 'a':
		PRINT_STRING(stcp,u1.d.xfile);
		PRINT_STRING(stcp,u1.d.Xparm);
		break;
	case 'm':
		PRINT_STRING(stcp,u1.m.xfile);
		break;
	case 'h':
		PRINT_STRING(stcp,u1.h.xfile);
		PRINT_STRING(stcp,u1.h.server);
		PRINT_U64(stcp,u1.h.fileid);
		PRINT_VAL(stcp,u1.h.fileclass);
		PRINT_STRING(stcp,u1.h.tppool);
		PRINT_VAL(stcp,u1.h.retenp_on_disk);
		PRINT_VAL(stcp,u1.h.mintime_beforemigr);
		PRINT_VAL(stcp,u1.h.flag);
		break;
	}
	serrno = save_serrno;
}

/* Idem than strtol() but returns 0 if OK, -1 if error, result in &ouput */
int DLL_DECL stage_strtoi(output,nptr,endptr,base)
	int *output;
	char *nptr;
	char **endptr;
	int base;
{
	long thislong;
	int rc = 0;

	errno = 0;
	thislong = strtol (nptr, endptr, base);
	if ((**endptr != '\0') || (((thislong == LONG_MIN) || (thislong == LONG_MAX)) && (errno == ERANGE))) {
		if (thislong <= INT_MIN) {
			*output = INT_MIN;
			serrno = errno = ERANGE;
		} else if (thislong >= INT_MAX) {
			*output = INT_MAX;
			serrno = errno = ERANGE;
		} else {
			*output = (int) thislong;
			serrno = errno = EINVAL;
		}
		if (**endptr == '\0') {
			/* We force the caller to have an error anyway, just checking **endptr */
			*endptr = forced_endptr_error;
		}
		rc = -1;
	} else {
		if ((thislong < INT_MIN) || (thislong > INT_MAX)) {
			if (thislong < INT_MIN) {
				*output = INT_MIN;
			} else if (thislong > INT_MAX) {
				*output = INT_MAX;
			} else {
				*output = (int) thislong;
            }
			if (**endptr == '\0') {
				/* We force the caller to have an error anyway, just checking **endptr */
				*endptr = forced_endptr_error;
			}
			serrno = errno = ERANGE;
			rc = -1;
		} else {
			*output = (int) thislong;
		}
	}
	return(rc);
}

void DLL_DECL stage_util_time(this,timestr)
     time_t this;
     char *timestr;
{
  time_t this_time = time(NULL);
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  struct tm tmstruc;
#endif /* _REENTRANT || _THREAD_SAFE */
  struct tm *tp;

#if ((defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32))
  localtime_r(&(this),&tmstruc);
  tp = &tmstruc;
#else
  tp = localtime(&(this));
#endif /* _REENTRANT || _THREAD_SAFE */
  if ((this_time >= this) && ((this_time - this) > SIXMONTHS)) {
	  /* Too much in past */
	  strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else if ((this_time < this) && ((this - this_time) > SIXMONTHS)) {
	  /* Too much in feature...! */
	  strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else {
	  strftime(timestr,64,strftime_format,tp);
  }
}

void DLL_DECL stage_util_retenp(this,timestr)
     int this;
     char *timestr;
{
	char *p;
	time_t allunits[5]  = { ONE_YEAR    , ONE_DAY    , ONE_HOUR    , ONE_MINUTE  ,           1 };
	char *allformats[5] = { "%dY", "%dD", "%dH", "%dM" , "%dS" };
	int i;
	
	if (this < 0) {
		strcpy(timestr,"-1");
		return;
	}

	p = timestr;

	*p = '\0';
	for (i = 0; i < 5; i++) {
		if (this >= allunits[i]) {
			Csnprintf (p, 64 - strlen(timestr), allformats[i], this / allunits[i]);
			timestr[64-1] = '\0';
			if (strlen(timestr) >= 64) return;
			p += strlen(p);
			this %= allunits[i];
			if (this <= 0) return;
			/* Something remains - we need to add a separator */
			if (strlen(timestr) >= 63) return;
			*p++ = ',';
			*p = '\0';
		}
	}
}

int DLL_DECL stage_util_status2string(output, maxsize, status)
	char *output;
	size_t maxsize;
	int status;
{
	char *thisp;
	int i;
	struct stgcat_entry static_stcp;
	struct stgcat_entry *stcp = &static_stcp;

	struct flag2name status2name0[] = { /* Must be ANDed with 0xF */
		{ STAGEIN    , "STAGEIN"    },
		{ STAGEOUT   , "STAGEOUT"   },
		{ STAGEWRT   , "STAGEWRT"   },
		{ STAGEPUT   , "STAGEPUT"   },
		{ STAGEALLOC , "STAGEALLOC" },
        { 0                , NULL   }
	};

	struct flag2name status2name1[] = { /* Must be ANDed with 0xF0 */
		{ WAITING_SPC  , "WAITING_SPC"    },
		{ WAITING_REQ  , "WAITING_REQ"    },
		{ STAGED       , "STAGED"         },
		{ KILLED       , "KILLED"         },
		{ STG_FAILED   , "STG_FAILED"     },
		{ PUT_FAILED   , "PUT_FAILED"     },
        { 0            , NULL             }
	};

	struct flag2name status2name2[] = { /* Can be checked directly */
		{ STAGED_LSZ   , "STAGED_LSZ"     },
		{ STAGED_TPE   , "STAGED_TPE"     },
		{ CAN_BE_MIGR  , "CAN_BE_MIGR"    },
		{ LAST_TPFILE  , "LAST_TPFILE"    },
		{ BEING_MIGR   , "BEING_MIGR"     },
		{ WAITING_MIGR , "WAITING_MIGR"   },
		{ WAITING_NS   , "WAITING_NS"     },
		{ STAGE_RDONLY , "STAGE_RDONLY"   },
        { 0            , NULL             }
	};

	if (output == NULL) {
		serrno = EFAULT;
		return(-1);
	}

	stcp->status = status;
	thisp = output;
	*thisp = '\0';

	/* Do the main status first */
	i = -1;
	while (1) {
		if (status2name0[++i].name == NULL) break;
		if ((stcp->status & 0xF) == status2name0[i].flag) {
			if (strlen(output) > (maxsize - 3)) break;
			if (*thisp != '\0') {
				strcat(thisp, "|");
			}
			if (strlen(thisp) > (maxsize - 1 - strlen(status2name0[i].name) - 1)) break;
			strcat(thisp, status2name0[i].name);
		}
	}

	/* Do then the secondary status */
	i = -1;
	while (1) {
		if (status2name1[++i].name == NULL) break;
		if ((stcp->status & 0xF0) == status2name1[i].flag) {
			if (strlen(output) > (maxsize - 3)) break;
			if (*thisp != '\0') {
				strcat(thisp, "|");
			}
			if (strlen(thisp) > (maxsize - 1 - strlen(status2name1[i].name) - 1)) break;
			strcat(thisp, status2name1[i].name);
		}
	}

	/* And the end */
	i = -1;
	while (1) {
		if (status2name2[++i].name == NULL) break;
		if ((stcp->status & status2name2[i].flag) == status2name2[i].flag) {
			if (strlen(output) > (maxsize - 3)) break;
			if (*thisp != '\0') {
				strcat(thisp, "|");
			}
			if (strlen(thisp) > (maxsize - 1 - strlen(status2name2[i].name) - 1)) break;
			strcat(thisp, status2name2[i].name);
		}
	}

	if (! *thisp) {
		serrno = ENOENT;
	}
	return(*thisp != '\0' ? 0 : -1);
}

/* Returns the maximum tape file sequence allowed v.s. label type */
/* Will return -1 if not limited */
int DLL_DECL stage_util_maxtapefseq(labeltype)
	char *labeltype;
{
	if (labeltype == NULL) return(-1);

	if ((strcmp(labeltype,"al") == 0) ||  /* Ansi Label */
		(strcmp(labeltype,"sl") == 0))    /* Standard Label */
		return(9999);
	if (strcmp(labeltype,"aul") == 0)     /* Ansi (extended) User Label */
		return(INT_MAX / 3);
	if ((strcmp(labeltype,"nl") == 0) ||  /* No Label */
		(strcmp(labeltype,"blp") == 0))   /* Bypass Label Type */
		return(INT_MAX);

	return(-1);                           /* Unknown type : not limited */
}

/* Will return -1 if error */
/*              0 if OK but not unit */
/*              1 if OK with unit */
int DLL_DECL stage_util_check_for_strutou64(str)
	char *str;
{
	/* We accept only the following format */
	/* [blanks]<digits>[k|M|G|T|P] */

	char *p = (char *) str;

	while (isspace (*p)) p++;	/* skip leading spaces */
	while (*p) {
		if (! isdigit (*p)) break;
		p++;
	}
	/* End of the digits section:  */
	/* Either there is a supported unit, either there is none */

	switch (*p) {
	case '\0':
		return(0); /* Ok and no unit */
	case 'b':      /* Not supported by u64 - we say it is bytes - u64 will ignore it, simply */
	case 'k':
	case 'M':
	case 'G':
	case 'T':
	case 'P':
		switch (*++p) {
		case '\0':
			return(1); /* Ok with unit */
		default:
			return(-1); /* Unit followed by anything, not valid */
		}
	default:
		return(-1); /* Unknown unit */
	}
}

int DLL_DECL stage_util_charconv2string(output, maxsize, charconv)
	char *output;
	size_t maxsize;
	int charconv;
{
	char *thisp;
	int i;

	struct flag2name charconv2name[] = { /* Must be ANDed with 0xF */
		{ FIXVAR     , "FIXVAR"  },
		{ ASCCONV    , "ASCCONV" },
		{ EBCCONV    , "EBCCONV" },
        { 0          , NULL      }
	};

	if (output == NULL) {
		serrno = EFAULT;
		return(-1);
	}

	thisp = output;
	*thisp = '\0';

	i = -1;
	while (1) {
		if (charconv2name[++i].name == NULL) break;
		if ((charconv & charconv2name[i].flag) == charconv2name[i].flag) {
			if (strlen(output) > (maxsize - 3)) break;
			if (*thisp != '\0') {
				strcat(thisp, "|");
			}
			if (strlen(thisp) > (maxsize - 1 - strlen(charconv2name[i].name) - 1)) break;
			strcat(thisp, charconv2name[i].name);
		}
	}

	if (! *thisp) {
		serrno = ENOENT;
	}
	return(*thisp != '\0' ? 0 : -1);
}

int DLL_DECL stage_util_E_Tflags2string(output, maxsize, E_Tflags)
	char *output;
	size_t maxsize;
	int E_Tflags;
{
	char *thisp;
	int i;

	struct flag2name E_Tflags2name[] = { /* Must be ANDed with 0xF */
		{ SKIPBAD    , "SKIPBAD"  },
		{ KEEPFILE   , "KEEPFILE" },
		{ NOTRLCHK   , "NOTRLCHK" },
        { 0          , NULL      }
	};

	if (output == NULL) {
		serrno = EFAULT;
		return(-1);
	}

	thisp = output;
	*thisp = '\0';

	i = -1;
	while (1) {
		if (E_Tflags2name[++i].name == NULL) break;
		if ((E_Tflags & E_Tflags2name[i].flag) == E_Tflags2name[i].flag) {
			if (strlen(output) > (maxsize - 3)) break;
			if (*thisp != '\0') {
				strcat(thisp, "|");
			}
			if (strlen(thisp) > (maxsize - 1 - strlen(E_Tflags2name[i].name) - 1)) break;
			strcat(thisp, E_Tflags2name[i].name);
		}
	}

	if (! *thisp) {
		serrno = ENOENT;
	}
	return(*thisp != '\0' ? 0 : -1);
}

/* Sort of cut/paste from CASTOR/rtcopy/rtcpd_MainCntl.c */
/* Same remark as for that routine: this is thread unsafe because of getgrent() on UNIX */
/* That's why there is a call to Cmutex */
int DLL_DECL stage_util_validuser(username,uid,gid)
	char *username;
	uid_t uid;
	gid_t gid;
{
	struct passwd *pwd;
	struct group  *grp;
	int rc = -1;
#ifndef _WIN32
    char **gr_mem;
#endif

	if (Cmutex_lock(&stage_util_validuser_mutex,10) < 0) {
		return(-1);
	}

	if (username != NULL) {
		if ((pwd = Cgetpwnam(username)) == NULL) {
			serrno = SEUSERUNKN; /* Unknown user */
			goto stage_util_validuser_return;
		}
		/* Check that uid matches username */
		if (pwd->pw_uid != uid) {
			/* Either unknown entry either invalid entry */
			if ((pwd = Cgetpwuid(uid)) == NULL) {
				serrno = SEUSERUNKN; /* Unknown user */
			} else {
				serrno = ESTUSER; /* Invalid user */
			}
			goto stage_util_validuser_return;
		}
	} else {
		if ((pwd = Cgetpwuid(uid)) == NULL) {
			serrno = SEUSERUNKN; /* Unknown user */
			goto stage_util_validuser_return;
		}
	}

	/* Check that gid matches uid */
	if (pwd->pw_gid == gid) {
		/* Yes: gid is the primary group of username(uid) */
		rc = 0;
		goto stage_util_validuser_return;
	}
#ifndef _WIN32
	if (username != NULL) {
		setgrent();
		while ((grp = getgrent()) != NULL) {
			if (pwd->pw_gid == grp->gr_gid) continue;
			for (gr_mem = grp->gr_mem; gr_mem != NULL && *gr_mem != NULL; gr_mem++) {
				if (strcmp(*gr_mem,username) == 0) {
					rc = 0;
					goto stage_util_validuser_return;
				}
			}
		}
		endgrent();
	}
	/* Not found in secondary groups - perhaps in our group extension? */
	if (stage_util_newacct(pwd,gid) == 0) {
		rc = 0;
		goto stage_util_validuser_return;
	}
	/* Nope */
	serrno = ESTGROUP;
#else
	/* Cannot do that on Windows */
	serrno = ESTGROUP;
#endif
	
  stage_util_validuser_return:
	Cmutex_unlock(&stage_util_validuser_mutex);
	return(rc);
}

static int stage_util_newacct(pwd,gid)
	struct passwd *pwd;
	gid_t gid;
{
    char buf[BUFSIZ] ;
    char *def_acct ;
    struct group *gr ;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
    char acctstr[7];

	acctstr[0] = '\0';
    /* get default account */
    if ( getacctent(pwd,NULL,buf,sizeof(buf)) == NULL ) {
		return(-1);
	}
    if ( strtok(buf,":") == NULL || (def_acct= strtok(NULL,":")) == NULL ) {
		return(-1);
	}
    if ( (strlen(def_acct) == 6) && (*(def_acct+3) == '$') &&   /* uuu$gg */
         ((gr = Cgetgrgid(gid)) != NULL)) {
        strncpy(acctstr,def_acct,4);
        strncpy(acctstr+4,gr->gr_name,2); /* new uuu$gg */
        if ( getacctent(pwd,acctstr,buf,sizeof(buf)) )
            return(0);      /* newacct was executed */
    }
    return(-1);
}

int  DLL_DECL stage_util_max_stcp_per_request() {
	/* Stager protocol must not exceed MAX_NETDATA_SIZE bytes */
	/* We let 1000 bytes for the header, the rest for the structures */
	/* and we want:
	   total_reqzise < MAX_NETDATA_SIZE
	   x * sizeof(struct stgcat_entry) + 1000 < MAX_NETDATA_SIZE
	   x < (MAX_NETDATA_SIZE - 1000) / sizeof(struct stgcat_entry)
	*/

	return((MAX_NETDATA_SIZE - 1000) / sizeof(struct stgcat_entry));
}

int  DLL_DECL stage_util_max_stpp_per_request() {
	/* Stager protocol must not exceed MAX_NETDATA_SIZE bytes */
	/* We let 1000 bytes for the header, the rest for the structures */
	/* and we want:
	   total_reqzise < MAX_NETDATA_SIZE
	   x * sizeof(struct stgpath_entry) + 1000 < MAX_NETDATA_SIZE
	   x < (MAX_NETDATA_SIZE - 1000) / sizeof(struct stgpath_entry)
	*/

	return((MAX_NETDATA_SIZE - 1000) / sizeof(struct stgpath_entry));
}


