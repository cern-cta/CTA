/*
 * $Id: stage_util.c,v 1.13 2002/01/23 13:54:30 jdurand Exp $
 */

#include <sys/types.h>
#include <stdlib.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <winsock2.h>
#endif
#include "osdep.h"
#include "u64subr.h"
#include "serrno.h"
#include "stage_api.h"

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
#define PRINT_U64(st,member) {                                              \
    char tmpbuf[21];                                                        \
	fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) ,	                \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}

#define DUMP_CHAR(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define PRINT_CHAR(st,member) fprintf(stdout, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define DUMP_STRING(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)
#define PRINT_STRING(st,member) fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)

extern char *getenv();         /* To get environment variables */
extern char *getconfent();     /* To get configuration entries */

char *forced_endptr_error = "Z";
void DLL_DECL stage_util_time _PROTO((time_t, char *));
void DLL_DECL stage_sleep _PROTO((int)); /* Sleep thread-safe */

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

#define STGMAGIC_DEFAULT STGMAGIC2
#define STGMAGIC_DEFAULT_STRING "STGMAGIC2"

int DLL_DECL stage_stgmagic()
{
  char *p;
  char *func = "stage_stgmagic";
  long int stgmagic;
  char *dp;

  /* We check existence of an STG STGMAGIC from environment variable or configuration */
  if (((p = getenv("STAGE_STGMAGIC")) != NULL) || ((p = getconfent("STG","STGMAGIC")) != NULL)) {
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
#ifdef STAGER_SIDE_SERVER_SUPPORT
    case STGMAGIC4:
      return(STGMAGIC4);
#endif
    default:
      stage_errmsg(func, STG02, "Magic Number", "Configuration", "Using default magic number " STGMAGIC_DEFAULT_STRING);
      return(STGMAGIC_DEFAULT);
    }
  } else {
    return(STGMAGIC_DEFAULT);
  }
}

void DLL_DECL dump_stpp(rpfd, stpp, funcrep)
	int rpfd;
	struct stgpath_entry *stpp;
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
	int (*funcrep) _PROTO((int, int, ...));
#else
	int (*funcrep) _PROTO(());
#endif
{
	if ((funcrep == NULL) || (stpp == NULL)) return;

	funcrep(rpfd, MSG_OUT, "----------------------------------\n");
	funcrep(rpfd, MSG_OUT, "Path entry  -   dump of reqid %d\n", stpp->reqid);
	funcrep(rpfd, MSG_OUT, "----------------------------------\n");
	DUMP_VAL(rpfd,stpp,reqid);
	DUMP_STRING(rpfd,stpp,upath);
}

void DLL_DECL dump_stcp(rpfd, stcp, funcrep)
	int rpfd;
	struct stgcat_entry *stcp;
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
	int (*funcrep) _PROTO((int, int, ...));
#else
	int (*funcrep) _PROTO(());
#endif
{
	int i;

	if ((funcrep == NULL) || (stcp == NULL)) return;

	funcrep(rpfd, MSG_OUT, "-------------------------------------\n");
	funcrep(rpfd, MSG_OUT, "Catalog entry - dump of reqid %d\n", stcp->reqid);
	funcrep(rpfd, MSG_OUT, "-------------------------------------\n");
	DUMP_VAL(rpfd,stcp,reqid);
	DUMP_VAL(rpfd,stcp,blksize);
	DUMP_STRING(rpfd,stcp,filler);
	DUMP_CHAR(rpfd,stcp,charconv);
	DUMP_CHAR(rpfd,stcp,keep);
	DUMP_VAL(rpfd,stcp,lrecl);
	DUMP_VAL(rpfd,stcp,nread);
	DUMP_STRING(rpfd,stcp,poolname);
	DUMP_STRING(rpfd,stcp,recfm);
	DUMP_U64(rpfd,stcp,size);
	DUMP_STRING(rpfd,stcp,ipath);
	DUMP_CHAR(rpfd,stcp,t_or_d);
	DUMP_STRING(rpfd,stcp,group);
	DUMP_STRING(rpfd,stcp,user);
	DUMP_VAL(rpfd,stcp,uid);
	DUMP_VAL(rpfd,stcp,gid);
	DUMP_VAL(rpfd,stcp,mask);
	DUMP_VALHEX(rpfd,stcp,status);
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
#ifdef STAGER_SIDE_SERVER_SUPPORT
		DUMP_VAL(rpfd,stcp,u1.t.side);
#endif
		DUMP_STRING(rpfd,stcp,u1.t.tapesrvr);
		DUMP_CHAR(rpfd,stcp,u1.t.E_Tflags);
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
		break;
	}
}


void DLL_DECL print_stpp(stpp)
	struct stgpath_entry *stpp;
{
	if (stpp == NULL) return;

	fprintf(stdout, "----------------------------------\n");
	fprintf(stdout, "Path entry  -   dump of reqid %d\n", stpp->reqid);
	fprintf(stdout, "----------------------------------\n");
	PRINT_VAL(stpp,reqid);
	PRINT_STRING(stpp,upath);
}

void DLL_DECL print_stcp(stcp)
	struct stgcat_entry *stcp;
{
	int i;

	if (stcp == NULL) return;

	fprintf(stdout, "-------------------------------------\n");
	fprintf(stdout, "Catalog entry - dump of reqid %d\n", stcp->reqid);
	fprintf(stdout, "-------------------------------------\n");
	PRINT_VAL(stcp,reqid);
	PRINT_VAL(stcp,blksize);
	PRINT_STRING(stcp,filler);
	PRINT_CHAR(stcp,charconv);
	PRINT_CHAR(stcp,keep);
	PRINT_VAL(stcp,lrecl);
	PRINT_VAL(stcp,nread);
	PRINT_STRING(stcp,poolname);
	PRINT_STRING(stcp,recfm);
	PRINT_U64(stcp,size);
	PRINT_STRING(stcp,ipath);
	PRINT_CHAR(stcp,t_or_d);
	PRINT_STRING(stcp,group);
	PRINT_STRING(stcp,user);
	PRINT_VAL(stcp,uid);
	PRINT_VAL(stcp,gid);
	PRINT_VAL(stcp,mask);
	PRINT_VALHEX(stcp,status);
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
#ifdef STAGER_SIDE_SERVER_SUPPORT
		PRINT_VAL(stcp,u1.t.side);
#endif
		PRINT_STRING(stcp,u1.t.tapesrvr);
		PRINT_CHAR(stcp,u1.t.E_Tflags);
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
		break;
	}
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
  if ((this - this_time) > SIXMONTHS) {
    strftime(timestr,64,strftime_format_sixmonthsold,tp);
  } else {
    strftime(timestr,64,strftime_format,tp);
  }
}
