/*
 * $Id: stage_util.c,v 1.5 2001/07/12 11:00:38 jdurand Exp $
 */

#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#ifndef _WIN32
#include <sys/time.h>
#include <unistd.h>
#else
#include <winsock2.h>
#endif
#include "stage.h"
#include "stage_api.h"
#include "osdep.h"
#include "u64subr.h"

#ifdef __STDC__
#define NAMEOFVAR(x) #x
#else
#define NAMEOFVAR(x) "x"
#endif

#define DUMP_VAL(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define PRINT_VAL(st,member) fprintf(stdout, "%-23s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define DUMP_VALHEX(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define PRINT_VALHEX(st,member) fprintf(stdout, "%-23s : %20lx (hex)\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define DUMP_U64(rpfd,st,member) {                                          \
    char tmpbuf[21];                                                        \
	funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) ,	            \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}
#define PRINT_U64(st,member) {                                          \
    char tmpbuf[21];                                                        \
	fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) ,	            \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}

#define DUMP_CHAR(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define PRINT_CHAR(st,member) fprintf(stdout, "%-23s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define DUMP_STRING(rpfd,st,member) funcrep(rpfd, MSG_OUT, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)
#define PRINT_STRING(st,member) fprintf(stdout, "%-23s : %20s\n", NAMEOFVAR(member) , st->member)

extern char *getenv();         /* To get environment variables */
extern char *getconfent();     /* To get configuration entries */

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
/* The default is STGMAGIC3, while STGMAGIC2 can be forced to dial with old */
/* stager daemons */

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
	if (funcrep == NULL) return;

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

	if (funcrep == NULL) return;

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
	DUMP_U64(rpfd,stcp,c_time);
	DUMP_U64(rpfd,stcp,a_time);
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
		DUMP_STRING(rpfd,stcp,u1.t.tapesrvr);
		DUMP_CHAR(rpfd,stcp,u1.t.E_Tflags);
		for (i = 0; i < MAXVSN; i++) {
			DUMP_STRING(rpfd,stcp,u1.t.vid[i]);
			DUMP_STRING(rpfd,stcp,u1.t.vsn[i]);
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
		break;
	}
}


void DLL_DECL print_stpp(stpp)
	struct stgpath_entry *stpp;
{
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
	PRINT_U64(stcp,c_time);
	PRINT_U64(stcp,a_time);
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
		PRINT_STRING(stcp,u1.t.tapesrvr);
		PRINT_CHAR(stcp,u1.t.E_Tflags);
		for (i = 0; i < MAXVSN; i++) {
			PRINT_STRING(stcp,u1.t.vid[i]);
			PRINT_STRING(stcp,u1.t.vsn[i]);
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
		break;
	}
}

