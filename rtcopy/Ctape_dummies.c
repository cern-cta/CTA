/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_dummies.c,v $ $Revision: 1.5 $ $Date: 2000/02/11 16:13:47 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * Ctape_dummies.c - Ctape dummy interface
 */

#if defined(CTAPE_DUMMIES)

#include <stdlib.h>
#if defined(_WIN32)
#include <io.h>
#include <process.h>
#include <winsock2.h>
#endif /* _WIN32 */
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/stat.h>

#include <pwd.h>
#include <net.h>
#include <osdep.h>
#include <Castor_limits.h>
#include <vdqm_constants.h>
#include <vdqm_api.h>
#include <Ctape_api.h>
#include <serrno.h>
#include <log.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>


char *errbuf = NULL;
int errbufsz = 0;

static int jobID;
static char dgn[CA_MAXDGNLEN+1];
static char unit[CA_MAXUNMLEN+1];

#define CTAPE_DECL \
    int _rc = 0; \
    int rc = 0; \
    int status = 0; \
    int value; \
    int VolReqID = 0; \
    char msg[80];

#define CTAPE_BODY(Z) \
    fprintf Z; \
    fprintf(stdout,"RC=?, errno=?, msg=?\n"); \
    scanf("%d %d %[^\n]",&rc,&value,msg); \
    fflush(stdin); \
    if ( rc == -1 ) { \
      serrno = value; \
      errbuf = rtcpd_CtapeErrMsg(); \
    } \
    _rc = 0; \
    jobID = getpgrp(); \
    if ( errbuf != NULL ) strcpy(errbuf,msg); \
    if ( rc == 0 && strstr(#Z,"Ctape_mount") != NULL ) {\
      status = VDQM_UNIT_ASSIGN; \
      value = VolReqID; \
      if ( serrno != ETDNP ) \
        _rc = vdqm_UnitStatus(NULL,NULL,dgn,NULL,unit,&status,&value,0); \
      status = VDQM_VOL_MOUNT; \
      value = 0; \
      _rc = open((char *)a,O_CREAT | O_RDWR,S_IREAD | S_IWRITE); close(_rc); \
      chmod((char *)a,S_IREAD | S_IWRITE); \
      if ( serrno != ETDNP ) \
        _rc = vdqm_UnitStatus(NULL,(char *)b,dgn,NULL,unit,&status,&value,jobID); \
    } \
    if ( rc != 0 && strstr(#Z,"Ctape_position") != NULL ) { \
      status = VDQM_VOL_UNMOUNT; \
      value = getpgrp(); \
      if ( serrno != ETDNP ) \
        _rc = vdqm_UnitStatus(NULL,NULL,dgn,NULL,unit,&status,&value,jobID); \
    } \
    if ( rc == 0 && strstr(#Z,"Ctape_rls") != NULL ) { \
      status = VDQM_UNIT_RELEASE; \
      value = 0; \
      if ( serrno != ETDNP ) \
        _rc = vdqm_UnitStatus(NULL,NULL,dgn, \
                           NULL,unit,&status,&value,jobID); \
      if ( status & VDQM_VOL_UNMOUNT ) \
        status = VDQM_VOL_UNMOUNT | VDQM_UNIT_FREE; \
        if ( serrno != ETDNP ) \
          _rc = vdqm_UnitStatus(NULL,NULL,dgn, \
                              NULL,unit,&status,NULL,0); \
    } \
    if ( _rc == -1 ) rtcp_log(LOG_ERR,"%s: vdqm_UnitStatus(): %s\n",#Z,sstrerror(serrno)); \
    return(rc);

int Ctape_InitDummy() {
    int status;

    fprintf(stdout,"Ctape_InitDummy(): DGN=?, unit=?\n");
    scanf("%s %s",dgn,unit);
    status = VDQM_UNIT_UP | VDQM_UNIT_FREE;

    return(vdqm_UnitStatus(NULL,NULL,dgn,NULL,unit,&status,NULL,0));
}

int Ctape_config(char *a, int b, int c) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_config(%s,%d,%d)\n",a,b,c));
}


int Ctape_info(char *tp_path, 
               int *blocksize, 
               unsigned int *blockid,
               char *density, 
               char *devtype, 
               char *drive,
               char *fid,
               int *fseq, 
               int *lrecl, 
               char *recfm)  {
    int rc;
    char msg[80];

    fprintf(stdout,"Ctape_info(%s,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx)\n",
        tp_path,blocksize,blockid,density,devtype,drive,fid,fseq,lrecl,recfm);
    fprintf(stdout,"RC=?, msg=?\n");
    scanf("%d %s",&rc,msg);
    if ( errbuf != NULL ) strcpy(errbuf,msg);

    if ( rc == 0 ) {
        if ( blocksize != NULL ) {
            fprintf(stdout,"blocksize=?\n");
            scanf("%d",blocksize);
        }
        if ( blockid != NULL ) {
            fprintf(stdout,"blockid=?\n");
            scanf("%u",blockid);
        }
        if ( density != NULL ) {
            fprintf(stdout,"density=?\n");
            scanf("%s",density);
        }
        if ( devtype != NULL ) {
            fprintf(stdout,"devtype=?\n");
            scanf("%s",devtype);
        }
        if ( drive != NULL ) {
            fprintf(stdout,"drive=?\n");
            scanf("%s",drive);
        }
        if ( fid != NULL ) {
            fprintf(stdout,"fid=?\n");
            scanf("%s",fid);
        }
        if ( fseq != NULL ) {
            fprintf(stdout,"fseq=?\n");
            scanf("%d",fseq);
        }
        if ( lrecl != NULL ) {
            fprintf(stdout,"lrecl=?\n");
            scanf("%d",lrecl);
        }
        if ( recfm != NULL ) {
            fprintf(stdout,"recfm=?\n");
            scanf("%s",recfm);
        }
    }
    return(rc);
}

int Ctape_kill(char *a) {
    char *b;
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_kill(%s)\n",a));
}
int Ctape_mount(char *a, char *b, int c, char *d, char *e, char *f, int g,
                char *h, char *i, int j) {
    CTAPE_DECL
    int fd;
    fd = open(a,O_CREAT | O_TRUNC);
    if ( fd != -1 ) close(fd);
    VolReqID = j;
    CTAPE_BODY((stdout,"Ctape_mount(%s,%s,%d,%s,%s,%s,%d,%s,%s,%d)\n",
        a,b,c,d,e,f,g,h,i,j));
}
int Ctape_position(char *a, int b, int c, int d, unsigned int e, int f, int g,
                   int h, char *i, char *j, int k, int l, int m, int n) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_position(%s,%d,%d,%d,%d,%d,%d,%d,%s,%s,%d,%d,%d,%d)\n",
        a,b,c,d,e,f,g,h,i,j,k,l,m,n));
}
int Ctape_reserve(int a, struct dgn_rsv *b) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_reserve(%d,%s,%d)\n",a,b->name,b->num));
}
int Ctape_rls(char *a, int b) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_rls(%s,%d)\n",a,b));
}
int Ctape_rstatus(char *a, struct rsv_status *b, int c, int d) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_rstatus(%s,0x%x,%d,%d)\n",a,b,c,d));
}
void Ctape_seterrbuf(char *a, int b) {
    errbuf = a;
    errbufsz = b;
    return;
}
int Ctape_status(char *a, struct drv_status *b, int c) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_rstatus(%s,0x%x,%d)\n",a,b,c));
}

int gettperror(int fd, char *msgaddr) {
    int rc = 0;
    char msg[80];
    fprintf(stdout,"gettperror(%d,0x%x)\n",fd,msgaddr);
    fprintf(stdout,"RC=?, msg=?\n");
    scanf("%d %s",&rc,msg);
    msgaddr = msg;
    return(rc);
}

int wrttrllbl(int a, char *b, char *c) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"wrttrllbl(%d,%s,%s)\n",a,b,c));
}

int deltpfil(int a, char *b) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"deltpfil(%d,%s)\n",a,b));
}

int wrthdrlbl(int a, char *b) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"wrthdrlbl(%d,%s)\n",a,b));
}

int checkeofeov(int a, char *b) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"checkeofeov(%d,%s)\n",a,b));
}

#endif /* CTAPE_DUMMIES */

