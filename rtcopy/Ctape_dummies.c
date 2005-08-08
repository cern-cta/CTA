/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_dummies.c,v $ $Revision: 1.17 $ $Date: 2005/08/08 16:02:08 $ CERN IT/ADC Olof Barring";
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
#include <Cuuid.h>
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
      errbuf = rtcpd_GetCtapeErrBuf(); \
    } \
    _rc = 0; \
    jobID = rtcpd_jobID(); \
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
      value = rtcpd_jobID(); \
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

void (*Ctape_dmpmsg) _PROTO((int, const char *, ...)) = NULL;

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
               unsigned char *blockid,
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
        (a==NULL ? "(nil)" : a),(b==NULL ? "(nil)" : b),c,
        (d==NULL ? "(nil)" : d),(e==NULL ? "(nil)" : e),
        (f==NULL ? "(nil)" : f),g,(h==NULL ? "(nil)" : h),
        (i==NULL ? "(nil)" : i),j));
}
int Ctape_position(char *a, int b, int c, int d, unsigned char *e, int f, int g,
                   int h, char *i, char *j, char *k, int l, int m, int n, int o) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_position(%s,%d,%d,%d,0x%lx,%d,%d,%d,%s,%s,%s,%d,%d,%d,%d)\n",
        a,b,c,d,e,f,g,h,i,j,k,l,m,n,o));
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

#if defined(_AIX) && defined(_IBMR2)
int getdvrnam(char *path, char *drvname) {
    fprintf(stdout,"getdvrnam(%s,%s) called\n",
            (path != NULL ? path : "(nil)"),
            (drvname != NULL ? drvname : "(nil)"));
    return(0);
}
#endif /* _AIX && _IBMR2 */

int gettperror(int fd, char *path, char **msgaddr) {
    int rc = 0;
    static char msg[80];
    fprintf(stdout,"gettperror(%d,%s)\n",fd,path);
    fprintf(stdout,"RC=?, msg=?\n");
    scanf("%d %[^\n]",&rc,msg);
    if ( msgaddr != NULL ) *msgaddr = msg;
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

int get_compression_stats(int a, char *b, char *c, COMPRESSION_STATS *d) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"get_compression_stats(%d,%s,%s,0x%lx)\n",a,b,c,d));
}

int clear_compression_stats(int a, char *b, char *c) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"clear_compression_stats(%d,%s,%s)\n",a,b,c));
}

int Ctape_dmpinit(char *a, char *b, char *c, char *d, char *e, int f, int g,
                  int h, int i, int j, int k, int l, int m) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_dmpinit(%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d)\n",
                a,b,c,d,e,f,g,h,i,j,k,l,m));
}

int Ctape_dmpfil(char *a, char *b, int *c, char *d, int *e, int *f, 
                 int *g, char *h, u_signed64 *i) {
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_dmpfil(0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx,0x%lx)\n",a,b,c,d,e,f,g,h,i));
}

int Ctape_dmpend() {
    int a,b;
    CTAPE_DECL
    CTAPE_BODY((stdout,"Ctape_dmpend()\n"));
}

/*
 * Some stage API routines needed by RTCOPY
 */
int stage_setlog(void (*a) _PROTO((int, char *))) {
    return(0);
}

/*
int stage_updc_tppos(char *a, int b, int c, int d, char *e, char *f, int g, 
                     int h, char *i, char *j) {
    CTAPE_DECL
    fprintf(stdout,"stage_updc_tppos(%s,%d,%d,%d,%s,%s,%d,%d,%s,%s)\n",
            a,b,c,d,e,f,g,h,i,j);
    fprintf(stdout,"RC=?, errno=?, path=?\n"); 
    scanf("%d %d %[^\n]",&rc,&value,msg); 
    fflush(stdin); 
    if ( *msg != '\0' ) strcpy(j,msg);
    if ( rc == -1 ) {
        serrno = value;
    }
    return(rc);
}

int stage_updc_filcp(char *a, int b, int c, char *d, u_signed64 e, int f, int g,
                     int h, char *i, char *j, int k, int l, char *m, char *n) {
    CTAPE_DECL
    fprintf(stdout,"stage_updc_filcp(%s,%d,%d,%s,%d,%d,%d,%d,%s,%s,%d,%d,%s,%s)\n",
            a,b,c,d,(int)e,f,g,h,i,j,k,l,m,n);
    fprintf(stdout,"RC=?, errno=?, path=?\n");
    scanf("%d %d %[^\n]",&rc,&value,msg);
    fflush(stdin); 
    if ( *msg != '\0' ) strcpy(n,msg);
    if ( rc == -1 ) {
        serrno = value;
    }
    return(rc);
}
*/
ebc2asc(p, len)
char *p;
int len;
{
	int i;
	char *q;
	static int e2atab[256] = {

0x00,0x01,0x02,0x03,0x20,0x09,0x20,0x7F,0x20,0x20,0x20,0x0B,0x0C,0x0D,0x0E,0x0F,

0x10,0x11,0x12,0x13,0x20,0x0A,0x08,0x20,0x18,0x19,0x20,0x20,0x1C,0x1D,0x1E,0x1F,

0x20,0x20,0x1C,0x20,0x20,0x0A,0x17,0x1B,0x20,0x20,0x20,0x20,0x20,0x05,0x06,0x07,

0x20,0x20,0x16,0x20,0x20,0x1E,0x20,0x04,0x20,0x20,0x20,0x20,0x14,0x15,0x20,0x1A,

0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x2E,0x3C,0x28,0x2B,0x7C,

0x26,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x21,0x24,0x2A,0x29,0x3B,0x5E,

0x2D,0x2F,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x2C,0x25,0x5F,0x3E,0x3F,

0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x60,0x3A,0x23,0x40,0x27,0x3D,0x22,

0x20,0x61,0x62,0x63,0x64,0x65,0x66,0x67,0x68,0x69,0x20,0x20,0x20,0x20,0x20,0x20,

0x20,0x6A,0x6B,0x6C,0x6D,0x6E,0x6F,0x70,0x71,0x72,0x20,0x20,0x20,0x20,0x20,0x20,

0x20,0x7E,0x73,0x74,0x75,0x76,0x77,0x78,0x79,0x7A,0x20,0x20,0x20,0x5B,0x20,0x20,

0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x20,0x5D,0x20,0x20,

0x7B,0x41,0x42,0x43,0x44,0x45,0x46,0x47,0x48,0x49,0x20,0x20,0x20,0x20,0x20,0x20,

0x7D,0x4A,0x4B,0x4C,0x4D,0x4E,0x4F,0x50,0x51,0x52,0x20,0x20,0x20,0x20,0x20,0x20,

0x5C,0x20,0x53,0x54,0x55,0x56,0x57,0x58,0x59,0x5A,0x20,0x20,0x20,0x20,0x20,0x20,

0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x20,0x20,0x20,0x20,0x20,0x20
};

	for (q = p, i = 0; i < len; q++, i++)
		*q = e2atab[*q & 0xff];
}

asc2ebc(p, len)
char *p;
int len;
{
	int i;
	char *q;
	static int a2etab[256] = {

0x00,0x01,0x02,0x03,0x37,0x2D,0x2E,0x2F,0x16,0x05,0x25,0x0B,0x0C,0x0D,0x0E,0x0F,

0x10,0x11,0x12,0x13,0x3C,0x3D,0x32,0x26,0x18,0x19,0x3F,0x27,0x22,0x1D,0x35,0x1F,

0x40,0x5A,0x7F,0x7B,0x5B,0x6C,0x50,0x7D,0x4D,0x5D,0x5C,0x4E,0x6B,0x60,0x4B,0x61,

0xF0,0xF1,0xF2,0xF3,0xF4,0xF5,0xF6,0xF7,0xF8,0xF9,0x7A,0x5E,0x4C,0x7E,0x6E,0x6F,

0x7C,0xC1,0xC2,0xC3,0xC4,0xC5,0xC6,0xC7,0xC8,0xC9,0xD1,0xD2,0xD3,0xD4,0xD5,0xD6,

0xD7,0xD8,0xD9,0xE2,0xE3,0xE4,0xE5,0xE6,0xE7,0xE8,0xE9,0xAD,0xE0,0xBD,0x5F,0x6D,

0x79,0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89,0x91,0x92,0x93,0x94,0x95,0x96,

0x97,0x98,0x99,0xA2,0xA3,0xA4,0xA5,0xA6,0xA7,0xA8,0xA9,0xC0,0x4F,0xD0,0xA1,0x07,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,

0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40,0x40
};

	for (q = p, i = 0; i < len; q++, i++)
		*q = a2etab[*q & 0xff];
}

#endif /* CTAPE_DUMMIES */

