/*
 * $Id: xyopen.c,v 1.14 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* xyopen.c     Remote File I/O - Open a Fortran Logical Unit           */

/*
 * C bindings :
 *
 * rfio_xyopen(char *filename, char *host, int lun, int lrecl,
 *            char *chopt, int *irc)
 *
 * FORTRAN bindings :
 *
 * XYOPEN(INTEGER*4 LUN, INTEGER*4 LRECL, CHARACTER*(*) CHOPT, INTEGER*4 IRC)
 * XYOPN(CHARACTER*(*) FILENAME, CHARACTER*(*)HOST, INTEGER*4 LUN,
 *       INTEGER*4 LRECL, CHARACTER*(*) CHOPT, INTEGER*4 IRC)
 */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1
#include "rfio.h"
#include <sys/param.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <pwd.h>
#include <Cpwd.h>
#include <rfio_xy.h>

RFILE *ftnlun[MAXFTNLUN];       /* Fortran logical units       */

extern char *getacct();
extern int switch_open();

int rfio_xysock(int lun)
{
  if ( ftnlun[lun] == NULL )
    return -1 ;
  else
    return ftnlun[lun]->s ;
}

int
rfio_xyopen_ext(char *name,
		char *node,
		int lun,
		int lrecl,
		char *chopt,
		int *irc,
		uid_t uid,
		gid_t gid,
		int key,
		char *reqhost)
{
  register int    s;              /* socket descriptor            */
  int             status;         /* remote fopen() status        */
  int     len;                    /* total string length          */
  int     i;                      /* general purpose index        */
  char    * p ;                   /* buffer pointer               */
  WORD    access, format;         /* Fortran file characteristics */
  LONG    openopt;                /* Fortran file open options    */
  LONG  append ;
  LONG    trunc  ;                /* Trunc mode   */
  int  filen  ;
  RFILE  *fd    ;                /* remote file desciptor        */
  char    *host  ;                /* actual host to be used       */
  char    *filepath;              /* actual file path to be used  */
  char    *account;               /* account string               */
  char    localhost[MAXHOSTNAMELEN];
  int  acc, parserc;
  WORD  uid_ext;
  WORD gid_ext;
  struct  passwd *pw;
  int  rt ;
  char     rfio_buf[BUFSIZ];

  INIT_TRACE("RFIO_TRACE");
  *irc = 0;
  TRACE(2,"--->","OPTIONS: %s , %s, %d,%d,%s,%d,%d,%d,%d,%s",name,node,lun,lrecl,chopt,*irc,uid,gid,key,reqhost);
  /*
   * First allocate a remote file descriptor
   */
  if ((fd = (RFILE *)malloc(sizeof(RFILE))) == NULL)        {
    END_TRACE();
    return(errno);
  }

  /*
   * Analyze options
   */

  access = FFFACC_S;              /* Default is access SEQUENTIAL */
  format = FFFFORM_U;             /* Default is unformatted       */
  openopt = FFOOPT_0;             /* Default is no options        */
  for (i=0;i< (int)strlen(chopt);i++)   {
    switch (chopt[i])       {
    case 'u':
    case 'U':
      format = FFFFORM_U; break;
    case 'f':
    case 'F':
      free((char *)fd);
      END_TRACE();
      return(SEBADFFORM);
    case 's':
    case 'S':
      access = FFFACC_S; break;
    case 'd':
    case 'D':
      access = FFFACC_D; break;
    case 'a':
    case 'A':
      openopt |= FFOOPT_A;
      break;
    case 'e':
    case 'E':
      openopt |= FFOOPT_E;
      break;
    case ' ': break;
    default :
      free((char *)fd);
      END_TRACE();
      return(SEBADFOPT);
    }
  }
  if ((openopt & FFOOPT_A) && (access == FFFACC_D))       {
    free((char *)fd);
    END_TRACE();
    return(SEINCFOPT);
  }

  fd->unit = lun;
  fd->access = access;
  fd->format = format;
  fd->recl = lrecl;
  fd->blank = FFFBLNK_N;          /* currently unsupported        */
  fd->opnopt = openopt;
  fd->ftype = FFTYPE_F;
  fd->passwd = key ;


  /*
   *  File name parsing
   */
  host = node;
  filepath = name;
  striptb(node);
  if (strcmp(node,"") == 0)       {
    /* empty node, so, it may be a remote file syntax       */
    if (strcmp(name, "") == 0)      {
      /* empty node & name, it may be assigned        */
      if ((name=lun2fn(lun)) == NULL)        {
        free((char *)fd);
        END_TRACE();
        return(errno);
      }
    }
  }

  if (gethostname(localhost, sizeof(localhost)) < 0)      {
    TRACE(2, "rfio", "gethostname () failed.");
    free((char *)fd);
    return(-1);
  }

  if  ( (strcmp(node,"") == 0) || (strcmp(localhost, node)==0 ) || (strcmp(node,"localhost") == 0))      {
    /*
     * The file is local
     */
    if (!(parserc = rfio_parse(name,&host,&filepath))) {
      strcpy( fd->host , "localhost" );
      ftnlun[lun]=fd;
      filen= strlen(filepath) ;
      append = openopt & FFOOPT_A;
      trunc = openopt & FFOOPT_T ;
      acc=(int)access;
      *irc=switch_open(&acc,&lun,filepath, &filen, &lrecl, &append,&trunc,LLM);
      TRACE(2, "rfio", "rxyopen (local) : %d", *irc);
      END_TRACE();
      rfio_errno = 0;
      return(*irc);
    } else {
      if (parserc < 0) {
        END_TRACE();
        return(-1);
      }
    }
    TRACE(3, "rfio", "rfio_xyopen: name %s host %s filepath %s", name, host, filepath);
  }

  /*
   * The only user allowed to make requests with uid !=0 or gid !=0
   * is root.
   */
  if (uid!=0 || gid!=0)
    /*
     * DIRECT ACCESS: user specifies target
     */
    fd->mapping=0;
  /*
   * MAPPED ACCESS: user will be mapped
   */
  else
    fd->mapping=1;

  if ( (int)strlen(filepath) > MAXFILENAMSIZE)       {
    free((char *)fd);
    END_TRACE();
    return(SEFNAM2LONG);
  }
  strncpy( fd->host , host, RESHOSTNAMELEN );

  uid_ext=(WORD)uid;
  gid_ext=(WORD)gid;
  /*
   * Initialize RFILE structure
   */
  fd->magic = RFIO_MAGIC;
  fd->s = -1;
  fd->uid = (uid_ext==0 ? geteuid(): (uid_t)uid_ext);
  fd->gid = (gid_ext==0 ? getegid(): (gid_t)gid_ext);
  (void) umask(fd->umask=umask(0));
  fd->bufsize = 0;
  fd->ftype = FFTYPE_F;
#if !defined(VM) && !defined(MVS)
  fd->binary = 0;                 /* no translation needed        */
#endif /* VM */
  fd->unit = lun;
  fd->access = access;
  fd->format = format;
  fd->recl = lrecl;
  fd->blank = FFFBLNK_N;          /* currently unsupported        */
  fd->opnopt = openopt;

  s = rfio_connect(host,&rt);
  if (s < 0)      {
    free((char *)fd);
    END_TRACE();
    if (serrno) return(serrno); else return(errno);
  }
  TRACE(2, "rfio", "rfio_xyopen: setsockopt(SOL_SOCKET, SO_KEEPALIVE)");
  status = 1 ;
  if (setsockopt(fd->s, SOL_SOCKET, SO_KEEPALIVE,(char *)&status , sizeof (int) ) == -1)
    TRACE(2, "rfio" ,"rfio_xyopen: setsockopt(SO_KEEPALIVE) failed");
  if ( !rt && !fd->mapping ) {
    fd->uid=geteuid() ;
    fd->gid=getegid() ;
    TRACE(3,"rfio", "re-setting (uid,gid) to %d,%d",fd->uid,fd->gid) ;
    fd->mapping = 1 ;
  }

  fd->s = s;
  ftnlun[lun]=fd;

  if ( (pw = Cgetpwuid(geteuid()) ) == NULL ) {
    TRACE(2, "rfio" ,"rfio_open: Cgetpwuid() error %s",strerror(errno));
    free ((char *)fd);
    END_TRACE();
    return(errno);
  }

  /* if ((account = getacct()) == NULL) */ account = "";

  TRACE(3, "rfio", "rfio_xyopen: uid %d gid %d umask %o ftype %d user %s",
        fd->uid, fd->gid, fd->umask, fd->ftype,pw->pw_name);
  TRACE(3, "rfio", "rfio_xyopen: %d lun %d format %d access %d lrecl",
        fd->unit, fd->format, fd->access, fd->recl);
  TRACE(3, "rfio", "rfio_xyopen: account: %s", account);
  TRACE(3, "rfio", "rfio_xyopen: filepath: %s", filepath);
  len = 7*WORDSIZE + 4*LONGSIZE + strlen(account) + strlen(filepath) + strlen(pw->pw_name) + strlen(reqhost) + 4 ;
  p= rfio_buf ;
  marshall_WORD(p, B_RFIO_MAGIC) ;
  marshall_WORD(p, RQST_XYOPEN) ;
  marshall_LONG(p, len) ;
  p= rfio_buf + RQSTSIZE ;
  marshall_WORD(p, fd->uid) ;
  marshall_WORD(p, fd->gid) ;
  marshall_WORD(p, fd->umask) ;
  marshall_WORD(p, fd->ftype) ;
  marshall_LONG(p, fd->unit) ;
  marshall_WORD(p, fd->format) ;
  marshall_WORD(p, fd->access) ;
  marshall_LONG(p, fd->recl) ;
  marshall_LONG(p, fd->opnopt) ;
  marshall_STRING(p, account) ;
  marshall_STRING(p, filepath) ;
  marshall_STRING(p,pw->pw_name) ;
  marshall_STRING(p,reqhost);
  marshall_LONG(p, fd->passwd);
  marshall_WORD(p, fd->mapping);
  TRACE(3,"rfio","rfio_xyopen: sending %d bytes",RQSTSIZE+len) ;
  if (netwrite_timeout(s,rfio_buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
    TRACE(3, "rfio", "rfio_xyopen: write(): ERROR occured (errno=%d)", errno);
    free((char *)fd); ftnlun[lun]=(RFILE *) NULL;
    return( (serrno ? serrno : errno) );
  }
  TRACE(3, "rfio", "rfio_xyopen: reading %d bytes", LONGSIZE);
  if (netread_timeout(s,rfio_buf,LONGSIZE,RFIO_CTRL_TIMEOUT) != LONGSIZE) {
    TRACE(3, "rfio", "rfio_xyopen: read(): ERROR occured (errno=%d)", errno);
    free((char *)fd); ftnlun[lun]=(RFILE *) NULL;
    return( (serrno ? serrno : errno) );
  }
  p= rfio_buf ;
  unmarshall_LONG(p, status);
  TRACE(2, "rfio", "rfio_xyopen: return %d ",status);
  if (status)     {
    close(s);
    free((char *)fd); ftnlun[lun]=(RFILE *) NULL;
    rfio_errno = status ;
  }
  *irc = status;
  TRACE(1, "rfio", "rfio_xyopen: status: %d, irc: %d",status,*irc);
  END_TRACE();
  return(status);
}

int rfio_xyopen(char    *name,
                char *node,
                int     lun,
                int lrecl,
                char *chopt,
                int     *irc)
{
  char rh[1];
  rh[0]='\0';
  return( rfio_xyopen_ext(name,node,lun,lrecl,chopt,irc,(uid_t)0,(gid_t)0,0,rh) );
}

/*
 * Fortran wrapper
 */

void xyopen_(int     *flun,
             int *flrecl,
             char    *fchopt,
             int *firc,
             int     fchoptl)
{
  char    *chopt;                 /* "C" character strings        */
  int     status;                 /* xyopen return status         */

  INIT_TRACE("RFIO_TRACE");       /* initialize trace if any      */

  /*
   * convert fortran arguments
   */
  if ((chopt = malloc(fchoptl+1)) == NULL)        {
    *firc = -errno;
    return;
  }
  strncpy(chopt, fchopt, fchoptl); chopt[fchoptl] = '\0';

  TRACE(1,"rfio","XYOPEN(%d,%d,%s,%d)",*flun,*flrecl,chopt,*firc);

  /*
   * Here comes real code
   */
  TRACE(1,"rfio","xyopen will return with code %d",*firc);
  status = rfio_xyopen("","",*flun,*flrecl,chopt,firc);
  TRACE(1, "rfio", "XYOPEN: status: %d, irc: %d",status,*firc);
  END_TRACE();
  if (status) *firc = -status;    /* system errors have precedence */
  free(chopt);
  return;
}

void xyopn_(char    *fname,
            char  *fnode,
            int     *flun,
            int *flrecl,
            char *fchopt,
            int *firc,
            int     fnamel,
            int fnodel,
            int fchoptl)
{
  char    *name, *node, *chopt;   /* "C" character strings        */
  int     status;                 /* xyopn return status          */

  INIT_TRACE("RFIO_TRACE");       /* initialize trace if any      */

  /*
   * convert fortran arguments
   */
  if ((name = malloc((unsigned) fnamel+1)) == NULL) {
    *firc = -errno;
    return;
  }
  if ((node = malloc((unsigned) fnodel+1)) == NULL) {
    *firc = -errno;
    free(name);
    return;
  }
  if ((chopt = malloc((unsigned) fchoptl+1)) == NULL) {
    *firc = -errno;
    free(node);
    free(name);
    return;
  }

  strncpy(name, fname, fnamel); name[fnamel] = '\0';
  strncpy(node, fnode, fnodel); node[fnodel] = '\0';
  strncpy(chopt, fchopt, fchoptl); chopt[fchoptl] = '\0';
  striptb(name);
  striptb(node);
  striptb(chopt);
  TRACE(1,"rfio","XYOPN(%s, %s, %d, %d, %s, %d)",
        name,node,*flun,*flrecl,chopt,*firc);

  status = rfio_xyopen(name,node,*flun,*flrecl,chopt,firc);
  TRACE(1, "rfio", "XYOPN: status: %d, irc: %d",status,*firc);
  END_TRACE();
  if (status) *firc = -status;    /* system errors have precedence */
  free(name); free(node); free(chopt);
  return;
}
