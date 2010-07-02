/*
 * rfcp.c,v 1.61 2004/03/02 16:18:33 jdurand Exp
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <signal.h>
#include <fcntl.h>
#define RFIO_KERNEL 1
#include "rfio.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "osdep.h"
#include "stager_api.h"
#include "u64subr.h"
#include <unistd.h>
#include "Cglobals.h"
#include "Castor_limits.h"
#include "stage_constants.h"
#include "rtcp_constants.h"
#include "stager_client_commandline.h"

#ifndef TRANSFER_UNIT
#define TRANSFER_UNIT   131072
#endif

#if !defined(vms)
#define SYERR 2
#define USERR 1
#define OK    0
#else
#define SYERR 2*2
#define USERR 1*2
#define OK    1
#endif  /* vms */

extern int tStageHostKey;
extern int tStagePortKey;
extern int tSvcClassKey;

extern int serrno ;
extern int rfio_errno ;
extern char *getconfent() ;
#if sgi
extern char *strdup _PROTO((CONST char *));
#endif

#if defined(vms) && (vms == 1)
#include <unixio.h>
#if !defined(S_ISDIR)
#define S_ISDIR(m) (((m)&S_IFMT) == S_IFDIR)
#endif /* S_ISDIR */
#if !defined(S_ISCHR)
#define S_ISCHR(m) (((m)&S_IFMT) == S_IFCHR)
#endif /* S_ISCHR */
#if !defined(S_ISBLK)
#define S_ISBLK(m)  (((m)&S_IFMT) == S_IFBLK)
#endif /* S_ISBLK */
#define unlink delete
#endif /* vms */

void usage();
off64_t copyfile _PROTO((int, int, char *, u_signed64));
void copyfile_stglog _PROTO((int, char *));
/* instructions for the signal handler v.s. stcp_output (bit-wise flag)*/
/*  0: do nothing */
/*  1: free stcp_output */
/*  2: call stage_clr_Path on stcp_output->ipath */
/*  4: call stage_clr_Path on stpp_input.upath */
/*  8: exit */
void copyfile_stgcleanup _PROTO((int));
#define CLEANER_NOOP 0
#define CLEANER_FREE 1
#define CLEANER_CLR  2
#define CLEANER_CLR2 4
#define CLEANER_EXIT 8
int nstcp_output = 0;
struct stgcat_entry *stcp_output = NULL;
int have_maxsize;
u_signed64 maxsize;
int v2 = 0;
u_signed64 inpfile_size = 0;

/* Signal handler - Simplify the POSIX sigaction calls */
#ifdef __STDC__
typedef void    Sigfunc(int);
Sigfunc *_rfio_signal(int, Sigfunc *);
#else
typedef void    Sigfunc();
Sigfunc *_rfio_signal();
#endif

/**
 * used to be defined in stage_api.c when CASTOR1 code was still
 * around. Since it is only used by rfcp now, I've moved it here
 */
int rc_castor2shift(int rc) {
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

int main(argc, argv)
     int argc;
     char *argv[];
{
  int argvindx;  /* argument index in program argv */
  int binmode = 0;
  int c;
  int cfargc;  /* number of arguments in command file */
  char **cfargv = 0; /* arguments in command file */
  int cfargvindx; /* argument index in command file */
  char *curargv; /* current argv */
  int  fd1, fd2;
  int incmdfile = 0; /* 1 = processing command file */
  struct stat64 sbuf, sbuf2;
  off64_t size;
  time_t starttime, endtime;
  int rc ;
  int rc2 ;
  char filename[BUFSIZ], filename_sav[BUFSIZ], inpfile[BUFSIZ], outfile[BUFSIZ] ;
  char *cp , *ifce ;
  char ifce1[8] ;
  char ifce2[8] ;
  char *host1, *host2, *path1, *path2 ;
  char shost1[32];
  int l1, l2 ;
  int v;

  extern char * getifnam() ;
  int input_is_local = 1;

  /* Init important variable for the cleaner */
  path1=path2=NULL;
  have_maxsize = -1;
  maxsize = 0;
  cfargc = 0;
  cfargvindx = 0;
  inpfile[0] = '\0';
  outfile[0] = '\0';
  memset(&sbuf,'\0',sizeof(sbuf));
  memset(&sbuf2,'\0',sizeof(sbuf2));
  for (argvindx = 1; argvindx < argc || cfargvindx < cfargc; ) {
    if (cfargvindx >= cfargc) {
      incmdfile = 0;
    }
    curargv = incmdfile ? cfargv[cfargvindx++] : argv[argvindx++];
    if (strcmp (curargv, "-s") == 0) {
      curargv = incmdfile ? cfargv[cfargvindx++] : argv[argvindx++];
      /* We verify that curargv do not contain other characters but digits */
      if (strspn(curargv,"0123456789") != strlen(curargv)) {
        fprintf(stderr,"-s option value should contain only digits\n");
        exit(USERR);
      }
      maxsize = strtou64(curargv);
      if (maxsize == 0) {
        usage();
      }
      have_maxsize = 1;
      curargv = incmdfile ? cfargv[cfargvindx++] : argv[argvindx++];
      if (strcmp(curargv, "-v2") == 0) {
        if (v2) {
          usage(); /* Option yet parsed */
        }
        v2 = 1;
        curargv = incmdfile ? cfargv[cfargvindx++] : argv[argvindx++];
      }
    } else if (strcmp(curargv, "-v2") == 0) {
      if (v2) {
        usage(); /* Option yet parsed */
      }
      v2 = 1;
      continue;
    }

    /* Special treatment for filenames starting with /scratch/... */
    if (inpfile[0] == '\0') {
      if (!strncmp ("/scratch/", curargv, 9) &&
          (cp = getconfent ("SHIFT", "SCRATCH", 0)) != NULL) {
        strcpy (inpfile, cp);
        strcat (inpfile, curargv+9);
      } else
        strcpy (inpfile, curargv);
    } else {
      if (!strncmp ("/scratch/", curargv, 9) &&
          (cp = getconfent ("SHIFT", "SCRATCH", 0)) != NULL) {
        strcpy (outfile, cp);
        strcat (outfile, curargv+9);
      } else
        strcpy (outfile, curargv);
    }
  }

  if (! outfile[0]) {
    usage();
  }

  /* We remove double slashes in both inpfile and outfile */
  if (rfio_stat64(outfile, &sbuf2) == 0) {
    if (S_ISDIR(sbuf2.st_mode)) {
      /* Not allowed if inpfile is stdin */
      if (strcmp(inpfile,"-") == 0) {
        fprintf (stderr, "Not valid output when input is the stdin\n");
        exit (USERR);
      }
      if ( (cp = strrchr(inpfile,'/'))  != NULL  ) {
        cp++;
      } else {
        cp = inpfile;
      }
      sprintf(filename, "%s/%s", outfile, cp);
    } else {
      strcpy(filename,outfile);
    }
  }
  /* Exit if there was a communication error with the name server */
  else if ((serrno == SENOSHOST) ||
           (serrno == SENOSSERV) ||
           (serrno == SECOMERR)) {
    fprintf(stderr,"Error on rfio_stat64(%s): %s\n", outfile, sstrerror(serrno));
    exit (USERR);
  } else {
    strcpy(filename,outfile);
  }

  /* We remove double slashes in renegerated outfile */
  strcpy(filename_sav,filename);

  l1 = rfio_parseln( inpfile , &host1, &path1, NORDLINKS ) ;
  if (l1 < 0) {
    fprintf(stderr,"%s\n",sstrerror(serrno));
    exit (USERR);
  }

  if ( l1 ) {
    strcpy(shost1,host1) ;
    input_is_local = 0;
  }

  strcpy( filename, path1 );

  l2 = rfio_parseln( filename_sav , &host2, &path2, NORDLINKS ) ;
  if (l2 < 0) {
    fprintf(stderr,"%s\n",sstrerror(serrno));

    exit(USERR);
  }



  /* Command is of the form cp f1 f2. */
  serrno = rfio_errno = 0;
  if (strcmp(inpfile,"-") != 0) {
    rc = rfio_stat64(inpfile, &sbuf);
    if ( rc == 0 && ( S_ISDIR(sbuf.st_mode) || S_ISCHR(sbuf.st_mode)
                      || S_ISBLK(sbuf.st_mode)
                      ) ) {
      fprintf(stderr,"file %s: Not a regular file\n",inpfile);

      exit(USERR) ;
    } else if (rc == 0) {
      inpfile_size = (u_signed64) sbuf.st_size;
    } else {
      rfio_perror(inpfile);
      exit(USERR) ;
    }
  } else {
    sbuf.st_mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH; /* Default open mode */
  }
  strcpy(filename,filename_sav);

  if ( ( l1 == 0 && l2 == 0 && (!memcmp(&sbuf,&sbuf2,sizeof(sbuf))) ) ||
       ( l1 && l2 && !strcmp( shost1, host2 ) && (!memcmp(&sbuf,&sbuf2,sizeof(sbuf))) ) ) {
    fprintf(stderr,"files are identical \n");

    exit (USERR) ;
  }

  /* Activate new transfer mode for source file */
  if (! v2) {
    v = RFIO_STREAM;
    rfiosetopt(RFIO_READOPT,&v,4);
  }

  serrno = rfio_errno = 0;
  if (rfio_HsmIf_IsHsmFile(inpfile)) {
    /* The input is a CASTOR file - we need a signal handler because rfio_open() calls the stager */
    _rfio_signal (SIGHUP, copyfile_stgcleanup);
    _rfio_signal (SIGQUIT, copyfile_stgcleanup);
    _rfio_signal (SIGINT, copyfile_stgcleanup);
    _rfio_signal (SIGTERM, copyfile_stgcleanup);
  }
  if (rfio_HsmIf_IsHsmFile(inpfile) && (have_maxsize > 0) && (inpfile_size > 0)) {
    /* User specified a CASTOR file in input with a limited size in bytes */
    /* We cannot use rfio_open() directly because then the stager will try to */
    /* stage the full file */
    serrno = rfio_errno = 0;
    fd1 = rfio_HsmIf_open(inpfile,O_RDONLY|binmode ,0644, 1, 0);
  } else {
    serrno = rfio_errno = 0;
    if (strcmp(inpfile,"-") == 0) {
      fd1 = fileno(stdin);
    } else {
      fd1 = rfio_open64(inpfile,O_RDONLY|binmode ,0644);
    }
  }
  if (fd1 < 0) {
    if (serrno) {
      rfio_perror(inpfile);
      c = (serrno == ESTKILLED) ? 196 : ((serrno == ENOSPC) ? ENOSPC : ((serrno == EBUSY) ? EBUSY : rc_castor2shift(-serrno)));
      if (c == serrno) c = SYERR; /* No match serrno <-> exit code */
    } else {
      switch (rfio_errno) {
      case EACCES:
      case EISDIR:
      case ENOENT:
      case EPERM :
        rfio_perror(inpfile);
        c = USERR;
        break ;
      case ENOSPC :
        rfio_perror(inpfile);
        c = ENOSPC;
        break ;
      case EBUSY :
        rfio_perror(inpfile);
        c = EBUSY;
        break ;
      case 0:
        switch(errno) {
        case EACCES:
        case EISDIR:
        case ENOENT:
        case EPERM :
          rfio_perror(inpfile);
          c = USERR;
          break;
        case ENOSPC :
          rfio_perror(inpfile);
          c = ENOSPC;
          break;
        case EBUSY :
          rfio_perror(inpfile);
          c = EBUSY;
          break;
        default:
          rfio_perror(inpfile);
          c = SYERR;
        }
        break;
      default:
        rfio_perror(inpfile);
        c = SYERR;
      }
    }
    exit (c);
  }
  if ( (ifce=getifnam(fd1))==NULL ) {
    strcpy(ifce1,"local");
  } else {
    strcpy(ifce1,ifce);
  }

  /* Activate new transfer mode for target file */
  if (! v2) {
    v = RFIO_STREAM;
    rfiosetopt(RFIO_READOPT,&v,4);
  }
  serrno = rfio_errno = 0;

  fd2 = rfio_open64(filename, O_WRONLY|O_CREAT|O_TRUNC|binmode ,sbuf.st_mode & 0777);
  if (fd2 < 0) {
    if (serrno) {
      rfio_perror(outfile);
      c = (serrno == ESTKILLED) ? 196 : ((serrno == ENOSPC) ? ENOSPC : ((serrno == EBUSY) ? EBUSY : rc_castor2shift(-serrno)));
      if (c == serrno) c = SYERR; /* No match serrno <-> exit code */
    } else {
      switch (rfio_errno) {
      case EACCES:
      case EISDIR:
      case ENOENT:
      case EPERM :
        rfio_perror(outfile);
        c = USERR;
        break;
      case ENOSPC :
        rfio_perror(outfile);
        c = ENOSPC;
        break;
      case EBUSY :
        rfio_perror(outfile);
        c = EBUSY;
        break;
      case 0:
        switch(errno) {
        case EACCES:
        case EISDIR:
        case ENOENT:
        case EPERM :
          rfio_perror(outfile);
          c = USERR;
          break;
        case ENOSPC :
          rfio_perror(outfile);
          c = ENOSPC;
          break;
        case EBUSY :
          rfio_perror(outfile);
          c = EBUSY;
          break;
        default:
          rfio_perror(outfile);
          c = SYERR;
        }
        break;
      default:
        rfio_perror(outfile);
        c = SYERR;
      }
    }

    exit (c);
  }
  /*
   * Get interfaces names
   */
  if ( (ifce=getifnam(fd2))==NULL ) {
    strcpy(ifce2,"local");
  } else {
    strcpy(ifce2,ifce);
  }

  time(&starttime);
  size = copyfile(fd1, fd2, outfile, maxsize);

  time(&endtime);
  if (size > 0) {
    char tmpbuf[21];
    char tmpbuf2[21];
    fprintf(stdout,"%s bytes in %d seconds through %s (in) and %s (out)", u64tostr(size, tmpbuf, 0), (int) (endtime-starttime),ifce1,ifce2);
    l1 = endtime-starttime ;
    if (l1 > 0) {
      fprintf(stdout," (%d KB/sec)\n",(int) (size/(1024*l1)) );
    } else {
      fprintf(stdout,"\n");
    }

    struct stat64 st;
    int rc = rfio_stat64(filename,&st);
    if (rc < 0) {
      fprintf(stdout, "Error on rfio_stat(%s): %s\n", filename, sstrerror(serrno));
    } else {
      fprintf(stdout,"%s bytes in remote file\n", u64tostr(st.st_size, tmpbuf, 0));
    }

    if (have_maxsize < 0) {
      rc2 = ((off64_t)inpfile_size == size ? OK : ((strcmp(inpfile,"-") == 0) ? OK : SYERR));
      if (rc2 == SYERR) {
          fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", sstrerror(SESYSERR), u64tostr(size, tmpbuf, 0), u64tostr(inpfile_size, tmpbuf2, 0));
          fprintf(stderr,"The local file size may have changed during the transfer process\n");
      }
    } else {
      if (maxsize < inpfile_size) { /* If input is "-", inpfile_size is zero */
        rc2 = ((off64_t)maxsize == size ? OK : SYERR);
        if (rc2 == SYERR) {
            fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", sstrerror(SESYSERR), u64tostr(size, tmpbuf, 0), u64tostr(maxsize, tmpbuf2, 0));
        }
      } else {
        rc2 = ((off64_t)inpfile_size == size ? OK : ((strcmp(inpfile,"-") == 0) ? OK : SYERR));
        if (rc2 == SYERR) {
            fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", sstrerror(SESYSERR), u64tostr(size, tmpbuf, 0), u64tostr(inpfile_size, tmpbuf2, 0));
        }
      }
    }
  } else if (size < 0) {
    rc2 = SYERR;
  } else {
    char tmpbuf[21];
    fprintf(stdout,"%s bytes transferred !!\n",u64tostr(size, tmpbuf, 0));
    rc2 = (inpfile_size == 0 ? OK : SYERR);
  }

  exit(rc2);
}

off64_t copyfile(fd1, fd2, name, maxsize)
     int fd1, fd2;
     u_signed64 maxsize;
     char *name;
{
  int n;
  mode_t mode;
  int m = -1;
  struct stat64 sbuf;
  off64_t effsize=0;
  char  *p;
  int bufsize = TRANSFER_UNIT;
  char * cpbuf;
  extern char *getenv();  /* External declaration */
  u_signed64 total_bytes = 0;
  int save_error_for_read=0;

  if ((p=getenv("RFCPBUFSIZ")) == NULL) {
    if ((p=getconfent("RFCP","BUFFERSIZE",0)) == NULL) {
      bufsize=TRANSFER_UNIT;
    } else  {
      bufsize=atoi(p);
    }
  } else {
    bufsize=atoi(p);
  }
  if (bufsize<=0) bufsize=TRANSFER_UNIT;

  if ( ( cpbuf = malloc(bufsize) ) == NULL ) {
    perror("malloc");
    exit (SYERR);
  }

  do {
    if (have_maxsize != -1) {
      if ((maxsize - total_bytes) < (u_signed64)bufsize)
        bufsize = maxsize - total_bytes;
    }
    serrno = rfio_errno = 0;
    n = rfio_read(fd1, cpbuf, bufsize);
    if (n > 0) {
      int count = 0;

      total_bytes += n;
      effsize += n;
      serrno = rfio_errno = 0;
      while (count != n && (m = rfio_write(fd2, cpbuf+count, n-count)) > 0) {
        count += m;
      }
      if (m < 0) {
        int save_err = rfio_serrno();
        /* Write failed.  Don't keep truncated regular file. */
        rfio_perror("rfcp");
          if (rfio_close(fd2) < 0) {
            rfio_perror("close target");
          }
        rfio_stat64(name, &sbuf); /* check for special files */
        mode = sbuf.st_mode & S_IFMT;
        if (mode == S_IFREG) rfio_unlink(name);
        free(cpbuf);
        exit((save_err == ENOSPC) ? ENOSPC : ((serrno == EBUSY) ? EBUSY : SYERR));
      }
    } else {
      if (n < 0) {
        rfio_perror("cp: read error");
        save_error_for_read = rfio_serrno();
        break;
      }
    }
  } while ((total_bytes != maxsize) && (n > 0));
  serrno = rfio_errno = 0;
  if (rfio_close(fd1) < 0) {
    if (save_error_for_read == SECHECKSUM) {
      /* if there was a checksum error we will return Bad checksum rc==200 */
      free(cpbuf);
      exit(200);
    }
    rfio_perror("close source");
    free(cpbuf);
    exit(SYERR);
  }

  serrno = rfio_errno = 0;
  if (rfio_close(fd2) < 0) {
    int save_errno=rfio_serrno();
    rfio_perror("close target");
    rfio_stat64(name, &sbuf); /* check for special files */
    mode = sbuf.st_mode & S_IFMT;
    if (mode == S_IFREG) {
      rfio_unlink(name);
    }
    free(cpbuf);
    /* for checksum error we have to return Bad checksum rc==200 */
    save_errno= (save_errno == SECHECKSUM) ? 200:SYERR;
    exit(save_errno);
  }

  free(cpbuf);
  return(effsize);
}

void usage()
{
  fprintf(stderr,"Usage:  rfcp [-s maxsize] [-v2] f1 f2; or rfcp f1 <dir2>\n");
  exit(USERR);
}


void copyfile_stglog(type, msg)
     int type;
     char *msg;
{
  if (type == 1) {   // MSG_ERR
    fprintf(stderr, "%s", msg);
  } else {
    /* Cosmetics: stager logs usually contains already a new line */
    if ((msg != NULL) && (msg[0] != '\0')) {
      if (msg[strlen(msg)-1] == '\n') {
        msg[strlen(msg)-1] = '\0';
      }
      TRACE(2,"rfio","copyfile_stglog: %s",msg);
    }
  }
}

void copyfile_stgcleanup(sig)
     int sig;
{
  TRACE(2,"rfio","copyfile_stgcleanup: Received signal No %d", sig);
  TRACE(2,"rfio","copyfile_stgcleanup: Sending stage_kill(%d)", sig);
  /* BC 2005-03-14 */
  /* Disabling this function for the new stager as stage_rm is not implemented yet */
  exit(USERR);
  return;
}

Sigfunc *_rfio_signal(signo, func)
int signo;
Sigfunc *func;
{
  struct sigaction act, oact;
  int n = 0;

  act.sa_handler = func;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (signo == SIGALRM) {
#ifdef SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT; /* SunOS 4.x */
#endif
  } else {
#ifdef SA_RESTART
    act.sa_flags |= SA_RESTART;  /* SVR4, 44BSD */
#endif
  }
  n = sigaction(signo, &act, &oact);
  if (n < 0) {
    return(SIG_ERR);
  }
  switch (signo) {
  case SIGHUP:
    TRACE(2,"rfio","_rfio_signal: Trapping SIGHUP");
    break;
  case SIGQUIT:
    TRACE(2,"rfio","_rfio_signal: Trapping SIGQUIT");
    break;
  case SIGINT:
    TRACE(2,"rfio","_rfio_signal: Trapping SIGINT");
    break;
  case SIGTERM:
    TRACE(2,"rfio","_rfio_signal: Trapping SIGTERM");
    break;
  default:
    TRACE(2,"rfio","_rfio_signal: Trapping signal No %d", signo);
    break;
  }
  return(oact.sa_handler);
}
