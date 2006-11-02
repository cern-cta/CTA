/*
 * rfcp.c,v 1.61 2004/03/02 16:18:33 jdurand Exp
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)rfcp.c,v 1.61 2004/03/02 16:18:33 CERN/IT/DS/HSM Felix Hassine, Olof Barring, Jean-Damien Durand";
#endif /* not lint */

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
#ifdef CNS_ROOT
#include "stage_api.h" /* The CASTOR-aware rfcp needs the stage API */
#include "stager_api.h" /* For the CASTOR new stager */  
#endif
#include "u64subr.h"
#ifndef _WIN32
#include <unistd.h>
#endif
#include "Castor_limits.h"

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
extern int tCastorVersionKey;

extern int serrno ;
extern int rfio_errno ;
extern char *getconfent() ;
#if sgi
extern char *strdup _PROTO((CONST char *));
#endif

EXTERN_C int DLL_DECL use_castor2_api _PROTO(());

/* getDefaultForGlobal defined in the parse.c file */
int getDefaultForGlobal(char** host,int* port,char** svc,int* version);


#if defined(vms) && (vms == 1)
#include <unixio.h>
#if !defined(S_ISDIR)
#define S_ISDIR(m)	(((m)&S_IFMT) == S_IFDIR)
#endif /* S_ISDIR */
#if !defined(S_ISCHR)
#define S_ISCHR(m)	(((m)&S_IFMT) == S_IFCHR)
#endif /* S_ISCHR */
#if !defined(S_ISBLK)
#define S_ISBLK(m) 	(((m)&S_IFMT) == S_IFBLK)
#endif /* S_ISBLK */
#define	unlink	delete
#endif /* vms */


void usage();
off64_t copyfile _PROTO((int, int, char *, u_signed64));
off64_t copyfile_hpss _PROTO((int, int, char *, struct stat64 *, u_signed64));
#ifdef CNS_ROOT
#ifdef hpux
int copyfile_stage _PROTO(());
int copyfile_stage_from_local _PROTO(());
#else
int copyfile_stage _PROTO((char *, char *, mode_t, u_signed64, int));
int copyfile_stage_from_local _PROTO((char *, char *, mode_t, u_signed64, int));
#endif
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
static int copyfile_stgcleanup_instruction = CLEANER_NOOP;
int nstcp_output = 0;
struct stgcat_entry *stcp_output = NULL;
struct stgpath_entry stpp_input;
#endif /* CNS_ROOT */
int have_maxsize;
u_signed64 maxsize;
int v2 = 0;
u_signed64 inpfile_size = 0;

#ifndef _WIN32
/* Signal handler - Simplify the POSIX sigaction calls */
#ifdef __STDC__
typedef void    Sigfunc(int);
Sigfunc *_rfio_signal(int, Sigfunc *);
#else
typedef void    Sigfunc();
Sigfunc *_rfio_signal();
#endif
#else
#define _rfio_signal(a,b) signal(a,b)
#endif



int rfio_copy(char *inpfile, char *outfile, char *filename, struct stat64 *sbuf, int have_maxsize, u_signed64 inpfile_size,int binmode, int input_is_local){

	int c;
	int  fd1, fd2;
	int incmdfile = 0;	/* 1 = processing command file */
	struct stat64 sbuf2;
	off64_t size;
	time_t starttime, endtime;
	int rc ;
	int rc2 ;
	char *cp , *ifce ;
	char ifce1[8] ;
	char ifce2[8] ;
	char *host1, *host2, *path1, *path2 ;
	char shost1[32];
	int l1, l2 ;
	int v;

	extern char * getifnam() ;

#if defined(_WIN32)
	WSADATA wsadata;
#endif

#ifdef CNS_ROOT
	TRACE(1,"rfio","rfcp: Setting stager log callback");
	stage_setlog((void (*) _PROTO((int,char *)))&copyfile_stglog);

	if (! use_castor2_api()) {

	  if (rfio_HsmIf_IsHsmFile(filename) && (! input_is_local)) {
		/* We switch to the explicit STAGE mode */
		int rc = copyfile_stage((char *) inpfile, (char *) (filename), (mode_t) (sbuf->st_mode & 0777), (((have_maxsize > 0) && (inpfile_size > maxsize)) ? maxsize : inpfile_size), (int) (( sbuf->st_dev  == 0 && sbuf->st_ino  == 1 ) ? 1 : 0));
		if (rc >= 0) {
		 exit(rc);
		}
		/* When rc < 0 this means we switch to the old buf inefficient method */
		/* The user would not accept we always fail just because the input filename is too long */
		/* Note that this switch requires anyway the output filename to be in the limits */
	  } else if (rfio_HsmIf_IsHsmFile(filename)) {
		/* The output is in CASTOR, and input is local or maxsize possibly not bound exactly to the MB unit */
		int rc = copyfile_stage_from_local((char *) inpfile, (char *)filename, (mode_t) (sbuf->st_mode & 0777), (((have_maxsize > 0) && (inpfile_size > maxsize)) ? maxsize : inpfile_size), (int) (( sbuf->st_dev  == 0 && sbuf->st_ino  == 1 ) ? 1 : 0));
		if (rc >= 0) {
		 exit(rc);
		}
		/* In theory it cannot happen (!) copyfile_stage_from_local() returns >= 0 always */
		/* When rc < 0 this means we switch to the old buf inefficient method */
		/* The user would not accept we always fail just because the input filename is too long */
		/* Note that this switch requires anyway the output filename to be in the limits */
	  } else if (rfio_HsmIf_IsHsmFile(inpfile)) {
		/* Output is not CASTOR but input is : we will follow the classic rfcp behaviour */
		/* but we want neverthless to handle the termination signals v.s. stager */
		TRACE(2,"rfio","Instructing signal handler to exit");
		copyfile_stgcleanup_instruction = CLEANER_EXIT;
	  }
	}
#endif

#ifdef CNS_ROOT
	
	if (rfio_HsmIf_IsHsmFile(inpfile) && (have_maxsize > 0) && (inpfile_size > 0)) {
		/* User specified a CASTOR file in input with a limited size in bytes */
		/* We cannot use rfio_open() directly because then the stager will try to */
		/* stage the full file */
		serrno = rfio_errno = 0;
		fd1 = rfio_HsmIf_open_limbysz(inpfile,O_RDONLY|binmode ,0644, (maxsize < inpfile_size) ? maxsize : inpfile_size, 1);
	} else {
#endif /* CNS_ROOT */
		serrno = rfio_errno = 0;
		if (strcmp(inpfile,"-") == 0) {
			fd1 = fileno(stdin);
		} else {
			fd1 = rfio_open64(inpfile,O_RDONLY|binmode ,0644);
		}
#ifdef CNS_ROOT
	}
#endif /* CNS_ROOT */
	if (fd1 < 0) {
		if (serrno) {
			rfio_perror(inpfile);
#ifdef CNS_ROOT
			c = (serrno == ESTKILLED) ? REQKILD : ((serrno == ENOSPC) ? ENOSPC : ((serrno == EBUSY) ? EBUSY : rc_castor2shift(-serrno)));
			if (c == serrno) c = SYERR; /* No match serrno <-> exit code */
#else
			c = SYERR;
#endif
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
#if defined(_WIN32)
		WSACleanup();
#endif
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
	
        fd2 = rfio_open64(filename, O_WRONLY|O_CREAT|O_TRUNC|binmode ,sbuf->st_mode & 0777);
	if (fd2 < 0) {
		if (serrno) {
			rfio_perror(outfile);
#ifdef CNS_ROOT
			c = (serrno == ESTKILLED) ? REQKILD : ((serrno == ENOSPC) ? ENOSPC : ((serrno == EBUSY) ? EBUSY : rc_castor2shift(-serrno)));
			if (c == serrno) c = SYERR; /* No match serrno <-> exit code */
#else
			c = SYERR;
#endif
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
#if defined(_WIN32)
		WSACleanup();
#endif
		
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


	if ( rfio_HsmIf_GetHsmType(fd1,NULL) == RFIO_HSM_CNS ) {
		if (rfio_close(fd1) < 0) {
			rfio_perror("close source");
#if defined(_WIN32)
			WSACleanup();
#endif
			
			exit(SYERR);
		}
	}
	if ( rfio_HsmIf_GetHsmType(fd2,NULL) == RFIO_HSM_CNS ) {
		if (rfio_close(fd2) < 0) {
			struct stat64 sbuf;
			mode_t mode;
			rfio_perror("close target");
			rfio_stat64(filename, &sbuf);	/* check for special files */
			mode = sbuf.st_mode & S_IFMT;
			if (mode == S_IFREG) {
				rfio_unlink(filename);
			}
#if defined(_WIN32)
			WSACleanup();
#endif
			
			exit(SYERR);
		}
	}

	time(&endtime);
	if (size>0)  {
		char tmpbuf[21];
		char tmpbuf2[21];
		fprintf(stdout,"%s bytes in %d seconds through %s (in) and %s (out)", u64tostr(size, tmpbuf, 0), (int) (endtime-starttime),ifce1,ifce2);
		l1 = endtime-starttime ;
		if ( l1 > 0) {
			fprintf(stdout," (%d KB/sec)\n",(int) (size/(1024*l1)) );
		} else {
			fprintf(stdout,"\n");
		}
		if (have_maxsize < 0) {
			rc2 = (inpfile_size == size ? OK : ((strcmp(inpfile,"-") == 0) ? OK : SYERR));
			if (rc2 == SYERR) {
#ifdef _WIN32
				if (binmode != O_BINARY) {
					fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", "Warning", u64tostr(size, tmpbuf, 0), u64tostr(inpfile_size, tmpbuf2, 0));
					rc = OK;
				} else {
#endif
				fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", sstrerror(SESYSERR), u64tostr(size, tmpbuf, 0), u64tostr(inpfile_size, tmpbuf2, 0));
#ifdef _WIN32
				}
#endif
			}
		} else {
			if (maxsize < inpfile_size) { /* If input is "-", inpfile_size is zero */
				rc2 = (maxsize == size ? OK : SYERR);
				if (rc2 == SYERR) {
#ifdef _WIN32
					if (binmode != O_BINARY) {
						fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", "Warning", u64tostr(size, tmpbuf, 0), u64tostr(maxsize, tmpbuf2, 0));
						rc = OK;
					} else {
#endif
						fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", sstrerror(SESYSERR), u64tostr(size, tmpbuf, 0), u64tostr(maxsize, tmpbuf2, 0));
#ifdef _WIN32
					}
#endif
				}
			} else {
				rc2 = (inpfile_size == size ? OK : ((strcmp(inpfile,"-") == 0) ? OK : SYERR));
				if (rc2 == SYERR) {
#ifdef _WIN32
					if (binmode != O_BINARY) {
						fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", "Warning", u64tostr(size, tmpbuf, 0), u64tostr(inpfile_size, tmpbuf2, 0));
						rc = OK;
					} else {
#endif
						fprintf(stderr,"%s : got %s bytes instead of %s bytes\n", sstrerror(SESYSERR), u64tostr(size, tmpbuf, 0), u64tostr(inpfile_size, tmpbuf2, 0));
#ifdef _WIN32
					}
#endif
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
#if defined(_WIN32)
	WSACleanup();
#endif
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
	extern char *getenv();		/* External declaration */
	u_signed64 total_bytes = 0;
	
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
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (SYERR);
	}

	do {
		if (have_maxsize != -1) {
			if ((maxsize - total_bytes) < bufsize)
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
				if ( rfio_HsmIf_GetHsmType(fd2,NULL) != RFIO_HSM_CNS ) {
					if (rfio_close(fd2) < 0) {
						rfio_perror("close target");
					}
				}
				rfio_stat64(name, &sbuf);	/* check for special files */
				mode = sbuf.st_mode & S_IFMT;
				if (mode == S_IFREG) rfio_unlink(name);
#if defined(_WIN32)
				WSACleanup();
#endif
				free(cpbuf);
				exit((save_err == ENOSPC) ? ENOSPC : ((serrno == EBUSY) ? EBUSY : SYERR));
			}
		} else {
			if (n < 0) {
				rfio_perror("cp: read error");
				break;
			}
		}
	} while ((total_bytes != maxsize) && (n > 0));
	serrno = rfio_errno = 0;
	if (rfio_HsmIf_GetHsmType(fd1,NULL) != RFIO_HSM_CNS && rfio_close(fd1) < 0) {
		rfio_perror("close source");
#if defined(_WIN32)
		WSACleanup();
#endif
		free(cpbuf);
		exit(SYERR);
	}

	serrno = rfio_errno = 0;
	if (rfio_HsmIf_GetHsmType(fd2,NULL) != RFIO_HSM_CNS && rfio_close(fd2) < 0) {
		rfio_perror("close target");
		rfio_stat64(name, &sbuf);	/* check for special files */
		mode = sbuf.st_mode & S_IFMT;
		if (mode == S_IFREG) {
			rfio_unlink(name);
		}
#if defined(_WIN32)
		WSACleanup();
#endif
		free(cpbuf);
		exit(SYERR);
	}
	
	free(cpbuf);
	return(effsize);
}

void usage()
{
	fprintf(stderr,"Usage:  rfcp [-s maxsize] [-v2] f1 f2; or rfcp f1 <dir2>\n");
	exit(USERR);
}


#ifdef CNS_ROOT
int copyfile_stage(input,output,mode,input_size,input_is_hpss)
	char *input;
	char *output;
	mode_t mode;
	u_signed64 input_size;
	int input_is_hpss;
{
	struct stgcat_entry stcp_input;
	int rc;
	char tmpbuf[21];

	/* In the mode stage_in + stage_wrt_deferred the cleaner instruction is to free and exit */
	/* if needed stcp_output */
	TRACE(2,"rfio","copyfile_stage: Instructing signal handler to free and exit");
	copyfile_stgcleanup_instruction = CLEANER_FREE|CLEANER_EXIT;

	/* If input is an HSM file, it is then a stage_in_hsm, with limitation to STAGE_MAX_HSMLENGTH characters on input */
	/* If not an HSM file, it is then a stage_in_disk, with limitation to (CA_MAXHOSTNAMELEN+MAXPATH) characters on input */
	/* The output in any case is an HSM file, limited to STAGE_MAX_HSMLENGTH characters */

	/* The stager is unfortunately limited to a lower limit for length */
	if (((rfio_HsmIf_IsHsmFile(input) || input_is_hpss) && (strlen(input) > STAGE_MAX_HSMLENGTH)) ||
		((! (rfio_HsmIf_IsHsmFile(input) || input_is_hpss)) && (strlen(input) > (CA_MAXHOSTNAMELEN+MAXPATH)))) {
		if (strlen(output) <= STAGE_MAX_HSMLENGTH) {
			/* We can swith to the old but unefficient method */
			return(-1);    
		} else {
			fprintf(stderr,"%s: %s\n", input, strerror(ENAMETOOLONG));
			return(USERR);    
		}
	}
	if (strlen(output) > STAGE_MAX_HSMLENGTH) {
		fprintf(stderr,"%s: %s\n", output, strerror(ENAMETOOLONG));
		return(USERR);    
	}

	/* The output is a CASTOR file - we need a signal handler because we call the stager */
#if ! defined(_WIN32)
	_rfio_signal (SIGHUP, copyfile_stgcleanup);
	_rfio_signal (SIGQUIT, copyfile_stgcleanup);
#endif
	_rfio_signal (SIGINT, copyfile_stgcleanup);
	_rfio_signal (SIGTERM, copyfile_stgcleanup);

	memset(&stcp_input, 0, sizeof(struct stgcat_entry));

	/* In STAGE mode, we always stage_in first the input */
	if ((maxsize > 0) && (inpfile_size > maxsize)) {
		input_size = maxsize;
		stcp_input.size = input_size;
	}
  get_input_retry:
	if (rfio_HsmIf_IsHsmFile(input) || input_is_hpss) {
		strcpy(stcp_input.u1.h.xfile,input);
		TRACE(2,"rfio","copyfile_stage: Calling stage_in_hsm of %s", input);
		rc = stage_in_hsm((u_signed64) STAGE_DEFERRED|STAGE_NOLINKCHECK,
						  (int) O_RDWR,            /* open flags */
						  (char *) NULL,           /* hostname */
						  (char *) NULL,           /* pooluser */
						  (int) 1,                 /* nstcp_input */
						  (struct stgcat_entry *) &stcp_input, /* stcp_input */
						  (int *) &nstcp_output,   /* nstcp_output */
						  (struct stgcat_entry **) &stcp_output, /* stcp_output */
						  (int) 0,                 /* nstpp_input */
						  (struct stgpath_entry *) NULL /* stpp_input */
			);
	} else {
		strcpy(stcp_input.u1.d.xfile,input);
		stcp_input.size = input_size;
		TRACE(2,"rfio","copyfile_stage: Calling stage_in_disk of %s", input);
		rc = stage_in_disk((u_signed64) STAGE_DEFERRED|STAGE_NOLINKCHECK,
						   (int) O_RDWR,            /* open flags */
						   (char *) NULL,           /* hostname */
						   (char *) NULL,           /* pooluser */
						   (int) 1,                 /* nstcp_input */
						   (struct stgcat_entry *) &stcp_input, /* stcp_input */
						   (int *) &nstcp_output,   /* nstcp_output */
						   (struct stgcat_entry **) &stcp_output, /* stcp_output */
						   (int) 0,                 /* nstpp_input */
						  (struct stgpath_entry *) NULL /* stpp_input */
			);
	}

	if (rc != 0) {
		/* Unless EBUSY or ENOSPC we do not retry, especially if the error is USERR */
		if ((serrno == EBUSY) || (serrno == ENOSPC)) {
			fprintf(stderr,"%s: %s - Retry in 60 seconds\n", input, sstrerror(serrno));
			sleep(60);
			goto get_input_retry;
		} else {
			fprintf(stderr,"%s: %s\n", input, sstrerror(serrno));
			if (stcp_output != NULL) free(stcp_output);
			stcp_output = NULL;
			return(rc_castor2shift(serrno));
		}
	}

	/* Stage_in is successful, we just send a stage_wrt deferred... */

	strcpy(stpp_input.upath, stcp_output->ipath);
	strcpy(stcp_input.u1.h.xfile,output);
	TRACE(2,"rfio","copyfile_stage: Calling deferred stage_wrt_hsm from %s to %s", stpp_input.upath, stcp_input.u1.h.xfile);
	rc = stage_wrt_hsm((u_signed64) STAGE_DEFERRED|STAGE_NOLINKCHECK, /* Deferred stagewrt */
					   (int) 0,                 /* open flags */
					   (char *) NULL,           /* hostname */
					   (char *) NULL,           /* pooluser */
					   (int) 1,                 /* nstcp_input */
					   (struct stgcat_entry *) &stcp_input, /* stcp_input */
					   (int *) NULL,            /* nstcp_output */
					   (struct stgcat_entry **) NULL, /* stcp_output */
					   (int) 1,                 /* nstpp_input */
					   (struct stgpath_entry *) &stpp_input /* stpp_input */
		);

	if (rc != 0) {
		fprintf(stderr,"%s: %s\n", output, sstrerror(serrno));
		if (stcp_output != NULL) free(stcp_output);
		stcp_output = NULL;
		return(rc_castor2shift(serrno));
	} else {
		if ((((maxsize > 0) && (inpfile_size > maxsize)) ? input_size : stcp_output->actual_size) > 0) {
			fprintf(stdout,"%s bytes ready for migration\n", ((maxsize > 0) && (inpfile_size > maxsize)) ?
					u64tostr((u_signed64) input_size, tmpbuf, 0) :            
					u64tostr((u_signed64) stcp_output->actual_size, tmpbuf, 0)
				);
		}
		if (stcp_output != NULL) free(stcp_output);
		stcp_output = NULL;
		return(OK);
	}
}



int copyfile_stager(input,output,mode,input_size,input_is_hpss)
	char *input;
	char *output;
	mode_t mode;
	u_signed64 input_size;
	int input_is_hpss;
{
        struct stage_io_fileresp *response;
        struct stage_io_fileresp *putResponse;
	char *requestId, *url;
	int rc;
	char tmpbuf[21];
        int* auxVal;
        char ** auxPoint;
        struct stage_options opts;
        opts.stage_host=NULL;
        opts.stage_port=0;
        opts.service_class=NULL;
        opts.stage_version=0;
        
        int ret=Cglobals_get(& tStageHostKey, (void**) &auxPoint,sizeof(void*));
	if(ret==0){
		opts.stage_host=*auxPoint;
	}
	ret=Cglobals_get(& tStagePortKey, (void**) &auxVal,sizeof(int));
        if(ret==0){
		opts.stage_port=*auxVal;
	}
	ret=Cglobals_get(& tCastorVersionKey, (void**) &auxVal,sizeof(int));
	if(ret==0){
		opts.stage_version=*auxVal;
        }
	ret=Cglobals_get(& tSvcClassKey, (void**) &auxPoint,sizeof(void*));
	if (ret==0){
                opts.service_class=*auxPoint;
	}
        ret= getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class,&opts.stage_version);

	TRACE(2,"rfio","copyfile_stager: Calling stage_get of %s", input);
	for(;;) {
	  rc = stage_open(NULL,
			  MOVER_PROTOCOL_RFIO,
			  input,
			  O_RDONLY,
			  0,
			  0,
			  &response,
			  NULL,
			  &opts);
	  
	  if (rc != 0) {
	    /* Unless EBUSY or ENOSPC we do not retry, especially if the error is USERR */
	    if ((serrno == EBUSY) || (serrno == ENOSPC)) {
	      fprintf(stderr,"%s: %s - Retry in 60 seconds\n", input, sstrerror(serrno));
	      sleep(60);
	      continue;
	    } else {
	      fprintf(stderr,"%s: %s\n", input, sstrerror(serrno));
	      if (response != NULL) free(response);
	      return(rc_castor2shift(serrno));
	    }
	  }
	  break;
	}  

	if (response == NULL) {
	  TRACE(3,"rfio","Received NULL response");
	  serrno = SEINTERNAL;
	  return -1;
	}
          
	if (response->errorCode != 0) {
	  TRACE(3,"rfio","stage_open error: %d/%s",
		response->errorCode, response->errorMessage);
	  serrno = response->errorCode;
	  return -1;
	}

	url = stage_geturl(response);
	if (url == 0) {
	  free(response);
	  return -1;
	}

	/* Stage get successfull*/
	TRACE(2,"rfio","copyfile_stager: Calling stage_put of %s", output);
	for(;;) {
	  rc = stage_open(NULL,
			  MOVER_PROTOCOL_RFIO,
			  output,
			  O_WRONLY | O_TRUNC,
			  mode,
			  input_size,
			  &putResponse,
			  NULL,
			  &opts);
	  
	  if (rc != 0) {
	    /* Unless EBUSY or ENOSPC we do not retry, especially if the error is USERR */
	    if ((serrno == EBUSY) || (serrno == ENOSPC)) {
	      fprintf(stderr,"%s: %s - Retry in 60 seconds\n", input, sstrerror(serrno));
	      sleep(60);
	      continue;
	    } else {
	      fprintf(stderr,"%s: %s\n", input, sstrerror(serrno));
	      if (response != NULL) free(response);
	      return(rc_castor2shift(serrno));
	    }
	  }
	  break;
	}  

	if (response == NULL) {
	  TRACE(3,"rfio","Received NULL response");
	  serrno = SEINTERNAL;
	  return -1;
	}
          
	if (response->errorCode != 0) {
	  TRACE(3,"rfio","stage_open error: %d/%s",
		response->errorCode, response->errorMessage);
	  serrno = response->errorCode;
	  return -1;
	}

	url = stage_geturl(response);
	if (url == 0) {
	  free(response);
	  return -1;
	}

	return(OK);
	
}


void copyfile_stglog(type, msg)
	int type;
	char *msg;
{
	if (type == MSG_ERR) {
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
	if (use_castor2_api()) {
	  /* BC 2005-03-14 */
	  /* Disabling this function for the new stager as stage_rm is not implemented yet */
	  exit(USERR);
	  return;
	}

	stage_kill(sig);
	if (copyfile_stgcleanup_instruction != CLEANER_NOOP) {
		if ((copyfile_stgcleanup_instruction & CLEANER_CLR) == CLEANER_CLR) {
			if (stcp_output != NULL) {
				if (stcp_output->ipath[0] != '\0') {
					TRACE(2,"rfio","copyfile_stgcleanup: Clearing %s", stcp_output->ipath);
					stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
				} else {
					TRACE(2,"rfio","copyfile_stgcleanup: stcp_output->ipath is empty, nothing to clear");
				}
			} else {
				TRACE(2,"rfio","copyfile_stgcleanup: stcp_output == NULL, nothing to clear");
			}
		}
		if ((copyfile_stgcleanup_instruction & CLEANER_CLR2) == CLEANER_CLR2) {
			if (stpp_input.upath[0] != '\0') {
				TRACE(2,"rfio","copyfile_stgcleanup: Clearing %s", stpp_input.upath);
				stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stpp_input.upath);
			} else {
				TRACE(2,"rfio","copyfile_stgcleanup: stpp_input.upath is empty, nothing to clear");
			}
		}
		if ((copyfile_stgcleanup_instruction & CLEANER_FREE) == CLEANER_FREE) {
			if (stcp_output != NULL) {
				TRACE(2,"rfio","copyfile_stgcleanup: Freeing stcp_output");
				free(stcp_output);
			} else {
				TRACE(2,"rfio","copyfile_stgcleanup: stcp_output == NULL, not freeing stcp_output");
			}
			stcp_output = NULL;
		}
		if ((copyfile_stgcleanup_instruction & CLEANER_EXIT) == CLEANER_EXIT) {
			TRACE(2,"rfio","copyfile_stgcleanup: Exit with status USERR");
			exit(USERR);
		}
	}
}

int copyfile_stage_from_local(input,output,mode,input_size,input_is_hpss)
	char *input;
	char *output;
	mode_t mode;
	u_signed64 input_size;
	int input_is_hpss;
{
	struct stgcat_entry stcp_input;
	int rc;
	char tmpbuf[21];
	int v;
	int fd1, fd2;
	char *ifce ;
	char ifce1[8] ;
	char ifce2[8] ;
	u_signed64 size;
	int do_sleep = 0;
#if defined(_WIN32)
	int binmode = O_BINARY;
#else
	int binmode = 0;
#endif
	extern char * getifnam() ;
	int rc2;

	/* In stage_out+stage_updc mode the first cleaner instructions is to clean, free and exit */
	TRACE(2,"rfio","copyfile_stage_from_local: Instructing signal handler to clear, free and exit");
	copyfile_stgcleanup_instruction = CLEANER_CLR|CLEANER_FREE|CLEANER_EXIT;

	/* The stager is unfortunately limited to a lower limit for length */
	if (strlen(output) > STAGE_MAX_HSMLENGTH) {
		fprintf(stderr,"%s: %s\n", output, strerror(ENAMETOOLONG));
		return(USERR);    
	}

	/* The output is a CASTOR file - we need a signal handler because we call the stager */
#if ! defined(_WIN32)
	_rfio_signal (SIGHUP, copyfile_stgcleanup);
	_rfio_signal (SIGQUIT, copyfile_stgcleanup);
#endif
	_rfio_signal (SIGINT, copyfile_stgcleanup);
	_rfio_signal (SIGTERM, copyfile_stgcleanup);

	memset(&stcp_input, 0, sizeof(struct stgcat_entry));

	/* In STAGE mode from local disk, we always stage_out first for the output */
  get_output_retry:
	strcpy(stcp_input.u1.h.xfile,output);
	stcp_input.size = input_size;
	TRACE(2,"rfio","copyfile_stage_from_local: Calling stage_out_hsm on %s", output);
	rc = stage_out_hsm((u_signed64) STAGE_NOLINKCHECK,
					   (int) 0,                 /* open flags */
					   (mode_t) mode,           /* open mode (c.f. also umask) */
					   (char *) NULL,           /* hostname */
					   (char *) NULL,           /* pooluser */
					   (int) 1,                 /* nstcp_input */
					   (struct stgcat_entry *) &stcp_input, /* stcp_input */
					   (int *) &nstcp_output,   /* nstcp_output */
					   (struct stgcat_entry **) &stcp_output, /* stcp_output */
					   (int) 0,                 /* nstpp_input */
					   (struct stgpath_entry *) NULL /* stpp_input */
		);

	if (do_sleep) {
		fprintf(stderr,"%s: Retry in 60 seconds\n", output);
		sleep(60);
	}

	if (rc != 0) {
		/* Unless EBUSY or ENOSPC we do not retry, especially if the error is USERR */
		if ((serrno == EBUSY) || (serrno == ENOSPC)) {
			fprintf(stderr,"%s: %s - Retry in 60 seconds\n", output, sstrerror(serrno));
			sleep(60);
			goto get_output_retry;
		} else {
			fprintf(stderr,"%s: %s\n", output, sstrerror(serrno));
			if (stcp_output != NULL) {
				if (stcp_output->ipath != '\0') {
					TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
					stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
				} else {
					TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
				}
				free(stcp_output);
				stcp_output = NULL;
			}
			return(rc_castor2shift(serrno));
		}
	}

  copy_retry:
	/* Stage_out is successful, we do the normal RFIO copyfile disk->disk stuff */
	/* given that the output file is now in stcp_output->ipath */

	/* By definition, this routine is execute when input is local and output is CASTOR */
	/* so there is NO notion of HPSS worker there */

	/* The following is merely a copy of the original source code, simplified for disk->disk */

	/* Activate new transfer mode for source file */
	if (! v2) {
		v = RFIO_STREAM;
		rfiosetopt(RFIO_READOPT,&v,4); 
	}

	serrno = rfio_errno = 0 ;
	if (strcmp(input,"-") == 0) {
		fd1 = fileno(stdin);
	} else {
		fd1 = rfio_open64(input,O_RDONLY|binmode ,0644);
	}
	if (fd1 < 0) {
		int c;

		if (serrno) {
			rfio_perror(input);
			c = SYERR;
		} else {
			switch (rfio_errno) {
			case EACCES:
			case EISDIR:
			case ENOENT:
			case EPERM :
				rfio_perror(input);
				c = USERR;
				break ;
			case 0:
				switch(errno) {
				case EACCES:
				case EISDIR:
				case ENOENT:
				case EPERM :
					rfio_perror(input);
					c = USERR;
					break;
				default:
					rfio_perror(input);
					c = SYERR;
				}
				break;
			default:
				rfio_perror(input);
				c = SYERR;
			}
		}
#if defined(_WIN32)
		WSACleanup();
#endif
		if (stcp_output != NULL) {
			if (stcp_output->ipath[0] != '\0') {
				TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
				stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
			} else {
				TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
			}
			free(stcp_output);
			stcp_output = NULL;
		}
		exit (c);
	}
	if ( (ifce=getifnam(fd1))==NULL ) 
		strcpy(ifce1,"local");
	else
		strcpy(ifce1,ifce) ;

	/* Activate new transfer mode for target file */
	if (! v2) { 
		v = RFIO_STREAM;
		rfiosetopt(RFIO_READOPT,&v,4); 
	}

	serrno = rfio_errno = 0 ;
	fd2 = rfio_open64(stcp_output->ipath , O_WRONLY|O_CREAT|O_TRUNC|binmode ,mode);
	if (fd2 < 0) {
		int c;

		if (serrno) {
			rfio_perror(stcp_output->ipath);
			c = SYERR;
		} else {
			switch (rfio_errno) {
			case EACCES:
			case EISDIR:
			case ENOENT:
			case EPERM :
				rfio_perror(stcp_output->ipath); 
				c = USERR;
				break;
			case 0:
				switch(errno) {
				case EACCES:
				case EISDIR:
				case ENOENT:
				case EPERM :
					rfio_perror(stcp_output->ipath); 
					c = USERR;
					break;
				default:
					rfio_perror(stcp_output->ipath);
					c = SYERR;
				}
				break;
			default:
				rfio_perror(stcp_output->ipath);
				c = SYERR;
			}
		}
#if defined(_WIN32)
		WSACleanup();
#endif
		if (stcp_output != NULL) {
			if (stcp_output->ipath[0] != '\0') {
				TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
				stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
			} else {
				TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
			}
			free(stcp_output);
			stcp_output = NULL;
		}
		exit (c);
	}
	/*
	 * Get interfaces names
	 */
	if ( (ifce=getifnam(fd2))==NULL ) 
		strcpy(ifce2,"local");
	else
		strcpy(ifce2,ifce) ;

	{
		char *name = stcp_output->ipath;
		int n;
		mode_t mode;
		int m = -1;
		struct stat64 sbuf;
		char  *p;
		int bufsize = TRANSFER_UNIT;
		char *cpbuf;
		extern char *getenv();		/* External declaration */
		u_signed64 total_bytes = 0;

		size = 0;

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
#if defined(_WIN32)
			WSACleanup();
#endif
			if (stcp_output != NULL) {
				if (stcp_output->ipath[0] != '\0') {
					TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
					stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
				} else {
					TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
				}
				free(stcp_output);
				stcp_output = NULL;
			}
			exit (SYERR);
		}

		do {
			if (have_maxsize != -1) {
				if ((maxsize - total_bytes) < bufsize)
					bufsize = maxsize - total_bytes;
			}
			serrno = rfio_errno = 0;
			n = rfio_read(fd1, cpbuf, bufsize);
			if (n > 0) {
				int count = 0;

				total_bytes += n;
				size += n;
				serrno = rfio_errno = 0;
				while (count != n && (m = rfio_write(fd2, cpbuf+count, n-count)) > 0)
					count += m;
				if (m < 0) {
					int save_serrno = rfio_serrno();
					/* Write failed.  Don't keep truncated regular file. */
					rfio_perror("rfcp");
					if (rfio_close(fd2) < 0) rfio_perror("close target");
					if (rfio_close(fd1) < 0) rfio_perror("close source");
					if (save_serrno == ENOSPC) {
						/* We say to the stager that we had an ENOSPC and we want to have a garbage collector */
						/* running - we will be waiting for a new path */
					  enospc_retry:
						memset(&stpp_input, 0, sizeof(struct stgpath_entry));
						strcpy(stpp_input.upath, stcp_output[0].ipath);
						free(stcp_output);
						stcp_output = NULL;
						TRACE(2,"rfio","copyfile_stage_from_local: Notifying ENOSPC with %s", stpp_input.upath);
						rc = stage_updc((u_signed64) STAGE_NOLINKCHECK,
										(char *) NULL,           /* hostname */
										(char *) NULL,           /* pooluser */
										(int) ENOSPC,            /* status */
										(int *) &nstcp_output,   /* nstcp_output */
										(struct stgcat_entry **) &stcp_output, /* stcp_output */
										(int) 1,                 /* nstpp_input */
										(struct stgpath_entry *) &stpp_input /* stpp_input */
							);
						if (rc != 0) {
							/* Unless ENOSPC again, we do not retry, especially if the error is USERR */
							if (serrno == ENOSPC && (strcmp(input,"-") != 0)) {
								/* We will not retry if input is stdin: everything is lost... */
								fprintf(stderr,"%s: %s - Retry in 60 seconds\n", output, sstrerror(serrno));
								sleep(60);
								goto enospc_retry;
							} else {
								fprintf(stderr,"%s: %s\n", output, sstrerror(serrno));
								if (stcp_output != NULL) {
									if (stcp_output->ipath != '\0') {
										if (serrno != EACCES) {
											TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
											stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
										} else {
											TRACE(2,"rfio","copyfile_stage_from_local: EACCES - Not clearing %s", stcp_output->ipath);
										}
									} else {
										TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
									}
									free(stcp_output);
									stcp_output = NULL;
								}
								free(cpbuf);
								return(rc_castor2shift(serrno));
							}
						} else {
							/* Space have been allocated again - we retry the copy */
							fprintf(stderr,"%s: %s - Retry in 60 seconds\n", output, sstrerror(save_serrno));
							sleep(60);
							free(cpbuf);
							goto copy_retry;
						}
					}
					if (stcp_output != NULL) {
						if (stcp_output->ipath[0] != '\0') {
							TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
							stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
						} else {
							TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
						}
						free(stcp_output);
						stcp_output = NULL;
					}
					rfio_perror("rfcp");
					rfio_stat64(name, &sbuf);	/* check for special files */
					mode = sbuf.st_mode & S_IFMT;
					if (mode == S_IFREG) rfio_unlink(name);
#if defined(_WIN32)
					WSACleanup();
#endif
					free(cpbuf);
					exit(SYERR);
				}
			} else {
				if (n < 0) {
					rfio_perror("cp: read error");
					break;
				}
			}
		} while ((total_bytes != maxsize) && (n > 0));
		serrno = rfio_errno = 0;
		if (rfio_close(fd1) < 0) {
			rfio_perror("close source");
#if defined(_WIN32)
			WSACleanup();
#endif
			if (stcp_output != NULL) {
				if (stcp_output->ipath[0] != '\0') {
					TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
					stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
				} else {
					TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
				}
				free(stcp_output);
				stcp_output = NULL;
			}
			free(cpbuf);
			exit(SYERR);
		}

		serrno = rfio_errno = 0;
		if (rfio_close(fd2) < 0) {
			int save_errno = rfio_serrno();
			rfio_perror("close target");
			
			rfio_stat64(name, &sbuf);	/* check for special files */
			mode = sbuf.st_mode & S_IFMT;
			if (mode == S_IFREG) 
				rfio_unlink(name);
#if defined(_WIN32)
			WSACleanup();
#endif
			if (stcp_output != NULL) {
				/* Suppose we have ENOSPC; the output is an HSM file - we can retry */
				if (save_errno == ENOSPC && (strcmp(input,"-") != 0)) {
					/* The enospc_retry will do the free(cpbuf) it it decides to return */
					goto enospc_retry;
				}
				if (stcp_output->ipath != '\0') {
					TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stcp_output->ipath);
					stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stcp_output->ipath);
				} else {
					TRACE(2,"rfio","copyfile_stage_from_local: stcp_output->ipath is empty, nothing to clear");
				}
				free(stcp_output);
				stcp_output = NULL;
			}
			free(cpbuf);
			exit(SYERR);
		}
		/* size contains the number of bytes effectively copied */
		free(cpbuf);
	}

	if (size > 0) {
		if (have_maxsize < 0) {
			rc2 = (input_size == size ? OK : ((strcmp(input,"-") == 0) ? OK : SYERR));
		} else {
			if (maxsize < input_size) { /* When input is "-", input_size is zero */
				rc2 = (maxsize == size ? OK : SYERR);
			} else {
				rc2 = (input_size == size ? OK : ((strcmp(input,"-") == 0) ? OK : SYERR));
			}
		}
	} else    {
		fprintf(stdout,"%d bytes transferred !!\n",(int) size);
		rc2 = (input_size == 0 ? OK : SYERR);
	}

	if (rc2 != OK) {
		return(rc2);
	}

	{
		/* Copy was successful - we tell so to the stager */
		memset(&stpp_input, 0, sizeof(struct stgpath_entry));
		strcpy(stpp_input.upath, stcp_output[0].ipath);
		free(stcp_output);
		stcp_output = NULL;

		/* In stage_out+stage_updc mode the second cleaner instructions is to clean and exit */
		/* but on stpp_input for the ipath */
		copyfile_stgcleanup_instruction = CLEANER_CLR2|CLEANER_EXIT;

		rc2 = stage_updc((u_signed64) STAGE_NOLINKCHECK,
						 (char *) NULL,           /* hostname */
						 (char *) NULL,           /* pooluser */
						 (int) 0,                 /* status */
						 (int *) NULL,          /* nstcp_output */
						 (struct stgcat_entry **) NULL, /* stcp_output */
						 (int) 1,                 /* nstpp_input */
						 (struct stgpath_entry *) &stpp_input /* stpp_input */
			);
		if (rc2 != 0) {
			if (stpp_input.upath[0] != '\0') {
				if (serrno != EACCES) {
					TRACE(2,"rfio","copyfile_stage_from_local: Clearing %s", stpp_input.upath);
					stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, stpp_input.upath);
				} else {
					TRACE(2,"rfio","copyfile_stage_from_local: EACCES - Not clearing %s", stpp_input.upath);
				}
			} else {
				TRACE(2,"rfio","copyfile_stage_from_local: stpp_input.upath is empty, nothing to clear");
			}
			fprintf(stderr,"%s: %s\n", output, sstrerror(serrno));
			return(rc_castor2shift(serrno));
		}
	}

	if (size > 0) {
		fprintf(stdout,"%s bytes ready for migration\n",u64tostr((u_signed64) size, tmpbuf, 0));
	}

	return(OK);

}
#endif /* CNS_ROOT */

int cleanpath(output,input)
	char *output;
	char *input;
{
	char *dup, *p;

	if (use_castor2_api()) {
	  /* BC 2005-01-05 */
	  /* Disabling this function as it clashes with the TURL parsing: rfio://.... */
	  return 0;
	}


	if (input == NULL) {
		serrno = EINVAL;
		return(-1);
	}

	if ((p = dup = strdup(input)) == NULL) {
		serrno = SEINTERNAL;
		return(-1);
	}

	/*
	 * Removes multiple occurences of '/' character
	 */

	while ((p = strchr(p,'/')) != NULL) {
		/* 'p' points to a '/' */
		size_t psize;
		if ((psize = strspn(p++,"/")) > 1) {
			/* And there is 'psize' of them */
			--psize;
			/* We copy the rest of the string keeping the first one */
			memmove(p,&(p[psize]),strlen(&(p[psize])) + 1);
		} else {
			++p;
		}
	}
	if (output != NULL) {
		/* User has to provide enough space for it */
		/* We do memmove() because output and input can be same area */
		memmove(output,dup,strlen(dup)+1);
	}
	free(dup);
	return(0);
}

#ifndef _WIN32
Sigfunc *_rfio_signal(signo, func)
	int signo;
	Sigfunc *func;
{
	struct sigaction	act, oact;
	int n = 0;

	act.sa_handler = func;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
		act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
	} else {
#ifdef	SA_RESTART
		act.sa_flags |= SA_RESTART;		/* SVR4, 44BSD */
#endif
	}
	n = sigaction(signo, &act, &oact);
	if (n < 0) {
		return(SIG_ERR);
	}
	switch (signo) {
#if ! defined(_WIN32)
	case SIGHUP:
		TRACE(2,"rfio","_rfio_signal: Trapping SIGHUP");
		break;
	case SIGQUIT:
		TRACE(2,"rfio","_rfio_signal: Trapping SIGQUIT");
		break;
#endif
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
#endif /* #ifndef _WIN32 */
