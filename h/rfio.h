/*
 * $RCSfile: rfio.h,v $ $Revision: 1.25 $ $Date: 2002/08/28 05:40:44 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* 
 * rfio.h     -  Remote File Access internal declarations
 */

#ifndef _RFIO_H_INCLUDED_
#define _RFIO_H_INCLUDED_

#ifdef CHECKI
#undef CHECKI
#endif
#define CHECKI       5       /* max interval to check for work to be done */

#include <stdio.h>              /* standard Input/Output                */
#include <sys/types.h>          /* standard data types                  */

#if !defined(_WIN32)
#include <unistd.h>		/* Standardized definitions		*/
#else /* _WIN32 */
#ifndef STATBITS_H
#include <statbits.h>           /* File access macros for WIN32         */
#endif
#endif /* _WIN32 */

#ifndef _DIRENT_WIN32_H
#include <dirent.h>             /* standard directory definitions       */
#endif

/*
 * Common includes needed by internal (kernel) routines.
 */
#if defined(RFIO_KERNEL)
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <sys/socket.h>         /* socket interface                     */
#include <netinet/in.h>         /* internet types                       */
#include <netdb.h>              /* network database                     */
#endif
#include <errno.h>              /* standard error numbers               */
#include <string.h>             /* string handling routines             */
#if ! defined(_WIN32)
#include <sys/file.h>           /* define L_XTND, L_INCR and L_SET      */
#endif
#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>         /*some of the defs are in this file     */
#endif /* AIX */
#include <time.h>
#if defined(HPSS) /* To avoid clash with dce    */
#include "../h/marshall.h"
#else /*  HPSS */
#include <marshall.h>           /* marshalling macros and definitions   */
#endif /* HPSS */
#include <serrno.h>             /* special error numbers                */
#include <trace.h>              /* tracing definitions                  */

#include <net.h>                        /* networking specifics         */

#include <socket_timeout.h>             /* socket timeout routines */

#endif /* RFIO_KERNEL */

#ifndef _RFIO_CONSTANTS_H_INCLUDED_
#include <rfio_constants.h>
#endif /* _RFIO_CONSTANTS_H_INCLUDED_ */

#if !defined(_WIN32)
#ifndef min
#define min(a,b)      (((a)<(b)) ? (a):(b))
#endif
#endif /* _WIN32 */

/*
 * Internal (kernel) structures
 */
#if defined(RFIO_KERNEL)
struct iobuf {                  /* I/O buffer data structure            */
	char    *base;          /* I/O buffer base pointer              */
	unsigned int hsize;     /* I/O buffer header size               */
	unsigned int dsize;     /* I/O buffer data  size                */
	char    *ptr;           /* I/O buffer current pointer           */
	int count;              /* I/O buffer count                     */
} ;

typedef struct {
	FILE    fp;             /* File pointer mapping, must be first !*/
	int     magic;          /* Magic number                         */
	int     s;              /* Socket descriptor in use             */
	int     uid;            /* Requestor's uid                      */
	int     gid;            /* Requestor's gid                      */
	int     umask;          /* Requestor's umask                    */
	char    *mode;          /* Access mode indicator                */
	char    *spec;          /* File path                            */
	char    host[RESHOSTNAMELEN];
			      	/* Host name                            */
	int     bufsize;        /* socket buffer size                   */
	int     ftype;          /* File type (Fortran, C)               */
	int     binary;         /* Binary on non-binary mode            */
	int	offset;		/* offset in the file			*/
	/* 
	 * FORTRAN characteristics      
	 */
	int     unit;           /* Fortran assigned unit                */
	int     access;         /* Access method                        */
	int     format;         /* File format                          */
	int     recl;           /* File record length                   */
	int     blank;          /* File blank padding                   */
	int     opnopt;         /* File open options                    */
	/* 
	 * Buffered operations 
	 */
	struct	iobuf _iobuf ; 	/* I/O buffer                           */
	int 	lseekhow ;	/* Buffered lseek type ( -1 if none )	*/
	int	lseekoff ;	/* Buffered lseek offset		*/
	int	socset ;	/* Socket options set (bol)		*/
	/*
	 * Preseek operations.
	 * field 'preseek' takes the following values:
	 * 	0 not in preseek mode.
	 * 	1 preseek mode.
	 * 	2 dealing with the last preseek message.
	 */
	int	preseek  ;	/* Preseek mode ? 			*/
	int	nbrecord ;	/* Nb of records in the buffer		*/
	/*
	 * Read ahead operations
	 */
	int	eof ;		/* End of file has been reached		*/
	int	ahead ;		/* Are the read()'s read()'s ahead ?	*/
	int	readissued ;	/* Has the first read ahead been issued	*/
	/*
	 * Remote site operations
	 */
	int mapping;		/* Is mapping required ?		*/
	int passwd; 		/* passwd required if from rem. site    */

	/* RFIO version 3 writes */
	int first_write;
	int byte_written_to_network;

	/* RFIO version 3 reads */
	int first_read;
	int byte_read_from_network;
	int eof_received;
	int filesize;

	/* RFIO Version 3 used ? */
	int version3;   

	FILE	*fp_save;	/* save ptr to FILE struct returned by popen */
} RFILE ;

#define ioerrno(x) *(int *)(x->_iobuf.base + LONGSIZE)
#define iostat(x)  *(int *)(x->_iobuf.base + 2*LONGSIZE)
#define iodata(x)   (x->_iobuf.base + x->_iobuf.hsize)

typedef struct {
        struct __RFIO_DIR {
	  int    dd_fd;
	  int    dd_loc;
	  int    dd_size;
	  char * dd_buf;
	} dp;                   /* Fake DIR pointer mapping             */

        int     magic;          /* Magic number                         */
        int     s;              /* Socket descriptor in use             */
        int     uid;            /* Requestor's uid                      */
        int     gid;            /* Requestor's gid                      */
        int     offset;         /* Directory offset                     */
	char    host[RESHOSTNAMELEN];
			      	/* Host name                            */
	/*
	 * Remote site operations
	 */
	int mapping;		/* Is mapping required ?		*/
	int passwd; 		/* passwd required if from rem. site    */
} RDIR;

#if defined(_WIN32)
typedef RDIR DIR;
#endif /* _WIN32 */

/*
 * Define RFIO statistic structure
 */
struct rfiostat	{
	long readop ;                   /* read() count            	*/
	long aheadop;			/* readahead() count 		*/ 
	long writop ;                   /* write() count                */
	long flusop ;                   /* flush() count                */
	long statop ;                   /* stat() count                 */
	long seekop ;                   /* seek() count                 */
	long presop ;			/* preseek() count		*/
        long mkdiop ;                   /* mkdir() count                */
	long renaop ;			/* rename() count		*/
	long lockop ;			/* lockf() count		*/
	long rnbr ;                     /* byte read count              */
	long wnbr ;                     /* byte written count           */
} ;
#endif /* RFIO_KERNEL */

#ifndef _RFIO_API_H_INCLUDED_
#include <rfio_api.h>
#endif /* _RFIO_API_H_INCLUDED_ */

#if !defined(RFIO_KERNEL)
#if defined(feof)
#undef  feof
#endif /* feof */
#if defined(ferror)
#undef  ferror
#endif /* ferror */
#if defined(getc)
#undef  getc
#endif /* getc */
#define feof            rfio_feof
#define ferror          rfio_ferror
#define fopen           rfio_fopen
#define fclose          rfio_fclose
#define fflush          rfio_fflush
#define fwrite          rfio_fwrite
#define fread           rfio_fread
#define fseek           rfio_fseek
#define ftell           rfio_ftell
#define stat(X,Y)       rfio_stat(X,Y)
#define chdir           rfio_chdir
#define getc            rfio_getc
#define getcwd          rfio_getcwd
#define open            rfio_open
#define close           rfio_close
#define write           rfio_write
#define read            rfio_read
#define perror          rfio_perror
#define fstat           rfio_fstat
#define lstat           rfio_lstat
#define lseek           rfio_lseek
#define rename          rfio_rename
#define unlink          rfio_unlink
#define symlink         rfio_symlink
#define mkdir           rfio_mkdir
#define rmdir           rfio_rmdir
#define chmod           rfio_chmod
#define opendir         rfio_opendir
#define readdir         rfio_readdir
#define closedir        rfio_closedir
#if defined(rewinddir)
#undef rewinddir
#endif /* rewinddir */
#define rewinddir       rfio_rewinddir
#endif /* RFIO_KERNEL */

#endif /* _RFIO_H_INCLUDED_ */

