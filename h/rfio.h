/*
 * $RCSfile: rfio.h,v $ $Revision: 1.9 $ $Date: 2000/05/29 14:17:39 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* 
 * rfio.h     -  Remote File Access internal declarations
 */

#ifndef _RFIO_H_INCLUDED_
#define _RFIO_H_INCLUDED_

#include <stdio.h>              /* standard Input/Output                */
#if !defined(apollo) && ! defined(_WIN32)
#include <unistd.h>		/* Standardized definitions		*/
#endif	/* !apollo */

#if !defined(_WIN32)
#include <sys/types.h>          /* standard data types                  */
#include <dirent.h>             /* standard directory definitions       */
#endif /* _WIN32 */

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
#if !defined(apollo)
#include <string.h>             /* string handling routines             */
#else
#include <strings.h>            /* strings operations                   */
#endif  /* ! apollo     */
#if ! defined(_WIN32)
#include <sys/file.h>           /* define L_XTND, L_INCR and L_SET      */
#endif
#include <sys/stat.h>           /* file status definitions              */
#if defined(_WIN32)
#include <time.h>
#else
#include <sys/time.h>           /* for time declarations                */
#endif
#if defined(HPSS) /* To avoid clash with dce    */
#include "../h/marshall.h"
#else /*  HPSS */
#include <marshall.h>           /* marshalling macros and definitions   */
#endif /* HPSS */
#include <serrno.h>             /* special error numbers                */
#include <rfio_errno.h>
#include <trace.h>              /* tracing definitions                  */

#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>         /*some of the defs are in this file     */
#endif /* AIX */

#include <net.h>                        /* networking specifics         */

#include <socket_timeout.h>             /* socket timeout routines */

#endif /* RFIO_KERNEL */

#include <osdep.h>

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

#if !defined(_WIN32)
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
#endif  /* _WIN32 */

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

/*
 * Define structure for preseek requests.
 */
#if !defined(SOLARIS) && !(defined(__osf__) && defined(__alpha)) && !defined(HPUX1010) && !defined(IRIX6) && !defined(linux) && !defined(AIX42) && !defined(IRIX5)
struct iovec {
	int iov_base ;
	int iov_len ; 
} ;
#endif

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
#define feof            rfio_feof
#define ferror          rfio_ferror
#define fopen           rfio_fopen
#define fclose          rfio_fclose
#define fflush          rfio_fflush
#define fwrite          rfio_fwrite
#define fread           rfio_fread
#define fseek           rfio_fseek
/* The following clashes with struct stat in sys/stat.h */
/* #define stat            rfio_stat                    */
#define open            rfio_open
#define close           rfio_close
#define write           rfio_write
#define read            rfio_read
#define perror          rfio_perror
#define fstat           rfio_fstat
#define lseek           rfio_lseek
#define unlink          rfio_unlink
#define symlink         rfio_symlink
#define mkdir           rfio_mkdir
#define rmdir           rfio_rmdir
#define chmod           rfio_chmod
#if !defined(_WIN32)
#define opendir         rfio_opendir
#define readdir         rfio_readdir
#define closedir        rfio_closedir
#if defined(rewinddir)
#undef rewinddir
#endif /* rewinddir */
#define rewinddir       rfio_rewinddir
#endif /* _WIN32 */
#endif /* RFIO_KERNEL */

#endif /* _RFIO_H_INCLUDED_ */

