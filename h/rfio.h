/*
 * $Id: rfio.h,v 1.6 2000/02/15 15:29:30 fabien Exp $
 *
 * $Log: rfio.h,v $
 * Revision 1.6  2000/02/15 15:29:30  fabien
 * Added support for initial lseek in RFIO version 3 protocol
 *
 * Revision 1.5  1999/12/14 14:35:06  jdurand
 * Added IRIX5 in the negative test to know if struct iovec is to be defined
 *
 * Revision 1.4  1999-12-09 14:46:19+01  jdurand
 * Fixed copyright and sccsid
 *
 * Revision 1.3  1999-12-09 10:49:47+01  baran
 * Included rfio_errno.h.  Removed declaration of rfio_errno.
 *
 * Revision 1.2  1999/07/20 20:07:22  jdurand
 * Added -lnsl for Linux
 *
 */

/*
 * @(#)rfio.h	3.51 01/08/99 CERN IT-PDP/IP Frederic Hemmer
 */

/*
 * Copyright (C) 1990-1998 by CERN/IT/PDP/IP
 * All rights reserved
 */

/* rfio.h       Remote File Access routines header                      */

#ifndef _RFIO_H_INCLUDED_
#define _RFIO_H_INCLUDED_

#define RFIO_CTRL_TIMEOUT 10
#define RFIO_DATA_TIMEOUT 300

#include <stdio.h>              /* standard Input/Output                */
#if !defined(apollo) && ! defined(_WIN32)
#include <unistd.h>		/* Standardized definitions		*/
#endif	/* !apollo */

#if !defined(_WIN32)
#include <sys/types.h>          /* standard data types                  */
#include <dirent.h>             /* standard directory definitions       */
#endif /* _WIN32 */

#if !defined(_WIN32)
#ifndef min
#define min(a,b)      (((a)<(b)) ? (a):(b))
#endif
#endif /* _WIN32 */

#if defined(_WIN32)
#define 	MAXHOSTNAMELEN	64
#endif

/*
 * Remote file I/O C routines
 */

/* extern int      rfio_errno;  */    /* global rfio error number             */
extern int      rfio_fclose();  /* RFIO's fclose()                      */
extern int	rfio_fflush();	/* RFIO's fflush()			*/
extern int      rfio_fwrite();  /* RFIO's fwrite()                      */
extern int      rfio_fread();   /* RFIO's fread()                       */
extern int      rfio_stat();    /* RFIO's stat()                        */
extern int      rfio_open();    /* RFIO's open()                        */
extern int      rfio_close();   /* RFIO's close()                       */
extern int      rfio_write();   /* RFIO's write()                       */
extern int      rfio_read();    /* RFIO's read()                        */
extern void     rfio_perror();  /* RFIO's perror()                      */
extern int      rfio_fstat();   /* RFIO's fstat()                       */
extern int      rfio_lseek();   /* RFIO's lseek()                       */
extern int      rfio_preseek(); /* RFIO's preseek()                     */
extern int 	rfstatfs() ;	/* RFIO's local statfs() 		*/
extern int 	rfio_statfs() ; /* RFIO's statfs()			*/
extern int      rfio_fwrite();  /* RFIO's pwrite()                      */
extern int      rfio_fread();   /* RFIO's pread()                       */
extern int	rfio_fseek();	/* RFIO's fseek()			*/
extern int      rfio_pclose();  /* RFIO's pclose()  			*/
extern char *	rfio_serror();	/* RFIO's sys_errlist string retriever	*/
extern int	rfio_mkdir();	/* RFIO's mkdir()			*/
extern int	rfio_rmdir();   /* RFIO's rmdir()			*/
extern int	rfio_rename();	/* RFIO's rename()			*/
extern int	rfio_lockf();	/* RFIO's flock()			*/
extern int	rfio_chmod();	/* RFIO's chmod()			*/
#if !defined(_WIN32)
extern struct dirent *rfio_readdir(); 
                                /* RFIO's readdir()                     */
extern int      rfio_rewinddir(); 
                                /* RFIO's rewinddir()                   */
extern int      rfio_closedir();/* RFIO's closedir()                    */
#endif /* _WIN32 */

/*
 * RFIO options.
 */
#define RFIO_READOPT 	1
#define RFIO_NETOPT 	2
#define RFIO_NETRETRYOPT 3
#define RFIO_CONNECTOPT	4	


#define RFIO_READBUF	1
#define	RFIO_READAHEAD	2
#define RFIO_STREAM     16
#define RFIO_STREAM_DONE 32

#define RFIO_NONET	1
#define RFIO_NET	2
#define RFIO_RETRYIT  	1
#define RFIO_NOTIME2RETRY 2
#define LLTM 1
#define LLM  2
#define RDLINKS         0
#define NORDLINKS       1
#define RFIO_NOLOCAL	0
#define RFIO_FORCELOCAL 1

/*
 * Constants
 */ 

#define RESHOSTNAMELEN 20
#define MAXMCON 10

/*
 * RFIO structure for file system status
 */

struct rfstatfs {
        long totblks  ;      /* Total number of blocks       */
        long freeblks ;      /* Number of free blocks        */
        long bsize    ;      /* Block size                   */
        long totnods  ;      /* Total number of inodes       */
        long freenods ;      /* Number of free inodes        */
} ;

#ifdef RFIO_KERNEL

/*
 * Request message size.
 * !!! WARNING !!!
 * HAS TO BE SMALLER THAN 256 BYTES BECAUSE OF ULTRANET !!!!
 * !!! WARNING !!!
 */
#define RQSTSIZE	3*(LONGSIZE+WORDSIZE)


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
/*
 * Declaring here rfio_popen(0 as it refers to RFILE
 */
extern RFILE *	rfio_popen() ;	/* RFIO's popen() 			*/

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
/*
 * Declaring here rfio_opendir() as it refers to RDIR
 */
extern RDIR *   rfio_opendir(); /* RFIO's opendir()                     */
#endif  /* _WIN32 */


#define LOCAL_IO        1       /* use local I/O for local file syntax  */

/*
 * Include files
 */

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
#endif	/* ! apollo	*/
#if ! defined(_WIN32)
#include <sys/file.h>		/* define L_XTND, L_INCR and L_SET	*/
#endif
#include <sys/stat.h>           /* file status definitions              */
#if defined(_WIN32)
#include <time.h>
#else
#include <sys/time.h>		/* for time declarations		*/
#endif
#if defined(RFIO_KERNEL) && defined(HPSS) /* To avoid clash with dce	*/
#include "../h/marshall.h"
#else /* RFIO_KERNEL && HPSS */
#include <marshall.h>           /* marshalling macros and definitions   */
#endif /* RFIO_KERNEL && HPSS */
#include <serrno.h>             /* special error numbers                */
#include <rfio_errno.h>
#include <trace.h>              /* tracing definitions                  */

#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>		/*some of the defs are in this file     */
#endif /* AIX */

#include <net.h>                        /* networking specifics         */

#include <socket_timeout.h>             /* socket timeout routines */

#define RFIO_MAGIC 	0X0100       /* Remote File I/O magic number         */
#define B_RFIO_MAGIC 	0X0200       /* Remote File I/O magic number         */

/* FH to be in shift.conf, only default should be here */
/*efine IOBUFFERSIZE 128*1024 *//* Buffered read buffer                 */
#define DEFIOBUFSIZE 128*1024   /* Default IO buffer size               */

#define DEFRETRYCNT     38880   /* Default retry count 38880*60=1 week  */
#define DEFRETRYINT     60      /* Default retry interval               */

#define _NFFILE 10              /* Maximum simultaneously open files    */
#define MAXFILENAMSIZE 1024     /* Maximum length of a file path name   */
#define MAXFILEMODSIZE  256     /* Maximum length of a file mode string */
#define MAXIOBUFSIZ 200*1024    /* Maximum network buffer size          */
#define MAXACCTSIZE     20      /* Maximum account name length          */
#define MAXFTNLUN       99      /* max number of Fortran logical units  */
#if defined(_WIN32)
#define MAXRFD			1024	/* Bigger for NT because allocated socket fds are high */
#else
#define MAXRFD          256     /* max number of remote file descriptors*/
#endif
#define MAXCOMSIZ	256	/* Maximum length of a user's command	*/
#define MAXVMSTRING	256	/* Maximum length for VMS string	*/
#if defined(HPSS)
#define RFIO_MAX_PORTS  10	/* Maximum number of network stripes	*/
#endif /* HPSS */

#define DEFASNFNAM      ".rfassign"     /* default assign file name     */

#define FFTYPE_C        1       /* C File Format                        */
#define FFTYPE_F        2       /* Fortran File Format                  */


/* Fortran File Characteristics                                         */

#define FFFACC_S        0x0101  /* Fortran Sequential File              */
#define FFFACC_D        0x0102  /* Fortran Direct Access File           */
#define FFFFORM_D       0x0201  /* Fortran Formatted format             */
#define FFFFORM_U       0x0201  /* Fortran Unformatted format           */
#define FFFBLNK_N       0x0301  /* Fortran Blank to NULL                */
#define FFFBLNK_Z       0x0302  /* Fortran Blank to ZERO                */

/* Fortran READ method */

#define FFREAD_N        0x1001  /* Use native Fortran read              */
#define FFREAD_C        0x1002  /* Use subroutine Fortran read          */

/* Fortran OPEN options */

#define FFOOPT_0        0x0000  /* No options                           */
#define FFOOPT_A        0x0001  /* Open in append mode                  */
#define FFOOPT_E        0x0002  /* Erase on failure                     */
#define FFOOPT_T      	0x0004  /* Truncate in direct format            */

/*
 * Request type
 */

#define RQST_ERRMSG     0x0100          /* request for remote error msg */
#define RQST_OPEN       0x2001          /* request for open()           */
#define RQST_READ       0x2002          /* request for read()           */
#define RQST_WRITE      0x2003          /* request for write()          */
#define RQST_CLOSE      0x2004          /* request for close()          */
#define RQST_READAHEAD	0x2005		/* request for read() ahead	*/
#define RQST_FIRSTREAD	0x2006		/* first answer to readahead()	*/
#define RQST_STAT       0x2007          /* request for stat()           */
#define RQST_FSTAT      0x2008          /* request for fstat()          */
#define RQST_LSEEK      0x2009          /* request for lseek()          */
#define RQST_FIRSTSEEK	0x200a		/* request for first preseek()	*/
#define RQST_PRESEEK	0x200b		/* request for preseek()	*/
#define RQST_LASTSEEK	0x200c		/* request for last preseek()	*/
#define RQST_XYOPEN     0x3001          /* request for xyopen()         */
#define RQST_XYREAD     0x3002          /* request for xyread()         */
#define RQST_XYWRIT     0x3003          /* request for xywrit()         */
#define RQST_XYCLOS     0x3004          /* request for xyclos()         */
#define RQST_SYMLINK    0x3005          /* request for symlink()        */
#define RQST_STATFS 	0x3006		/* request for statfs() 	*/
#define RQST_LSTAT 	0x3007		/* request for lstat()		*/
#define RQST_POPEN	0x3008		/* request for popen()		*/
#define RQST_PCLOSE	0x3009		/* request for pclose()		*/
#define RQST_FREAD	0x300a		/* request for fread() 		*/
#define RQST_FWRITE	0x300b 		/* request for fwrite() 	*/
#define RQST_ACCESS	0x300c		/* request for access()		*/
#define RQST_CHKCON	0x4001		/* request for connection check */
#define RQST_READLINK	0x4002		/* request for readlink() 	*/
#define RQST_MKDIR      0x4003          /* request for mkdir()          */
#define RQST_CHOWN      0x4004          /* request for chown()          */
#define RQST_RENAME	0x4005		/* request for rename()		*/
#define RQST_LOCKF	0x4006		/* request for lockf()		*/
#define RQST_MSTAT	0x4007 		/* request for rfio_mstat()	*/
#define RQST_END	0x4008		/* request for end of rfiod 	*/
#define RQST_MSTAT_SEC	0x4010		/* request for secure mstat()	*/
#define	RQST_STAT_SEC	0x4011		/* request for secure stat()	*/
#define	RQST_LSTAT_SEC	0x4012		/* request for secure lstat()	*/

#define REP_ERROR       0x5000
#define REP_EOF         0x5001
#define RQST_OPEN_V3    0x5003
#define RQST_CLOSE_V3   0x5004
#define RQST_READ_V3    0x5005
#define RQST_WRITE_V3   0x5006
#define RQST_LSEEK_V3   0x5007
#define RQST_OPENDIR    0x500a		/* request for opendir()	*/
#define RQST_READDIR    0x500b		/* request for readdir()	*/
#define RQST_CLOSEDIR   0x500c		/* request for closedir()	*/
#define RQST_REWINDDIR  0x500d		/* request for rewinddir()	*/
#define RQST_RMDIR	0x500e		/* request for rmdir()		*/
#define	RQST_CHMOD	0x500f		/* request for chmod()		*/

#if defined(HPSS)
#define RQST_READLIST   0x6002
#define RQST_WRITELIST  0x6003
#define RQST_SETCOS     0x6004
#endif /* HPSS */

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

/*
 * Globals.
 */
extern RFILE    *ftnlun[];              /* Fortran lun descriptor table */
extern RFILE    *rfilefdt[];            /* Remote file desciptors table */

extern int      rfio_parse();   /* parse file path                      */
extern int      rfio_parseln(); /* parse file path                      */
extern int      rfio_connect(); /* connect remote rfio server           */
extern RFILE *  rfio_fopen();   /* RFIO's fopen()                       */
extern void striptb();          /* strip trailing blanks                */
extern char *getconfent();      /* get configuration file entry         */
extern char *getacct();         /* get account string name              */
extern char *lun2fn();          /* resolve lun to filename translation  */

#else   /* !RFIO_KERNEL */

extern FILE *   rfio_fopen();   /* RFIO's fopen()                       */

#if !defined(_WIN32) & !(defined(__hpux) & !defined(_INCLUDE_POSIX_SOURCE))
extern DIR *    rfio_opendir(); /* RFIO's opendir()                     */
extern struct dirent * rfio_readdir();
                                /* RFIO's readdir()                     */
#endif /* _WIN32 */

#if defined(feof)
#undef 	feof
#endif /* feof */
#if defined(ferror)
#undef 	ferror
#endif /* ferror */
#define feof		rfio_feof
#define ferror		rfio_ferror
#define fopen           rfio_fopen
#define fclose          rfio_fclose
#define fflush		rfio_fflush
#define fwrite          rfio_fwrite
#define fread           rfio_fread
#define fseek		rfio_fseek
/* The following clashes with struct stat in sys/stat.h */
/* #define stat            rfio_stat                    */
#define open            rfio_open
#define close           rfio_close
#define write           rfio_write
#define read            rfio_read
#define perror          rfio_perror
#define fstat           rfio_fstat
#define lseek           rfio_lseek
#define unlink		rfio_unlink
#define symlink		rfio_symlink
#define	mkdir		rfio_mkdir
#define rmdir		rfio_rmdir
#define	chmod		rfio_chmod
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

