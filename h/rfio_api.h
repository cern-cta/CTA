/*
 * $RCSfile: rfio_api.h,v $ $Revision: 1.1 $ $Date: 2000/05/29 14:16:45 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * Copyright (C) 2000 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * rfio_api.h    -   Remote File Access API definitions 
 */

#ifndef _RFIO_API_H_INCLUDED_
#define _RFIO_API_H_INCLUDED_

#ifndef _RFIO_CONSTANTS_H_INCLUDED_
#include <rfio_constants.h>
#endif /* _RFIO_CONSTANTS_H_INCLUDED_ */

struct rfstatfs {
        long totblks  ;      /* Total number of blocks       */
        long freeblks ;      /* Number of free blocks        */
        long bsize    ;      /* Block size                   */
        long totnods  ;      /* Total number of inodes       */
        long freenods ;      /* Number of free inodes        */
} ;

/*
 * Internal data types 
 */
#if defined(RFIO_KERNEL)
#ifndef _RFIO_H_INCLUDED_
#include <rfio.h>
#endif /* _RFIO_H_INCLUDED_ */
#endif /* RFIO_KERNEL */

/*
 * RFIO library routines. Common internal and external prototypes
 */

EXTERN_C int DLL_DECL rfio_end _PROTO((void));
EXTERN_C int DLL_DECL rfio_stat _PROTO((char *, struct stat *));
EXTERN_C int DLL_DECL rfio_mstat _PROTO((char *, struct stat *));
EXTERN_C int DLL_DECL rfio_open _PROTO((char *, int, int));
EXTERN_C int DLL_DECL rfio_close _PROTO((int));
EXTERN_C int DLL_DECL rfio_write _PROTO((int, char *, int));
EXTERN_C int DLL_DECL rfio_read _PROTO((int, char *, int));
EXTERN_C void DLL_DECL rfio_perror _PROTO((char *));
EXTERN_C int DLL_DECL rfio_fstat _PROTO((int, struct stat *));
EXTERN_C int DLL_DECL rfio_lseek _PROTO((int, int, int));
EXTERN_C int DLL_DECL rfio_preseek _PROTO((int, int, struct iovec *));
EXTERN_C int DLL_DECL rfstatfs _PROTO((char *, struct rfstatfs *)) ;
EXTERN_C int DLL_DECL rfio_statfs _PROTO((char *, struct rfstatfs *)) ;
EXTERN_C char DLL_DECL *rfio_serror _PROTO((void));
EXTERN_C int DLL_DECL rfio_mkdir _PROTO((char *, int));
EXTERN_C int DLL_DECL rfio_rmdir _PROTO((char *));
EXTERN_C int DLL_DECL rfio_rename _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_lockf _PROTO((int, int, long));
EXTERN_C int DLL_DECL rfio_chmod _PROTO((char *, int));

/*
 * RFIO library routines with different internal and external prototypes
 */
#if defined(RFIO_KERNEL)
/*
 * Internal (KERNEL) prototypes
 */
EXTERN_C RFILE DLL_DECL *rfio_fopen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_fclose _PROTO((RFILE *));
EXTERN_C int DLL_DECL rfio_fflush _PROTO((RFILE *));
EXTERN_C int DLL_DECL rfio_fwrite _PROTO((char *, int, int, RFILE *));
EXTERN_C int DLL_DECL rfio_fread _PROTO((char *, int, int, RFILE *));
EXTERN_C int DLL_DECL rfio_fseek _PROTO((RFILE *, long int, int));
EXTERN_C RFILE DLL_DECL *rfio_popen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_pclose _PROTO((RFILE *));
#if !defined(_WIN32)
EXTERN_C struct dirent DLL_DECL *rfio_readdir _PROTO((RDIR *));
EXTERN_C int DLL_DECL rfio_rewinddir _PROTO((RDIR *));
EXTERN_C int DLL_DECL rfio_closedir _PROTO((RDIR *));
EXTERN_C RDIR DLL_DECL *rfio_opendir _PROTO((char *));
#endif /* !_WIN32 */
#else /* RFIO_KERNEL */
/*
 * External prototypes
 */
EXTERN_C FILE DLL_DECL *rfio_fopen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_fclose _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_fflush _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_fwrite _PROTO((char *, int, int, FILE *));
EXTERN_C int DLL_DECL rfio_fread _PROTO((char *, int, int, FILE *));
EXTERN_C int DLL_DECL rfio_fseek _PROTO((FILE *, long int, int));
EXTERN_C FILE DLL_DECL *rfio_popen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_pclose _PROTO((FILE *));
#if !defined(_WIN32) && !(defined(__hpux) && !defined(_INCLUDE_POSIX_SOURCE))
EXTERN_C struct dirent DLL_DECL *rfio_readdir _PROTO((DIR *));
EXTERN_C int DLL_DECL rfio_rewinddir _PROTO((DIR *));
EXTERN_C int DLL_DECL rfio_closedir _PROTO((DIR *));
EXTERN_C DIR DLL_DECL *rfio_opendir _PROTO((char *));
#endif /* !_WIN32 && !__hpux && !_INCLUDE_POSIX_SOURCE */
#endif /* RFIO_KERNEL */

/*
 * Purely internal globals
 */
#if defined(RFIO_KERNEL)
EXTERN_C RFILE DLL_DECL *ftnlun[];        /* Fortran lun descriptor table */
EXTERN_C RFILE DLL_DECL *rfilefdt[];      /* Remote file desciptors table */

EXTERN_C int DLL_DECL rfio_parse _PROTO((char *, char **, char **, int));
                                      /* parse file path                  */
EXTERN_C int DLL_DECL rfio_parseln _PROTO((char *, char **, char **));
                                      /* parse file path                  */
EXTERN_C int DLL_DECL rfio_connect _PROTO((char *, int));
                                     /* connect remote rfio server        */
EXTERN_C void striptb();          /* strip trailing blanks                */
EXTERN_C char *getconfent();      /* get configuration file entry         */
EXTERN_C char *getacct();         /* get account string name              */
EXTERN_C char *lun2fn();          /* resolve lun to filename translation  */

EXTERN_C int DLL_DECL rfio_HsmIf_access _PROTO((const char *, int));
EXTERN_C int DLL_DECL rfio_HsmIf_chdir _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_chmod _PROTO((const char *, mode_t));
EXTERN_C int DLL_DECL rfio_HsmIf_chown _PROTO((const char *, uid_t, gid_t));
EXTERN_C int DLL_DECL rfio_HsmIf_close _PROTO((int));
EXTERN_C int DLL_DECL rfio_HsmIf_mkdir _PROTO((const char *, mode_t));
EXTERN_C int DLL_DECL rfio_HsmIf_open _PROTO((const char *, int, mode_t));
EXTERN_C int DLL_DECL rfio_HsmIf_rename _PROTO((const char *, const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_rmdir _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_stat _PROTO((const char *, struct stat *));
EXTERN_C int DLL_DECL rfio_HsmIf_unlink _PROTO((const char *));
#endif /* RFIO_KERNEL */

#endif /* _RFIO_API_H_INCLUDED_ */
