/*
 * $RCSfile: rfio_api.h,v $ $Revision: 1.43 $ $Date: 2006/12/14 15:10:05 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * Copyright (C) 2000-2003 by CERN IT-PDP/DM
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

#ifndef _RFIO_ERRNO_H_INCLUDED_
#include <rfio_errno.h>
#endif /* _RFIO_ERRNO_H_INCLUDED_ */

struct rfstatfs {
        long totblks  ;      /* Total number of blocks       */
        long freeblks ;      /* Number of free blocks        */
        long bsize    ;      /* Block size                   */
        long totnods  ;      /* Total number of inodes       */
        long freenods ;      /* Number of free inodes        */
} ;
#include <stdio.h>
#ifndef _DIRENT_WIN32_H
#include <dirent.h>
#endif
#include <sys/stat.h>
#if !defined(RFIO_KERNEL)
#if defined(_WIN32)
typedef void * DIR;
#endif
#endif

/*
 * Define structure for preseek requests.
 */
#if !defined(_WIN32)
#include <sys/uio.h>
#else
struct iovec {
        int iov_base ;
        int iov_len ;
} ;
#endif
#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
struct iovec64 {
        off64_t iov_base ;
        int iov_len ;
} ;
#endif


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

EXTERN_C int DLL_DECL rfio_access _PROTO((const char *, int));
EXTERN_C int DLL_DECL rfio_chdir _PROTO((char *));
EXTERN_C int DLL_DECL rfio_chmod _PROTO((char *, int));
EXTERN_C int DLL_DECL rfio_chown _PROTO((char *, int, int));
EXTERN_C int DLL_DECL rfio_close _PROTO((int));
EXTERN_C int DLL_DECL rfio_close_v3 _PROTO((int));
EXTERN_C int DLL_DECL rfio_end _PROTO((void));  /* Close connections opened by rfio_mstat() */
EXTERN_C int DLL_DECL rfio_symend _PROTO((void)); /* Close connections opened by rfio_msymlink() */
EXTERN_C int DLL_DECL rfio_unend _PROTO((void)); /* Close connections opened by rfio_munlink() */
EXTERN_C int DLL_DECL rfio_fchmod _PROTO((int, int));
EXTERN_C int DLL_DECL rfio_fchown _PROTO((int, int, int));
EXTERN_C int DLL_DECL rfio_fstat _PROTO((int, struct stat *));
EXTERN_C char DLL_DECL *rfio_getcwd _PROTO((char *, int));
EXTERN_C int DLL_DECL rfio_lockf _PROTO((int, int, long));
EXTERN_C off_t DLL_DECL rfio_lseek _PROTO((int, off_t, int));
EXTERN_C int DLL_DECL rfio_lstat _PROTO((char *, struct stat *));
EXTERN_C int DLL_DECL rfio_mkdir _PROTO((char *, int));
EXTERN_C int DLL_DECL rfio_mstat _PROTO((char *, struct stat *));
EXTERN_C int DLL_DECL rfio_munlink _PROTO((char *));
EXTERN_C int DLL_DECL rfio_msymlink _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_mstat_reset _PROTO((void));  /* Reset connections opened by rfio_mstat() [fork() case] */
EXTERN_C int DLL_DECL rfio_munlink_reset _PROTO((void));  /* Reset connections opened by rfio_mstat() [fork() case] */
EXTERN_C int DLL_DECL rfio_msymlink_reset _PROTO((void));  /* Reset connections opened by rfio_mstat() [fork() case] */
#if defined(RFIO_KERNEL)
EXTERN_C int DLL_DECL rfio_open _PROTO((char *, int, int));
#else /* RFIO_KERNEL */
EXTERN_C int DLL_DECL rfio_open _PROTO((char *, int, ...));
#endif /* RFIO_KERNEL */
EXTERN_C int DLL_DECL rfio_open_v3 _PROTO((char *, int, int));
EXTERN_C void DLL_DECL rfio_perror _PROTO((char *));
EXTERN_C int DLL_DECL rfio_preseek _PROTO((int, struct iovec *, int));
EXTERN_C int DLL_DECL rfio_read _PROTO((int, void *, int));
EXTERN_C int DLL_DECL rfio_read_v3 _PROTO((int, char *, int));
EXTERN_C int DLL_DECL rfio_readlink _PROTO((char *, char *, int));
EXTERN_C int DLL_DECL rfio_rename _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_rmdir _PROTO((char *));
EXTERN_C int DLL_DECL rfio_serrno _PROTO((void));
EXTERN_C char DLL_DECL *rfio_serror _PROTO((void));
EXTERN_C int DLL_DECL rfio_stat _PROTO((char *, struct stat *));
EXTERN_C int DLL_DECL rfio_statfs _PROTO((char *, struct rfstatfs *)) ;
EXTERN_C int DLL_DECL rfio_symlink _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_unlink _PROTO((char *));
EXTERN_C int DLL_DECL rfio_write _PROTO((int, void *, int));
EXTERN_C int DLL_DECL rfio_write_v3 _PROTO((int, char *, int));
EXTERN_C int DLL_DECL rfioreadopt _PROTO((int));
EXTERN_C int DLL_DECL rfiosetopt _PROTO((int, int *, int));
EXTERN_C int DLL_DECL rfstatfs _PROTO((char *, struct rfstatfs *)) ;

#if defined(__alpha) && defined(__osf__)
#define fseeko64 fseek
#define fstat64 fstat
#define ftello64 ftell
#define lockf64 lockf
#define lseek64 lseek
#define lstat64 lstat
#define open64 open
#define stat64 stat
#endif
#if defined(_WIN32)
#define fstat64 _fstati64
#define lseek64 _lseeki64
#define open64 open
#define stat64 _stati64
#endif
#if defined(HPSSCLIENT)
EXTERN_C int rfio_setcos  (int, int, int);
EXTERN_C int rfio_readlist  (int, int, int, int, int (*worker)(int fd, char *buf, int buflen, off64_t offset),char *,int);
EXTERN_C int rfio_writelist (int, int, int, int, int (*worker)(int fd, char *buf, int buflen, off64_t offset),char *,int);
EXTERN_C int rfio_setcos64  (int, off64_t, int);
EXTERN_C off64_t rfio_readlist64  (int, off64_t, off64_t, int, int (*worker)(int fd, char *buf, int buflen, off64_t offset),char *,int);
EXTERN_C off64_t rfio_writelist64 (int, off64_t, off64_t, int, int (*worker)(int fd, char *buf, int buflen, off64_t offset),char *,int);
#endif

#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C int DLL_DECL rfio_close64_v3 _PROTO((int));
EXTERN_C int DLL_DECL rfio_fstat64 _PROTO((int, struct stat64 *));
EXTERN_C int DLL_DECL rfio_lockf64 _PROTO((int, int, off64_t));
EXTERN_C off64_t DLL_DECL rfio_lseek64 _PROTO((int, off64_t, int));
EXTERN_C off64_t DLL_DECL rfio_lseek64_v3 _PROTO((int, off64_t, int));
EXTERN_C int DLL_DECL rfio_lstat64 _PROTO((char *, struct stat64 *));
EXTERN_C int DLL_DECL rfio_mstat64 _PROTO((char *, struct stat64 *));
#if defined(RFIO_KERNEL)
EXTERN_C int DLL_DECL rfio_open64 _PROTO((char *, int, int));
#else /* RFIO_KERNEL */
EXTERN_C int DLL_DECL rfio_open64 _PROTO((char *, int, ...));
#endif /* RFIO_KERNEL */
EXTERN_C int DLL_DECL rfio_open64_v3 _PROTO((char *, int, int));
EXTERN_C int DLL_DECL rfio_preseek64 _PROTO((int, struct iovec64 *, int));
EXTERN_C int DLL_DECL rfio_read64_v3 _PROTO((int, char *, int));
EXTERN_C int DLL_DECL rfio_stat64  _PROTO((char *, struct stat64 *));
EXTERN_C int DLL_DECL rfio_write64_v3 _PROTO((int, char *, int));
#endif

/*
 * RFIO library routines with different internal and external prototypes
 */
#if defined(RFIO_KERNEL)
/*
 * Internal (KERNEL) prototypes
 */
EXTERN_C int DLL_DECL rfio_closedir _PROTO((RDIR *));
EXTERN_C int DLL_DECL rfio_fclose _PROTO((RFILE *));
EXTERN_C int DLL_DECL rfio_feof _PROTO((RFILE *));
EXTERN_C int DLL_DECL rfio_ferror _PROTO((RFILE *));
EXTERN_C int DLL_DECL rfio_fflush _PROTO((RFILE *));
EXTERN_C int DLL_DECL rfio_fileno _PROTO((RFILE *));
EXTERN_C RFILE DLL_DECL *rfio_fopen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_fread _PROTO((void *, int, int, RFILE *));
EXTERN_C int DLL_DECL rfio_fseek _PROTO((RFILE *, long int, int));
EXTERN_C long DLL_DECL rfio_ftell _PROTO((RFILE *));
EXTERN_C int DLL_DECL rfio_fwrite _PROTO((void *, int, int, RFILE *));
EXTERN_C int DLL_DECL rfio_getc _PROTO((RFILE *));
EXTERN_C RDIR DLL_DECL *rfio_opendir _PROTO((char *));
EXTERN_C int DLL_DECL rfio_pclose _PROTO((RFILE *));
EXTERN_C RFILE DLL_DECL *rfio_popen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_pread _PROTO((char *, int, int, RFILE *));
EXTERN_C int DLL_DECL rfio_pwrite _PROTO((char *, int, int, RFILE *));
EXTERN_C struct dirent DLL_DECL *rfio_readdir _PROTO((RDIR *));
EXTERN_C int DLL_DECL rfio_rewinddir _PROTO((RDIR *));
#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C RFILE DLL_DECL *rfio_fopen64 _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_fseeko64 _PROTO((RFILE *, off64_t, int));
EXTERN_C off64_t DLL_DECL rfio_ftello64 _PROTO((RFILE *));
#if defined(linux) || defined(SOLARIS)
EXTERN_C struct dirent64 DLL_DECL *rfio_readdir64 _PROTO((RDIR *));
#else
EXTERN_C struct dirent DLL_DECL *rfio_readdir64 _PROTO((RDIR *));
#endif
#endif
#else /* RFIO_KERNEL */
/*
 * External prototypes
 */
EXTERN_C int DLL_DECL rfio_closedir _PROTO((DIR *));
EXTERN_C int DLL_DECL rfio_fclose _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_feof _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_ferror _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_fflush _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_fileno _PROTO((FILE *));
EXTERN_C FILE DLL_DECL *rfio_fopen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_fread _PROTO((void *, int, int, FILE *));
EXTERN_C int DLL_DECL rfio_fseek _PROTO((FILE *, long int, int));
EXTERN_C long DLL_DECL rfio_ftell _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_fwrite _PROTO((void *, int, int, FILE *));
EXTERN_C int DLL_DECL rfio_getc _PROTO((FILE *));
EXTERN_C int DLL_DECL rfio_pclose _PROTO((FILE *));
EXTERN_C FILE DLL_DECL *rfio_popen _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_pread _PROTO((char *, int, int, FILE *));
EXTERN_C int DLL_DECL rfio_pwrite _PROTO((char *, int, int, FILE *));
#if !(defined(__hpux) && !defined(_INCLUDE_POSIX_SOURCE))
EXTERN_C DIR DLL_DECL *rfio_opendir _PROTO((char *));
EXTERN_C struct dirent DLL_DECL *rfio_readdir _PROTO((DIR *));
EXTERN_C int DLL_DECL rfio_rewinddir _PROTO((DIR *));
#endif /* !__hpux && !_INCLUDE_POSIX_SOURCE */
#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C FILE DLL_DECL *rfio_fopen64 _PROTO((char *, char *));
EXTERN_C int DLL_DECL rfio_fseeko64 _PROTO((FILE *, off64_t, int));
EXTERN_C off64_t DLL_DECL rfio_ftello64 _PROTO((FILE *));
#if defined(linux) || defined(SOLARIS)
EXTERN_C struct dirent64 DLL_DECL *rfio_readdir64 _PROTO((DIR *));
#else
EXTERN_C struct dirent DLL_DECL *rfio_readdir64 _PROTO((DIR *));
#endif
#endif
#endif /* RFIO_KERNEL */

/*
 * Purely internal globals
 */
#if defined(RFIO_KERNEL)
EXTERN_C RFILE DLL_DECL *ftnlun[];        /* Fortran lun descriptor table */
EXTERN_C char DLL_DECL *lun2fn _PROTO((int));
                                  /* resolve lun to filename translation  */
EXTERN_C RFILE DLL_DECL *rfilefdt[];      /* Remote file desciptors table */

EXTERN_C int DLL_DECL rfio_connect _PROTO((char *, int *));
                                     /* connect remote rfio server        */
EXTERN_C int DLL_DECL rfio_parse _PROTO((char *, char **, char **));
                                      /* parse file path                  */
EXTERN_C int DLL_DECL rfio_parseln _PROTO((char *, char **, char **, int));
                                      /* parse file path                  */
#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C int  DLL_DECL stat64tostat _PROTO((const struct stat64 *, struct stat *));
                                  /* copy from a stat64 struct to a stat  */
#endif
EXTERN_C void DLL_DECL striptb _PROTO((char *));  
                                  /* strip trailing blanks                */

EXTERN_C int DLL_DECL rfio_HsmIf_access _PROTO((const char *, int));
EXTERN_C int DLL_DECL rfio_HsmIf_chdir _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_chmod _PROTO((const char *, mode_t));
EXTERN_C int DLL_DECL rfio_HsmIf_chown _PROTO((const char *, uid_t, gid_t));
EXTERN_C int DLL_DECL rfio_HsmIf_close _PROTO((int));
EXTERN_C int DLL_DECL rfio_HsmIf_closedir _PROTO((DIR *));
EXTERN_C int DLL_DECL rfio_HsmIf_creat _PROTO((const char *, mode_t));
EXTERN_C char DLL_DECL *rfio_HsmIf_getcwd _PROTO((char *, int));
EXTERN_C int DLL_DECL rfio_HsmIf_getipath _PROTO((int, char *));
EXTERN_C int DLL_DECL rfio_HsmIf_mkdir _PROTO((const char *, mode_t));
EXTERN_C int DLL_DECL rfio_HsmIf_open _PROTO((const char *, int, mode_t, int));
EXTERN_C int DLL_DECL rfio_HsmIf_open_limbysz _PROTO((const char *, int, mode_t, U_HYPER, int));
EXTERN_C DIR DLL_DECL *rfio_HsmIf_opendir _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_read _PROTO((int, void *, int));
EXTERN_C struct dirent DLL_DECL *rfio_HsmIf_readdir _PROTO((DIR *));
EXTERN_C int DLL_DECL rfio_HsmIf_rename _PROTO((const char *, const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_reqtoput _PROTO((char *));
EXTERN_C void DLL_DECL rfio_HsmIf_rewinddir _PROTO((DIR *));
EXTERN_C int DLL_DECL rfio_HsmIf_rmdir _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_stat _PROTO((const char *, struct stat *));
EXTERN_C int DLL_DECL rfio_HsmIf_unlink _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_write _PROTO((int, void *, int));
EXTERN_C int DLL_DECL rfio_HsmIf_FindPhysicalPath _PROTO((char *, char **));
EXTERN_C int DLL_DECL rfio_HsmIf_FirstWrite _PROTO((int, void *, int));
EXTERN_C char DLL_DECL *rfio_HsmIf_GetCwdServer _PROTO(());
EXTERN_C int DLL_DECL rfio_HsmIf_GetCwdType _PROTO(());
EXTERN_C int DLL_DECL rfio_HsmIf_GetHsmType _PROTO((int, int *));
EXTERN_C int DLL_DECL rfio_HsmIf_IOError _PROTO((int, int));
EXTERN_C int DLL_DECL rfio_HsmIf_IsHsmFile _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_SetCwdServer _PROTO((const char *));
EXTERN_C int DLL_DECL rfio_HsmIf_SetCwdType _PROTO((int));

#endif /* RFIO_KERNEL */

#endif /* _RFIO_API_H_INCLUDED_ */
