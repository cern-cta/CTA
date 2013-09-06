/*
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

#ifndef _OSDEP_H_INCLUDED_
#include <osdep.h>
#endif /* _OSDEP_H_INCLUDED_ */
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
#include <dirent.h>
#include <sys/stat.h>

/*
 * Define structure for preseek requests.
 */
#include <sys/uio.h>
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

EXTERN_C int rfio_access (const char *, int);
EXTERN_C int rfio_chdir (char *);
EXTERN_C int rfio_chmod (char *, int);
EXTERN_C int rfio_chown (char *, int, int);
EXTERN_C int rfio_close (int);
EXTERN_C int rfio_close_v3 (int);
EXTERN_C int rfio_end (void);  /* Close connections opened by rfio_mstat() */
EXTERN_C int rfio_symend (void); /* Close connections opened by rfio_msymlink() */
EXTERN_C int rfio_unend (void); /* Close connections opened by rfio_munlink() */
EXTERN_C int rfio_fchmod (int, int);
EXTERN_C int rfio_fchown (int, int, int);
EXTERN_C int rfio_fstat (int, struct stat *);
EXTERN_C char *rfio_getcwd (char *, int);
EXTERN_C int rfio_lockf (int, int, long);
EXTERN_C off_t rfio_lseek (int, off_t, int);
EXTERN_C int rfio_lstat (char *, struct stat *);
EXTERN_C int rfio_mkdir (char *, int);
EXTERN_C int rfio_mstat (char *, struct stat *);
EXTERN_C int rfio_munlink (char *);
EXTERN_C int rfio_msymlink (char *, char *);
EXTERN_C int rfio_mstat_reset (void);  /* Reset connections opened by rfio_mstat() [fork() case] */
EXTERN_C int rfio_munlink_reset (void);  /* Reset connections opened by rfio_mstat() [fork() case] */
EXTERN_C int rfio_msymlink_reset (void);  /* Reset connections opened by rfio_mstat() [fork() case] */
#if defined(RFIO_KERNEL)
EXTERN_C int rfio_open (char *, int, int);
#else /* RFIO_KERNEL */
EXTERN_C int rfio_open (char *, int, ...);
#endif /* RFIO_KERNEL */
EXTERN_C int rfio_open_v3 (char *, int, int);
EXTERN_C void rfio_perror (char *);
EXTERN_C int rfio_preseek (int, struct iovec *, int);
EXTERN_C int rfio_read (int, void *, int);
EXTERN_C int rfio_read_v3 (int, char *, int);
EXTERN_C int rfio_readlink (char *, char *, int);
EXTERN_C int rfio_rename (char *, char *);
EXTERN_C int rfio_rmdir (char *);
EXTERN_C int rfio_serrno (void);
EXTERN_C char *rfio_serror (void);
EXTERN_C int rfio_stat (char *, struct stat *);
EXTERN_C int rfio_statfs (char *, struct rfstatfs *) ;
EXTERN_C int rfio_symlink (char *, char *);
EXTERN_C int rfio_unlink (char *);
EXTERN_C int rfio_write (int, void *, int);
EXTERN_C int rfio_write_v3 (int, char *, int);
EXTERN_C int rfioreadopt (int);
EXTERN_C int rfiosetopt (int, int *, int);
EXTERN_C int rfstatfs (char *, struct rfstatfs *) ;
EXTERN_C int rfio_smstat (int, char *, struct stat *, int);
EXTERN_C int rfio_lseek_v3 (int, int, int);

#if defined(__APPLE__)
#define fseeko64 fseek
#define fstat64 fstat
#define ftello64 ftell
#define lockf64 lockf
#define lseek64 lseek
#define lstat64 lstat
#define open64 open
#define stat64 stat
#endif

#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C int rfio_close64_v3 (int);
EXTERN_C int rfio_fstat64 (int, struct stat64 *);
EXTERN_C int rfio_lockf64 (int, int, off64_t);
EXTERN_C off64_t rfio_lseek64 (int, off64_t, int);
EXTERN_C off64_t rfio_lseek64_v3 (int, off64_t, int);
EXTERN_C int rfio_lstat64 (char *, struct stat64 *);
EXTERN_C int rfio_mstat64 (char *, struct stat64 *);
#if defined(RFIO_KERNEL)
EXTERN_C int rfio_open64 (char *, int, int);
#else /* RFIO_KERNEL */
EXTERN_C int rfio_open64 (char *, int, ...);
#endif /* RFIO_KERNEL */
EXTERN_C int rfio_open64_v3 (char *, int, int);
EXTERN_C int rfio_preseek64 (int, struct iovec64 *, int);
EXTERN_C int rfio_read64_v3 (int, char *, int);
EXTERN_C int rfio_stat64  (char *, struct stat64 *);
EXTERN_C int rfio_write64_v3 (int, char *, int);
EXTERN_C int rfio_smstat64 (int, char *, struct stat64 *, int);
#endif

/*
 * RFIO library routines with different internal and external prototypes
 */
#if defined(RFIO_KERNEL)
/*
 * Internal (KERNEL) prototypes
 */
EXTERN_C int rfio_closedir (RDIR *);
EXTERN_C int rfio_fclose (RFILE *);
EXTERN_C int rfio_feof (RFILE *);
EXTERN_C int rfio_ferror (RFILE *);
EXTERN_C int rfio_cleanup (int);
EXTERN_C int rfio_cleanup_v3 (int);
EXTERN_C int rfio_dircleanup (int);
EXTERN_C int rfio_fflush (RFILE *);
EXTERN_C int rfio_fileno (RFILE *);
EXTERN_C RFILE *rfio_fopen (char *, char *);
EXTERN_C int rfio_fread (void *, int, int, RFILE *);
EXTERN_C int rfio_fseek (RFILE *, long int, int);
EXTERN_C long rfio_ftell (RFILE *);
EXTERN_C int rfio_fwrite (void *, int, int, RFILE *);
EXTERN_C int rfio_getc (RFILE *);
EXTERN_C RDIR *rfio_opendir (char *);
EXTERN_C int rfio_pclose (RFILE *);
EXTERN_C RFILE *rfio_popen (char *, char *);
EXTERN_C int rfio_pread (char *, int, int, RFILE *);
EXTERN_C int rfio_pwrite (char *, int, int, RFILE *);
EXTERN_C struct dirent *rfio_readdir (RDIR *);
EXTERN_C int rfio_rewinddir (RDIR *);
#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C RFILE *rfio_fopen64 (char *, char *);
EXTERN_C int rfio_fseeko64 (RFILE *, off64_t, int);
EXTERN_C off64_t rfio_ftello64 (RFILE *);
#if defined(linux)
EXTERN_C struct dirent64 *rfio_readdir64 (RDIR *);
#else
EXTERN_C struct dirent *rfio_readdir64 (RDIR *);
#endif
#endif
EXTERN_C int rfio_filbuf (int, char*, int);
EXTERN_C int rfio_filbuf64 (int, char*, int);
EXTERN_C int rfio_open_ext (char*, int, int, uid_t, gid_t, int, char*, char*);
EXTERN_C int rfio_open_ext_v3 (char*, int, int, uid_t, gid_t, int, char*, char*);
EXTERN_C int rfio_open64_ext_v3 (char*, int, int, uid_t, gid_t, int, char*);
EXTERN_C void rfio_setup (RFILE*);
EXTERN_C int rfio_read_v2 (int, char*, int);
EXTERN_C int rfio_read64_v2 (int, char*, int);
EXTERN_C int rfio_write_v2 (int, char*, int);
EXTERN_C int rfio_write64_v2 (int, char*, int);
#else /* RFIO_KERNEL */
/*
 * External prototypes
 */
EXTERN_C int rfio_closedir (DIR *);
EXTERN_C int rfio_fclose (FILE *);
EXTERN_C int rfio_feof (FILE *);
EXTERN_C int rfio_ferror (FILE *);
EXTERN_C int rfio_fflush (FILE *);
EXTERN_C int rfio_fileno (FILE *);
EXTERN_C FILE *rfio_fopen (char *, char *);
EXTERN_C int rfio_fread (void *, int, int, FILE *);
EXTERN_C int rfio_fseek (FILE *, long int, int);
EXTERN_C long rfio_ftell (FILE *);
EXTERN_C int rfio_fwrite (void *, int, int, FILE *);
EXTERN_C int rfio_getc (FILE *);
EXTERN_C int rfio_pclose (FILE *);
EXTERN_C FILE *rfio_popen (char *, char *);
EXTERN_C int rfio_pread (char *, int, int, FILE *);
EXTERN_C int rfio_pwrite (char *, int, int, FILE *);
EXTERN_C DIR *rfio_opendir (char *);
EXTERN_C struct dirent *rfio_readdir (DIR *);
EXTERN_C int rfio_rewinddir (DIR *);
#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C FILE *rfio_fopen64 (char *, char *);
EXTERN_C int rfio_fseeko64 (FILE *, off64_t, int);
EXTERN_C off64_t rfio_ftello64 (FILE *);
#if defined(linux)
EXTERN_C struct dirent64 *rfio_readdir64 (DIR *);
#else
EXTERN_C struct dirent *rfio_readdir64 (DIR *);
#endif
#endif
#endif /* RFIO_KERNEL */


EXTERN_C int rfio_parse (char *, char **, char **);
                                      /* parse file path                  */

/*
 * Purely internal globals
 */
#if defined(RFIO_KERNEL)
EXTERN_C RFILE *ftnlun[];        /* Fortran lun descriptor table */
EXTERN_C char *lun2fn (int);
                                  /* resolve lun to filename translation  */
EXTERN_C RFILE *rfilefdt[];      /* Remote file desciptors table */

EXTERN_C int rfio_connect (char *, int *);
                                     /* connect remote rfio server        */
EXTERN_C int rfio_parseln (char *, char **, char **, int);
                                      /* parse file path                  */
#if ! defined(linux) || defined(_LARGEFILE64_SOURCE)
EXTERN_C int  stat64tostat (const struct stat64 *, struct stat *);
                                  /* copy from a stat64 struct to a stat  */
#endif
EXTERN_C void striptb (char *);  
                                  /* strip trailing blanks                */

EXTERN_C int rfio_HsmIf_access (const char *, int);
EXTERN_C int rfio_HsmIf_chdir (const char *);
EXTERN_C int rfio_HsmIf_chmod (const char *, mode_t);
EXTERN_C int rfio_HsmIf_chown (const char *, uid_t, gid_t);
EXTERN_C int rfio_HsmIf_close (int);
EXTERN_C int rfio_HsmIf_closedir (DIR *);
EXTERN_C char *rfio_HsmIf_getcwd (char *, int);
EXTERN_C int rfio_HsmIf_getipath (int, char *);
EXTERN_C int rfio_HsmIf_mkdir (const char *, mode_t);
EXTERN_C int rfio_HsmIf_open (const char *, int, mode_t, int, int);
EXTERN_C int rfio_HsmIf_open_limbysz (const char *, int, mode_t, U_HYPER, int);
EXTERN_C DIR *rfio_HsmIf_opendir (const char *);
EXTERN_C int rfio_HsmIf_read (int, void *, int);
EXTERN_C struct dirent *rfio_HsmIf_readdir (DIR *);
EXTERN_C int rfio_HsmIf_rename (const char *, const char *);
EXTERN_C int rfio_HsmIf_reqtoput (char *);
EXTERN_C void rfio_HsmIf_rewinddir (DIR *);
EXTERN_C int rfio_HsmIf_rmdir (const char *);
EXTERN_C int rfio_HsmIf_stat (const char *, struct stat *);
EXTERN_C int rfio_HsmIf_unlink (const char *);
EXTERN_C int rfio_HsmIf_write (int, void *, int);
EXTERN_C int rfio_HsmIf_FindPhysicalPath (char *, char **);
EXTERN_C int rfio_HsmIf_FirstWrite (int, void *, int);
EXTERN_C char *rfio_HsmIf_GetCwdServer ();
EXTERN_C int rfio_HsmIf_GetCwdType ();
EXTERN_C int rfio_HsmIf_GetHsmType (int, int *);
EXTERN_C int rfio_HsmIf_IOError (int, int);
EXTERN_C int rfio_HsmIf_IsHsmFile (const char *);
EXTERN_C int rfio_HsmIf_SetCwdServer (const char *);
EXTERN_C int rfio_HsmIf_SetCwdType (int);
EXTERN_C int rfio_HsmIf_IsHsmDirEntry (DIR *);
EXTERN_C int rfio_HsmIf_stat64 (const char *, struct stat64 *);

#endif /* RFIO_KERNEL */

#endif /* _RFIO_API_H_INCLUDED_ */
