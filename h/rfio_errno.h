/*
 * $Id: rfio_errno.h,v 1.2 1999/12/09 13:46:19 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: rfio_errno.h,v $ $Revision: 1.2 $ $Date: 1999/12/09 13:46:19 $
 */

/* rfio_errno.h   Thread safe rfio_errno  */

#ifndef _RFIO_ERRNO_H_INCLUDED_
#define _RFIO_ERRNO_H_INCLUDED_


#if !defined(EXTERN_C)
#if defined(__cplusplus)
#define EXTERN_C extern "C"
#else /* __cplusplus */
#define EXTERN_C extern
#endif /* __cplusplus */
#endif /* EXTERN_C */

#if defined(DLL_DECL)
#undef DLL_DECL
#endif /* DLL_DECL */
#if defined(_WIN32) && defined(_DLL)
#if defined(_EXPORTING)
#define DLL_DECL __declspec(dllexport)
#else /* _EXPORTING */
#define DLL_DECL __declspec(dllimport)
#endif /* _EXPORTING */
#else /* _WIN32 && _DLL */
#define DLL_DECL
#endif /* _WIN32 && _DLL */

#if defined(_REENTRANT) || defined(_THREAD_SAFE) || \
   (defined(_WIN32) && (defined(_MT) || defined(_DLL)))
/*
 * Multi-thread (MT) environment
 */
#if defined(__STDC__)
EXTERN_C int DLL_DECL *__rfio_errno(void);
#else /* __STDC__ */
EXTERN_C int DLL_DECL *__rfio_errno();
#endif /* __STDC__ */

/*
 * Thread safe rfio_errno. Note, __rfio_errno is defined in Cglobals.c rather
 * than rfio/error.c .
 */
#define rfio_errno (*__rfio_errno())

#else /* _REENTRANT || _THREAD_SAFE ... */
/*
 * non-MT environment
 */
extern  int     rfio_errno;                 /* Global error number          */
#endif /* _REENTRANT || _TREAD_SAFE */

#if defined(__STDC__)
#include <sys/types.h>
EXTERN_C char DLL_DECL *rfio_errmsg_r(int, int, char*, size_t); 
EXTERN_C char DLL_DECL *rfio_errmsg(int, int);
EXTERN_C char DLL_DECL *rfio_serror_r(char*, size_t);
EXTERN_C char DLL_DECL *rfio_serror(void);  
EXTERN_C void DLL_DECL rfio_perror(char *);
#else /* __STDC__ */
EXTERN_C char DLL_DECL *rfio_errmsg_r(); 
EXTERN_C char DLL_DECL *rfio_errmsg();
EXTERN_C char DLL_DECL *rfio_serror_r();
EXTERN_C char DLL_DECL *rfio_serror();  
EXTERN_C void DLL_DECL rfio_perror();
#endif /* __STDC__ */

#endif /* _RFIO_ERRNO_H_INCLUDED_ */
