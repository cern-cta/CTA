/*
 * $Id: Cnetdb.h,v 1.3 1999/07/30 15:59:39 obarring Exp $
 * $Log: Cnetdb.h,v $
 * Revision 1.3  1999/07/30 15:59:39  obarring
 * Add copyright notice
 *
 * Revision 1.2  1999/07/30 15:58:06  obarring
 * Add __STDC__ compilation branch for __h_errno() declaration
 *
 * Revision 1.1  1999/07/30 15:02:29  obarring
 * First version
 *
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef _CNETDB_H
#define _CNETDB_H

#ifndef EXTERN_C
#if defined(__cplusplus)
#define EXTERN_C extern "C"
#else /* __cplusplus */
#define EXTERN_C extern
#endif /* __cplusplus */
#endif /* EXTERN_C */

#if !(defined(_WIN32) && defined(_DLL))
#define DLL_DECL
#else /* _WIN32 && _DLL */
#ifdef DLL_DECL
#undef DLL_DECL
#endif /* DLL_DECL */
#ifdef _EXPORTING
#define DLL_DECL __declspec(dllexport) __cdecl
#else  /* _EXPORTING */
#define DLL_DECL __declspec(dllimport) __cdecl
#endif /* _EXPORTING */
#endif /* _WIN32 && _DLL */

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#if defined(hpux) || defined(HPUX10) || defined(sgi) || defined(SOLARIS)
#if defined(__STDC__)
EXTERN_C int DLL_DECL *__h_errno(void);
#else /* __STDC__ */
EXTERN_C int DLL_DECL *__h_errno();
#endif /* __STDC__ */
#define h_errno (*__h_errno())
#endif /* hpux || HPUX10 || sgi || SOLARIS */
#endif /* _REENTRANT || _THREAD_SAFE */


#if defined(__STDC__)
EXTERN_C struct hostent DLL_DECL *Cgethostbyname(const char *);
EXTERN_C struct hostent DLL_DECL *Cgethostbyaddr(const void *, size_t, int);
#else  /* __STDC__ */
EXTERN_C struct hostent DLL_DECL *Cgethostbyname();
EXTERN_C struct hostent DLL_DECL *Cgethostbyaddr();
#endif /* __STDC__ */

#endif /* _CNETDB_H */
