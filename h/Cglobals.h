/*
 * $Id: Cglobals.h,v 1.3 1999/07/30 16:00:12 obarring Exp $
 * $Log: Cglobals.h,v $
 * Revision 1.3  1999/07/30 16:00:12  obarring
 * Correct the copyright notice ....
 *
 * Revision 1.2  1999/07/30 15:59:10  obarring
 * Add copyright notice
 *
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CASTOR_GLOBALS_H
#define _CASTOR_GLOBALS_H    

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

#if defined(__STDC__)
EXTERN_C void DLL_DECL Cglobals_init(int (*)(int *, void **),
                                     int (*)(int *, void *),
                                     int (*)(void));
EXTERN_C void DLL_DECL Cglobals_get(int *, void **, size_t size);
EXTERN_C void DLL_DECL Cglobals_getTid(int *);
#else /* __STDC__ */
EXTERN_C void DLL_DECL Cglobals_init();
EXTERN_C void DLL_DECL Cglobals_get();
EXTERN_C void DLL_DECL Cglobals_getTid();
#endif /* __STDC__ */

#endif /* _CASTOR_GLOBALS_H */
