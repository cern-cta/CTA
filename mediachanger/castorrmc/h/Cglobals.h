/*
 * $Id: Cglobals.h,v 1.1 1999/07/20 15:08:37 obarring Exp $
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
                                     int (*)(int *, void *));
EXTERN_C void DLL_DECL Cglobals_get(int *, void **, size_t size);
#else /* __STDC__ */
EXTERN_C void DLL_DECL Cglobals_init();
EXTERN_C void DLL_DECL Cglobals_get();
#endif /* __STDC__ */

#endif /* _CASTOR_GLOBALS_H */
