/*
 * $Id: Cpool_api.h,v 1.2 1999/07/20 08:51:11 jdurand Exp $
 *
 * $Log: Cpool_api.h,v $
 * Revision 1.2  1999/07/20 08:51:11  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Added missing Id and Log CVS's directives
 *
 */

#ifndef __cpool_api_h
#define __cpool_api_h

#include <Cthread_api.h>

#ifndef EXTERN_C
#if defined(__cplusplus)
#define EXTERN_C extern "C"
#else
#define EXTERN_C extern
#endif /* __cplusplus */
#endif /* EXTERN_C */

/* Memory allocation wrappers - JUST used in case of         */
/* Cthread_environment() returning CTHREAD_MULTI_PROCESS     */
/* On WIN32 this will NEVER occur. And it will cause compile */
/* errors. So we explicitely disable it on this OS. As a     */
/* a consequence, this will only be compiled on UNIX-like    */
/* systems                                                   */
/* 07-JUN-1999       Jean-Damien Durand and Olof Barring     */

#ifndef _WIN32
#define malloc(a)        Cpool_malloc(__FILE__,__LINE__,a)
#define free(a)          Cpool_free(__FILE__,__LINE__,a)
#define realloc(a,b)     Cpool_realloc(__FILE__,__LINE__,a,b)
#define calloc(a,b)      Cpool_calloc(__FILE__,__LINE__,a,b)
#endif /* _WIN32 */

/* ---------------------------------------------- */
/* Externalize API if Cthread is built as Dynamic */
/* Load Library under Windows (DLL_DECL)          */
/* ---------------------------------------------- */

#if defined(__STDC__)
#ifndef _WIN32
EXTERN_C void DLL_DECL *Cpool_calloc(char *, int, size_t, size_t);
EXTERN_C void DLL_DECL *Cpool_malloc(char *, int, size_t);
EXTERN_C void DLL_DECL  Cpool_free(char *, int, void *);
EXTERN_C void DLL_DECL *Cpool_realloc(char *, int, void *, size_t);
#endif /* _WIN32 */
EXTERN_C int  DLL_DECL  Cpool_create(int, int *);
EXTERN_C int  DLL_DECL  Cpool_assign(int, void *(*)(void *), void *, int);
EXTERN_C int  DLL_DECL  Cpool_next_index(int);
#else
#ifndef _WIN32
EXTERN_C void DLL_DECL *Cpool_calloc();
EXTERN_C void DLL_DECL *Cpool_malloc();
EXTERN_C void DLL_DECL  Cpool_free();
EXTERN_C void DLL_DECL *Cpool_realloc();
#endif /* _WIN32 */
EXTERN_C int  DLL_DECL  Cpool_create();
EXTERN_C int  DLL_DECL  Cpool_assign();
EXTERN_C int  DLL_DECL  Cpool_next_index();
#endif /* __STDC__ */

#endif /* __cpool_api_h */










