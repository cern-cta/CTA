/*
 * Cpool_api.h,v 1.4 1999-10-20 21:07:45+02 jdurand Exp
 *
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef __cpool_api_h
#define __cpool_api_h

#include <Cthread_api.h>
/* Memory allocation wrappers - JUST used in case of         */
/* Cthread_environment() returning CTHREAD_MULTI_PROCESS     */
/* On WIN32 this will NEVER occur. And it will cause compile */
/* errors. So we explicitely disable it on this OS. As a     */
/* a consequence, this will only be compiled on UNIX-like    */
/* systems                                                   */
/* 07-JUN-1999       Jean-Damien Durand and Olof Barring     */

#ifndef _WIN32
#ifndef malloc
#define malloc(a)        Cpool_malloc(__FILE__,__LINE__,a)
#endif
#ifndef free
#define free(a)          Cpool_free(__FILE__,__LINE__,a)
#endif
#ifndef realloc
#define realloc(a,b)     Cpool_realloc(__FILE__,__LINE__,a,b)
#endif
#ifndef calloc
#define calloc(a,b)      Cpool_calloc(__FILE__,__LINE__,a,b)
#endif
#endif /* _WIN32 */

#ifndef _WIN32
EXTERN_C void DLL_DECL *Cpool_calloc _PROTO((char *, int, size_t, size_t));
EXTERN_C void DLL_DECL *Cpool_malloc _PROTO((char *, int, size_t));
EXTERN_C void DLL_DECL  Cpool_free _PROTO((char *, int, void *));
EXTERN_C void DLL_DECL *Cpool_realloc _PROTO((char *, int, void *, size_t));
#endif /* _WIN32 */
EXTERN_C int  DLL_DECL  Cpool_create _PROTO((int, int *));
EXTERN_C int  DLL_DECL  Cpool_assign _PROTO((int, void *(*)(void *), void *, int));
#define Cpool_next_index(poolnb) Cpool_next_index_timeout(poolnb,-1)
EXTERN_C int  DLL_DECL  Cpool_next_index_timeout _PROTO((int, int));

#endif /* __cpool_api_h */

