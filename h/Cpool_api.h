/*
 * Cpool_api.h,v 1.4 1999-10-20 21:07:45+02 jdurand Exp
 *
 * Cpool_api.h,v
 * Revision 1.4  1999-10-20 21:07:45+02  jdurand
 * Removed the unnecessary <osdep.h> header inclusion
 *
 * Revision 1.3  1999/10/14 12:04:28  jdurand
 * Introduced _PROTO macro
 *
 * Revision 1.2  1999-07-20 10:51:11+02  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Added missing Id and Log CVS's directives
 *
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
#define malloc(a)        Cpool_malloc(__FILE__,__LINE__,a)
#define free(a)          Cpool_free(__FILE__,__LINE__,a)
#define realloc(a,b)     Cpool_realloc(__FILE__,__LINE__,a,b)
#define calloc(a,b)      Cpool_calloc(__FILE__,__LINE__,a,b)
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

