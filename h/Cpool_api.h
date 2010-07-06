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

EXTERN_C void *Cpool_calloc (char *, int, size_t, size_t);
EXTERN_C void *Cpool_malloc (char *, int, size_t);
EXTERN_C void  Cpool_free (char *, int, void *);
EXTERN_C void *Cpool_realloc (char *, int, void *, size_t);
#define Cpool_create(nbreq,nbget) Cpool_create_ext(nbreq,nbget,NULL)
#define Cpool_assign(poolid,addr,args,timeout) Cpool_assign_ext(poolid,NULL,addr,args,timeout)
#define Cpool_next_index(poolnb) Cpool_next_index_timeout(poolnb,-1)
#define Cpool_next_index_timeout(poolnb,timeout) Cpool_next_index_timeout_ext(poolnb,NULL,timeout)

/* ===================================== */
/* Extended version (a-la-Cthread's ext) */
/* goal: get rid of internal mutexes     */
/* ===================================== */

EXTERN_C int   Cpool_create_ext (int, int *, void **);
EXTERN_C int   Cpool_assign_ext (int, void *, void *(*)(void *), void *, int);
EXTERN_C int   Cpool_next_index_timeout_ext (int, void *, int);

#endif /* __cpool_api_h */

