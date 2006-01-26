/*
 * Cthread_api.h,v 1.11 2001/11/30 11:18:11 CERN IT-PDP/DM Jean-Damien Durand
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef __Cthread_api_h
#define __Cthread_api_h

#include <Cthread_flags.h>
#include <osdep.h>

/* The following prototypes are resumed to a single        */
/* Cthread function, with the correct parameters (timeout) */

#define Cthread_mutex_lock(a)        Cthread_lock_mtx(a,-1)
#define Cthread_mutex_trylock(a)     Cthread_lock_mtx(a,0)
#define Cthread_mutex_timedlock(a,b) Cthread_lock_mtx(a,b)
#define Cthread_cond_wait(a)         Cthread_wait_condition(a,-1)
#define Cthread_cond_timedwait(a,b)  Cthread_wait_condition(a,b)

#define Cthread_mutex_lock_addr(a)   Cthread_lock_mtx_addr(a)
#define Cthread_mutex_lock_ext(a)    Cthread_lock_mtx_ext(a,-1)
#define Cthread_mutex_trylock_ext(a) Cthread_lock_mtx_ext(a,0)
#define Cthread_mutex_timedlock_ext(a,b) Cthread_lock_mtx_ext(a,b)
#define Cthread_cond_wait_ext(a)         Cthread_wait_condition_ext(a,-1)
#define Cthread_cond_timedwait_ext(a,b)  Cthread_wait_condition_ext(a,b)

/* Cthread knows if a signal is to be broadcasted or not */
#define Cthread_cond_signal(a)       Cthread_cond_broadcast(a)
#define Cthread_cond_signal_ext(a)   Cthread_cond_broadcast_ext(a)

/* ---------------------------------------------- */
/* Wrapper macros to have the most useful debug   */
/* information as possible                        */
/* ---------------------------------------------- */
#define  Cthread_create(a,b)          Cthread_Create(__FILE__,__LINE__,a,b)
#define  Cthread_create_detached(a,b) Cthread_Create_Detached(__FILE__,__LINE__,a,b)
#define  Cthread_join(a,b)            Cthread_Join(__FILE__,__LINE__,a,b)
#define  Cthread_lock_mtx(a,b)        Cthread_Lock_Mtx(__FILE__,__LINE__,a,b)
#define  Cthread_lock_mtx_addr(a)     Cthread_Lock_Mtx_addr(__FILE__,__LINE__,a)
#define  Cthread_lock_mtx_ext(a,b)    Cthread_Lock_Mtx_ext(__FILE__,__LINE__,a,b)
#define  Cthread_mutex_unlock(a)      Cthread_Mutex_Unlock(__FILE__,__LINE__,a)
#define  Cthread_mutex_unlock_ext(a)  Cthread_Mutex_Unlock_ext(__FILE__,__LINE__,a)
#define  Cthread_wait_condition(a,b)  Cthread_Wait_Condition(__FILE__,__LINE__,a,b)
#define  Cthread_wait_condition_ext(a,b)  Cthread_Wait_Condition_ext(__FILE__,__LINE__,a,b)
#define  Cthread_cond_broadcast(a)    Cthread_Cond_Broadcast(__FILE__,__LINE__,a)
#define  Cthread_cond_broadcast_ext(a) Cthread_Cond_Broadcast_ext(__FILE__,__LINE__,a)
#define  Cthread_detach(a)            Cthread_Detach(__FILE__,__LINE__,a)
#define  Cthread_mutex_destroy(a)     Cthread_Mutex_Destroy(__FILE__,__LINE__,a)
#define  Cthread_self()               Cthread_Self(__FILE__,__LINE__)
#define  Cthread_getspecific(a,b)     Cthread_Getspecific(__FILE__,__LINE__,a,b)
#define  Cthread_setspecific(a,b)     Cthread_Setspecific(__FILE__,__LINE__,a,b)
#define  Cthread_kill(a,b)            Cthread_Kill(__FILE__,__LINE__,a,b)
#define  Cthread_exit(a)              Cthread_Exit(__FILE__,__LINE__,a)

EXTERN_C int   DLL_DECL  Cthread_init _PROTO((void));
EXTERN_C int   DLL_DECL  Cthread_Create _PROTO((char *, int, void *(*)(void *), void *));
EXTERN_C int   DLL_DECL  Cthread_Create_Detached _PROTO((char *, int, void *(*)(void *), void *)); 
EXTERN_C int   DLL_DECL  Cthread_Join _PROTO((char *, int, int , int  **));
EXTERN_C int   DLL_DECL  Cthread_Lock_Mtx _PROTO((char *, int, void *, int));
EXTERN_C int   DLL_DECL  Cthread_Lock_Mtx_init _PROTO((void *, int));
EXTERN_C int   DLL_DECL  Cthread_Mutex_Unlock _PROTO((char *, int, void *));
EXTERN_C int   DLL_DECL  Cthread_Mutex_Unlock_init _PROTO((void *));
EXTERN_C int   DLL_DECL  Cthread_Mutex_Unlock_ext _PROTO((char *, int, void *));
EXTERN_C int   DLL_DECL  Cthread_Wait_Condition _PROTO((char *, int, void *, int));
EXTERN_C int   DLL_DECL  Cthread_Wait_Condition_ext _PROTO((char *, int, void *, int));
EXTERN_C int   DLL_DECL  Cthread_Cond_Broadcast _PROTO((char *, int, void *));
EXTERN_C int   DLL_DECL  Cthread_Cond_Broadcast_ext _PROTO((char *, int, void *));
EXTERN_C int   DLL_DECL  Cthread_Detach _PROTO((char *, int, int));
EXTERN_C int   DLL_DECL  Cthread_Mutex_Destroy _PROTO((char *, int, void *));
EXTERN_C int   DLL_DECL  Cthread_Self _PROTO((char *, int));
EXTERN_C int   DLL_DECL  Cthread_Self0 _PROTO((void));
EXTERN_C int   DLL_DECL _Cthread_self _PROTO((void));
EXTERN_C int   DLL_DECL  Cthread_Getspecific _PROTO((char *, int, int *, void **));
EXTERN_C int   DLL_DECL  Cthread_Getspecific0 _PROTO((int *, void **));
EXTERN_C int   DLL_DECL  Cthread_Getspecific_init _PROTO((int *, void **));
EXTERN_C int   DLL_DECL  Cthread_Setspecific _PROTO((char *, int, int *, void *));
EXTERN_C int   DLL_DECL  Cthread_Setspecific0 _PROTO((int *, void *));
EXTERN_C int   DLL_DECL  Cthread_proto _PROTO((void));
EXTERN_C int   DLL_DECL  Cthread_isproto _PROTO((char *));
EXTERN_C int   DLL_DECL  Cthread_environment _PROTO((void));
EXTERN_C void  DLL_DECL  Cthread_unprotect _PROTO((void));
EXTERN_C void  DLL_DECL *Cthread_Lock_Mtx_addr _PROTO((char *, int, void *));
EXTERN_C int   DLL_DECL  Cthread_Lock_Mtx_ext _PROTO((char *, int, void *, int));
EXTERN_C int   DLL_DECL  Cthread_Kill _PROTO((char *, int, int, int));
EXTERN_C void  DLL_DECL  Cthread_Exit _PROTO((char *, int, void *));

#endif /* __Cthread_api_h */




