#ifndef _CSEC_PLUGIN_H
#define _CSEC_PLUGIN_H

#include "Csec_common_trunk_r21843.h"
#include "Cpwd_trunk_r21843.h"
#include "Cmutex_trunk_r21843.h"
#include "serrno_trunk_r21843.h"

#define CSEC_METHOD_NAME_X(METH, MECH) __CONCAT(METH,MECH)
#define CSEC_METHOD_NAME(METH, MECH) CSEC_METHOD_NAME_X(METH, MECH)

#define FP csec_funcptr
#define FPARG Csec_plugin_funcptrs_t *FP

#define MAKE_CALLERS(RT) \
MAKE_CALLER_FUNC_N0(RT,Csec_activate) \
MAKE_CALLER_FUNC_N0(RT,Csec_deactivate) \
MAKE_CALLER_FUNC_N0(RT,Csec_init_context) \
MAKE_CALLER_FUNC_N0(RT,Csec_reinit_context) \
MAKE_CALLER_FUNC_N0(RT,Csec_delete_connection_context) \
MAKE_CALLER_FUNC_N0(RT,Csec_delete_creds) \
MAKE_CALLER_FUNC_N2(RT,Csec_acquire_creds,char *,int) \
MAKE_CALLER_FUNC_N3(RT,Csec_server_establish_context_ext,int,char *,int) \
MAKE_CALLER_FUNC_N1(RT,Csec_client_establish_context,int) \
MAKE_CALLER_FUNC_N3(RT,Csec_map2name,const char *,char *, int) \
MAKE_CALLER_FUNC_N5(RT,Csec_get_service_name,int,char *,char *,char *,int)

#define CSEC_EXTRARG FPARG, Csec_context_t *
#define CSEC_SIMPLENAME(x) x
#define CSEC_FPNAME(x) (*x)

#define CSEC_DECLARE_FUNCTIONS(RT,FP,MECH) \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_activate,MECH))                     (CSEC_EXTRARG); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_deactivate,MECH))                   (CSEC_EXTRARG); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_init_context,MECH))                 (CSEC_EXTRARG); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_reinit_context,MECH))               (CSEC_EXTRARG); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_delete_connection_context,MECH))    (CSEC_EXTRARG); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_delete_creds,MECH))                 (CSEC_EXTRARG); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_acquire_creds,MECH))                (CSEC_EXTRARG, char *, int); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_server_establish_context_ext,MECH)) (CSEC_EXTRARG, int, char *, int); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_client_establish_context,MECH))     (CSEC_EXTRARG, int); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_map2name,MECH))                     (CSEC_EXTRARG, const char *, char *, int); \
  RT __CONCAT(FP,NAME)(__CONCAT(Csec_get_service_name,MECH))             (CSEC_EXTRARG, int, char *, char *, char *, int);

/* for filling/passing and using function pointers to the plugin */
typedef struct Csec_plugin_funcptrs_s {
  /* special thread specific variable from Cglobals.c */
  int *(*C__serrno) (void);
  int *_serrno;

  struct passwd *(*Cgetpwuid) (uid_t);
  int (*Cmutex_lock) (void *, int);
  int (*Cmutex_unlock) (void *);
  int (*Csec_context_is_client) (Csec_context_t *);
  int (*Csec_errmsg) (char *, char *, ...);
  int (*_Csec_recv_token) (int, csec_buffer_t, int, int *);
  int (*_Csec_send_token) (int, csec_buffer_t, int, int);
  int (*Csec_trace) (char *, char *, ...);
  int (*Csec_isIdAService) (const char *, const char *);
  struct hostent *(*Cgethostbyaddr) (const void *, size_t, int);
} Csec_plugin_funcptrs_t;

typedef struct Csec_plugin_pluginptrs_s {
  void *handle;
  CSEC_DECLARE_FUNCTIONS(int,CSEC_FP,)
} Csec_plugin_pluginptrs_t;

#ifdef _CSEC_CALLS_PLUGIN

#ifdef KRB4
CSEC_DECLARE_FUNCTIONS(EXTERN_C int,CSEC_SIMPLE,_KRB4)
#endif
#ifdef KRB5
CSEC_DECLARE_FUNCTIONS(EXTERN_C int,CSEC_SIMPLE,_KRB5)
#endif
#ifdef GSI
CSEC_DECLARE_FUNCTIONS(EXTERN_C int,CSEC_SIMPLE,_GSI)
CSEC_DECLARE_FUNCTIONS(EXTERN_C int,CSEC_SIMPLE,_GSI_pthr)
#endif
CSEC_DECLARE_FUNCTIONS(EXTERN_C int,CSEC_SIMPLE,_ID)

#ifdef serrno
#define _CSEC_C__SERRNO_P &C__serrno
#else
#define _CSEC_C__SERRNO_P NULL
#endif

#define FILL_FUNCPTR_STRUCT(x) \
Csec_plugin_funcptrs_t x = { \
  _CSEC_C__SERRNO_P, \
  &serrno, \
  &Cgetpwuid, \
  &Cmutex_lock, \
  &Cmutex_unlock, \
  &Csec_context_is_client, \
  &Csec_errmsg, \
  &_Csec_recv_token, \
  &_Csec_send_token, \
  &Csec_trace, \
  &Csec_isIdAService, \
  &Cgethostbyaddr };

#define PLUGINFP(CTX,x) (((Csec_plugin_pluginptrs_t *)((CTX)->shhandle))->x)

#define MAKE_CALLER_FUNC_N0(RT,NAME) \
RT __CONCAT(NAME,_caller) (Csec_context_t *ctx) { \
  FILL_FUNCPTR_STRUCT(funcs) \
  return (*PLUGINFP(ctx,NAME))(&funcs, ctx); }

#define MAKE_CALLER_FUNC_N1(RT,NAME,T2) \
RT __CONCAT(NAME,_caller) (Csec_context_t *ctx, T2 t2) { \
  FILL_FUNCPTR_STRUCT(funcs) \
  return (*PLUGINFP(ctx,NAME))(&funcs, ctx, t2); }

#define MAKE_CALLER_FUNC_N2(RT,NAME,T2,T3) \
RT __CONCAT(NAME,_caller) (Csec_context_t *ctx, T2 t2, T3 t3) { \
  FILL_FUNCPTR_STRUCT(funcs) \
  return (*PLUGINFP(ctx,NAME))(&funcs, ctx, t2, t3); }

#define MAKE_CALLER_FUNC_N3(RT,NAME,T2,T3,T4) \
RT __CONCAT(NAME,_caller) (Csec_context_t *ctx, T2 t2, T3 t3, T4 t4) { \
  FILL_FUNCPTR_STRUCT(funcs) \
  return (*PLUGINFP(ctx,NAME))(&funcs, ctx, t2, t3, t4); }

#define MAKE_CALLER_FUNC_N5(RT,NAME,T2,T3,T4,T5,T6) \
RT __CONCAT(NAME,_caller) (Csec_context_t *ctx, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6) { \
  FILL_FUNCPTR_STRUCT(funcs) \
  return (*PLUGINFP(ctx,NAME))(&funcs, ctx, t2, t3, t4, t5, t6); }

MAKE_CALLERS(static int)

#else

#ifdef KRB4
CSEC_DECLARE_FUNCTIONS(int,CSEC_SIMPLE,_KRB4)
#endif
#ifdef KRB5
CSEC_DECLARE_FUNCTIONS(int,CSEC_SIMPLE,_KRB5)
#endif
#ifdef GSI
CSEC_DECLARE_FUNCTIONS(int,CSEC_SIMPLE,_GSI)
CSEC_DECLARE_FUNCTIONS(int,CSEC_SIMPLE,_GSI_pthr)
#endif
CSEC_DECLARE_FUNCTIONS(int,CSEC_SIMPLE,_ID)

#ifndef serrno
#define serrno (*FP->_serrno)
#else
#undef serrno
#define serrno (*_Csec_plugin_serrno(FP))
static int *_Csec_plugin_serrno(FPARG) {
  if (FP->C__serrno != NULL) {
    return (*FP->C__serrno)();
  }
  return FP->_serrno;
}
#endif

#if defined(Cgetpwuid) || defined(Cmutex_lock) || defined(Cmutex_unlock) || defined(Csec_context_is_client) || defined(Csec_errmsg) || defined(_Csec_recv_token) || defined(_Csec_send_token) || defined(Csec_trace) || defined(Csec_isIdAService) || defined(Cgethostbyaddr)
#error unexpected redefine
#endif

#define Cgetpwuid (*FP->Cgetpwuid)
#define Cmutex_lock (*FP->Cmutex_lock)
#define Cmutex_unlock (*FP->Cmutex_unlock)
#define Csec_context_is_client (*FP->Csec_context_is_client)
#define Csec_errmsg (*FP->Csec_errmsg)
#define _Csec_recv_token (*FP->_Csec_recv_token)
#define _Csec_send_token (*FP->_Csec_send_token)
#define Csec_trace (*FP->Csec_trace)
#define Csec_isIdAService (*FP->Csec_isIdAService)
#define Cgethostbyaddr (*FP->Cgethostbyaddr)

#endif /* _CSEC_CALLS_PLUGIN */
#endif /* Csec_plugin.h */
