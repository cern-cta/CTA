/* Crmd_recv_from_users.h,v 1.1 2003/10/13 14:09:54 jdurand Exp */

#ifndef __Crmd_recv_from_users_h
#define __Crmd_recv_from_users_h

#include <stdio.h>
#include <sys/types.h>
#include "Castor_limits.h"
#include "osdep.h"
#include "rm_macros.h"
#include "rm_constants.h"
#include "Crmd_recv_from_maui.h"
#ifdef LSF
/* LSF support */
#include "lsf.h"
#include "lsbatch.h"
#endif

/* Variables to define in your main application and to initialize - see rmmaster.c */
extern int Crm_user_port; /* Port to bind for user */
extern u_signed64 globaljobid;
#ifdef LSF
extern int lsf_flag;
#endif
#ifdef MAUI
extern int maui_flag;
#endif
extern char *appname;

/* External prototypes */
EXTERN_C void *Crmd_recv_from_users _PROTO((void *));
EXTERN_C void *Crmd_recv_from_users_purge _PROTO((void *));
EXTERN_C void DLL_DECL Crmd_exit _PROTO((int));
EXTERN_C void DLL_DECL Crmd_recv_from_users_init _PROTO(());
#ifdef LSF
EXTERN_C int DLL_DECL Crmd_process_user_lsf_proxy _PROTO(());
EXTERN_C int  DLL_DECL Crmd_process_user_lsf_request _PROTO((int *, u_signed64, struct rmjob *, LS_LONG_INT *));
#endif

#endif /* __Crmd_recv_from_users_h */

