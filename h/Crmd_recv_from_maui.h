/* Crmd_recv_from_maui.h,v 1.4 2004/03/24 10:19:25 jdurand Exp */

#ifndef __Crmd_recv_from_maui
#define __Crmd_recv_from_maui

#include <sys/types.h>
#include "rm_struct.h"
#include "Crmd_tape_feature.h"

/* Variables to define in your main application and to initialize - see Crmd_recv_from_maui.c */
extern int Crm_wiki_port;
extern int tape_survey_flag;
extern int nrmjobs;
extern void *_nrmjobs_cthread_structure;
extern char *username;
extern struct rmjob *rmjobs;
extern int Crm_wiki_port;

/* External prototypes */
EXTERN_C void DLL_DECL Crmd_exit _PROTO((int));
EXTERN_C void DLL_DECL *Crmd_recv_from_maui _PROTO((void *));
EXTERN_C void DLL_DECL *Crmd_recv_from_maui_setlog _PROTO((void (*)(int, char *, ...)));
EXTERN_C void DLL_DECL  Crmd_recv_from_maui_init _PROTO(());

#endif /* __Crmd_recv_from_rmnode */
