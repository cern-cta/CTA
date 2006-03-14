/* $Id: Crmd_recv_from_rmnode.h,v 1.2 2006/03/14 16:50:53 motiakov Exp $ */

#ifndef __Crmd_recv_from_rmnode
#define __Crmd_recv_from_rmnode

#include "rm_struct.h"

/* Variables to define in your main application and to initialize - see Crmd_recv_from_rmnode.c */
extern int Crm_port;                             /* Port on which the thread is listening */
extern int nrmnodes;                             /* Number of nodes */
extern int test_flag;                            /* --test option , computed when receiving rmnode information */
extern struct rmnode *rmnodes;                   /* List of known nodes */
extern void *_nrmnodes_cthread_structure;        /* Cthread structure for nrmnodes mutex */

/* External prototypes */
EXTERN_C void DLL_DECL Crmd_exit _PROTO((int));
EXTERN_C void DLL_DECL *Crmd_recv_from_rmnode _PROTO((void *));
EXTERN_C int  DLL_DECL Crmd_recv_from_rmnode_getnode _PROTO((char *, struct rmnode *));
EXTERN_C void DLL_DECL Crmd_recv_from_rmnode_init _PROTO(());
EXTERN_C void DLL_DECL Crmd_strip_local_domain _PROTO((char *, const char *));

#endif /* __Crmd_recv_from_rmnode */
