/* $Id */

#ifndef __Crmd_checkpoint
#define __Crmd_checkpoint

#include <sys/types.h>

/* Variables to define in your main application and to initialize - see Crmd_recv_from_maui.c */
extern char *checkpoint_directory;               /* Where are the checkpoint files */
extern int checkpoint_nodes_interval;            /* Interval in seconds between each nodes checkpoint */
extern int checkpoint_jobs_interval;             /* Interval in seconds between each jobs checkpoint */
extern int checkpoint_nodes_lifetime;            /* Allowed maximum lifetime if we reload a nodes checkpoint */
extern int checkpoint_jobs_lifetime;             /* Allowed maximum lifetime if we reload a jobs checkpoint */
extern char *checkpoint_directory;               /* Location of the checkpoint directory */
extern struct rmjob *rmjobs;                     /* List of known jobs */
extern void *_nrmjobs_cthread_structure;         /* Cthread structure for nrmjobs mutex */
extern int nrmnodes;                             /* Number of nodes */
extern struct rmnode *rmnodes;                   /* List of known nodes */
extern void *_nrmnodes_cthread_structure;        /* Cthread structure for nrmnodes mutex */

/* External prototypes */
EXTERN_C void DLL_DECL Crmd_checkpoint_init _PROTO(());
EXTERN_C void DLL_DECL *Crmd_checkpoint_jobs _PROTO((void *));
EXTERN_C void DLL_DECL *Crmd_checkpoint_nodes _PROTO((void *));
EXTERN_C int  DLL_DECL Crmd_reload_jobs _PROTO(());
EXTERN_C int  DLL_DECL Crmd_reload_nodes _PROTO(());

#endif  /* __Crmd_checkpoint */
