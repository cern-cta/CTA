/* $Id */

#ifndef __Crmd_survey
#define __Crmd_survey

#include <sys/types.h>
#include "rm_struct.h"

/* Variables to define in your main application and to initialize - see Crmd_recv_from_maui.c */
extern int nrmjobs;                              /* Number of jobs */
extern struct rmjob *rmjobs;                     /* List of known jobs */
extern void *_nrmjobs_cthread_structure;         /* Cthread structure for nrmjobs mutex */
extern int nrmnodes;                             /* Number of nodes */
extern struct rmnode *rmnodes;                   /* List of known nodes */
extern void *_nrmnodes_cthread_structure;        /* Cthread structure for nrmnodes mutex */

/* External prototypes */
EXTERN_C void DLL_DECL Crmd_survey_init _PROTO(());
EXTERN_C void DLL_DECL *Crmd_survey_jobs _PROTO((void *));
EXTERN_C void DLL_DECL *Crmd_survey_nodes _PROTO((void *));

#endif  /* __Crmd_survey */
