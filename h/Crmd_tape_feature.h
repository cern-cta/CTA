/* Crmd_tape_feature.h,v 1.3 2003/12/11 17:53:34 jdurand Exp */

#ifndef __Crmd_tape_feature_h
#define __Crmd_tape_feature_h

#include <sys/types.h>
#include "Castor_limits.h"
#include "osdep.h"
#include "rm_constants.h"
#include "net.h" /* For SOCKET definition, used in VDQM */
#include "vdqm_constants.h"
#include "vdqm_api.h"
#include "vdqm.h"

/* Structure to use */
struct vdqm_reqlist {
    vdqmVolReq_t volreq;
    vdqmDrvReq_t drvreq;
    struct vdqm_reqlist *next;
    struct vdqm_reqlist *prev; 
};

/* Variables to define in your main application and to initialize - see Crmd_recv_from_maui.c */
extern char *tape_feature;
extern void *_tape_feature_cthread_structure;
/* Variables to define in your main application and to initialize - see Crmd_tape_feature.c */
extern int Crm_user_port;
extern u_signed64 globaljobid;

/* External prototypes */
EXTERN_C void DLL_DECL *Crmd_tape_feature _PROTO((void *));
EXTERN_C void DLL_DECL  Crmd_tape_feature_init _PROTO(());

#endif /* __Crmd_tape_feature_h */

