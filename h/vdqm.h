/*
 * $Id: vdqm.h,v 1.7 2008/11/06 09:55:05 murrayc3 Exp $
 */

/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vdqm.h,v $ $Revision: 1.7 $ $Date: 2008/11/06 09:55:05 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * vdqm.h - Common definitions for server and client API
 */

#ifndef _VDQM_H
#define _VDQM_H

#include "net.h"
#include "vdqm_messages.h"
#include "Castor_limits.h"
#include "Cregexp.h"
#ifdef VDQMCSEC
#include "Csec_api.h"
#endif 


/*
 * Network definitions
 */
typedef struct vdqmnw {
    int listen_socket;
    int accept_socket;
    int connect_socket;
#ifdef VDQMCSEC
    Csec_context_t sec_ctx;
    uid_t Csec_uid;
    gid_t Csec_gid;
    int Csec_service_type;
#endif
} vdqmnw_t;

/*
 * Prototypes for network interface routines
 */
int vdqm_InitNW (vdqmnw_t **);
int vdqm_InitNWOnPort (vdqmnw_t **, int);
int vdqm_Listen (vdqmnw_t *);
int vdqm_AcknPing (vdqmnw_t *, int);
int vdqm_AcknRollback (vdqmnw_t *);
int vdqm_AcknCommit (vdqmnw_t *);
int vdqm_RecvAckn (vdqmnw_t *);
int vdqm_RecvPingAckn (vdqmnw_t *);
int vdqm_RecvReq (vdqmnw_t *, vdqmHdr_t *, vdqmVolReq_t *, vdqmDrvReq_t *);
int vdqm_SendReq (vdqmnw_t *, vdqmHdr_t *, vdqmVolReq_t *, vdqmDrvReq_t *);
int vdqm_SendPing (vdqmnw_t *, vdqmHdr_t *, vdqmVolReq_t *);
int vdqm_CleanUp (vdqmnw_t *,int);
int vdqm_ConnectToVDQM (vdqmnw_t **, char *);
int vdqm_CloseConn (vdqmnw_t *);
int vdqm_ConnectToRTCP (int *, char *);
int vdqm_SendToRTCP (int, vdqmVolReq_t *, vdqmDrvReq_t *);
int vdqm_GetRTCPReq (char *, vdqmVolReq_t *, vdqmDrvReq_t *);
int vdqm_SendRTCPAckn (int, int *, int *, char *);
int vdqm_GetRTCPPort (void);
int vdqm_SendVolPriority_Transfer (vdqmnw_t *, vdqmVolPriority_t *);
int vdqm_RecvVolPriority_Transfer (vdqmnw_t *, vdqmVolPriority_t *);
int vdqm_SendDelDrv_Transfer (vdqmnw_t *, vdqmDelDrv_t *);
int vdqm_SendDedicate_Transfer (vdqmnw_t *, vdqmDedicate_t *);
int vdqm_AggregatorVolReq_Send (vdqmnw_t *,vdqmHdr_t *, vdqmVolReq_t *);
int vdqm_AggregatorVolReq_Recv (vdqmnw_t *,vdqmHdr_t *, vdqmVolReq_t *);

#if defined(VDQMSERV)
/*
 * Server specific definitions
 */

/*
 * Various circular list operations. Used by queue operations.
 */
#define CLIST_ITERATE_BEGIN(X,Y) {if ( (X) != NULL ) {(Y)=(X); do {
#define CLIST_ITERATE_END(X,Y) Y=(Y)->next; } while ((X) != (Y));}}
#define CLIST_INSERT(X,Y) {if ((X) == NULL) {X=(Y); (X)->next = (X)->prev = (X);} \
else {(Y)->next=(X); (Y)->prev=(X)->prev; (Y)->next->prev=(Y); (Y)->prev->next=(Y);}}
#define CLIST_DELETE(X,Y) {if ((Y) != NULL) {if ( (Y) == (X) ) (X)=(X)->next; if ((Y)->prev != (Y) && (Y)->next != (Y)) {\
(Y)->prev->next = (Y)->next; (Y)->next->prev = (Y)->prev;} else {(X)=NULL;}}}
 
/*
 * Internal data types. Begin with the Volume Record.
 */
typedef struct vdqm_volrec {
    int magic;
    int update;  /* record has been updated (for DB replication) */
    vdqmVolReq_t vol;
    struct vdqm_drvrec *drv;
    struct vdqm_volrec *next;
    struct vdqm_volrec *prev;
    struct vdqm_volrec *attached; /* For tape-to-tape copy jobs */
} vdqm_volrec_t;

/*
 * Now the Drive Record
 */  
typedef struct vdqm_drvrec {
    int magic;
    int update;  /* record has been updated (for DB replication) */
    Cregexp_t *expbuf;
    Cregexp_t *newexpbuf;
    vdqmDrvReq_t drv;
    struct vdqm_volrec *vol;
    struct vdqm_drvrec *next;
    struct vdqm_drvrec *prev;
} vdqm_drvrec_t;

typedef struct dgn_element {
    void *lock;
    char dgn[CA_MAXDGNLEN+1];
    int dgnindx;
    vdqm_volrec_t *vol_queue;
    vdqm_drvrec_t *drv_queue;
    struct dgn_element *next;
    struct dgn_element *prev;
} dgn_element_t;

/*
 * Replication client list
 */
typedef struct vdqmReplica {
    vdqmnw_t nw;
    int failed;
    char host[CA_MAXHOSTNAMELEN+1];
} vdqmReplica_t;

typedef struct vdqmReplicaList {
    int magic;
    vdqmReplica_t Replica;
    struct vdqmReplicaList *next;
    struct vdqmReplicaList *prev;
} vdqmReplicaList_t;

/*
 * Prototypes (server specific)
 */

/*
 * Thread pool interface
 */
int vdqm_InitPool (vdqmnw_t **);
int vdqm_GetPool (int, vdqmnw_t *, vdqmnw_t *);
int vdqm_ReturnPool (vdqmnw_t *);
/*
 * Queue operations interface
 */
void *vdqm_ProcReq (void *);
int vdqm_WaitForReqs (int);
int vdqm_SetError (int);
int vdqm_GetError (void);
int vdqm_InitQueueOp (void);
int vdqm_InitDgnQueue (dgn_element_t *);
int vdqm_LockAllQueues (void);
int vdqm_UnlockAllQueues (void);
int vdqm_NewVolReqID (int *);
int vdqm_NewDrvReqID (int *);
int vdqm_NewVolReq (vdqmHdr_t *, vdqmVolReq_t *);
int vdqm_NewDrvReq (vdqmHdr_t *, vdqmDrvReq_t *);
int vdqm_DelVolReq (vdqmVolReq_t *);
int vdqm_DelDrvReq (vdqmDrvReq_t *);
int vdqm_GetVolQueue (char *, vdqmVolReq_t *, void **, void **);
int vdqm_GetDrvQueue (char *, vdqmDrvReq_t *, void **, void **);
int vdqm_GetQueuePos (vdqmVolReq_t *);
int vdqm_QueueOpRollback (vdqmVolReq_t *, vdqmDrvReq_t *);
int vdqm_DedicateDrv (vdqmDrvReq_t *);
/*
 * Administrative operations
 */
int vdqm_DrvMatch (vdqm_volrec_t *, vdqm_drvrec_t *);
int vdqm_ResetDedicate (vdqm_drvrec_t *);
int vdqm_SetDedicate (vdqm_drvrec_t *);
/*
 * Drives list handling operations
 */
void vdqm_init_drive_file (char *);
int vdqm_load_queue ();
int vdqm_save_queue ();
/*
 * Replica (Cdb client) interface
 */
int vdqm_UpdateReplica (dgn_element_t *);
int vdqm_DeleteFromReplica (vdqmVolReq_t *, vdqmDrvReq_t *);
int vdqm_ReplVolReq (vdqmHdr_t *, vdqmVolReq_t *);
int vdqm_ReplDrvReq (vdqmHdr_t *, vdqmDrvReq_t *);
int vdqm_CheckReplicaHost (vdqmnw_t *);
int vdqm_AddReplica (vdqmnw_t *, vdqmHdr_t *);
int vdqm_StartReplicaThread (void);
int vdqm_DumpQueues (vdqmnw_t *);
int vdqm_GetReplica (vdqmnw_t *, vdqmReplica_t *);
/* 
 * RTCOPY interface
 */
int vdqm_GetRTCPPort (void);
int vdqm_StartJob (vdqm_volrec_t *);
int vdqm_OnRollback (void);
void *vdqm_OnRollbackThread (void *);

#endif /* VDQMSERV */

#endif /* VDQM_H */
