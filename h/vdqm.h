/*
 * $Id: vdqm.h,v 1.1 2004/07/30 13:11:17 motiakov Exp $
 */

/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vdqm.h,v $ $Revision: 1.1 $ $Date: 2004/07/30 13:11:17 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * vdqm.h - Common definitions for server and client API
 */

#ifndef _VDQM_H
#define _VDQM_H

#include "Castor_limits.h"
#include "Cregexp.h"
#ifdef CSEC
#include "Csec_api.h"
#endif 

typedef struct vdqmHdr {
    int magic;
    int reqtype;
    int len;
} vdqmHdr_t;


typedef struct vdqmVolReq {
    int VolReqID;
    int DrvReqID;    /* Drive request ID for running requests */
    int priority;
    int client_port;
    int recvtime;
    int clientUID;
    int clientGID;
    int mode;       /* WRITE_ENABLE/WRITE_DISABLE from Ctape_constants.h */
    char client_host[CA_MAXHOSTNAMELEN+1];
    char volid[CA_MAXVIDLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    char drive[CA_MAXUNMLEN+1];
    char dgn[CA_MAXDGNLEN+1];
    char client_name[CA_MAXUSRNAMELEN+1];
} vdqmVolReq_t;
/*
 * Length of marshalled structure. Don't forget to update
 * if entries are added
 */
#define VDQM_VOLREQLEN(X) ( 8*LONGSIZE + \
    strlen(X->volid) + strlen(X->client_host) + strlen(X->server) + \
    strlen(X->drive) + strlen(X->dgn) + strlen(X->client_name) + 6 )

typedef struct vdqmVolAckn {
    int VolReqID;
    int queuepos;
} vdqmVolAckn_t;

typedef struct vdqmdDrvReq {
    int status;
    int DrvReqID;
    int VolReqID;          /* Volume request ID for running requests */
    int jobID;
    int recvtime;
    int resettime;         /* Last time counters were reset */
    int usecount;          /* Usage counter (total number of VolReqs so far)*/
    int errcount;          /* Drive error counter */
    int MBtransf;          /* MBytes transfered in last request. */
    int mode;       /* WRITE_ENABLE/WRITE_DISABLE from Ctape_constants.h */
    u_signed64 TotalMB;    /* Total MBytes transfered.           */
    char reqhost[CA_MAXHOSTNAMELEN+1];
    char volid[CA_MAXVIDLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    char drive[CA_MAXUNMLEN+1];
    char dgn[CA_MAXDGNLEN+1];
    char dedicate[CA_MAXLINELEN+1];
    /* Regexp optimization */
    char newdedicate[CA_MAXLINELEN+1];
    short is_uid;
    short is_gid;
    short is_name;
    short no_uid;
    short no_gid;
    short no_name;
    short no_host;
    short no_vid;
    short no_mode;
    short no_date;
    short no_time;
    short no_age;
    uid_t uid;
    gid_t gid;
    char  name[CA_MAXUSRNAMELEN+1];
} vdqmDrvReq_t;
/*
 * Length of marshalled structure. Don't forget to update
 * if entries are added
 */
#define VDQM_DRVREQLEN(X) ( 10 * LONGSIZE + QUADSIZE + strlen(X->volid) + \
    strlen(X->server) + strlen(X->drive) + strlen(X->dgn) + \
    strlen(X->dedicate) + 5 )

typedef struct vdqmDrvAckn {
    int status;
} vdqmDrvAckn_t;


/*
 * Network definitions
 */
typedef struct vdqmnw {
    SOCKET listen_socket;
    SOCKET accept_socket;
    SOCKET connect_socket;
#ifdef CSEC
    Csec_context_t sec_ctx;
    uid_t Csec_uid;
    gid_t Csec_gid;
    int Csec_service_type;
#endif
} vdqmnw_t;

/*
 * Prototypes for network interface routines
 */
int vdqm_InitNW _PROTO((vdqmnw_t **));
int vdqm_InitNWOnPort _PROTO((vdqmnw_t **, int));
int vdqm_Listen _PROTO((vdqmnw_t *));
int vdqm_AcknPing _PROTO((vdqmnw_t *, int));
int vdqm_AcknRollback _PROTO((vdqmnw_t *));
int vdqm_AcknCommit _PROTO((vdqmnw_t *));
int vdqm_RecvAckn _PROTO((vdqmnw_t *));
int vdqm_RecvPingAckn _PROTO((vdqmnw_t *));
int vdqm_RecvReq _PROTO((vdqmnw_t *, vdqmHdr_t *, vdqmVolReq_t *, vdqmDrvReq_t *));
int vdqm_SendReq _PROTO((vdqmnw_t *, vdqmHdr_t *, vdqmVolReq_t *, vdqmDrvReq_t *));
int vdqm_SendPing _PROTO((vdqmnw_t *, vdqmHdr_t *, vdqmVolReq_t *));
int vdqm_CleanUp _PROTO((vdqmnw_t *,int));
int vdqm_ConnectToVDQM _PROTO((vdqmnw_t **, char *));
int vdqm_CloseConn _PROTO((vdqmnw_t *));
int vdqm_ConnectToRTCP _PROTO((SOCKET *, char *));
int vdqm_SendToRTCP _PROTO((SOCKET, vdqmVolReq_t *, vdqmDrvReq_t *));
int vdqm_GetRTCPReq _PROTO((char *, vdqmVolReq_t *, vdqmDrvReq_t *));
int vdqm_SendRTCPAckn _PROTO((SOCKET, int *, int *, char *));
int vdqm_GetRTCPPort _PROTO((void));

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
int vdqm_InitPool _PROTO((vdqmnw_t **));
int vdqm_GetPool _PROTO((int, vdqmnw_t *, vdqmnw_t *));
int vdqm_ReturnPool _PROTO((vdqmnw_t *));
/*
 * Queue operations interface
 */
void *vdqm_ProcReq _PROTO((void *));
int vdqm_WaitForReqs _PROTO((int));
int vdqm_SetError _PROTO((int));
int vdqm_GetError _PROTO((void));
int vdqm_InitQueueOp _PROTO((void));
int vdqm_InitDgnQueue _PROTO((dgn_element_t *));
int vdqm_LockAllQueues _PROTO((void));
int vdqm_UnlockAllQueues _PROTO((void));
int vdqm_NewVolReqID _PROTO((int *));
int vdqm_NewDrvReqID _PROTO((int *));
int vdqm_NewVolReq _PROTO((vdqmHdr_t *, vdqmVolReq_t *));
int vdqm_NewDrvReq _PROTO((vdqmHdr_t *, vdqmDrvReq_t *));
int vdqm_DelVolReq _PROTO((vdqmVolReq_t *));
int vdqm_DelDrvReq _PROTO((vdqmDrvReq_t *));
int vdqm_GetVolQueue _PROTO((char *, vdqmVolReq_t *, void **, void **));
int vdqm_GetDrvQueue _PROTO((char *, vdqmDrvReq_t *, void **, void **));
int vdqm_GetQueuePos _PROTO((vdqmVolReq_t *));
int vdqm_QueueOpRollback _PROTO((vdqmVolReq_t *, vdqmDrvReq_t *));
int vdqm_DedicateDrv _PROTO((vdqmDrvReq_t *));
/*
 * Administrative operations
 */
int vdqm_DrvMatch _PROTO((vdqm_volrec_t *, vdqm_drvrec_t *));
int vdqm_ResetDedicate _PROTO((vdqm_drvrec_t *));
int vdqm_SetDedicate _PROTO((vdqm_drvrec_t *));
/*
 * Drives list handling operations
 */
void vdqm_init_drive_file _PROTO ((char *));
int vdqm_load_queue _PROTO (());
int vdqm_save_queue _PROTO (());
/*
 * Replica (Cdb client) interface
 */
int vdqm_UpdateReplica _PROTO((dgn_element_t *));
int vdqm_DeleteFromReplica _PROTO((vdqmVolReq_t *, vdqmDrvReq_t *));
int vdqm_ReplVolReq _PROTO((vdqmHdr_t *, vdqmVolReq_t *));
int vdqm_ReplDrvReq _PROTO((vdqmHdr_t *, vdqmDrvReq_t *));
int vdqm_CheckReplicaHost _PROTO((vdqmnw_t *));
int vdqm_AddReplica _PROTO((vdqmnw_t *, vdqmHdr_t *));
int vdqm_StartReplicaThread _PROTO((void));
int vdqm_DumpQueues _PROTO((vdqmnw_t *));
int vdqm_GetReplica _PROTO((vdqmnw_t *, vdqmReplica_t *));
/* 
 * RTCOPY interface
 */
int vdqm_GetRTCPPort _PROTO((void));
int vdqm_StartJob _PROTO((vdqm_volrec_t *));
int vdqm_OnRollback _PROTO((void));
void *vdqm_OnRollbackThread _PROTO((void *));

#endif /* VDQMSERV */

#endif /* VDQM_H */
