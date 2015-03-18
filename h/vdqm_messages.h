/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * vdqm_messages.h - Common VDQM message definitions for server and client API
 */

#pragma once

#include "Castor_limits.h"


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
 * Length of marshalled structure. Don't forget to update if entries are added.
 * Note that a string requires strlen + 1 bytes of storage, hence the "+ 6" at
 * the end of the VDQM_DEDICATELEN macro.
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
 * Length of marshalled structure. Don't forget to update if entries are added.
 * Note that a string requires strlen + 1 bytes of storage.
 */
#define VDQM_DRVREQLEN(X) ( 10 * LONGSIZE + QUADSIZE + strlen(X->volid) + \
    strlen(X->server) + strlen(X->drive) + strlen(X->dgn) + \
    strlen(X->dedicate) + 5 )

typedef struct vdqmDrvAckn {
    int status;
} vdqmDrvAckn_t;

/*
 * Message used to send the priority of a volume to the VDQM2.
 *
 * Note that this message will be sent with VDQM magic number: VDQM2
 */
typedef struct vdqmVolPriority {
    int  priority;
    int  clientUID;
    int  clientGID;
    char clientHost[CA_MAXHOSTNAMELEN+1];
    char vid[CA_MAXVIDLEN+1];
    int  tpMode;
    int  lifespanType;
} vdqmVolPriority_t;
/*
 * Length of marshalled structure. Don't forget to update if entries are added.
 * Note that a string requires strlen + 1 bytes of storage, hence the "+ 2" at
 * the end of the VDQM_VOLPRIORITYLEN macro.
 */
#define VDQM_VOLPRIORITYLEN(X) ( 5*LONGSIZE + \
  strlen(X->clientHost) + strlen(X->vid) + 2 )

/*
 * Message used to send a delete drive message to the VDQM.
 *
 * Note that this message will be sent with VDQM magic number: VDQM3
 */
typedef struct vdqmDelDrv {
    int  clientUID;
    int  clientGID;
    char clientHost[CA_MAXHOSTNAMELEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    char drive[CA_MAXUNMLEN+1];
    char dgn[CA_MAXDGNLEN+1];
} vdqmDelDrv_t;
/*
 * Length of marshalled structure. Don't forget to update if entries are added.
 * Note that a string requires strlen + 1 bytes of storage, hence the "+ 4" at
 * the end of the VDQM_DELDRVLEN macro.
 */
#define VDQM_DELDRVLEN(X) ( 2*LONGSIZE + strlen(X->clientHost) + \
  strlen(X->server) + strlen(X->drive) +  strlen(X->dgn) + 4 )

/*
 * Message used to send a dedicate to the VDQM.
 *
 * Note that this message will be sent with VDQM magic number: VDQM3
 */
typedef struct vdqmDedicate {
    int  clientUID;
    int  clientGID;
    char clientHost[CA_MAXHOSTNAMELEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    char drive[CA_MAXUNMLEN+1];
    char dgn[CA_MAXDGNLEN+1];
    char dedicate[CA_MAXLINELEN+1];
} vdqmDedicate_t;
/*
 * Length of marshalled structure. Don't forget to update if entries are added.
 * Note that a string requires strlen + 1 bytes of storage, hence the "+ 5" at
 * the end of the VDQM_DEDICATELEN macro.
 */
#define VDQM_DEDICATELEN(X) ( 2*LONGSIZE + strlen(X->clientHost) + \
  strlen(X->server) + strlen(X->drive) + strlen(X->dgn) + strlen(X->dedicate) \
  + 5 )

