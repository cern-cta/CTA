/*
 * $Id: vdqm_api.h,v 1.17 2008/10/11 11:33:34 murrayc3 Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */

/*
 * vdqm_api.h - VDQM client API library definitions
 */

#ifndef _VDQM_API_H
#define _VDQM_API_H

#include <osdep.h>
#include <vdqm_constants.h>
#include <vdqm.h>

EXTERN_C int vdqm_admin (vdqmnw_t *, int);
EXTERN_C int vdqm_PingServer (vdqmnw_t *, char *, int);
EXTERN_C int vdqm_Connect (vdqmnw_t **);
EXTERN_C int vdqm_Disconnect (vdqmnw_t **);
EXTERN_C int vdqm_SendVolReq (vdqmnw_t *, int *, char *, char *, char *, char *, int, int);
EXTERN_C int vdqm_UnitStatus (vdqmnw_t *, char *, char *, char *, char *, int *, int *, int);
EXTERN_C int vdqm_DelVolumeReq (vdqmnw_t *, int, char *, char *, char *, char *, int);
EXTERN_C int vdqm_DelDrive (vdqmnw_t *, char *, char *, char *);
EXTERN_C int vdqm_GetClientAddr (char *, char *, int *, int *, int *, int *, char *, char *, char *);
EXTERN_C int vdqm_AcknClientAddr (int, int, int, char *);
EXTERN_C int vdqm_NextDrive (vdqmnw_t **, vdqmDrvReq_t *);
EXTERN_C int vdqm_NextVol (vdqmnw_t **, vdqmVolReq_t *);
EXTERN_C int vdqm_DedicateDrive (vdqmnw_t *, char *, char *, char *, char *);

/*
 * Functions that use VDQM magic number: VDQM2
 */
EXTERN_C int vdqm_SendVolPriority (char*, int, int, int);

/*
 * Functions that use VDQM magic number: VDQM3
 */
EXTERN_C int vdqm_SendDelDrv (char*, char*, char*);
EXTERN_C int vdqm_SendDedicate (char*, char*, char*, char*);

/*
 * Functions that use VDQM magic number: VDQM4
 */
EXTERN_C int vdqm_CreateRequestForAggregator (vdqmnw_t *nw, int *reqID, char *VID, char *dgn, char *server, char *unit, int mode, int client_port);
EXTERN_C int vdqm_QueueRequestForAggregator (vdqmnw_t *nw);

#endif /* _VDQM_API_H */
