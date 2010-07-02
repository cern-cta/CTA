/*
 * $Id: vdqm_api.h,v 1.17 2008/10/11 11:33:34 murrayc3 Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vdqm_api.h,v $ $Revision: 1.17 $ $Date: 2008/10/11 11:33:34 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * vdqm_api.h - VDQM client API library definitions
 */

#ifndef _VDQM_API_H
#define _VDQM_API_H

#include <osdep.h>
#include <vdqm_constants.h>
#include <vdqm.h>

EXTERN_C int vdqm_admin _PROTO((vdqmnw_t *, int));
EXTERN_C int vdqm_PingServer _PROTO((vdqmnw_t *, char *, int));
EXTERN_C int vdqm_Connect _PROTO((vdqmnw_t **));
EXTERN_C int vdqm_Disconnect _PROTO((vdqmnw_t **));
EXTERN_C int vdqm_SendVolReq _PROTO((vdqmnw_t *, int *, char *, char *, char *, char *, int, int));
EXTERN_C int vdqm_UnitStatus _PROTO((vdqmnw_t *, char *, char *, char *, char *, int *, int *, int));
EXTERN_C int vdqm_DelVolumeReq _PROTO((vdqmnw_t *, int, char *, char *, char *, char *, int));
EXTERN_C int vdqm_DelDrive _PROTO((vdqmnw_t *, char *, char *, char *));
EXTERN_C int vdqm_GetClientAddr _PROTO((char *, char *, int *, int *, int *, int *, char *, char *, char *));
EXTERN_C int vdqm_AcknClientAddr _PROTO((SOCKET, int, int, char *));
EXTERN_C int vdqm_NextDrive _PROTO((vdqmnw_t **, vdqmDrvReq_t *));
EXTERN_C int vdqm_NextVol _PROTO((vdqmnw_t **, vdqmVolReq_t *));
EXTERN_C int vdqm_DedicateDrive _PROTO((vdqmnw_t *, char *, char *, char *, char *));

/*
 * Functions that use VDQM magic number: VDQM2
 */
EXTERN_C int vdqm_SendVolPriority _PROTO((char*, int, int, int));

/*
 * Functions that use VDQM magic number: VDQM3
 */
EXTERN_C int vdqm_SendDelDrv _PROTO((char*, char*, char*));
EXTERN_C int vdqm_SendDedicate _PROTO((char*, char*, char*, char*));

/*
 * Functions that use VDQM magic number: VDQM4
 */
EXTERN_C int vdqm_CreateRequestForAggregator _PROTO((vdqmnw_t *nw, int *reqID, char *VID, char *dgn, char *server, char *unit, int mode, int client_port));
EXTERN_C int vdqm_QueueRequestForAggregator _PROTO((vdqmnw_t *nw));

#endif /* _VDQM_API_H */
