/*
 * $Id: vdqm_api.h,v 1.8 2000/03/09 15:13:00 obarring Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vdqm_api.h,v $ $Revision: 1.8 $ $Date: 2000/03/09 15:13:00 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * vdqm_api.h - VDQM client API library definitions
 */

#ifndef _VDQM_API_H
#define _VDQM_API_H

#include <osdep.h>
#include <vdqm_constants.h>
#include <vdqm.h>

EXTERN_C int DLL_DECL vdqm_admin _PROTO((vdqmnw_t *, int));
EXTERN_C int DLL_DECL vdqm_PingServer _PROTO((vdqmnw_t *, char *, int));
EXTERN_C int DLL_DECL vdqm_Connect _PROTO((vdqmnw_t **));
EXTERN_C int DLL_DECL vdqm_Disconnect _PROTO((vdqmnw_t **));
EXTERN_C int DLL_DECL vdqm_SendVolReq _PROTO((vdqmnw_t *, int *, char *, char *, char *, char *, int, int));
EXTERN_C int DLL_DECL vdqm_UnitStatus _PROTO((vdqmnw_t *, char *, char *, char *, char *, int *, int *, int));
EXTERN_C int DLL_DECL vdqm_DelVolumeReq _PROTO((vdqmnw_t *, int, char *, char *, char *, char *, int));
EXTERN_C int DLL_DECL vdqm_DelDrive _PROTO((vdqmnw_t *, char *, char *, char *));
EXTERN_C int DLL_DECL vdqm_GetClientAddr _PROTO((char *, char *, int *, int *, int *, int *, char *, char *));
EXTERN_C int DLL_DECL vdqm_AcknClientAddr _PROTO((SOCKET, int, int, char *));
EXTERN_C int DLL_DECL vdqm_NextDrive _PROTO((vdqmnw_t **, vdqmDrvReq_t *));
EXTERN_C int DLL_DECL vdqm_NextVol _PROTO((vdqmnw_t **, vdqmVolReq_t *));

#endif /* _VDQM_API_H */
