/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vdqm_api.h,v $ $Revision: 1.4 $ $Date: 1999/11/19 13:59:55 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * vdqm_api.h - VDQM client API library definitions
 */

#ifndef _VDQM_API_H
#define _VDQM_API_H

#include "vdqm_constants.h"
#include "vdqm.h"

#ifndef EXTERN_C
#if defined(__cplusplus)
#define EXTERN_C extern "C"
#else /* __cplusplus */
#define EXTERN_C extern
#endif /* __cplusplus */
#endif /* EXTERN_C */

#if defined(_WIN32) && defined(_DLL)
#ifdef _EXPORTING
#define DLL_DECL __declspec(dllexport)
#else /* _EXPORTING */
#define DLL_DECL __declspec(dllimport)
#endif /* _EXPORTING */
#else
#define DLL_DECL
#endif

#ifdef __STDC__
EXTERN_C int DLL_DECL vdqm_admin(vdqmnw_t *, int);
EXTERN_C int DLL_DECL vdqm_PingServer(vdqmnw_t *,int);
EXTERN_C int DLL_DECL vdqm_Connect(vdqmnw_t **);
EXTERN_C int DLL_DECL vdqm_Disconnect(vdqmnw_t **);
EXTERN_C int DLL_DECL vdqm_SendVolReq(vdqmnw_t *, int *, char *, char *, char *, char *, int);
EXTERN_C int DLL_DECL vdqm_UnitStatus(vdqmnw_t *, char *, char *, char *, char *, int *, int *, int);
EXTERN_C int DLL_DECL vdqm_DelVolumeReq(vdqmnw_t *, int, char *, char *, char *, char *, int);
EXTERN_C int DLL_DECL vdqm_DelDrive(vdqmnw_t *, char *, char *, char *);
EXTERN_C int DLL_DECL vdqm_GetClientAddr(char *, char *, int *, int *, int *, int *, char *, char *);
EXTERN_C int DLL_DECL vdqm_AcknClientAddr(SOCKET, int, int, char *);
EXTERN_C int DLL_DECL vdqm_NextDrive(vdqmnw_t **, vdqmDrvReq_t *);
EXTERN_C int DLL_DECL vdqm_NextVol(vdqmnw_t **, vdqmVolReq_t *);
#else /* __STDC__ */
EXTERN_C int DLL_DECL vdqm_admin();
EXTERN_C int DLL_DECL vdqm_PingServer();
EXTERN_C int DLL_DECL vdqm_Connect();
EXTERN_C int DLL_DECL vdqm_Disconnect();
EXTERN_C int DLL_DECL vdqm_SendVolReq();
EXTERN_C int DLL_DECL vdqm_UnitStatus();
EXTERN_C int DLL_DECL vdqm_DelVolumeReq();
EXTERN_C int DLL_DECL vdqm_DelDrive();
EXTERN_C int DLL_DECL vdqm_NextDrive();
EXTERN_C int DLL_DECL vdqm_NextVol();
EXTERN_C int DLL_DECL vdqm_GetClientAddr();
EXTERN_C int DLL_DECL vdqm_AcknClientAddr();
#endif /* __STDC__ */

#endif /* _VDQM_API_H */
