/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */


/*
 * $RCSfile: rtcp_api.h,v $ $Revision: 1.14 $ $Date: 2004/03/05 08:41:06 $ CERN IT/ADC Olof Barring
 */

/*
 * rtcp_api.h - rtcopy client API prototypes
 */

#if !defined(RTCP_API_H)
#define RTCP_API_H

#include <osdep.h>
#include <Cuuid.h>
#include <Ctape_constants.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <net.h>
typedef struct rtcpc_sockets {
    SOCKET *listen_socket;
    SOCKET accept_socket;
    SOCKET abort_socket;
    SOCKET *proc_socket[100];
    int nb_proc_sockets;
} rtcpc_sockets_t;


EXTERN_C int DLL_DECL rtcpc_InitReq _PROTO((
                                            rtcpc_sockets_t **,
                                            int *,
                                            tape_list_t *
                                            ));
EXTERN_C int DLL_DECL rtcpc_SelectServer _PROTO((
                                                 rtcpc_sockets_t **,
                                                 tape_list_t *,
                                                 char *,
                                                 int,
                                                 int *
                                                 ));
EXTERN_C int DLL_DECL rtcpc_sendReqList _PROTO((
                                                rtcpHdr_t *,
                                                rtcpc_sockets_t **,
                                                tape_list_t *
                                                ));
EXTERN_C int DLL_DECL rtcpc_sendEndOfReq _PROTO((
                                                 rtcpHdr_t *,
                                                 rtcpc_sockets_t **,
                                                 tape_list_t *
                                                 ));
EXTERN_C int DLL_DECL rtcpc_runReq _PROTO((
                                           rtcpHdr_t *,
                                           rtcpc_sockets_t **,
                                           tape_list_t *
                                           ));
EXTERN_C int DLL_DECL rtcpc _PROTO((
                                    tape_list_t *
                                    ));
EXTERN_C int DLL_DECL rtcp_CallTMS _PROTO((
                                           tape_list_t *,
                                           char *
                                           ));
EXTERN_C void DLL_DECL rtcp_SetErrTxt _PROTO((
                                              int,
                                              char *,
                                              ...
                                              ));
EXTERN_C int DLL_DECL rtcpc_BuildReq _PROTO((
                                             tape_list_t **,
                                             int,
                                             char **
                                             ));
EXTERN_C int DLL_DECL rtcpc_GetDeviceQueues _PROTO((
                                                    char *,
                                                    char *,
                                                    int *,
                                                    int *,
                                                    int *
                                                    ));
EXTERN_C int DLL_DECL rtcp_RetvalSHIFT _PROTO((
                                               tape_list_t *,
                                               file_list_t *,
                                               int *
                                               ));
EXTERN_C void DLL_DECL rtcpc_FreeReqLists _PROTO((
                                                  tape_list_t **
                                                  ));
EXTERN_C int DLL_DECL rtcp_NewTapeList _PROTO((
                                               tape_list_t **,
                                               tape_list_t **,
                                               int
                                               ));
EXTERN_C int DLL_DECL rtcp_NewFileList _PROTO((
                                               tape_list_t **,
                                               file_list_t **,
                                               int
                                               ));
EXTERN_C int DLL_DECL dumpTapeReq _PROTO((
                                          tape_list_t *
                                          ));
EXTERN_C int DLL_DECL dumpFileReq _PROTO((
                                          file_list_t *
                                          ));
EXTERN_C int DLL_DECL rtcpc_CheckRetry _PROTO((
                                               tape_list_t *
                                               ));
EXTERN_C int DLL_DECL rtcpc_kill _PROTO((
                                         void
                                         ));
EXTERN_C int DLL_DECL rtcpc_validCksumAlg _PROTO((
                                                 char *
                                                 ));

#endif /* RTCP_API_H */

