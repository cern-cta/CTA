/*
 * $Id: rtcp_api.h,v 1.3 1999/12/09 13:46:20 jdurand Exp $
 */

#if !defined(RTCP_API_H)
#define RTCP_API_H

#include <osdep.h>
#include <Ctape_constants.h>
#include <rtcp_constants.h>
#include <rtcp.h>

EXTERN_C int DLL_DECL rtcpc _PROTO((tape_list_t *));
EXTERN_C int DLL_DECL rtcpcCallTMS _PROTO((tape_list_t *));
EXTERN_C void DLL_DECL rtcp_SetErrTxt _PROTO((int, char *, ...));
EXTERN_C int DLL_DECL rtcpc_BuildReq _PROTO((tape_list_t **, int, char **));
EXTERN_C int DLL_DECL dumpTapeReq _PROTO((tape_list_t *));
EXTERN_C int DLL_DECL dumpFileReq _PROTO((file_list_t *));

#endif /* RTCP_API_H */

