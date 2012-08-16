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
    int *listen_socket;
    int accept_socket;
    int abort_socket;
    int *proc_socket[100];
    int nb_proc_sockets;
} rtcpc_sockets_t;


EXTERN_C int rtcpc_InitReq (
			    rtcpc_sockets_t **,
			    int *,
			    tape_list_t *
			    );
EXTERN_C int rtcpc_SelectServer (
				 rtcpc_sockets_t **,
				 tape_list_t *,
				 char *,
				 int,
				 int *
				 );
EXTERN_C int rtcpc_sendReqList (
				rtcpHdr_t *,
				rtcpc_sockets_t **,
				tape_list_t *
				);
EXTERN_C int rtcpc_sendEndOfReq (
				 rtcpHdr_t *,
				 rtcpc_sockets_t **,
				 tape_list_t *
				 );
EXTERN_C int rtcpc_runReq (
			   rtcpHdr_t *,
			   rtcpc_sockets_t **,
			   tape_list_t *
			   );
EXTERN_C int rtcpc (
		    tape_list_t *
		    );
EXTERN_C int rtcp_CallTMS (
			   tape_list_t *,
			   char *
			   );
EXTERN_C void rtcp_SetErrTxt (
			      int,
			      char *,
			      ...
			      );
EXTERN_C int rtcpc_BuildReq (
			     tape_list_t **,
			     int,
			     char **
			     );
EXTERN_C int rtcpc_GetDeviceQueues (
				    char *,
				    char *,
				    int *,
				    int *,
				    int *
				    );
EXTERN_C int rtcp_RetvalSHIFT (
			       tape_list_t *,
			       file_list_t *,
			       int *
			       );
EXTERN_C void rtcpc_FreeReqLists (
				  tape_list_t **
				  );
EXTERN_C int rtcp_NewTapeList (
			       tape_list_t **,
			       tape_list_t **,
			       int
			       );
EXTERN_C int rtcp_NewFileList (
			       tape_list_t **,
			       file_list_t **,
			       int
			       );
EXTERN_C int rtcpc_InitDumpTapeReq (
				    rtcpDumpTapeRequest_t *
				    );
EXTERN_C int rtcpc_BuildDumpTapeReq (
				     tape_list_t **,
				     int,
				     char *[]

				     );
EXTERN_C int dumpTapeReq (
			  tape_list_t *
			  );
EXTERN_C int dumpFileReq (
			  file_list_t *
			  );
EXTERN_C int rtcpc_CheckRetry (
			       tape_list_t *
			       );
EXTERN_C int rtcpc_kill (
			 void
			 );
EXTERN_C int rtcpc_validCksumAlg (
				  char *
				  );

#endif /* RTCP_API_H */

