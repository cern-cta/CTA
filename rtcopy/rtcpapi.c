/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpapi.c - RTCOPY client API library
 */

#include <stdlib.h>
#include <time.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Cmutex.h>
#include <Cnetdb.h>
#include <Ctape_api.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcp_server.h>
#include <vdqm_api.h>
#include <common.h>
#include <ctype.h>
#include <serrno.h>

extern int rtcp_CleanUp (int **, int);
extern int rtcp_SendReq (int *, rtcpHdr_t *, rtcpClientInfo_t *, 
			 rtcpTapeRequest_t *, rtcpFileRequest_t *);
extern int rtcp_RecvAckn (int *, int);
extern int rtcp_CloseConnection (int *);
