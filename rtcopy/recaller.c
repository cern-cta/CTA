/******************************************************************************
 *                      recaller.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: recaller.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/11 16:18:24 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: recaller.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/11 16:18:24 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#endif /* _WIN32 */
#include <sys/stat.h>
#include <errno.h>
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <common.h>
#include <Cgetopt.h>
#include <Cinit.h>
#include <Cuuid.h>
#include <Cpwd.h>
#include <Cnetdb.h>
#include <Cpool_api.h>
#include <dlf_api.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <vmgr_api.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld.h>
#include <rtcpcld_messages.h>
#include "castor/Constants.h"
extern int rtcpc_runReq_ext _PROTO((
                                    rtcpHdr_t *,
                                    rtcpc_sockets_t **,
                                    tape_list_t *,
                                    int (*)(void *(*)(void *), void *)
                                    ));
extern int (*rtcpc_ClientCallback) _PROTO((
                                           rtcpTapeRequest_t *, 
                                           rtcpFileRequest_t *
                                           ));

static int callbackThreadPool = -1;
static void *segmCountLock = NULL;
static int segmSubmitted = 0, segmCompleted = 0, segmFailed = 0;
static void *abortLock = NULL;
static int requestAborted = 0;
static int segmentFailed = 0;

Cuuid_t childUuid, mainUuid;
tape_list_t *vidChildTape = NULL;

int inChild = 1;

int main(
         argc,
         argv
         )
     int argc;
     char **argv;
{
  
  return(0);
}
