/******************************************************************************
 *                      migrator.c
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
 * @(#)$RCSfile: migrator.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/11 16:18:24 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: migrator.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/11 16:18:24 $ Olof Barring";
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
#include <castor/Constants.h>
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

Cuuid_t childUuid, mainUuid;
int inChild = 1;

int migratorCallback(
                     tapereq,
                     filereq
                     )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  char buf[1024];
  fprintf(stdout,"file name for %s.%d ?\n",
          tapereq->vid,filereq->tape_fseq);
  fflush(stdout);
  scanf("%s",buf);
  if ( (*buf == '\0') || (strcmp(buf,".") == 0) ) {
    filereq->proc_status = RTCP_FINISHED;
  } else {
    strcpy(filereq->file_path,buf);
    filereq->proc_status = RTCP_WAITING;
    strcpy(filereq->recfm,"F");
    filereq->concat = NOCONCAT;
    filereq->convert = ASCCONV;
    filereq->def_alloc = 0;
  }
  return(0);
}

/** myDispatch() - FILE request dispatch function to be used by RTCOPY API
 *
 * @param func - function to be dispatched
 * @param arg - pointer to opaque function argument
 *
 * myDispatch() creates a thread for dispatching the processing of FILE request
 * callbacks in the extended RTCOPY API. This allows for handling weakly
 * blocking callbacks like tppos to not be serialized with potentially strongly
 * blocking callbascks like filcp or request for more work.
 * 
 * Note that the Cthread_create_detached() API cannot be passed directly in
 * the RTCOPY API because it is a macro and not a proper function prototype.
 *
 * @return -1 = error otherwise the Cthread id (>=0)
 */
int myDispatch(
               func,
               arg
               )
     void *(*func)(void *);
     void *arg;
{
  return(Cpool_assign(
                      callbackThreadPool,
                      func,
                      arg,
                      -1
                      ));  
}

static int initThreadPool() 
{
  int poolsize;
  char *p;

  if ( (p = getenv("MIGRATOR_THREAD_POOL")) != (char *)NULL ) {
    poolsize = atoi(p);
  } else if ( ( p = getconfent("MIGRATOR","THREAD_POOL",0)) != (char *)NULL ) {
    poolsize = atoi(p);
  } else {
#if defined(RTCPD_THREAD_POOL)
    poolsize = RTCPD_THREAD_POOL;
#else /* RTCPD_THREAD_POOL */
    poolsize = 3;         /* Set some reasonable default */
#endif /* RTCPD_TRHEAD_POOL */
  }
  callbackThreadPool = Cpool_create(poolsize,&poolsize);
  if ( (callbackThreadPool == -1) || (poolsize <= 0) ) return(-1);
  return(0);
}

int main(
         argc,
         argv
         )
     int argc;
     char **argv;
{
  tape_list_t *tape = NULL;
  char *vidChildFacility = MIGRATOR_FACILITY_NAME, cmdline[CA_MAXLINELEN+1];
  SOCKET sock = 0;
  int rc, c, i, save_serrno;

  /*
   * If we are started by the rtcpclientd, the main accept socket has been
   * duplicated to file descriptor 0
   */
  SOCKET origSocket = 0;

  /* Initializing the C++ log */
  /* Necessary at start of program and after any fork */
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);

  Cuuid_create(
               &childUuid
               );
  
  /*
   * Initialise DLF
   */
  (void)rtcpcld_initLogging(
                            vidChildFacility
                            );
  cmdline[0] = '\0';
  c=0;
  for (i=0; (i<CA_MAXLINELEN) && (c<argc);) {
    strcat(cmdline,argv[c++]);
    strcat(cmdline," ");
    i = strlen(cmdline)+1;
  }
  
  (void)dlf_write(
                  childUuid,
                  DLF_LVL_SYSTEM,
                  RTCPCLD_MSG_MIGRATOR_STARTED,
                  (struct Cns_fileid *)NULL,
                  1,
                  "COMMAND",
                  DLF_MSG_PARAM_STR,
                  cmdline
                  );

  /*
   * Create our tape list
   */
  rc = rtcp_NewTapeList(&tape,NULL,WRITE_ENABLE);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcp_NewTapeList()");
    return(-1);
  }

  rc = rtcpcld_parseWorkerCmd(
                              argc,
                              argv,
                              tape,
                              &sock
                              );
  if ( rc == 0 ) {
    rc = initThreadPool();
    if ( rc == -1 ) {
      LOG_SYSCALL_ERR("initThreadPool()");
    } else {
      rc = rtcpcld_runWorker(
                             tape,
                             &sock,
                             myDispatch,
                             migratorCallback
                             );
    }
  }
  if ( rc == -1 ) save_serrno = serrno;

  rc = rtcpcld_workerFinished(
                              tape,
                              rc,
                              save_serrno
                              );
  return(rc);
}
