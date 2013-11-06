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
#define RFIO_KERNEL 1
#include <rfio_api.h>
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

/*
 * Kill mechanism is not thread-safe if several threads run rtcpc() in
 * parallel. It is used by rtcopy server when running old SHIFT clients.
 */
static rtcpc_sockets_t *last_socks = NULL;
static tape_list_t *last_tape = NULL;
static int rtcpc_killed = 0;

static int rtcpc_ResetKillInfo() {
  last_socks = NULL;
  last_tape = NULL;
  return(0);
}

int rtcpc_finished(rtcpc_sockets_t **socks, rtcpHdr_t *hdr, tape_list_t *tape) {
  int i, rc, retval, save_serrno;

  if ( rtcpc_killed == 0 ) (void)rtcpc_ResetKillInfo();
  rc = retval = save_serrno = 0;
  if ( socks != NULL && *socks != NULL ) {
    if ( (*socks)->abort_socket != -1 && hdr != NULL ) {
      hdr->magic = RTCOPY_MAGIC;
      hdr->len = -1;
      rc = rtcp_SendReq(&((*socks)->abort_socket),hdr,NULL,NULL,NULL);
      if ( hdr->reqtype == RTCP_ENDOF_REQ ) {
        if ( rc == -1 ) {
          save_serrno = serrno;
          rtcp_log(LOG_ERR,"rtcpc_finished() rtcp_SendReq(ENDOF_REQ): %s\n",
                   sstrerror(serrno));
        } else {
          rtcp_log(LOG_DEBUG,
                   "rtcpc_finished(): Receive acknowledge\n");
          rc = rtcp_RecvAckn(&((*socks)->abort_socket),hdr->reqtype);
          if ( rc == -1 ) {
            save_serrno = serrno;
            rtcp_log(LOG_ERR,"rtcpc_finished() rtcp_RecvAckn(TAPE_REQ): %s\n",
                     sstrerror(serrno));
          }
        }
      }
      (void)rtcp_CloseConnection(&((*socks)->abort_socket));
      if ( rc == -1 ) retval = -1;
    }
    for (i=0; i<100; i++) {
      if ( (*socks)->proc_socket[i] != NULL &&
           *((*socks)->proc_socket[i]) != -1 ) {
        (void)rtcp_CloseConnection((*socks)->proc_socket[i]);
        free((*socks)->proc_socket[i]);
        (*socks)->proc_socket[i] = NULL;
      }
    }
    (void)rtcp_CleanUp(&((*socks)->listen_socket),0);
    free(*socks);
    *socks = NULL;
  } 

  /*
   * We only cancel VDQM job if it hasn't been assigned. Otherwise
   * it is automatically cancelled when the tape server releases the drive.
   */
  if ( tape != NULL && tape->tapereq.VolReqID > 0 &&
       tape->tapereq.TStartRtcpd <= 0 ) {
    rtcp_log(LOG_DEBUG,"rtcpc_finished() cancelling VolReqID %d, VID: %s dgn: %s\n",
             tape->tapereq.VolReqID,tape->tapereq.vid,tape->tapereq.dgn);
    rc = vdqm_DelVolumeReq(NULL,tape->tapereq.VolReqID,tape->tapereq.vid,
                           tape->tapereq.dgn,NULL,NULL,0);
    rtcp_log(LOG_DEBUG,"rtcpc_finished() vdqm_DelVolumeReq() returned rc=%d %s\n",
             rc,(rc != 0 ? sstrerror(serrno) : ""));
    serrno = save_serrno;
  }
  (void)rtcpc_ResetKillInfo();
  rc = retval;
  if ( save_serrno != 0 ) serrno = save_serrno;
  return(rc);
}

int rtcpc_kill() {
  rtcpHdr_t hdr;
  hdr.reqtype = RTCP_ABORT_REQ;
  rtcpc_killed = 1;
  return(rtcpc_finished(&last_socks,&hdr,last_tape));
}
