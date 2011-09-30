/******************************************************************************
 *                 rtcopy/rtcpd_tellClient.c
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
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include <stdlib.h>
#include <time.h>
#include <sys/param.h>
#include <sys/types.h>      /* Standard data types                */
#include <netdb.h>          /* Network "data base"                */
#include <sys/socket.h>
#include <netinet/in.h>     /* Internet data types                */
#include <netinet/tcp.h>    /* S. Murray 31/03/09 TCP definitions */
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>

#include <Castor_limits.h>
#include <net.h>
#include <log.h>
#include <osdep.h>
#include <Cnetdb.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>


/*
 * Process global used in the server when replying to clients. The
 * value should be set once in rtcpd_MainCntl() upon reception of the
 * first client message.
 *
 * This process global variable is defined (memory allocated for it) in
 * rtcopy/rtcp_SendRecv.c
 */
extern int client_magic;

extern int AbortFlag;


/** tellClient() - send updated request information to client (server only)
 *
 * @param SOCKET *client_socket - pointer to the client socket
 * @param tape_list_t *tape - tape list element to be sent to client
 * @param file_list_t *file - file list element to be sent to client
 * @param int status - input flag for error condition
 *
 * Send request processing update to client. The RTCOPY client/server
 * protocol imposes that the client first send the full request to
 * the server. The server will then send back the same request
 * structures one by one as the processing progresses. 
 * Normally each tape request structure is sent back twice: 
 *  1) after mount (to return unit name) and 
 *  2) after processing associated with that tape request has finished 
 *     (successfully or not). 
 * File requests are normally also returned twice: 
 *  1) after position and 
 *  2) after finished copying (succesfully or not).
 * There might be interleaved messages if there are local retries. For
 * tape read request, the tapeIOthread only returns the file request
 * after position. It is up to the diskIOthread (rtcp_Disk.c) to 
 * update after finished copying. For tape write request it is the
 * tapeIOthread which returns both messages.
 *
 * The tellClient() routine only handles a single tape or file request
 * (not both at the same time) per call. This means that exactly one
 * of either tape or file should normally be == NULL. If both are specified
 * and status != 0 we check for the error condition and send the associated
 * request structure (normally there should only be one of them). If
 * both are specified with status == 0 the file request is sent. 
 *
 * Don't do anything in case of a client abort. He's already closed and
 * shutdown his side.
 *
 * If it is a file request update, check if client has flagged the request
 * for more work (proc_status == RTCP_REQUEST_MORE_WORK). If so, enter
 * rtcpd_GetRequestList() to receive the request updates.
 *
 */
int tellClient(
               SOCKET *client_socket,
               tape_list_t *tape, 
               file_list_t *file,
               int status
               ) {
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;
  tape_list_t *tlTmp = NULL;
  rtcpHdr_t hdr;
  int rc, save_serrno;

  if ( client_socket == NULL ||
       *client_socket == INVALID_SOCKET ) {
    serrno = EINVAL;
    return(-1);
  }
  /*
   * No use trying to contact client if he has sent an abort...
   */
  if ( AbortFlag == 1 ) return(0);

  if ( tape != NULL ) tapereq = &tape->tapereq;
  if ( file != NULL ) filereq = &file->filereq;

  if ( tapereq == NULL && filereq == NULL ) {
    hdr.reqtype = RTCP_ENDOF_REQ;
    hdr.len = 0;
  }
  if ( tapereq != NULL && filereq != NULL ) {
    if ((tapereq->err.severity & (RTCP_FAILED|RTCP_RESELECT_SERV))!=0)
      filereq = NULL;
    else if ((filereq->err.severity & (RTCP_FAILED|RTCP_RESELECT_SERV))!=0)
      tapereq = NULL;
  }

  if ( tapereq != NULL ) {
    hdr.reqtype = RTCP_TAPE_REQ;
    if ( status != 0 || tapereq->tprc != 0 || 
         tapereq->err.severity != RTCP_OK ||
         *tapereq->err.errmsgtxt != '\0' ) { 
      hdr.reqtype = RTCP_TAPEERR_REQ;
      filereq = NULL;
    }

    if ( (tapereq->err.severity & (RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 &&
         *tapereq->err.errmsgtxt != '\0' ) 
      rtcp_log(
               LOG_ERR,
               "%s\n",
               tapereq->err.errmsgtxt
               );
     
    tapereq->tprc = status;
  }

  if ( filereq != NULL ) {
    hdr.reqtype = RTCP_FILE_REQ;
    if ( status != 0 || filereq->cprc != 0 || 
         filereq->err.severity != RTCP_OK ||
         *filereq->err.errmsgtxt != '\0' ) { 
      hdr.reqtype = RTCP_FILEERR_REQ;
      tapereq = NULL;
    }

    if ( (filereq->err.severity & (RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 &&
         *filereq->err.errmsgtxt != '\0' ) 
      rtcp_log(
               LOG_ERR,
               "%s\n",
               filereq->err.errmsgtxt
               );

    filereq->cprc = status;
  }
  if ( tapereq != NULL && filereq != NULL ) tapereq = NULL;

  if ( tapereq != NULL || filereq != NULL ) {
    rtcp_log(
             LOG_DEBUG,
             "tellClient(%d) send %s request, status=%d, rc=%d\n",
             *client_socket,
             (tapereq != NULL ? "tapereq" : "filereq"),
             status,
             (tapereq != NULL ? tapereq->tprc : filereq->cprc)
             );
    rtcp_log(
             LOG_DEBUG,
             "tellClient(%d) msg=%s\n",
             *client_socket,
             (tapereq != NULL ? tapereq->err.errmsgtxt : 
              filereq->err.errmsgtxt)
             );
    if ( filereq != NULL ) rtcp_log(
                                    LOG_DEBUG,
                                    "tellClient(%d) send proc_status=%d\n",
                                    *client_socket,
                                    filereq->proc_status
                                    );
  } else {
    rtcp_log(
             LOG_DEBUG,
             "tellClient(%d) send EOR to client\n",
             *client_socket
             );
  }
 
  if ( client_magic != 0 ) hdr.magic = client_magic;
  else hdr.magic = RTCOPY_MAGIC;
  rc = rtcp_SendReq(
                    client_socket,
                    &hdr,
                    NULL,
                    tapereq,
                    filereq
                    );
  if ( rc == -1 ) {
    save_serrno = serrno;
    rtcp_log(
             LOG_ERR,
             "tellClient(%d) rtcp_SendReq(): %s\n",
             *client_socket,
             sstrerror(serrno)
             );
    rtcpd_SetReqStatus(
                       tape,
                       file,
                       save_serrno,
                       RTCP_SYERR | RTCP_FAILED
                       );
    serrno = save_serrno;
    return(-1);
  }
  /*
   * If client asks for more work, receive all the new work before
   * the acknowledge of RTCP_REQUEST_MORE_WORK
   */
  if ( (hdr.reqtype == RTCP_FILE_REQ) && 
       (filereq->proc_status == RTCP_REQUEST_MORE_WORK) ) {
    /*
     * Client wants to add more file requests. Should be safe
     * to hook them on here because the caller should have
     * made sure that the request list is locked starting
     * from this element
     */
    if ( tape != NULL ) {
      rtcp_log(
               LOG_DEBUG,
               "tellClient(%d) get list of new requests from client\n",
               *client_socket
               );
      tlTmp = tape;
      rc = rtcpd_GetRequestList(
                                client_socket, 
                                NULL, 
                                NULL,
                                &tlTmp,
                                file
                                );
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(
                 LOG_ERR,
                 "tellClient(%d) rtcpd_GetRequestList(): %s\n",
                 *client_socket,
                 sstrerror(serrno)
                 );
        rtcpd_SetReqStatus(
                           tape,
                           file,
                           save_serrno,
                           RTCP_UNERR | RTCP_FAILED
                           );
        serrno = save_serrno;
        return(-1);
      }
    } else {
      rtcp_log(
               LOG_ERR,
               "tellClient(%d) RTCP_REQUEST_MORE_WORK without tape list!\n",
               *client_socket
               );
      rtcpd_SetReqStatus(
                         tape,
                         file,
                         SEINTERNAL,
                         RTCP_UNERR|RTCP_FAILED
                         );
      return(-1);
    }
        
  }
  rc = rtcp_RecvAckn(
                     client_socket,
                     hdr.reqtype
                     );
  if ( rc == -1 ) {
    save_serrno = serrno;
    rtcp_log(
             LOG_ERR,
             "tellClient(%d) rtcp_RecvAckn(): %s\n",
             *client_socket,
             sstrerror(serrno)
             );
    rtcpd_SetReqStatus(
                       tape,
                       file,
                       save_serrno,
                       RTCP_SYERR | RTCP_FAILED
                       );
    serrno = save_serrno;
    return(-1);
  } else if ( rc != hdr.reqtype ) {
    rtcp_log(
             LOG_ERR,
             "tellClient(%d) Ackn reqtype mismatch 0x%x <-> 0x%x\n",
             *client_socket,
             rc,
             hdr.reqtype
             );
    save_serrno = SEINTERNAL;
    rtcpd_SetReqStatus(
                       tape,
                       file,
                       save_serrno,
                       RTCP_SYERR | RTCP_FAILED
                       );
    serrno = save_serrno;
    return(-1);
  }
  return(0);
}
