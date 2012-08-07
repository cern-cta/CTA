/*
 * $Id: rtcp_SendRecv.c,v 1.12 2009/06/11 09:39:13 murrayc3 Exp $
 *
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcp_SendRecv.c - Send and receive RTCOPY request and acknowledge messages.
 */

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
#include <marshall.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>
#ifdef RTCOPYCSEC
#include "Csec_api.h"
#endif

#define REQTYPE(Y,X) ( X == RTCP_##Y##_REQ || X == RTCP_##Y##ERR_REQ )

typedef enum direction {SendTo,ReceiveFrom} direction_t;

#define marshall_BLOCKID(X,Y) {marshall_OPAQUE(X,Y,4);} 
#define unmarshall_BLOCKID(X,Y) {unmarshall_OPAQUE(X,Y,4);}

#define marshall_STRN(X,Y) {if ( strlen(Y) > sizeof(Y)-1 ) {rtcp_log(LOG_ERR,"rtcp_Transfer() marshall_STRN(%s,%d) failed\n",#Y,sizeof(Y)-1); serrno = E2BIG; return(-1);} else marshall_STRING(X,Y);}
#define unmarshall_STRN(X,Y) {if ( unmarshall_STRINGN(X,Y,sizeof(Y)) == -1 ) {rtcp_log(LOG_ERR,"rtcp_Transfer(): unmarshall_STRN(%s,%d) failed\n",#Y,sizeof(Y)); serrno = E2BIG; return(-1);}}

#define DO_MARSHALL(X,Y,Z,W) { \
    if ( W == SendTo ) {marshall_##X(Y,Z);} \
    else {unmarshall_##X(Y,Z);} }

/*
 * Process global used in the server when replying to clients. The
 * value should be set once in rtcpd_MainCntl() upon reception of the
 * first client message.
 */
int client_magic = 0;

static int rtcp_Transfer(SOCKET *s,
                         rtcpHdr_t *hdr,
                         rtcpClientInfo_t *client,
                         rtcpTapeRequest_t *tapereq,
                         rtcpFileRequest_t *filereq,
                         direction_t whereto) {

  rtcpErrMsg_t err;
  char common_buffer[RTCP_MSGBUFSIZ];
  char vmsopt_dummy[RTCP_VMSOPTLEN+1];
  char *hdrbuf, *buf, *tmp;
  char *p,unit[CA_MAXUNMLEN+1];
  int magic, reqtype, len, dummyInt;
  int rc, save_serrno;

  if ( s == NULL || *s == INVALID_SOCKET ) {
    serrno = EINVAL;
    return(-1);
  }

  reqtype = -1;
  len = magic = 0;
  if ( hdr != NULL ) magic = hdr->magic;
  else magic = client_magic;
  vmsopt_dummy[0] = '\0';

  /*
   * Need common buffer because of vdqm_GetClienAddr()
   */
  hdrbuf = &common_buffer[0];
  buf = &common_buffer[RTCP_HDRBUFSIZ];

  if ( whereto == ReceiveFrom ) {
    rc = netread_timeout(*s,hdrbuf,RTCP_HDRBUFSIZ,RTCP_NETTIMEOUT);
    switch (rc) {
    case -1:
      rtcp_log(LOG_ERR,"rtcp_Transfer() netread(%d,HDR): %s\n",*s,
               neterror());
      serrno = SECOMERR;
      return(-1);
    case 0:
      rtcp_log(LOG_ERR,"rtcp_Transfer() netread(%d,HDR): connection dropped\n",*s);
      serrno = SECONNDROP;
      return(-1);
    }
  }
  p = hdrbuf;
  DO_MARSHALL(LONG,p,magic,whereto);
  DO_MARSHALL(LONG,p,reqtype,whereto);
  DO_MARSHALL(LONG,p,len,whereto);
  if ( whereto == ReceiveFrom ) {
    rtcp_log(LOG_DEBUG,"rtcp_Transfer() received hdr magic: 0x%x type: 0x%x length: %d\n",magic,reqtype,len);
    if ( !VALID_MAGIC(magic) ) {
      rtcp_log(LOG_ERR,"rtcp_Transfer() invalid magic number 0x%x\n",
               magic);
      serrno = SEBADVERSION;
      return(-1);
    }
    if ( hdr != NULL ) {
      hdr->magic = magic;
      hdr->reqtype = reqtype;
      hdr->len = len;
    }
    /*
     * Check if it is an old client.
     */
    if ( !REQTYPE(TAPE,reqtype) && !REQTYPE(FILE,reqtype) &&
         reqtype != VDQM_CLIENTINFO ) {
      /*
       * Old client requests are handled separately. Here we
       * just return the request type.
       */
      return(reqtype);
    }
  }
  if ( VALID_MSGLEN(len) ) {
    rc = netread_timeout(*s,buf,len,RTCP_NETTIMEOUT);
    switch (rc) {
    case -1:
      rtcp_log(LOG_ERR,"rtcp_Transfer() netread(%d,REQ): %s\n",*s,
               neterror());
      serrno = SECOMERR;
      return(-1);
    case 0:
      rtcp_log(LOG_ERR,"rtcp_Transfer() netread(%d,REQ): connection dropped\n",*s);
      serrno = SECONNDROP;
      return(-1);
    }
  } else if ( len > 0 ) {
    rtcp_log(LOG_ERR,"rtcp_Transfer() invalid msg length %d\n",len);
    serrno = SEUMSG2LONG;
    return(-1);
  }

  if ( (REQTYPE(TAPE,reqtype) && tapereq == NULL) ||
       (REQTYPE(FILE,reqtype) && filereq == NULL ) ) {
    rtcp_log(LOG_ERR,"rtcp_Transfer(): no buffer for reqtype=0x%x\n",
             reqtype);
    serrno = EINVAL;
    return(-1);
  }
  if ( whereto == SendTo ) {
    if ( hdr != NULL && RTCP_VALID_REQTYPE(hdr->reqtype) ) 
      reqtype  = hdr->reqtype;
    else if ( tapereq != NULL ) reqtype = RTCP_TAPE_REQ;
    else if ( filereq != NULL ) reqtype = RTCP_FILE_REQ;
    else {
      rtcp_log(LOG_ERR,"rtcp_Transfer(): cannot determine request type to send\n");
      serrno = SEOPNOTSUP;
      return(-1);
    }
  }
 
  rtcp_log(LOG_DEBUG,"rtcp_Transfer(): client_magic=0x%x, magic=0x%x\n",
           client_magic,magic);
  p = buf;
  if ( REQTYPE(TAPE,reqtype) && tapereq != NULL ) {
    DO_MARSHALL(STRN,p,tapereq->vid,whereto);
    DO_MARSHALL(STRN,p,tapereq->vsn,whereto);
    DO_MARSHALL(STRN,p,tapereq->label,whereto);
    DO_MARSHALL(STRN,p,tapereq->devtype,whereto);
    DO_MARSHALL(STRN,p,tapereq->density,whereto);
    /*
     * We need to remember the unit name since some
     * messages may pass without it
     */
    *unit = '\0';
    if ( whereto == ReceiveFrom && *tapereq->unit != '\0' ) 
      strcpy(unit,tapereq->unit);
    DO_MARSHALL(STRN,p,tapereq->unit,whereto);
    if ( whereto == ReceiveFrom && *tapereq->unit == '\0' ) 
      strcpy(tapereq->unit,unit);
    DO_MARSHALL(LONG,p,tapereq->VolReqID,whereto);
    DO_MARSHALL(LONG,p,tapereq->jobID,whereto);
    DO_MARSHALL(LONG,p,tapereq->mode,whereto);
    DO_MARSHALL(LONG,p,tapereq->start_file,whereto);
    DO_MARSHALL(LONG,p,tapereq->end_file,whereto);
    DO_MARSHALL(LONG,p,tapereq->side,whereto);
    DO_MARSHALL(LONG,p,tapereq->tprc,whereto);
    DO_MARSHALL(LONG,p,tapereq->TStartRequest,whereto);
    DO_MARSHALL(LONG,p,tapereq->TEndRequest,whereto);
    DO_MARSHALL(LONG,p,tapereq->TStartRtcpd,whereto);
    DO_MARSHALL(LONG,p,tapereq->TStartMount,whereto);
    DO_MARSHALL(LONG,p,tapereq->TEndMount,whereto);
    DO_MARSHALL(LONG,p,tapereq->TStartUnmount,whereto);
    DO_MARSHALL(LONG,p,tapereq->TEndUnmount,whereto);
    if ( magic > RTCOPY_MAGIC_OLD0 ) {
      DO_MARSHALL(UUID,p,tapereq->rtcpReqId,whereto);
      tmp = rtcp_voidToString((void *)&(tapereq->rtcpReqId),sizeof(Cuuid_t));
      rtcp_log(LOG_DEBUG,"rtcp_Transfer() %s rtcpReqId=%s\n",
               (whereto == SendTo ? "sent" : "received"),
               (tmp == NULL ? "(null)" : tmp));
      if ( tmp != NULL ) free(tmp);
    }
  }
  if ( REQTYPE(FILE,reqtype) && filereq != NULL ) {
    DO_MARSHALL(STRN,p,filereq->file_path,whereto);
    DO_MARSHALL(STRN,p,filereq->tape_path,whereto);
    DO_MARSHALL(STRN,p,filereq->recfm,whereto);
    DO_MARSHALL(STRN,p,filereq->fid,whereto);
    DO_MARSHALL(STRN,p,filereq->ifce,whereto);
    DO_MARSHALL(STRN,p,filereq->stageID,whereto);
    if ( magic <= RTCOPY_MAGIC_OLD0) DO_MARSHALL(STRN,p,vmsopt_dummy,whereto);
    DO_MARSHALL(LONG,p,filereq->VolReqID,whereto);
    DO_MARSHALL(LONG,p,filereq->jobID,whereto);
    DO_MARSHALL(LONG,p,filereq->stageSubreqID,whereto);
    DO_MARSHALL(LONG,p,filereq->umask,whereto);
    if ( magic <= RTCOPY_MAGIC_OLD0 ) DO_MARSHALL(LONG,p,dummyInt,whereto);
    DO_MARSHALL(LONG,p,filereq->position_method,whereto);
    DO_MARSHALL(LONG,p,filereq->tape_fseq,whereto);
    DO_MARSHALL(LONG,p,filereq->disk_fseq,whereto);
    DO_MARSHALL(LONG,p,filereq->blocksize,whereto);
    DO_MARSHALL(LONG,p,filereq->recordlength,whereto);
    DO_MARSHALL(LONG,p,filereq->retention,whereto);
    DO_MARSHALL(LONG,p,filereq->def_alloc,whereto);
    DO_MARSHALL(LONG,p,filereq->rtcp_err_action,whereto);
    DO_MARSHALL(LONG,p,filereq->tp_err_action,whereto);
    DO_MARSHALL(LONG,p,filereq->convert,whereto);
    DO_MARSHALL(LONG,p,filereq->check_fid,whereto);
    DO_MARSHALL(LONG,p,filereq->concat,whereto);
    DO_MARSHALL(LONG,p,filereq->proc_status,whereto);
    DO_MARSHALL(LONG,p,filereq->cprc,whereto);
    DO_MARSHALL(LONG,p,filereq->TStartPosition,whereto);
    DO_MARSHALL(LONG,p,filereq->TEndPosition,whereto);
    DO_MARSHALL(LONG,p,filereq->TStartTransferDisk,whereto);
    DO_MARSHALL(LONG,p,filereq->TEndTransferDisk,whereto);
    DO_MARSHALL(LONG,p,filereq->TStartTransferTape,whereto);
    DO_MARSHALL(LONG,p,filereq->TEndTransferTape,whereto);
    DO_MARSHALL(BLOCKID,p,filereq->blockid,whereto);
    DO_MARSHALL(HYPER,p,filereq->offset,whereto);
    DO_MARSHALL(HYPER,p,filereq->bytes_in,whereto);
    DO_MARSHALL(HYPER,p,filereq->bytes_out,whereto);
    DO_MARSHALL(HYPER,p,filereq->host_bytes,whereto);
    DO_MARSHALL(HYPER,p,filereq->nbrecs,whereto);
    DO_MARSHALL(HYPER,p,filereq->maxnbrec,whereto);
    DO_MARSHALL(HYPER,p,filereq->maxsize,whereto);
    DO_MARSHALL(HYPER,p,filereq->startsize,whereto);
    if ( magic > RTCOPY_MAGIC_OLD0 ) {
      rtcp_log(LOG_DEBUG,"%smarshall castorSegAttr()\n",
               (whereto == SendTo ? "" : "un")); 
      DO_MARSHALL(STRN,p,filereq->castorSegAttr.nameServerHostName,whereto);
      DO_MARSHALL(STRN,p,filereq->castorSegAttr.segmCksumAlgorithm,whereto);
      DO_MARSHALL(LONG,p,filereq->castorSegAttr.segmCksum,whereto);
      DO_MARSHALL(HYPER,p,filereq->castorSegAttr.castorFileId,whereto);
    }
    if ( magic > RTCOPY_MAGIC_OLD0 ) DO_MARSHALL(UUID,p,filereq->stgReqId,whereto);
  }
  /*
   * Is there an error annex
   */
  if ( (reqtype == RTCP_TAPEERR_REQ && tapereq != NULL) ||
       (reqtype == RTCP_FILEERR_REQ && filereq != NULL) ) {
    if ( REQTYPE(TAPE,reqtype) ) err = tapereq->err;
    else if ( REQTYPE(FILE,reqtype) ) err = filereq->err;
    DO_MARSHALL(STRN,p,err.errmsgtxt,whereto);
    DO_MARSHALL(LONG,p,err.severity,whereto);
    DO_MARSHALL(LONG,p,err.errorcode,whereto);
    DO_MARSHALL(LONG,p,err.max_tpretry,whereto);
    DO_MARSHALL(LONG,p,err.max_cpretry,whereto);
    if ( REQTYPE(TAPE,reqtype) ) tapereq->err = err;
    else if ( REQTYPE(FILE,reqtype) ) filereq->err = err;
  }

  if ( reqtype == VDQM_CLIENTINFO && tapereq != NULL && 
       client != NULL  ) {
    if ( whereto == ReceiveFrom ) {
      /*
       * Receive the client info. from VDQM. Note that
       * VolReqID is not copied into tapereq. The intention
       * is that the client should provide the VolReqID through
       * the tapereq (and filereq) messages and we crosscheck
       * this with rtcpClientInfo_t. If the client dies and
       * a new client re-use the same port it will have a
       * different VolReqID.
       */
      rc = vdqm_GetClientAddr(common_buffer,client->clienthost,
                              &(client->clientport),&(client->VolReqID),
                              &(client->uid),&(client->gid),client->name,tapereq->dgn,
                              tapereq->unit);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcp_Transfer() vdqm_GetClientAddr(): %s\n",
                 sstrerror(serrno));
        serrno = save_serrno;
        return(-1);
      }
    } else {
      rtcp_log(LOG_ERR,"rtcp_Transfer() internal error. VDQM request in Send mode.");
      serrno = SEINTERNAL;
      return(-1);
    }
  }

  if ( whereto == SendTo ) {
    /*
     * reqtype has already been determined above
     */
    if ( hdr != NULL && hdr->magic != 0 ) magic = hdr->magic;
    else if ( client_magic != 0 ) magic = client_magic;
    else magic = RTCOPY_MAGIC;
    len = 0;
 
    if ( REQTYPE(TAPE,reqtype) ) {
      len = RTCP_TAPEREQLEN(tapereq);
      if ( magic > RTCOPY_MAGIC_OLD0 ) {
        len += sizeof(Cuuid_t);
      }
      if ( reqtype == RTCP_TAPEERR_REQ ) 
        len+=RTCP_ERRMSGLEN(tapereq);

    } else if ( REQTYPE(FILE,reqtype) ) {
      len = RTCP_FILEREQLEN(filereq);
      if ( magic > RTCOPY_MAGIC_OLD0 ) {
        len += sizeof(Cuuid_t);
        len += RTCP_CSAMSGLEN(&(filereq->castorSegAttr));
      } else {
        /*
         * 04/12/2003: removed vmsopt and rfio_key from rtcpFileRequest_t
         */
        len += strlen(vmsopt_dummy) + 1;
        len += LONGSIZE;
      }
      if ( reqtype == RTCP_FILEERR_REQ ) 
        len+=RTCP_ERRMSGLEN(filereq);

    } else if ( hdr != NULL ) {
      /* 
       * This can only happen for "body-less" messages
       * like RTCP_ENDOF_REQ. Make sure length is zero.
       */
      len = 0;
      rtcp_log(LOG_DEBUG,"rtcp_Transfer(): send bodyless message\n");
    }
    p = hdrbuf;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,reqtype,whereto);
    DO_MARSHALL(LONG,p,len,whereto);
    rtcp_log(LOG_DEBUG,"rtcp_Transfer(): sending hdr magic: 0x%x reqtype: 0x%x length: %d\n",magic,reqtype,len); 
    rc = netwrite_timeout(*s,hdrbuf,RTCP_HDRBUFSIZ,RTCP_NETTIMEOUT);
    switch (rc) {
    case -1:
      rtcp_log(LOG_ERR,"rtcp_Transfer() netwrite(%d,HDR): %s\n",*s,
               neterror());
      serrno = SECOMERR;
      return(-1);
    case 0:
      rtcp_log(LOG_ERR,"rtcp_Transfer() netwrite(%d,HDR): connection dropped\n",*s);
      serrno = SECONNDROP;
      return(-1);
    }
    if ( VALID_MSGLEN(len) ) {
      rc = netwrite_timeout(*s,buf,len,RTCP_NETTIMEOUT);
      switch (rc) {
      case -1:
        rtcp_log(LOG_ERR,"rtcp_Transfer() netwrite(%d,REQ): %s\n",*s,
                 neterror());
        serrno = SECOMERR;
        return(-1);
      case 0:
        rtcp_log(LOG_ERR,"rtcp_Transfer() netwrite(%d,REQ): connection dropped\n",*s);
        serrno = SECONNDROP;
        return(-1);
      }
    } else if ( len > 0 ) {
      rtcp_log(LOG_ERR,"rtcp_Transfer() invalid msg length %d\n",
               len);
      serrno = SEUMSG2LONG;
      return(-1);
    } 
  }
  return(reqtype);
}    

int rtcp_SendReq(SOCKET *s,
                 rtcpHdr_t *hdr,
                 rtcpClientInfo_t *client,
                 rtcpTapeRequest_t *tape,
                 rtcpFileRequest_t *file) {
  direction_t whereto = SendTo;
  return(rtcp_Transfer(s,hdr,client,tape,file,whereto));
}

int rtcp_RecvReq(SOCKET *s,
                 rtcpHdr_t *hdr,
                 rtcpClientInfo_t *client,
                 rtcpTapeRequest_t *tape,
                 rtcpFileRequest_t *file) {
  direction_t whereto = ReceiveFrom;
  return(rtcp_Transfer(s,hdr,client,tape,file,whereto));
}

static int rtcp_TransferTpDump(SOCKET *s,
                               rtcpDumpTapeRequest_t *dumpreq,
                               direction_t whereto) {
  char buf[RTCP_MSGBUFSIZ];
  char *p;
  int rc, len;

  if ( s == NULL || dumpreq == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  len = RTCP_DUMPTAPEREQLEN(dumpreq);

  if ( whereto == ReceiveFrom ) {
    if ( VALID_MSGLEN(len) ) {
      rc = netread_timeout(*s,buf,len,RTCP_NETTIMEOUT);
      switch (rc) {
      case -1:
        rtcp_log(LOG_ERR,"rtcp_TransferTpDump() netread(%d,REQ): %s\n",*s,
                 neterror());
        serrno = SECOMERR;
        return(-1);
      case 0:
        rtcp_log(LOG_ERR,"rtcp_TransferTpDump() netread(%d,REQ): connection dropped\n",*s);
        serrno = SECONNDROP;
        return(-1);
      }
    } else if ( len > 0 ) {
      rtcp_log(LOG_ERR,"rtcp_TransferTpDump() invalid msg length %d\n",
               len);
      serrno = SEUMSG2LONG;
      return(-1);
    }
  }

  p = buf;
  DO_MARSHALL(LONG,p,dumpreq->maxbyte,whereto);
  DO_MARSHALL(LONG,p,dumpreq->blocksize,whereto);
  DO_MARSHALL(LONG,p,dumpreq->convert,whereto);
  DO_MARSHALL(LONG,p,dumpreq->tp_err_action,whereto);
  DO_MARSHALL(LONG,p,dumpreq->startfile,whereto);
  DO_MARSHALL(LONG,p,dumpreq->maxfile,whereto);
  DO_MARSHALL(LONG,p,dumpreq->fromblock,whereto);
  DO_MARSHALL(LONG,p,dumpreq->toblock,whereto);

  if ( whereto == SendTo ) {
    if ( VALID_MSGLEN(len) ) {
      rc = netwrite_timeout(*s,buf,len,RTCP_NETTIMEOUT);
      switch (rc) {
      case -1:
        rtcp_log(LOG_ERR,"rtcp_TransferTpDump() netwrite(%d,REQ): %s\n",*s,
                 neterror());
        serrno = SECOMERR;
        return(-1);
      case 0:
        rtcp_log(LOG_ERR,"rtcp_TransferTpDump() netwrite(%d,REQ): connection dropped\n",*s);
        serrno = SECONNDROP;
        return(-1);
      }
    } else if ( len > 0 ) {
      rtcp_log(LOG_ERR,"rtcp_TransferTpDump() invalid msg length %d\n",
               len);
      serrno = SEUMSG2LONG;
      return(-1);
    }

  }
  return(0);
}

int rtcp_SendTpDump(SOCKET *s, rtcpDumpTapeRequest_t *dumpreq) {
  direction_t whereto = SendTo;
  return(rtcp_TransferTpDump(s,dumpreq,whereto));
}

int rtcp_RecvTpDump(SOCKET *s, rtcpDumpTapeRequest_t *dumpreq) {
  direction_t whereto = ReceiveFrom;
  return(rtcp_TransferTpDump(s,dumpreq,whereto));
}

static int rtcp_TransAckn(SOCKET *s,
                          int reqtype,
                          void *data,
                          direction_t whereto) {
  char hdrbuf[RTCP_HDRBUFSIZ];
  int magic, recvreqtype, status, rc;

  if ( s == NULL || *s == INVALID_SOCKET ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( client_magic != 0 ) magic = client_magic;
  else magic = RTCOPY_MAGIC;
  recvreqtype = reqtype;
  status = 0;
  if ( data != NULL ) status = *(int *)data;

  if ( whereto == ReceiveFrom ) {
    serrno = 0;
    rc = netread_timeout(*s,hdrbuf,RTCP_HDRBUFSIZ,RTCP_NETTIMEOUT);
    switch (rc) {
    case -1: 
      rtcp_log(LOG_ERR,"rtcp_TransAckn() netread(%d,HDR): %s\n",*s,
               neterror());
      serrno = SECOMERR;
      return(-1);
    case 0:
      rtcp_log(LOG_ERR,"rtcp_TransAckn() netread(%d,HDR): connection dropped\n",*s);
      serrno = SECONNDROP;
      return(-1);
    }
  }

  {
    char *p = hdrbuf;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,recvreqtype,whereto);
    DO_MARSHALL(LONG,p,status,whereto);
  }
    
  if ( whereto == SendTo ) {
    if ( reqtype == VDQM_CLIENTINFO ) {
      rc = vdqm_AcknClientAddr(*s,status,0,NULL);
      closesocket(*s);
      *s = INVALID_SOCKET;
      return(rc);
    }
    rc = netwrite_timeout(*s,hdrbuf,RTCP_HDRBUFSIZ,RTCP_NETTIMEOUT);
    switch (rc) {
    case -1: 
      rtcp_log(LOG_ERR,"rtcp_TransAckn() netwrite(%d,HDR): %s\n",*s,
               neterror());
      serrno = SECOMERR;
      return(-1);
    case 0:
      rtcp_log(LOG_ERR,"rtcp_TransAckn() netwrite(%d,HDR): connection dropped\n",*s);
      serrno = SECONNDROP;
      return(-1);
    }
  }
    
  if ( data != NULL ) *(int *)data = status;
  return(recvreqtype);
}

int rtcp_SendAckn(SOCKET *s, int reqtype) {
  direction_t whereto = SendTo;
  return(rtcp_TransAckn(s,reqtype,NULL,whereto));
}

int rtcp_RecvAckn(SOCKET *s, int reqtype) {
  direction_t whereto = ReceiveFrom;
  return(rtcp_TransAckn(s,reqtype,NULL,whereto));
}

int rtcp_CloseConnection(SOCKET *s) {

  if ( s == NULL || *s == INVALID_SOCKET ) {
    serrno = EINVAL;
    return(-1);
  }

  (void) shutdown(*s,SD_BOTH);

  (void) closesocket(*s);

  *s = INVALID_SOCKET;
  return(0);
}


/*
 * Connect to rtcopy client (or server) listen port
 */
int rtcp_Connect(
                 SOCKET *connect_socket,
                 char *client_host,
                 int *client_port,
                 int whereto        /* Indicates whether we are connecting to the server or to the client */
                 ) {
  struct hostent *hp = NULL;
  struct sockaddr_in sin ; /* Internet address */
  int keepalive=1;

#ifdef RTCOPYCSEC
  uid_t Csec_uid;
  gid_t Csec_gid;
  int Csec_service_type;
  int c;
  char *p;
  int n;
  Csec_context_t sec_ctx;  
#endif

  if ( connect_socket == NULL || client_host == NULL || 
       client_port == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  *connect_socket = socket(AF_INET,SOCK_STREAM,0);
  if ( *connect_socket == INVALID_SOCKET ) {
    rtcp_log(
             LOG_ERR,
             "rtcp_Connect() socket(): %s\n",
             neterror()
             );
    serrno = SECOMERR;
    return(-1);
  }
  if ( (hp = Cgethostbyname(client_host)) == NULL ) {
    rtcp_log(
             LOG_ERR,
             "rtcp_Connect(): gethostbyname(%s), h_errno=%d, %s\n",
             client_host,
             h_errno,
             neterror()
             );
    closesocket(*connect_socket);
    *connect_socket = INVALID_SOCKET;
    serrno = SENOSHOST;
    return(-1);
  }
  sin.sin_port = htons((short)*client_port);
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

  if ( connect(
               *connect_socket,
               (struct sockaddr *)&sin, 
               sizeof(struct sockaddr_in)
               ) == SOCKET_ERROR) {
    rtcp_log(LOG_ERR,"rtcp_Connect() connect(): %s\n",neterror());
    closesocket(*connect_socket);
    *connect_socket = INVALID_SOCKET;
    serrno = SECOMERR;
    return(-1);
  }
  (void)setsockopt(
                   *connect_socket,
                   SOL_SOCKET,
                   SO_KEEPALIVE,
                   (char *)&keepalive,
                   sizeof(int)
                   );
   { /* S. Murray 31/03/09 */
     int tcp_nodelay = 1;
     setsockopt(*connect_socket, IPPROTO_TCP, TCP_NODELAY,
       (char *)&tcp_nodelay,sizeof(tcp_nodelay));
   }

#ifdef RTCOPYCSEC
  if (whereto == RTCP_CONNECT_TO_CLIENT) { /* We are the server */
    /*
     * Try to establish secure connection.
     */
    n = Csec_server_initContext(&sec_ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL);
    if ((n = Csec_server_establishContext(&sec_ctx, *connect_socket)) < 0) {
      rtcp_log(LOG_ERR,"rtcp_Connect(): CSEC: Could not establish server context\n");
      closesocket(*connect_socket);
      *connect_socket = INVALID_SOCKET;
      serrno = SECOMERR;
      return(-1);
    }
    /* Connection could be done from another castor service */
    if ((c = Csec_server_isClientAService(&sec_ctx)) >= 0) {
      rtcp_log(LOG_ERR,"rtcp_Connect(): CSEC: Client is castor service type %d\n", c);
      Csec_service_type = c;
    }
    else {
      char *username;
      if (Csec_server_mapClientToLocalUser(&sec_ctx, &username, &Csec_uid, &Csec_gid) != NULL) {
        rtcp_log(LOG_ERR,"rtcp_Connect(): CSEC: Client is %s (%d/%d)\n",
		 username,
                 Csec_uid,
                 Csec_gid);
        Csec_service_type = -1;
      }
      else {
        closesocket(*connect_socket);
        *connect_socket = INVALID_SOCKET;
        serrno = SECOMERR;
        return(-1);
      }
    }
    
  } else { /* We are the client */
    if (Csec_client_initContext(&sec_ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
      rtcp_log(LOG_ERR, "rtcp_Connect() Could not init context\n");
      closesocket(*connect_socket);
      *connect_socket = INVALID_SOCKET;
      serrno = ESEC_CTX_NOT_INITIALIZED;
      return(-1);
    }
    
    if(Csec_client_establishContext(&sec_ctx, *connect_socket)< 0) {
      rtcp_log(LOG_ERR, "rtcp_Connect() Could not establish context\n");
      closesocket(*connect_socket);
      *connect_socket = INVALID_SOCKET;
      serrno = ESEC_NO_CONTEXT;
      return(-1);
    }
    
    Csec_clearContext(&sec_ctx);
  }
#else
  (void)whereto;
#endif

  return(0);
}
int rtcpd_ConnectToClient(
                          SOCKET *connect_socket,
                          char *client_host,
                          int *client_port
                          ) {
  return(rtcp_Connect(connect_socket,client_host,client_port,RTCP_CONNECT_TO_CLIENT));
}

/** rtcp_ClientMsg() - send message to a SHIFT client (obsolete)
 *
 * @param SOCKET *s - point to client socket
 * @param char *msg - message to be sent
 *
 * Needed to support OLD SHIFT clients but other applications could use
 * it as well. Note: this routine is called by rtcp_log() -> we have 
 * to use log() to log our errors.
 */
int rtcp_ClientMsg(
                   SOCKET *s, 
                   char *msg
                   ) {
  int len,rc;
  char str[RTCP_SHIFT_BUFSZ];
  char *p;
    
  if ( s == NULL || *s == INVALID_SOCKET || msg == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  len = strlen(msg)+1;
  if ( len > RTCP_SHIFT_BUFSZ - 3*LONGSIZE ) {
    len = RTCP_SHIFT_BUFSZ - 3*LONGSIZE;
    str[RTCP_SHIFT_BUFSZ-1] = '\0';
  }

  p = str;
  marshall_LONG(p,RTCOPY_MAGIC_SHIFT);
  marshall_LONG(p,GIVE_OUTP);
  marshall_LONG(p,len);
  marshall_STRING(p,msg);

  rc = netwrite_timeout(
                        *s,
                        str,
                        3*LONGSIZE + len,
                        RTCP_NETTIMEOUT
                        );
  switch (rc) {
  case -1: 
    log(
        LOG_ERR,
        "rtcp_ClientMsg() netwrite(%d): %s\n",
        *s,
        neterror()
        );
    serrno = SECOMERR;
    rc = -1;
    break;
  case 0:
    log(
        LOG_ERR,
        "rtcp_ClientMsg() netwrite(%d): connection dropped\n",
        *s
        );
    serrno = SECONNDROP;
    rc = -1;
    break;
  }

  return(rc);
}

int rtcp_GetMsg(
                SOCKET *s, 
                char *msg, 
                int length
                ) {
  int rc;

  if ( msg == NULL || length < 0 ) {
    serrno = EINVAL;
    return(-1);
  }
  if ( length == 0 ) return(0);

  rc = netread_timeout(
                       *s,
                       msg,
                       length,
                       RTCP_NETTIMEOUT
                       );
  switch (rc) {
  case -1:
    log(
        LOG_ERR,
        "rtcp_GetMsg() netread(%d): %s\n",
        *s,
        neterror()
        );
    serrno = SECOMERR;
    rc = -1;
    break;
  case 0:
    log(
        LOG_ERR,
        "rtcp_GetMsg() netread(%d): connection dropped\n",
        *s
        );
    serrno = SECONNDROP;
    rc = -1;
    break;
  }
  return(rc);
}

int rtcp_SendOldCAckn(
                      SOCKET *s,
                      rtcpHdr_t *hdr
                      ) {
  char acknmsg[3*LONGSIZE];
  char *p;
  int rc;
  int i = 0;
  char rqst_str[][10] = { "TPDK", "DKTP", "PING", "ABORT", "DPTP",""};
  int rqst_types[] = {RQST_TPDK, RQST_DKTP, RQST_PING, \
                      RQST_ABORT, RQST_DPTP,-1};
  int ackn_types[] = {ACKN_TPDK, ACKN_DKTP, ACKN_PING, \
                      ACKN_ABORT, ACKN_DPTP,-1};

  if ( s == NULL || *s == INVALID_SOCKET || hdr == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  /*
   * No acks to info requests
   */
  if ( hdr->reqtype == RQST_INFO ) return(0);

  while ( rqst_types[i] != -1 && rqst_types[i] != hdr->reqtype ) i++;
  if ( rqst_types[i] != -1 ) {
    p = acknmsg;
    marshall_LONG(p,hdr->magic);
    marshall_LONG(p,ackn_types[i]);
    marshall_LONG(p,0);
    rtcp_log(
             LOG_DEBUG,
             "rtcp_SendOldCAckn() send ACKN_%s to client\n",
             rqst_str[i]
             );
    rc = netwrite_timeout(
                          *s,
                          acknmsg,
                          3*LONGSIZE,
                          RTCP_NETTIMEOUT
                          );
    switch (rc) {
    case -1:
      rtcp_log(
               LOG_ERR,
               "rtcp_SendOldCAckn() netwrite(%d,%d): %s\n",
               *s,
               3*LONGSIZE,
               neterror()
               );
      serrno = SECOMERR;
      return(-1);
    case 0:
      rtcp_log(
               LOG_ERR,
               "rtcp_SendOldCAckn() netwrite(%d,%d): connection dropped\n",
               *s,
               3*LONGSIZE
               );
      serrno = SECONNDROP;
      return(-1);
    }
  } else {
    serrno = SEOPNOTSUP;
    return(-1);
  }
  return(0);
}

int rtcp_SendOldCinfo(
                      SOCKET *s, 
                      rtcpHdr_t *hdr, 
                      shift_client_t *req
                      ) {
  char infomsg[6*LONGSIZE];
  char *p;
  int rc,queue;

  if ( s == NULL || *s == INVALID_SOCKET || hdr == NULL || req == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  queue = req->info.nb_used - req->info.nb_units + req->info.nb_queued;
  p = infomsg;
  marshall_LONG(p,hdr->magic);
  marshall_LONG(p,GIVE_INFO);
  marshall_LONG(p,3*LONGSIZE);
  marshall_LONG(p,req->info.status);
  marshall_LONG(p,queue);
  marshall_LONG(p,req->info.nb_units);

  rc = netwrite_timeout(
                        *s,
                        infomsg,
                        6*LONGSIZE,
                        RTCP_NETTIMEOUT
                        );
  switch (rc) {
  case -1:
    rtcp_log(
             LOG_ERR,
             "rtcp_SendOldCinfo() netwrite(%d,%d): %s\n",
             *s,
             6*LONGSIZE,neterror()
             );
    serrno = SECOMERR;
    return(-1);
  case 0:
    rtcp_log(
             LOG_ERR,
             "rtcp_SendOldCinfo() netwrite(%d,%d): connection dropped\n",
             *s,
             6*LONGSIZE
             );
    serrno = SECONNDROP;
    return(-1);
  }
  return(0);
}

