/******************************************************************************
 *                      rtcpcldapi_new.c
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
 * @(#)$RCSfile: rtcpcldapi.c,v $ $Revision: 1.61 $ $Release$ $Date: 2004/09/17 15:40:43 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldapi.c,v $ $Revision: 1.61 $ $Date: 2004/09/17 15:40:43 $ CERN-IT/ADC Olof Barring";
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#if !defined(_WIN32)
#include <unistd.h>
#endif /* !_WIN32 */
#include <net.h>
#include <osdep.h>
#include <log.h>
#include <Castor_limits.h>
#include <Cuuid.h>
#include <Ctape_constants.h>
#include <castor/stager/Tape.h>
#include <castor/stager/Segment.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <castor/stager/IStagerSvc.h>
#include <castor/Services.h>
#include <castor/BaseAddress.h>
#include <castor/db/DbAddress.h>
#include <castor/IAddress.h>
#include <castor/IObject.h>
#include <castor/IClient.h>
#include <castor/Constants.h>
#include <serrno.h>
#include <stdio.h>
#include <stdlib.h>
#include <rtcp_constants.h>
#include <Ctape_api.h>
#include <stage_api.h>
#include <Cmutex.h>
#include <Cglobals.h>
#include <common.h>
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld_messages.h>
#include <rtcpcld.h>
#include <rtcpcldapi.h>

#define MAX_INCOMPLETE_SEGMENTS (50)

#define LOG_FAILED_CALL(X,Y)  { \
        int _save_errno = errno; \
        int _save_serrno = serrno; \
        rtcp_log( \
                 LOG_ERR, \
                 "(%s:%d) %s %s (serrno=%d, errno=%d) %s:%s\n", \
                 __FILE__, \
                 __LINE__, \
                 (X), \
                 sstrerror((_save_serrno > 0 ? _save_serrno : _save_errno)), \
                 serrno, \
                 errno, \
                 (((Y) != NULL && *(Y) != '\0') ? "Additional Error information" : ""), \
                 (Y) \
                 ); \
        serrno = _save_serrno; \
        errno = _save_errno; \
}

extern int stage_setlog _PROTO((
                                void (*) _PROTO((int, char *))
                                ));
                                
int rtcpcldc_killed _PROTO((
                            tape_list_t *
                            ));

typedef struct ClientCntx 
{
  char clientAddr[CA_MAXHOSTNAMELEN+12];
  char vwAddress[CA_MAXHOSTNAMELEN+12];
  struct Cstager_Tape_t *currentTp;
} ClientCntx_t;

typedef struct RtcpcldcReqmap 
{
  ClientCntx_t *clientCntx;
  tape_list_t *tape;
  struct RtcpcldcReqmap *next;
  struct RtcpcldcReqmap *prev;
} RtcpcldcReqmap_t;

RtcpcldcReqmap_t *rtcpcldc_reqMap = NULL;
void *rtcpcldc_cleanupLock = NULL;

int (*rtcpcld_ClientCallback) _PROTO((
                                      rtcpTapeRequest_t *,
                                      rtcpFileRequest_t *
                                      )) = NULL;
int (*rtcpcld_MoreInfoCallback) _PROTO((
                                        tape_list_t *
                                        )) = NULL;

int rtcpcldc_ENOSPCkey = -1;

void rtcp_stglog(type,msg)
     int type;
     char *msg;
{
  if ( type == MSG_ERR ) rtcp_log(LOG_ERR,msg);
  return;
}

static int initOldStgUpdc(tape)
     tape_list_t *tape;
{
  int rc, *ENOSPC_occurred = NULL;
  if ( tape == NULL || tape->file == NULL ||
       *tape->file->filereq.stageID == '\0' ) return(0);
  rc = stage_setlog(rtcp_stglog);

  rc = Cglobals_get(&rtcpcldc_ENOSPCkey,(void **)&ENOSPC_occurred,sizeof(int));
  if ( rc == -1 || ENOSPC_occurred == NULL ) {
    LOG_FAILED_CALL("Cglobals_get()","");
    return(-1);
  }
  
  *ENOSPC_occurred = FALSE;
  return(rc);
}

static int updateOldStgCat(tape,file)
     tape_list_t *tape;
     file_list_t *file;
{
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq;
  int rc, status, retval, save_serrno, WaitTime, TransferTime;
  char newpath[CA_MAXPATHLEN+1];
  char recfm[10];   /* Record format (could include ,bin or ,-f77) */
  u_signed64 nb_bytes;
  int retry = 0, *ENOSPC_occurred = NULL;
  
  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  tapereq = &tape->tapereq;
  filereq = &file->filereq;
  
  rc = Cglobals_get(&rtcpcldc_ENOSPCkey,(void **)&ENOSPC_occurred,sizeof(int));
  if ( rc == -1 || ENOSPC_occurred == NULL ) {
    LOG_FAILED_CALL("Cglobals_get()","");
    return(-1);
  }
  
  rc = save_serrno = retval = 0;
  *newpath = '\0';
  
  if ( *filereq->stageID == '\0' ) return(0);
  
  WaitTime = filereq->TEndPosition - tapereq->TStartRequest;
  TransferTime = max((time_t)filereq->TEndTransferDisk,
                     (time_t)filereq->TEndTransferTape) -
    (time_t)filereq->TEndPosition;
  if ( WaitTime < 0 ) WaitTime = 0;
  if ( TransferTime < 0 ) TransferTime = 0;
  
  strcpy(recfm,filereq->recfm);
  if ( (filereq->convert & NOF77CW) != 0 ) {
    if ( *filereq->recfm == 'U' ) strcat(recfm,",bin");
    else if ( *filereq->recfm == 'F' ) strcat(recfm,",-f77");
  }

  /*
   * Don't do any stage_updc when tpwrite hit EOT (avoid PUT_FAILED)
   */
  if ( (tapereq->mode == WRITE_ENABLE) &&
       (filereq->cprc == -1) && (filereq->err.errorcode == ENOSPC) ) {
    rtcp_log(LOG_DEBUG,"updateOldStgCat() prevent stage_updc_filcp() for EOT on write\n");
    return(0);
  }
  
  rtcp_log(LOG_DEBUG,"updateOldStgCat(): fseq=%d, status=%d, concat=%d, err=%d\n",
           filereq->tape_fseq,filereq->proc_status, filereq->concat,
           filereq->err.errorcode);
  if ( filereq->concat != NOCONCAT ) {
    rtcp_log(LOG_DEBUG,
             "updateOldStgcat(): local updc does not support concate=%d\n",
             filereq->concat);
    serrno = SEOPNOTSUP;
    return(-1);
  }
  for ( retry=0; retry<RTCP_STGUPDC_RETRIES; retry++ ) {
    status = filereq->err.errorcode;
    if ( status == ERTLIMBYSZ || status == ERTBLKSKPD ||
         status == ERTMNYPARY ) status = 0;
    if ( (filereq->proc_status == RTCP_POSITIONED) && (status == 0) ) {
      rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() stage_updc_tppos(%s,%d,%d,%d,%s,%s,%d,%d,%s,%s,NULL,-1)\n",
               filereq->stageID,
               filereq->stageSubreqID,
               status,
               filereq->blocksize,
               tapereq->unit,
               filereq->fid,
               filereq->tape_fseq,
               filereq->recordlength,
               recfm,
               newpath);
      rc = stage_updc_tppos(filereq->stageID,
                            filereq->stageSubreqID,
                            status,
                            filereq->blocksize,
                            tapereq->unit,
                            filereq->fid,
                            filereq->tape_fseq,
                            filereq->recordlength,
                            recfm,
                            newpath,
                            NULL,
                            -1);
      save_serrno = serrno;
      if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_stageupdc() stage_updc_tppos(): %s\n",
                 sstrerror(save_serrno));
        switch (save_serrno) {
        case EINVAL:
          filereq->err.errorcode = save_serrno;
          strcpy(filereq->err.errmsgtxt,sstrerror(save_serrno));
          filereq->err.severity = RTCP_FAILED|RTCP_USERR;
          serrno = save_serrno;
          return(-1);
        case SECOMERR:
        case SESYSERR:
          break;
        default:
          if ( save_serrno == 0 ) save_serrno = SEINTERNAL;
          filereq->err.errorcode = save_serrno;
          strcpy(filereq->err.errmsgtxt,sstrerror(save_serrno));
          filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
          serrno = save_serrno;
          return(-1);
        }
        continue;
      }
    }

    if ( tapereq->mode == WRITE_ENABLE ) nb_bytes = filereq->bytes_in;
    else nb_bytes = filereq->bytes_out;
    if ( (filereq->proc_status == RTCP_POSITIONED &&
          ((filereq->err.errorcode == ENOSPC && 
            tapereq->mode == WRITE_DISABLE) ||
           (filereq->err.errorcode != 0 && filereq->err.max_cpretry<=0))) || 
         (filereq->proc_status == RTCP_FINISHED && (nb_bytes > 0)) ) {
      /*
       * Always give the return code
       */
      retval = 0;
      if ( filereq->err.errorcode == ENOSPC &&
           tapereq->mode == WRITE_DISABLE ) {
        retval = ENOSPC;
        *ENOSPC_occurred = TRUE;
      } else rc = rtcp_RetvalCASTOR(tape,file,&retval);
      
      *newpath = '\0';
      
      rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() stage_updc_filcp(%s,%d,%d,%s,%d,%d,%d,%d,%s,%s,%d,%d,%s,%s)\n",
               filereq->stageID,
               filereq->stageSubreqID,
               -retval,
               filereq->ifce,
               (int)nb_bytes,
               WaitTime,
               TransferTime,
               filereq->blocksize,
               tapereq->unit,
               filereq->fid,
               filereq->tape_fseq,
               filereq->recordlength,
               recfm,
               newpath);
      
      rc = stage_updc_filcp(filereq->stageID,
                            filereq->stageSubreqID,
                            -retval,
                            filereq->ifce,
                            nb_bytes,
                            WaitTime,
                            TransferTime,
                            filereq->blocksize,
                            tapereq->unit,
                            filereq->fid,
                            filereq->tape_fseq,
                            filereq->recordlength,
                            recfm,
                            newpath);
      save_serrno = serrno;
      if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_stageupdc() stage_updc_filcp(): %s\n",
                 sstrerror(save_serrno));
        switch (save_serrno) {
        case EINVAL:
          filereq->err.errorcode = save_serrno;
          strcpy(filereq->err.errmsgtxt,sstrerror(save_serrno));
          filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
          serrno = save_serrno;
          return(-1);
        case ENOSPC:
          filereq->err.errorcode = save_serrno;
          strcpy(filereq->err.errmsgtxt,sstrerror(save_serrno));
          if ( retval == ENOSPC ) 
            filereq->err.severity = RTCP_FAILED|RTCP_USERR;
          else 
            filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
          serrno = save_serrno;
          return(-1);
        case SECOMERR:
        case SESYSERR:
          break;
        default:
          if ( save_serrno == 0 ) save_serrno = SEINTERNAL;
          filereq->err.errorcode = save_serrno;
          strcpy(filereq->err.errmsgtxt,sstrerror(save_serrno));
          filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
          serrno = save_serrno;
          return(-1);
        }
      } else break; 
    }
    if ( rc == 0 ) break;
  } /* for ( retry=0; retry<RTCP_STGUPDC_RETRIES; retry++ ) */
  if ( rc == -1 ) {
    if ( save_serrno == SECOMERR || save_serrno == SESYSERR ) {
      filereq->err.errorcode = save_serrno;
      strcpy(filereq->err.errmsgtxt,sstrerror(save_serrno));
      filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    } else {
      if ( save_serrno == 0 ) save_serrno = SEINTERNAL;
      filereq->err.errorcode = save_serrno;
      strcpy(filereq->err.errmsgtxt,sstrerror(save_serrno));
      filereq->err.severity = RTCP_FAILED|RTCP_UNERR;
    }
    serrno = save_serrno;
    return(-1);
  }
  return(0);
}

static int updateCaller(tape,file)
     tape_list_t *tape;
     file_list_t *file;
{
  int rc;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  rtcp_log(LOG_DEBUG,"updateCaller() for fseq=%d, blockid=%.2d%.2d%.2d%.2d, path=%s\n",
           file->filereq.tape_fseq,
           (int)file->filereq.blockid[0],
           (int)file->filereq.blockid[1],
           (int)file->filereq.blockid[2],
           (int)file->filereq.blockid[3],
           file->filereq.file_path
           );
  if ( file->filereq.cprc != 0 ) {
    rtcp_log(
             LOG_DEBUG,
             "updateCaller() RC=%d, errorcode=%d, severity=0x%x, msg=%s\n",
             file->filereq.cprc,
             file->filereq.err.errorcode,
             file->filereq.err.severity,
             file->filereq.err.errmsgtxt
             );
  }
  
  if ( rtcpcld_ClientCallback != (int (*) _PROTO((rtcpTapeRequest_t *,
                                                  rtcpFileRequest_t *)))NULL ) {
    rc = rtcpcld_ClientCallback(&(tape->tapereq),&(file->filereq));
    if ( rc == -1 ) {
      LOG_FAILED_CALL("rtcpcld_ClientCallback()","");
      return(-1);
    }
  }

  rc = updateOldStgCat(tape,file);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("updateOldStgCat()","");
    return(-1);
  }
  
  return(0);
}

static int getMoreInfo(tape)
     tape_list_t *tape;
{
  int rc;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  rtcp_log(LOG_DEBUG,"getMoreInfo() for tape VID=%s, side=%d, mode=%d\n",
           tape->tapereq.vid,
           tape->tapereq.side,
           tape->tapereq.mode
           );
  
  if ( rtcpcld_MoreInfoCallback != (int (*) _PROTO((tape_list_t *)))NULL ) {
    rc = rtcpcld_MoreInfoCallback(tape);
    if ( rc == -1 ) {
      LOG_FAILED_CALL("rtcpcld_MoreInfoCallback()","");
      return(-1);
    }
  }

  return(0);
}

static int addSegment(
                      clientCntx,
                      file,
                      newSegm
                      )
     ClientCntx_t *clientCntx;
     file_list_t *file;
     struct Cstager_Segment_t **newSegm;
{
  int rc;
  struct Cstager_Tape_t *tp;
  char *myNotificationAddr;
  char *notificationAddr = NULL;
  rtcpFileRequest_t *filereq = NULL;
  struct Cstager_Segment_t *segment = NULL;
  enum SegmentStatusCodes segmentStatus;
  
  if ( clientCntx == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  myNotificationAddr = clientCntx->clientAddr;
  tp = clientCntx->currentTp;

  filereq = &(file->filereq);

  if ( !RTCP_FILEREQ_OK(filereq) ) {
    rtcp_log(
             LOG_ERR,
             "addSegment(): filereq (disk fseq=%d, tape fseq=%d) not OK! at least one string is not terminated\n",
             filereq->disk_fseq,
             filereq->tape_fseq
             );
    serrno = ERTBADREQ;
    return(-1);
  }
  if ( filereq->proc_status >= RTCP_FINISHED ) {
    rtcp_log(
             LOG_ERR,
             "addSegment(): attempt to add segment for already finished file request (disk fseq=%d, tape fseq=%d)\n",
             filereq->disk_fseq,
             filereq->tape_fseq
             );
    serrno = ERTBADREQ;
    return(-1);
  }

  rtcp_log(
           LOG_DEBUG,
           "Add segment: fseq=%d, blockid=%.2d%.2d%.2d%.2d, diskPath=%s\n",
           filereq->tape_fseq,
           (int)filereq->blockid[0],
           (int)filereq->blockid[1],
           (int)filereq->blockid[2],
           (int)filereq->blockid[3],
           filereq->file_path
           );

           
  notificationAddr = myNotificationAddr;
  

  serrno = 0;
  rc = Cstager_Segment_create(&segment);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cstager_Segment_create()","");
    if ( serrno == 0 ) serrno = errno;
    return(-1);
  }

  Cstager_Segment_setFseq(
                          segment,
                          filereq->tape_fseq
                          );
  Cstager_Segment_setFid(
                         segment,
                         filereq->fid
                         );
  Cstager_Segment_setDiskPath(
                              segment,
                              filereq->file_path
                              );
  Cstager_Segment_setCastorNsHost(
                                  segment,
                                  filereq->castorSegAttr.nameServerHostName
                                  );
  Cstager_Segment_setCastorFileId(
                                  segment,
                                  filereq->castorSegAttr.castorFileId
                                  );
  Cstager_Segment_setOffset(
                            segment,
                            filereq->offset
                            );
  Cstager_Segment_setBlockid(
                             segment,
                             filereq->blockid
                             );
  Cstager_Segment_setSegmCksumAlgorithm(
                                        segment,
                                        filereq->castorSegAttr.segmCksumAlgorithm
                                        );
  Cstager_Segment_setStgReqId(
                              segment,
                              filereq->stgReqId
                              );

  if ( filereq->proc_status < RTCP_WAITING )
    filereq->proc_status = RTCP_WAITING;
  
  if ( filereq->proc_status <= RTCP_WAITING ||
       filereq->proc_status == RTCP_POSITIONED ) {
    segmentStatus = SEGMENT_UNPROCESSED;
  } else if ( filereq->proc_status == RTCP_FINISHED ) {
    segmentStatus = SEGMENT_FILECOPIED;
  } else {
    segmentStatus = SEGMENT_UNKNOWN;
  }
  Cstager_Segment_setStatus(
                            segment,
                            segmentStatus
                            );

  if ( notificationAddr != NULL ) {
    Cstager_Segment_setClientAddress(
                                     segment,
                                     notificationAddr
                                     );
  }

  Cstager_Segment_setTape(
                          segment,
                          tp
                          );
  Cstager_Tape_addSegments(
                           tp,
                           segment
                           );

  if ( newSegm != NULL ) *newSegm = segment;
  return(0);
}

static int deleteSegmentFromDB(segment)
     struct Cstager_Segment_t *segment;
{
  int rc, save_serrno;
  struct C_Services_t **dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *segmIObj;
  
  if ( segment == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()","");
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 || dbSvc == NULL || *dbSvc == NULL ) {
    LOG_FAILED_CALL("rtcpcld_getDbSvc()","");
    return(-1);
  }

  segmIObj = Cstager_Segment_getIObject(segment);
  rc = C_Services_deleteRep(*dbSvc,iAddr,segmIObj,1);
  if ( rc != 0 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("C_Services_deleteRep()",
                    C_Services_errorMsg(*dbSvc));
    rtcp_log(LOG_ERR,"DB error: %s\n",C_Services_errorMsg(*dbSvc));
    C_IAddress_delete(iAddr);
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    serrno = save_serrno;
    return(-1);
  }

  C_IAddress_delete(iAddr);
  return(0);
}

static int newOrUpdatedSegment(
                               tape,
                               file,
                               clientCntx,
                               foundSegment
                               )
     tape_list_t *tape;
     file_list_t *file;
     ClientCntx_t *clientCntx;
     struct Cstager_Segment_t **foundSegment;
{
  int rc;
  struct Cstager_Segment_t *segm;
  struct Cstager_Tape_t *tp;
  rtcpFileRequest_t *filereq;
  char *clientAddr = NULL;
  unsigned char *blockid = NULL;
  char *diskPath = NULL;
  int newSegment = 0, updatedSegment = 0, fseq;

  if ( (tape == NULL) || (file == NULL) || (clientCntx == NULL ) ) {
    serrno = EINVAL;
    return(-1);
  }

  clientAddr = clientCntx->clientAddr;
  tp = clientCntx->currentTp;
  filereq = &file->filereq;

  rc = rtcpcld_findSegmentFromFile(
                                   file,
                                   tp,
                                   &segm,
                                   clientAddr
                                   );
  if ( rc == -1 ) return(-1);
  if ( rc == 0 ) {
    newSegment = 1;
  } else {
    newSegment = 0;
    Cstager_Segment_fseq(segm,&fseq);
    blockid = NULL;
    Cstager_Segment_blockid(segm,(CONST unsigned char **)&blockid);
    diskPath = NULL;
    Cstager_Segment_diskPath(segm,(CONST char **)&diskPath);
    if ( (fseq == filereq->tape_fseq) &&
         (memcmp(blockid,filereq->blockid,sizeof(filereq->blockid)) == 0) &&
         (strcmp(diskPath,filereq->file_path) == 0) ) {
      updatedSegment = 0;
    } else {
      if ( foundSegment != NULL ) *foundSegment = segm;
      updatedSegment = 1;
    }
  }
  return(newSegment + (updatedSegment*2));
}                               

static int updateSegment(
                         file,
                         segment
                         )
     file_list_t *file;
     struct Cstager_Segment_t *segment;
{
  rtcpFileRequest_t *filereq;
  
  if ( (file == NULL) || (segment == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  filereq = &(file->filereq);
  
  Cstager_Segment_setDiskPath(
                              segment,
                              filereq->file_path
                              );
  Cstager_Segment_setFseq(
                          segment,
                          filereq->tape_fseq
                          );
  Cstager_Segment_setBlockid(
                             segment,
                             filereq->blockid
                             );
  return(0);
}
  

static int updateSegmentInDB(
                             tape,
                             file,
                             segment
                             )
     tape_list_t *tape;
     file_list_t *file;
     struct Cstager_Segment_t *segment;
{
  struct C_Services_t **dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *segmIObj;
  rtcpFileRequest_t *filereq;
  int rc, save_serrno;
  ID_TYPE key = 0;

  if ( (segment == NULL) || (file == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  filereq = &(file->filereq);
  
  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()","");
    save_serrno = serrno;
    if ( serrno == 0 ) save_serrno = errno;
    filereq->err.errorcode = save_serrno;
    strcpy(filereq->err.errmsgtxt,"Could not create DB base address");
    filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 || dbSvc == NULL || *dbSvc == NULL ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcpcld_getDbSvc()","");
    filereq->err.errorcode = save_serrno;
    strcpy(filereq->err.errmsgtxt,"Could not create DB services");
    filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    serrno = save_serrno;
    return(-1);
  }
  segmIObj = Cstager_Segment_getIObject(segment);
  Cstager_Segment_id(segment,&key);
  if ( rtcpcldc_killed(tape) != 0 ) return(-1);
  rc = C_Services_updateRepNoRec(*dbSvc,iAddr,segmIObj,1);
  if ( rc != 0 ) {
    LOG_FAILED_CALL("C_Services_updateRepNoRec()",C_Services_errorMsg(*dbSvc));
    save_serrno = serrno;
    filereq->err.errorcode = serrno;
    strcpy(filereq->err.errmsgtxt,
           C_Services_errorMsg(*dbSvc));
    filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    C_IAddress_delete(iAddr);
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    rtcp_log(LOG_ERR,"DB error key=%lu: %s\n",key,filereq->err.errmsgtxt);
    serrno = save_serrno;
    return(-1);
  }
  C_IAddress_delete(iAddr);
  return(0);
}

static int addNewSegmentsToDB(
                              tape,
                              clientCntx
                              )
     tape_list_t *tape;
     ClientCntx_t *clientCntx;
{
  struct C_Services_t **dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_Tape_t *tp = NULL;
  struct Cstager_Segment_t **newSegms = NULL;
  rtcpTapeRequest_t *tapereq = NULL;
  int nbNew = 0;
  ID_TYPE keyNew = 0;
  int rc, save_serrno, i;
  
  if ( (tape == NULL ) || (clientCntx == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  tapereq = &(tape->tapereq);
  
  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()","");
    save_serrno = serrno;
    if ( serrno == 0 ) save_serrno = errno;
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("C_Services_create()","");
    serrno = save_serrno;
    return(-1);
  }

  Cstager_Tape_segments(clientCntx->currentTp,&newSegms,&nbNew);
  for ( i=0; i<nbNew; i++ ) {
    Cstager_Segment_id(newSegms[i],&keyNew);
    if ( keyNew == 0 ) {
      iObj = Cstager_Segment_getIObject(newSegms[i]);
      rc = C_Services_createRepNoRec(*dbSvc,iAddr,iObj,0);
      if ( rc == -1 ) {
        save_serrno = serrno;
        break;
      }
    }
  }
  
  if ( newSegms != NULL ) free(newSegms);

  if ( rtcpcldc_killed(tape) != 0 ) {
    save_serrno = serrno;
    (void)C_Services_rollback(*dbSvc,iAddr);
    rtcp_log(LOG_ERR,"Request aborted\n");
    serrno = save_serrno;
    return(-1);
  }
  
/*   iObj = Cstager_Tape_getIObject(tp); */

/*   rc = C_Services_updateRep(*dbSvc,iAddr,iObj,0); */
  if ( rc != 0 ) {
/*     LOG_FAILED_CALL("C_Services_updateRep()", */
/*                     C_Services_errorMsg(*dbSvc)); */
/*     save_serrno = serrno; */
    LOG_FAILED_CALL("C_Services_createRepNoRec()",
                    C_Services_errorMsg(*dbSvc));
    (void)C_Services_rollback(*dbSvc,iAddr);
    tapereq->err.errorcode = save_serrno;
    strncpy(tapereq->err.errmsgtxt,
            C_Services_errorMsg(*dbSvc),
            sizeof(tapereq->err.errmsgtxt)-1);
    tapereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    rtcp_log(LOG_ERR,"DB error: %s\n",
             tapereq->err.errmsgtxt);
    Cstager_Tape_delete(tp);
    serrno = save_serrno;
    return(-1);
  }

  rtcp_log(LOG_DEBUG,"addNewSegmentsToDB() commit updates\n");
  rc = C_Services_commit(*dbSvc,iAddr);
  if ( rc != 0 ) {
    LOG_FAILED_CALL("C_Services_commit()",
                    C_Services_errorMsg(*dbSvc));
    save_serrno = serrno;
    (void)C_Services_rollback(*dbSvc,iAddr);
    tapereq->err.errorcode = save_serrno;
    strncpy(tapereq->err.errmsgtxt,
            C_Services_errorMsg(*dbSvc),
            sizeof(tapereq->err.errmsgtxt)-1);
    tapereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    rtcp_log(LOG_ERR,"DB error: %s\n",
             tapereq->err.errmsgtxt);
    Cstager_Tape_delete(tp);
    serrno = save_serrno;
    return(-1);
  }
  
  
  /*
   * Update client context with the updated Tape instance
   */
  /*  Cstager_Tape_delete(clientCntx->currentTp); */
  /*  clientCntx->currentTp = tp; */
  C_IAddress_delete(iAddr);

  return(0);
}

static int validFile(
                     file
                     )
     file_list_t *file;
{
  unsigned char nullblkid[4] = {'\0', '\0', '\0', '\0'};
  if ( file == NULL ) return(0);
  if ( (*file->filereq.file_path != '\0' &&
        *file->filereq.file_path != '.') &&
       (file->filereq.tape_fseq > 0 ||
        memcmp(file->filereq.blockid,nullblkid,sizeof(nullblkid)) != 0) ) {
    return(1);
  }
  return(0);
}

static int doFullUpdate(
                        clientCntx,
                        tape,
                        nbIncompleteSegments
                        )
     tape_list_t *tape;
     ClientCntx_t *clientCntx;
     int nbIncompleteSegments;
{
  file_list_t *fl;
  struct Cstager_Segment_t *segm;
  enum SegmentStatusCodes segmStatus;
  int rc = 0, updated = 0, save_serrno, nbIncomplete = 0, nbIterations = 0;

  if ( (tape == NULL) || 
       (clientCntx == NULL) ||
       (clientCntx->currentTp == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  
  if ( rtcpcldc_killed(tape) == 1 ) return(-1);

  do {
    nbIterations++;
    CLIST_ITERATE_BEGIN(tape->file,fl) 
      {
        if ( fl->filereq.proc_status != RTCP_FINISHED ) {
          segm = NULL;
          rc = newOrUpdatedSegment(
                                   tape,
                                   fl,
                                   clientCntx,
                                   &segm
                                   );
          if ( rc == -1 ) {
            break;
          } else if ( rc == 1 ) {
            rtcp_log(LOG_DEBUG,
                     "doFullUpdate(): %s/%d(%s) new segment disk_fseq=%d, tape_fseq=%d, blockid=%.2d%.2d%.2d%.2d, %s\n",
                     tape->tapereq.vid,
                     tape->tapereq.side,
                     (tape->tapereq.mode == WRITE_DISABLE ? "r" : "w"),
                     fl->filereq.disk_fseq,
                     fl->filereq.tape_fseq,
                     (int)fl->filereq.blockid[0],
                     (int)fl->filereq.blockid[1],
                     (int)fl->filereq.blockid[2],
                     (int)fl->filereq.blockid[3],
                     fl->filereq.file_path
                     );
            if ( nbIncompleteSegments < MAX_INCOMPLETE_SEGMENTS ) {
              rc = addSegment(
                              clientCntx,
                              fl,
                              &segm
                              );
              if ( rc == -1 ) break;
              updated = 1;
              nbIncompleteSegments++;
            }
          } else if ( (rc == 2) && (segm != NULL) ) {
            rtcp_log(LOG_DEBUG,
                     "doFullUpdate(): %s/%d(%s) update segment disk_fseq=%d, tape_fseq=%d, blockid=%.2d%.2d%.2d%.2d, %s\n",
                     tape->tapereq.vid,
                     tape->tapereq.side,
                     (tape->tapereq.mode == WRITE_DISABLE ? "r" : "w"),
                     fl->filereq.disk_fseq,
                     fl->filereq.tape_fseq,
                     (int)fl->filereq.blockid[0],
                     (int)fl->filereq.blockid[1],
                     (int)fl->filereq.blockid[2],
                     (int)fl->filereq.blockid[3],
                     fl->filereq.file_path
                     );
            Cstager_Segment_status(segm,&segmStatus);
            if ( ((segmStatus == SEGMENT_WAITFSEQ) ||
                  (segmStatus == SEGMENT_WAITPATH)) &&
                 (validFile(fl) == 1) ) {
              nbIncompleteSegments--;
            }
            rc = updateSegment(fl,segm);
            if ( rc == -1 ) break;
            rc = updateSegmentInDB(tape,fl,segm);
            if ( rc == -1 ) break;
          }
        }
      }
    CLIST_ITERATE_END(tape->file,fl);
  } while ( (nbIncompleteSegments < MAX_INCOMPLETE_SEGMENTS) &&
            (nbIterations < 2) );
  
  if ( updated == 1 ) {
    rc = addNewSegmentsToDB(tape,clientCntx);
    if ( rc == -1 ) save_serrno = serrno;
  }

  if ( rc == -1 ) serrno = save_serrno;
  
  return(rc);
}

static int getUpdates(
                      tape,
                      clientCntx
                      )
     tape_list_t *tape;
     ClientCntx_t *clientCntx;
{
  struct Cstager_Tape_t *newTp = NULL;
  struct C_Services_t **svcs = NULL;
  struct Cstager_Segment_t **newSegmArray = NULL, *newSegment = NULL;
  enum Cstager_TapeStatusCodes_t tpOldStatus, tpNewStatus;
  enum Cstager_SegmentStatusCodes_t segmNewStatus;
  file_list_t *file;
  int rc, save_serrno, i;
  int callGetMoreInfo = 0, nbNewSegms = 0, nbIncompleteSegments = 0;
  char *segmCksumAlgorithm, *errmsgtxt, *vwAddress;
  unsigned char *blockid;
  ID_TYPE key;

  if ( (tape == NULL) || 
       (clientCntx == NULL) || 
       (clientCntx->currentTp == NULL)  ) {
    serrno = EINVAL;
    return(-1);
  }
  rtcp_log(LOG_DEBUG,"getUpdates() check for updates in DB\n");

  /*
   * Create an update (from db) copy of this Tape+Segments
   */
  Cstager_Tape_create(&newTp);
  if ( newTp == NULL ) return(-1);

  rc = rtcpcld_getDbSvc(&svcs);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("rtcpcld_getDbSvc()","");
    return(-1);
  }
  
  Cstager_Tape_id(clientCntx->currentTp,&key);
  Cstager_Tape_setId(newTp,key);
  rc = rtcpcld_updateTapeFromDB(newTp);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("rtcpcld_updateTapeFromDB()",
                    C_Services_errorMsg(*svcs));
    save_serrno = serrno;
    tape->tapereq.err.errorcode = serrno;
    strcpy(tape->tapereq.err.errmsgtxt,
           C_Services_errorMsg(*svcs));
    tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
    serrno = save_serrno;
    return(-1);
  }

  Cstager_Tape_status(clientCntx->currentTp,&tpOldStatus);
  Cstager_Tape_status(newTp,&tpNewStatus);
  rtcp_log(LOG_DEBUG,"getUpdates() tape status (old=%d, new=%d)\n",
           tpOldStatus,tpNewStatus);
  vwAddress = NULL;
  Cstager_Tape_vwAddress(newTp,(CONST char **)&vwAddress);
  if ( (vwAddress != NULL) &&
       (strncmp(clientCntx->vwAddress,
                vwAddress,
                sizeof(clientCntx->vwAddress))) ) {
    strncpy(
            clientCntx->vwAddress,
            vwAddress,
            sizeof(clientCntx->vwAddress)
            );
    rtcp_log(LOG_DEBUG,"getUpdates() VidWorker address %s\n",
             clientCntx->vwAddress);
  }
  if ( tpOldStatus != tpNewStatus ) {
    if ( tpNewStatus == TAPE_MOUNTED ) {
      strcpy(tape->tapereq.unit,"unknown");
      strcpy(tape->tapereq.server,"unknown");
    }
  }
  if ( tpNewStatus == TAPE_FAILED ) {
    Cstager_Tape_errorCode(newTp,&tape->tapereq.err.errorcode);
    Cstager_Tape_severity(newTp,&tape->tapereq.err.severity);
    errmsgtxt = NULL;
    Cstager_Tape_errMsgTxt(newTp,(CONST char **)&errmsgtxt);
    if ( errmsgtxt != NULL ) {
      strncpy(
              tape->tapereq.err.errmsgtxt,
              errmsgtxt,
              sizeof(tape->tapereq.err.errmsgtxt)-1
              );
    }
    if ( tape->tapereq.err.errorcode == 0 )
      tape->tapereq.err.errorcode = SEINTERNAL;
    if ( tape->tapereq.err.severity == RTCP_OK )
      tape->tapereq.err.severity = RTCP_FAILED | RTCP_UNERR;
    if ( *tape->tapereq.err.errmsgtxt == '\0' ) 
      strcpy(
             tape->tapereq.err.errmsgtxt,
             sstrerror(tape->tapereq.err.errorcode)
             );
    tape->tapereq.tprc = -1;
    serrno = tape->tapereq.err.errorcode;
    return(-1);
  }
  
  if ( callGetMoreInfo != -1 ) callGetMoreInfo = 0;
  Cstager_Tape_segments(newTp,&newSegmArray,&nbNewSegms);
  for ( i=0; i<nbNewSegms; i++ ) {
    newSegment = newSegmArray[i];
    
    Cstager_Segment_status(newSegment,&segmNewStatus);
    rc = rtcpcld_findFileFromSegment(
                                     newSegment,
                                     tape,
                                     clientCntx->clientAddr,
                                     &file
                                     );
    if ( rc == 0 ) continue;
    if ( rc == -1 ) {
      save_serrno = serrno;
      break;
    }
    /*
     * Skip all finished segments
     */
    if ( file->filereq.proc_status == RTCP_FINISHED ) continue;
    
    switch (segmNewStatus) {
    case SEGMENT_FILECOPIED:
      file->filereq.proc_status = RTCP_FINISHED;
      file->filereq.TEndTransferTape = 
        file->filereq.TEndTransferDisk = (int)time(NULL);
      Cstager_Segment_bytes_in(
                               newSegment,
                               &file->filereq.bytes_in
                               );
      Cstager_Segment_bytes_out(
                                newSegment,
                                &file->filereq.bytes_out
                                );
      Cstager_Segment_host_bytes(
                                 newSegment,
                                 &file->filereq.host_bytes
                                 );
      segmCksumAlgorithm = NULL;
      Cstager_Segment_segmCksumAlgorithm(
                                         newSegment,
                                         (CONST char **)&segmCksumAlgorithm
                                         );
      if ( segmCksumAlgorithm != NULL ) {
        strncpy(
                file->filereq.castorSegAttr.segmCksumAlgorithm,
                segmCksumAlgorithm,
                sizeof(file->filereq.castorSegAttr.segmCksumAlgorithm)-1
                );
      }
      
      Cstager_Segment_segmCksum(
                                newSegment,
                                &file->filereq.castorSegAttr.segmCksum
                                );
    case SEGMENT_COPYRUNNING:
      if ( segmNewStatus == SEGMENT_COPYRUNNING ) {
        /* 
         * Check that we haven't already processed this tape position
         */
        if ( file->filereq.proc_status == RTCP_POSITIONED ) break;
        file->filereq.proc_status = RTCP_POSITIONED;
        file->filereq.TStartPosition = file->filereq.TEndPosition = 
          file->filereq.TStartTransferTape = 
          file->filereq.TStartTransferDisk = (int)time(NULL);
      }
      blockid = NULL;
      Cstager_Segment_blockid(
                              newSegment,
                              (CONST unsigned char **)&blockid
                              );
      if ( blockid != NULL ) {
        memcpy(
               file->filereq.blockid,
               blockid,
               sizeof(file->filereq.blockid)
               );                         
      }
      
      Cstager_Segment_fseq(
                           newSegment,
                           &file->filereq.tape_fseq
                           );
      file->filereq.convert = ASCCONV;
      (void)rtcpc_GiveInfo(tape,file,0);
      rc = updateCaller(tape,file);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_DEBUG,"updateCaller() returned %d, serrno=%d. Set VID FAILED\n",
                 rc,save_serrno);
        (void)rtcpcld_setVIDFailedStatus(tape);
      }
      /* Remove finished segments */
      if ( segmNewStatus == SEGMENT_FILECOPIED ) 
        (void)deleteSegmentFromDB(newSegment);
      /* Allow client to add more requests to keep the stream running */
      if ( callGetMoreInfo == 0 ) callGetMoreInfo = 1;
      break;
    case SEGMENT_FAILED:
      callGetMoreInfo = -1;
      errmsgtxt = NULL;
      Cstager_Segment_errMsgTxt(
                                newSegment,
                                (CONST char **)&errmsgtxt
                                );
      if ( errmsgtxt != NULL ) {
        strncpy(
                file->filereq.err.errmsgtxt,
                errmsgtxt,
                sizeof(file->filereq.err.errmsgtxt)-1
                );
      }
      
      Cstager_Segment_errorCode(
                                newSegment,
                                &file->filereq.err.errorcode
                                );
      Cstager_Segment_severity(
                               newSegment,
                               &file->filereq.err.severity
                               );
      file->filereq.cprc = -1;
      save_serrno = file->filereq.err.errorcode;
      (void)rtcpc_GiveInfo(tape,file,0);
      (void)updateCaller(tape,file);
      rc = -1;
      break;
    case SEGMENT_WAITFSEQ:
    case SEGMENT_WAITPATH:
      nbIncompleteSegments++;
      if (callGetMoreInfo == 0 ) callGetMoreInfo = 1;
      break;
    case SEGMENT_UNKNOWN:
      rtcp_log(
               LOG_ERR,
               "getUpdates(): UNKNOWN status found for fseq=%d, path=%s\n",
               (file != NULL ? file->filereq.tape_fseq : -1),
               (file != NULL ? file->filereq.file_path : "(null)")
               );
      save_serrno = SEINTERNAL;
      rc = -1;
      break;
    default:
      break;
    }
    if ( rc == -1 ) break;
  }
  Cstager_Tape_delete(clientCntx->currentTp);
  clientCntx->currentTp = newTp;
  if ( rc == -1 ) {
    serrno = save_serrno;
    return(-1);
  }

  if ( callGetMoreInfo == 1 ) {
    rc = getMoreInfo(tape);
    if ( rc == -1 ) {
      save_serrno = serrno;
    } else {
      rc = doFullUpdate(clientCntx,tape,nbIncompleteSegments);
      if ( rc == -1 ) save_serrno = serrno;
    }
    if ( rc == -1 ) {
      serrno = save_serrno;
      return(rc);
    }
  }
  return(0);
}

int _findReqMap(
                clientCntx,
                tape,
                foundMap
                )
     ClientCntx_t *clientCntx;
     tape_list_t *tape;
     RtcpcldcReqmap_t **foundMap;
{
  int found = 0;
  RtcpcldcReqmap_t *map;

  if ( foundMap != NULL ) *foundMap = NULL;
  CLIST_ITERATE_BEGIN(rtcpcldc_reqMap,map) 
    {
      if ( ((clientCntx != NULL) && (map->clientCntx == clientCntx)) ||
           ((tape != NULL) && (map->tape == tape)) ) {
        found = 1;
        break;
      }
    }
  CLIST_ITERATE_END(rtcpcldc_reqMap,map);

  if ( (found == 1) && (foundMap != NULL) ) *foundMap = map;
  return(found);
}

int findReqMap(
               clientCntx,
               tape
               )
     ClientCntx_t **clientCntx;
     tape_list_t **tape;
{
  int rc, save_serrno;
  RtcpcldcReqmap_t *map = NULL;
  ClientCntx_t *cc;
  tape_list_t *tl;

  if ( clientCntx != NULL ) cc = *clientCntx;
  if ( tape != NULL ) tl = *tape;

  rc = Cmutex_lock(&rtcpcldc_reqMap,-1);
  if ( rc == -1 ) return(-1);
  
  rc = _findReqMap(cc,tl,&map);
  if ( rc == -1 ) save_serrno = serrno;
  if ( rc == 1) {
    if ( (clientCntx != NULL) && (cc == NULL) ) *clientCntx = map->clientCntx;
    if ( (tape != NULL) && (tl == NULL) ) *tape = map->tape;
  }
  (void)Cmutex_unlock(&rtcpcldc_reqMap);
  if ( rc == -1 ) serrno = save_serrno;
  return(rc);
}

int addReqMap(
              clientCntx,
              tape
              )
     ClientCntx_t *clientCntx;
     tape_list_t *tape;
{
  int rc,  save_serrno;
  RtcpcldcReqmap_t *newMap = NULL;
  
  rc = Cmutex_lock(&rtcpcldc_reqMap,-1);
  if ( rc == -1 ) return(-1);

  rc = _findReqMap(clientCntx,tape,&newMap);
  if ( rc == -1 ) save_serrno = serrno;
  if ( rc == 0 ) {
    newMap = (RtcpcldcReqmap_t *)calloc(1,sizeof(RtcpcldcReqmap_t));
    if ( newMap == NULL ) {
      save_serrno = errno;
      (void)Cmutex_unlock(&rtcpcldc_reqMap);
      serrno = save_serrno;
      return(-1);
    }
    newMap->clientCntx = clientCntx;
    newMap->tape = tape;
    CLIST_INSERT(rtcpcldc_reqMap,newMap);
  }
  (void)Cmutex_unlock(&rtcpcldc_reqMap);
  if ( rc == -1 ) serrno = save_serrno;
  return(rc);
}

int delReqMap(
              clientCntx,
              tape
              )
     ClientCntx_t *clientCntx;
     tape_list_t *tape;
{
  int rc,  save_serrno;
  RtcpcldcReqmap_t *map = NULL;
  
  rc = Cmutex_lock(&rtcpcldc_reqMap,-1);
  if ( rc == -1 ) return(-1);

  rc = _findReqMap(clientCntx,tape,&map);
  if ( rc == -1 ) save_serrno = serrno;
  if ( rc == 1 ) {
    CLIST_DELETE(rtcpcldc_reqMap,map);
    free(map);
  }
  (void)Cmutex_unlock(&rtcpcldc_reqMap);
  if ( rc == -1 ) serrno = save_serrno;
  return(rc);
}

int rtcpcldc_setKilled(
                       tape
                       )
     tape_list_t *tape;
{
  rtcp_log(LOG_ERR,"rtcpcldc_killed(): request aborting\n");
  (void)rtcpc_SetLocalErrorStatus(tape,
                                  ERTUSINTR,
                                  "request aborting",
                                  RTCP_FAILED|RTCP_USERR,
                                  -1);
  serrno = ERTUSINTR;
  return(0);
}

int rtcpcldc_killed(
                    tape
                    )
     tape_list_t *tape;
{
  if ( (tape != NULL) && (tape->tapereq.err.errorcode == ERTUSINTR) ) {
    serrno = ERTUSINTR;
    return(1);
  }
  return(0);
}

int rtcpcldc_kill(
                  tape
                  )
     tape_list_t *tape;
{
  rtcpHdr_t hdr;
  ClientCntx_t *clientCntx = NULL;
  char *server, *p;
  int rc, save_serrno, port, noLock = 0;
  SOCKET abortSocket = INVALID_SOCKET;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  (void)rtcpcldc_setKilled(tape);
  
  rc = findReqMap(&clientCntx,&tape);
  if ( (rc == 1) && (clientCntx != NULL) ) {
    p = strstr(clientCntx->vwAddress,":");
    if ( p == NULL ) {
      rtcp_log(LOG_ERR,"rtcpcldc_kill(): Incorrect format for VidWorker address: %s\n",
               clientCntx->vwAddress);
      serrno = SEINTERNAL;
      return(-1);
    }
    *p = '\0';
    server = clientCntx->vwAddress;
    port = atoi(++p);

    rc = rtcp_Connect(&abortSocket,server,&port,RTCP_CONNECT_TO_CLIENT);
    if ( rc == -1 ) {
      save_serrno = serrno;
      LOG_FAILED_CALL("rtcp_Connect()","");
      serrno = save_serrno;
      return(-1);
    }
    hdr.magic = RTCOPY_MAGIC;
    hdr.reqtype = RTCP_KILLJID_REQ;
    hdr.len = 0;
    
    rc = rtcp_SendReq(&abortSocket,&hdr,NULL,NULL,NULL);
    if ( rc == -1 ) {
      save_serrno = serrno;
      LOG_FAILED_CALL("rtcp_SendReq(RTCP_KILLJID_REQ)","");
      serrno = save_serrno;
      return(-1);
    }
    
    rc = rtcp_RecvAckn(&abortSocket,hdr.reqtype);
    if ( rc == -1 ) {
      save_serrno = serrno;
      LOG_FAILED_CALL("rtcp_RecvAckn(RTCP_KILLJID_REQ)","");
      serrno = save_serrno;
      return(-1);
    }
    if ( noLock == 0 ) (void)Cmutex_unlock(tape);
    (void)rtcp_CloseConnection(&abortSocket);
  }
  return(0);
}

void rtcpcldc_cleanup(
                      tape
                      )
     tape_list_t *tape;
{
  ClientCntx_t *clientCntx = NULL;
  struct Cstager_Segment_t **segmArray = NULL;
  char *clientAddr;
  int rc, i, nbSegms = 0;

  (void)Cmutex_lock(&rtcpcldc_cleanupLock,-1);
  rc = findReqMap(&clientCntx,&tape);
  if ( (rc == -1) || (clientCntx == NULL) ) {
    (void)Cmutex_unlock(&rtcpcldc_cleanupLock);
    return;
  }

  if ( clientCntx->currentTp != NULL ) {
    Cstager_Tape_segments(clientCntx->currentTp,&segmArray,&nbSegms);
    if ( segmArray == NULL ) {
      (void)Cmutex_unlock(&rtcpcldc_cleanupLock);
      return;
    }
    
    for ( i=0; i<nbSegms; i++ ) {
      clientAddr = NULL;
      Cstager_Segment_clientAddress(segmArray[i],(CONST char **)&clientAddr);
      if ( (clientAddr == NULL) || 
           (strcmp(clientAddr,clientCntx->clientAddr) == 0) )
        (void)deleteSegmentFromDB(segmArray[i]);
    }
    Cstager_Tape_delete(clientCntx->currentTp);
    clientCntx->currentTp = NULL;
  }
  (void)delReqMap(clientCntx,tape);
  free(clientCntx);
  (void)Cmutex_unlock(&rtcpcldc_cleanupLock);
  return;
}

int rtcpcldc(tape)
     tape_list_t *tape;
{
  struct C_IObject_t *tapeIObj = NULL;
  struct C_Services_t **dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  ClientCntx_t *clientCntx = NULL;
  enum Cstager_TapeStatusCodes_t currentStatus;
  file_list_t *fl;
  int rc, save_serrno, notificationPort = 0, maxfd = 0;
  int nbIncompleteSegments = 0;
  SOCKET *notificationSocket = NULL;
  char myHost[CA_MAXHOSTNAMELEN+1], *p;
  struct timeval timeout;
  time_t notificationTimeout;
  fd_set rd_set, rd_setcp;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  dontLog = 1;

  (void)initOldStgUpdc(tape);

  clientCntx = (ClientCntx_t *)calloc(1,sizeof(ClientCntx_t));
  if ( clientCntx == NULL ) {
    rtcp_log(
             LOG_ERR,
             "rtcpcldc() failed to initialise client context: %s\n",
             strerror(errno)
             );
    serrno = SESYSERR;
    return(-1);
  }
  rc = addReqMap(clientCntx,tape);
  if ( rc == -1 ) {
    rtcp_log(
             LOG_ERR,
             "rtcpcldc() addReqMap(): %s\n",
             sstrerror(save_serrno)
             );
    serrno = save_serrno;
    return(-1);
  }
  
  rc = rtcpcld_initNotifyByPort(&notificationSocket,&notificationPort);
  if ( (rc == -1) || (notificationSocket == NULL) ) {
    LOG_FAILED_CALL("rtcpcld_initNotifyByPort()","");
    return(-1);
  }
  
  rc = gethostname(myHost,sizeof(myHost)-1);
  sprintf(clientCntx->clientAddr,"%s:%d",myHost,notificationPort);
  notificationTimeout = RTCPCLD_NOTIFYTIMEOUT;
  if ( (p = (char *)getconfent("RTCPCLD","NOTIFYTIMEOUT",0)) != NULL ) {
    notificationTimeout = atoi(p);
  } else if ( (p = getenv("RTCPCLD_NOTIFYTIMEOUT")) != NULL ) {
    notificationTimeout = atoi(p);
  }
  rtcp_log(
           LOG_DEBUG,
           "Notification address = %s, timeout = %d\n",
           clientCntx->clientAddr,
           notificationTimeout
           );

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 || dbSvc == NULL || *dbSvc == NULL ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcpcld_getDbSvc()","");
    *dbSvc = NULL;
    strncpy(tape->tapereq.err.errmsgtxt,
            sstrerror(save_serrno),
            sizeof(tape->tapereq.err.errmsgtxt)-1);
    tape->tapereq.err.errorcode = save_serrno;
    tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
    serrno = save_serrno;
    return(-1);
  }

  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()","");
    save_serrno = serrno;
    strncpy(tape->tapereq.err.errmsgtxt,
            sstrerror(save_serrno),
            sizeof(tape->tapereq.err.errmsgtxt)-1);
    tape->tapereq.err.errorcode = save_serrno;
    tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
    rtcpcldc_cleanup(tape);
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);

  rc = rtcpcld_getStgDbSvc(&stgSvc);
  if ( rc == -1 || stgSvc == NULL ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcpcld_getStgDbSvc()","");
    stgSvc = NULL;
    C_IAddress_delete(iAddr);
    rtcpcldc_cleanup(tape);
    serrno = save_serrno;
    return(-1);
  }
  
  serrno = 0;
  rc = Cstager_IStagerSvc_selectTape(
                                     stgSvc,
                                     &clientCntx->currentTp,
                                     tape->tapereq.vid,
                                     tape->tapereq.side,
                                     tape->tapereq.mode
                                     );
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cstager_IStagerSvc_selectTape()",
                    C_Services_errorMsg(*dbSvc));
    if ( serrno == 0 ) serrno = errno;
    save_serrno = serrno;
    rtcp_log(LOG_ERR,"Cstager_IStagerSvc_selectTape() DB error: %s\n",
             Cstager_IStagerSvc_errorMsg(stgSvc));
    strncpy(tape->tapereq.err.errmsgtxt,
            Cstager_IStagerSvc_errorMsg(stgSvc),
            sizeof(tape->tapereq.err.errmsgtxt)-1);
    tape->tapereq.err.errorcode = save_serrno;
    tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
    (void)C_Services_rollback(*dbSvc,iAddr);
    C_IAddress_delete(iAddr);
    rtcpcldc_cleanup(tape);
    serrno = save_serrno;
    return(-1);
  }
  Cstager_Tape_status(clientCntx->currentTp,&currentStatus);
  if ( currentStatus != TAPE_PENDING &&
       currentStatus != TAPE_WAITVDQM &&
       currentStatus != TAPE_WAITMOUNT &&
       currentStatus != TAPE_MOUNTED ) {
    Cstager_Tape_setStatus(clientCntx->currentTp,TAPE_PENDING);
  } 
  Cstager_Tape_status(clientCntx->currentTp,&currentStatus);
  tape->tapereq.TStartRequest = (int)time(NULL);
  
  rtcp_log(
           LOG_DEBUG,
           "Tape req: vid=%s, side=%d, mode=%d, status=%d\n",
           tape->tapereq.vid,
           tape->tapereq.side,
           tape->tapereq.mode,
           currentStatus
           );
  
  CLIST_ITERATE_BEGIN(tape->file,fl) 
    {
      fl->filereq.err.severity = fl->filereq.err.severity &
        ~RTCP_RESELECT_SERV;
      fl->filereq.cprc = 0;
      if ( fl->filereq.proc_status != RTCP_FINISHED ){
        fl->filereq.err.errorcode = 0;
        fl->filereq.err.severity = 0;
        *fl->filereq.err.errmsgtxt = '\0';
        if ( (validFile(fl) == 1) || 
             (nbIncompleteSegments++ < MAX_INCOMPLETE_SEGMENTS) ) {
          rc = addSegment(
                          clientCntx,
                          fl,
                          NULL
                          );
          if ( rc == -1 ) {
            LOG_FAILED_CALL("addSegment()","");
            save_serrno = serrno;
            (void)C_Services_rollback(*dbSvc,iAddr);
            C_IAddress_delete(iAddr);
            rtcpcldc_cleanup(tape);
            serrno = save_serrno;
            return(-1);
          }
        }
      }
    }
  CLIST_ITERATE_END(tape->file,fl);
  
  tapeIObj = Cstager_Tape_getIObject(clientCntx->currentTp);

  if ( rtcpcldc_killed(tape) != 0 ) {
    save_serrno = serrno;
    (void)C_Services_rollback(*dbSvc,iAddr);
    C_IAddress_delete(iAddr);
    rtcpcldc_cleanup(tape);
    rtcp_log(LOG_ERR,"Request aborted\n");
    serrno = save_serrno;
    return(-1);
  }
  
  rc = C_Services_updateRep(*dbSvc,iAddr,tapeIObj,1);
  if ( rc != 0 ) {
    LOG_FAILED_CALL("C_Services_updateRep()",
                    C_Services_errorMsg(*dbSvc));
    save_serrno = serrno;
    (void)C_Services_rollback(*dbSvc,iAddr);
    tape->tapereq.err.errorcode = save_serrno;
    strncpy(tape->tapereq.err.errmsgtxt,
            C_Services_errorMsg(*dbSvc),
            sizeof(tape->tapereq.err.errmsgtxt)-1);
    tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
    C_IAddress_delete(iAddr);
    rtcpcldc_cleanup(tape);
    rtcp_log(LOG_ERR,"DB error: %s\n",
             tape->tapereq.err.errmsgtxt);
    serrno = save_serrno;
    return(-1);
  }
  (void)rtcpcld_notifyRtcpclientd();
  
  C_IAddress_delete(iAddr);

  FD_ZERO(&rd_set);
  FD_SET(*notificationSocket,&rd_set);
#ifndef _WIN32
  maxfd = (*notificationSocket)+1;
#endif /* _WIN32 */


  for (;;) {
    rd_setcp = rd_set;
    timeout.tv_sec = (time_t)notificationTimeout;
    timeout.tv_usec = 0;
    errno = 0;
    rc = select(maxfd,&rd_setcp,NULL,NULL,&timeout);
    if ( rc >= 0 ) {
      if ( rtcpcldc_killed(tape) == 1 ) {
        rc = -1;
        save_serrno = ERTUSINTR;
        break;
      }
      
      if ( rc > 0 ) (void)rtcpcld_getNotify(*notificationSocket);
      rc = getUpdates(tape,clientCntx);
      save_serrno = serrno;
      if ( rc == -1 ) break;
    } else {
      LOG_FAILED_CALL("select()","");
      break;
    }
    if ( rc == -1 ) {
      LOG_FAILED_CALL("Cmutex_lock()","");
      save_serrno = serrno;
      break;
    }
    rc = rtcpc_EndOfRequest(tape);
    if ( rc == -1 ) {
      LOG_FAILED_CALL("rtcpc_EndOfRequest()","");
      save_serrno = serrno;
    }

    Cstager_Tape_status(clientCntx->currentTp,&currentStatus);
    if ( (rc == 0) && (currentStatus == TAPE_FINISHED) ) {
      rtcp_log(LOG_INFO,
               "rtcpcldc() premature end of request forced by server\n");
      CLIST_ITERATE_BEGIN(tape->file,fl) 
        {
          if ( fl->filereq.proc_status < RTCP_FINISHED ) {
            fl->filereq.err.errorcode = ERTMORETODO;
            fl->filereq.err.severity = RTCP_RESELECT_SERV;
            strcpy(fl->filereq.err.errmsgtxt,
                   sstrerror(fl->filereq.err.errorcode));
            save_serrno = ERTMORETODO;
            rc = -1;
          }
        }
      CLIST_ITERATE_END(tape->file,fl);
    }
    if ( (rc == -1) || (rc == 1) ) break;
  }

  rtcpcldc_cleanup(tape);

  if ( rc == -1 ) {
    serrno = save_serrno;
    return(-1);
  }
  return(0);
}
