/******************************************************************************
 *                      rtcpcldapi.c
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
 * @(#)$RCSfile: rtcpcldapi.c,v $ $Revision: 1.24 $ $Release$ $Date: 2004/07/29 14:23:28 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldapi.c,v $ $Revision: 1.24 $ $Date: 2004/07/29 14:23:28 $ CERN-IT/ADC Olof Barring";
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
#include <stage.h>
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

#define LOG_FAILED_CALL(X)  { \
        int _save_errno = errno; \
        int _save_serrno = serrno; \
        rtcp_log( \
                 LOG_ERR, \
                 "(%s:%d) %s %s (serrno=%d, errno=%d)\n", \
                 __FILE__, \
                 __LINE__, \
                 (X), \
                 sstrerror((_save_serrno > 0 ? _save_serrno : _save_errno)), \
                 serrno, \
                 errno \
                 ); \
        serrno = _save_serrno; \
        errno = _save_errno; \
}

TpReqMap_t *runningReqsMap = NULL;

static int ENOSPCkey = -1;

int (*rtcpcld_ClientCallback) _PROTO((
                                      rtcpTapeRequest_t *,
                                      rtcpFileRequest_t *
                                      )) = NULL;
int (*rtcpcld_MoreInfoCallback) _PROTO((
                                        tape_list_t *
                                        )) = NULL;
static int getIAddr(
                    object,
                    iAddr
                    )
     struct C_IObject_t *object;
     struct C_IAddress_t **iAddr;
{
  struct Cdb_DbAddress_t *dbAddr;
  struct C_BaseAddress_t *baseAddr;
  int rc;
  ID_TYPE id;

  if ( object == NULL || iAddr == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = C_IObject_id(object,&id);
  if ( rc == -1 ) return(-1);

  rc = Cdb_DbAddress_create(id,"OraCnvSvc",SVC_ORACNV,&dbAddr);
  if ( rc == -1 ) return(-1);
  
  baseAddr = Cdb_DbAddress_getBaseAddress(dbAddr);
  *iAddr = C_BaseAddress_getIAddress(baseAddr);
  return(0);
}

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

  rc = Cglobals_get(&ENOSPCkey,(void **)&ENOSPC_occurred,sizeof(int));
  if ( rc == -1 || ENOSPC_occurred == NULL ) {
    LOG_FAILED_CALL("Cglobals_get()");
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
  
  rc = Cglobals_get(&ENOSPCkey,(void **)&ENOSPC_occurred,sizeof(int));
  if ( rc == -1 || ENOSPC_occurred == NULL ) {
    LOG_FAILED_CALL("Cglobals_get()");
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
      LOG_FAILED_CALL("rtcpcld_ClientCallback()");
      return(-1);
    }
  }

  rc = updateOldStgCat(tape,file);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("updateOldStgCat()");
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
      LOG_FAILED_CALL("rtcpcld_MoreInfoCallback()");
      return(-1);
    }
  }

  return(0);
}

static int newOrUpdatedSegment(
                               tapereq,
                               filereq,
                               tpList,
                               foundSegment
                               )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
     RtcpcldTapeList_t *tpList;
     RtcpcldSegmentList_t **foundSegment;
{
  RtcpcldTapeList_t *tpIterator;
  RtcpcldSegmentList_t *segmIterator;
  struct Cstager_Segment_t *segm;
  unsigned char *blockid;
  unsigned char nullblkid[4] = { '\0', '\0', '\0', '\0' };
  char *diskPath, *vid;
  char nullDiskPath[1] = {'\0'};
  char nullVID[1] = {'\0'};
  int side, mode, found = 0, newSegment = 0, updatedSegment = 0, fseq;
  
  if ( tapereq == NULL || filereq == NULL || tpList == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  CLIST_ITERATE_BEGIN(tpList,tpIterator) 
    {
      Cstager_Tape_vid(tpIterator->tp,(CONST char **)&vid);
      Cstager_Tape_tpmode(tpIterator->tp,&mode);
      Cstager_Tape_side(tpIterator->tp,&side);
      if ( vid == NULL ) vid = nullVID;
      if ( (strcmp(tapereq->vid,vid) == 0) &&
           (tapereq->side == side) &&
           (tapereq->mode == mode) ) {
        CLIST_ITERATE_BEGIN(tpIterator->segments,segmIterator) 
          {
            segm = segmIterator->segment;
            Cstager_Segment_fseq(segm,&fseq);
            Cstager_Segment_blockid(segm,(CONST unsigned char **)&blockid);
            Cstager_Segment_diskPath(segm,(CONST char **)&diskPath);
            if ( blockid == NULL ) blockid = nullblkid;
            if ( diskPath == NULL ) diskPath = nullDiskPath;
            if ( (fseq > 0) ||
                 ((memcmp(blockid,nullblkid,4) != 0)) ||
                 ((*diskPath != '\0') && (strcmp(diskPath,".") != 0)) ) {
              /*
               * Either the tape position MUST correspond ...
               */
              if ( ((fseq > 0) && 
                    (fseq == filereq->tape_fseq)) ||
                   ((memcmp(blockid,nullblkid,4) != 0) &&
                    (memcmp(blockid,filereq->blockid,4) == 0)) ) {
                found = 1;
                break;
              } else if ( (fseq <= 0) &&
                          ((memcmp(blockid,nullblkid,4) == 0)) &&
                          ((strcmp(diskPath,filereq->file_path) == 0)) ) {
                /*
                 * ... or, the tape position is not known and the disk file paths match
                 */
                found = 1;
                break;
              }
            }
            if ( found == 1 ) break;
          }
        CLIST_ITERATE_END(tpIterator->segments,segmIterator);
      }
      if ( found == 1 ) break;
    }
  CLIST_ITERATE_END(tpList,tpIterator);
  if ( found == 0 ) {
    newSegment = 1;
  } else {
    newSegment = 0;
    if ( (fseq == filereq->tape_fseq) &&
         (memcmp(blockid,filereq->blockid,sizeof(filereq->blockid)) == 0) &&
         (strcmp(diskPath,filereq->file_path) == 0) ) {
      updatedSegment = 0;
    } else {
      segmIterator->file->filereq = *filereq;
      if ( foundSegment != NULL ) *foundSegment = segmIterator;
      updatedSegment = 1;
    }
  }
  
  return(newSegment + (updatedSegment*2));
}

static int updateSegmentInDB(
                             segment
                             )
     RtcpcldSegmentList_t *segment;
{
  struct C_Services_t **dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *segmIObj;
  rtcpFileRequest_t *filereq;
  int rc, save_serrno;

  if ( segment == NULL || segment->segment == NULL || segment->file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  filereq = &(segment->file->filereq);
  Cstager_Segment_setDiskPath(
                              segment->segment,
                              filereq->file_path
                              );
  Cstager_Segment_setFseq(
                          segment->segment,
                          filereq->tape_fseq
                          );
  Cstager_Segment_setBlockid(
                             segment->segment,
                             filereq->blockid
                             );
  
  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()");
    save_serrno = serrno;
    if ( serrno == 0 ) save_serrno = errno;
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 || dbSvc == NULL || *dbSvc == NULL ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcpcld_getDbSvc()");
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    serrno = save_serrno;
    return(-1);
  }
  segmIObj = Cstager_Segment_getIObject(segment->segment);
  rc = C_Services_updateRep(*dbSvc,iAddr,segmIObj,1);
  if ( rc != 0 ) {
    LOG_FAILED_CALL("C_Services_updateRep()");
    save_serrno = serrno;
    filereq->err.errorcode = serrno;
    strcpy(filereq->err.errmsgtxt,
           C_Services_errorMsg(*dbSvc));
    filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    C_IAddress_delete(iAddr);
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    rtcp_log(LOG_ERR,"DB error: %s\n",filereq->err.errmsgtxt);
    serrno = save_serrno;
    return(-1);
  }
  return(0);
}

static int addSegment(
                      tpList,
                      file,
                      myNotificationAddr,
                      newSegm)
     RtcpcldTapeList_t *tpList;
     file_list_t *file;
     char *myNotificationAddr;
     RtcpcldSegmentList_t **newSegm;
{
  int rc;
  char *notificationAddr = NULL;
  RtcpcldSegmentList_t *rtcpcldSegm = NULL;
  enum SegmentStatusCodes segmentStatus;
  
  if ( tpList == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  rtcp_log(
           LOG_DEBUG,
           "Add segment: fseq=%d, blockid=%.2d%.2d%.2d%.2d, diskPath=%s\n",
           file->filereq.tape_fseq,
           (int)file->filereq.blockid[0],
           (int)file->filereq.blockid[1],
           (int)file->filereq.blockid[2],
           (int)file->filereq.blockid[3],
           file->filereq.file_path
           );
           
  notificationAddr = myNotificationAddr;
  
  rtcpcldSegm = (RtcpcldSegmentList_t *)
    calloc(1,sizeof(RtcpcldSegmentList_t));
  if ( rtcpcldSegm == NULL ) {
    LOG_FAILED_CALL("calloc()");
    serrno = SESYSERR;
    return(-1);
  }
  rtcpcldSegm->file = file;
  rtcpcldSegm->tp = tpList;
  serrno = 0;
  rc = Cstager_Segment_create(&rtcpcldSegm->segment);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cstager_Segment_create()");
    if ( serrno == 0 ) serrno = errno;
    return(-1);
  }
  CLIST_INSERT(tpList->segments, rtcpcldSegm);
  Cstager_Segment_setFseq(
                          rtcpcldSegm->segment,
                          file->filereq.tape_fseq
                          );
  Cstager_Segment_setDiskPath(
                              rtcpcldSegm->segment,
                              file->filereq.file_path
                              );
  Cstager_Segment_setCastorNsHost(
                                  rtcpcldSegm->segment,
                                  file->filereq.castorSegAttr.nameServerHostName
                                  );
  Cstager_Segment_setCastorFileId(
                                  rtcpcldSegm->segment,
                                  file->filereq.castorSegAttr.castorFileId
                                  );
  Cstager_Segment_setOffset(
                            rtcpcldSegm->segment,
                            file->filereq.offset
                            );
  Cstager_Segment_setBlockid(
                             rtcpcldSegm->segment,
                             file->filereq.blockid
                             );
  Cstager_Segment_setSegmCksumAlgorithm(
                                        rtcpcldSegm->segment,
                                        file->filereq.castorSegAttr.segmCksumAlgorithm
                                        );
  Cstager_Segment_setStgReqId(
                              rtcpcldSegm->segment,
                              file->filereq.stgReqId
                              );
  if ( file->filereq.proc_status == RTCP_WAITING ||
       file->filereq.proc_status == RTCP_POSITIONED ) {
    segmentStatus = SEGMENT_UNPROCESSED;
  } else if ( file->filereq.proc_status == RTCP_FINISHED ) {
    segmentStatus = SEGMENT_FILECOPIED;
  } else {
    segmentStatus = SEGMENT_UNKNOWN;
  }
  Cstager_Segment_setStatus(
                            rtcpcldSegm->segment,
                            segmentStatus
                            );
  rtcpcldSegm->oldStatus = segmentStatus;
  if ( notificationAddr == NULL ) {
    Cstager_Segment_clientAddress(
                                  rtcpcldSegm->prev->segment,
                                  (const char **)&notificationAddr
                                  );
  }
  
  if ( notificationAddr != NULL ) {
    Cstager_Segment_setClientAddress(
                                     rtcpcldSegm->segment,
                                     notificationAddr
                                     );
  }

  Cstager_Segment_setTape(
                          rtcpcldSegm->segment,
                          tpList->tp
                          );
  Cstager_Tape_addSegments(
                           tpList->tp,
                           rtcpcldSegm->segment
                           );

  if ( newSegm != NULL ) *newSegm = rtcpcldSegm;
  return(0);
}

static int addSegmentToDB(
                          filereq,
                          segment
                          )
     rtcpFileRequest_t *filereq;
     RtcpcldSegmentList_t *segment;
{
  struct C_Services_t **dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *segmIObj;
  int rc, save_serrno;
  
  if ( segment == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()");
    save_serrno = serrno;
    if ( serrno == 0 ) save_serrno = errno;
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("C_Services_create()");
    serrno = save_serrno;
    return(-1);
  }
  segmIObj = Cstager_Segment_getIObject(segment->segment);
  rc = C_Services_createRep(*dbSvc,iAddr,segmIObj,1);
  if ( rc != 0 ) {
    LOG_FAILED_CALL("C_Services_createRep()");
    save_serrno = serrno;
    strcpy(filereq->err.errmsgtxt,
           C_Services_errorMsg(*dbSvc));
    filereq->err.severity = RTCP_FAILED|RTCP_SYERR;
    C_IAddress_delete(iAddr);
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    rtcp_log(LOG_ERR,"DB error: %s\n",filereq->err.errmsgtxt);
    serrno = save_serrno;
    return(-1);
  }
  C_IAddress_delete(iAddr);
  return(0);
}

static int doFullUpdate(
                        tpList,
                        tape
                        )
     RtcpcldTapeList_t *tpList;
     tape_list_t *tape;
{
  tape_list_t *tl;
  file_list_t *fl;
  RtcpcldSegmentList_t *segm = NULL;
  int rc = 0;
  
  CLIST_ITERATE_BEGIN(tape,tl) 
    {
      CLIST_ITERATE_BEGIN(tl->file,fl) 
        {
          segm = NULL;
          rc = newOrUpdatedSegment(
                                   &tl->tapereq,
                                   &fl->filereq,
                                   tpList,
                                   &segm
                                   );
          if ( rc == -1 ) {
            break;
          } else if ( rc == 1 ) {
            rtcp_log(LOG_DEBUG,
                     "doFullUpdate(): %s/%d(%s) new segment fseq=%d, blockid=%.2d%.2d%.2d%.2d, %s\n",
                     tl->tapereq.vid,
                     tl->tapereq.side,
                     (tl->tapereq.mode == WRITE_DISABLE ? "r" : "w"),
                     fl->filereq.tape_fseq,
                     (int)fl->filereq.blockid[0],
                     (int)fl->filereq.blockid[1],
                     (int)fl->filereq.blockid[2],
                     (int)fl->filereq.blockid[3],
                     fl->filereq.file_path
                     );
            rc = addSegment(
                            tpList,
                            fl,
                            NULL,
                            &segm
                            );
            if ( rc == -1 ) break;
            if ( segm != NULL ) {
              rc = addSegmentToDB(
                                  &fl->filereq,
                                  segm
                                  );
              if ( rc == -1 ) break;
            }
          } else if ( (rc == 2) && (segm != NULL) ) {
            rtcp_log(LOG_DEBUG,
                     "doFullUpdate(): %s/%d(%s) update segment fseq=%d, blockid=%.2d%.2d%.2d%.2d, %s\n",
                     tl->tapereq.vid,
                     tl->tapereq.side,
                     (tl->tapereq.mode == WRITE_DISABLE ? "r" : "w"),
                     fl->filereq.tape_fseq,
                     (int)fl->filereq.blockid[0],
                     (int)fl->filereq.blockid[1],
                     (int)fl->filereq.blockid[2],
                     (int)fl->filereq.blockid[3],
                     fl->filereq.file_path
                     );
            rc = updateSegmentInDB(segm);
            if ( rc == -1 ) break;
          }
        }
      CLIST_ITERATE_END(tl->file,fl);
      if ( rc == -1 ) break;
    }
  CLIST_ITERATE_END(tape,tl);
  
  return(rc);
}

static int getUpdates(
                      svcs,
                      tpList
                      )
     struct C_Services_t *svcs;
     RtcpcldTapeList_t *tpList;
{
  RtcpcldTapeList_t *tpIterator;
  RtcpcldSegmentList_t *segmIterator;
  struct C_IObject_t *iObj;
  struct C_IAddress_t *iAddr;
  enum Cstager_TapeStatusCodes_t tpOldStatus, tpNewStatus;
  enum Cstager_SegmentStatusCodes_t segmOldStatus, segmNewStatus;
  tape_list_t *tape;
  file_list_t *file;
  int rc, save_serrno;
  char *segmCksumAlgorithm, *errmsgtxt, *vwAddress;
  unsigned char *blockid;
  ID_TYPE key;

  rtcp_log(LOG_DEBUG,"getUpdates() check for updates in DB\n");

  CLIST_ITERATE_BEGIN(tpList,tpIterator) 
    {
      tape = tpIterator->tape;
      /*
       * Create an update (from db) copy of this Tape+Segments
       */
      iObj = Cstager_Tape_getIObject(tpIterator->tp);
      rc = getIAddr(iObj,&iAddr);
      if ( rc != -1 ) {
        rc = C_Services_updateObj(svcs,iAddr,iObj);
        if ( rc == -1 ) LOG_FAILED_CALL("C_Services_updateObj()");
      } else {
        LOG_FAILED_CALL("getIAddr()");
      }
      
      if ( rc == -1 ) {
        save_serrno = serrno;
        tape->tapereq.err.errorcode = serrno;
        strcpy(tape->tapereq.err.errmsgtxt,
               C_Services_errorMsg(svcs));
        tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
        serrno = save_serrno;
        return(-1);
      }
      Cstager_Tape_id(tpIterator->tp,&key);
      (void)rtcpcld_setTapeKey(key);
      tpOldStatus = tpIterator->oldStatus;
      Cstager_Tape_status(tpIterator->tp,&tpNewStatus);
      rtcp_log(LOG_DEBUG,"getUpdates() tape status (old=%d, new=%d)\n",
               tpOldStatus,tpNewStatus);
      Cstager_Tape_vwAddress(tpIterator->tp,(CONST char **)&vwAddress);
      if ( (vwAddress != NULL) &&
           (strncmp(tpIterator->vwAddress,
                    vwAddress,
                    sizeof(tpIterator->vwAddress))) ) {
        strncpy(
                tpIterator->vwAddress,
                vwAddress,
                sizeof(tpIterator->vwAddress)
                );
        rtcp_log(LOG_DEBUG,"getUpdates() VidWorker address %s\n",
                 tpIterator->vwAddress);
      }
      if ( tpOldStatus != tpNewStatus ) {
        tpIterator->oldStatus = tpNewStatus;
        if ( tpNewStatus == TAPE_MOUNTED ) {
          strcpy(tape->tapereq.unit,"unknown");
          strcpy(tape->tapereq.server,"unknown");
        }
      }
      if ( tpNewStatus == TAPE_FAILED ) {
        Cstager_Tape_errorCode(tpIterator->tp,&tape->tapereq.err.errorcode);
        Cstager_Tape_severity(tpIterator->tp,&tape->tapereq.err.severity);
        Cstager_Tape_errMsgTxt(tpIterator->tp,(CONST char **)&errmsgtxt);
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
        return(-1);
      }
      
      CLIST_ITERATE_BEGIN(tpIterator->segments,segmIterator) 
        {
          /*
           * Don't bother to find any updates for finished files
           */
          file = segmIterator->file;
          if ( file->filereq.proc_status < RTCP_FINISHED ) {
            segmOldStatus = segmIterator->oldStatus;
            Cstager_Segment_status(segmIterator->segment,&segmNewStatus);
            if ( segmOldStatus != segmNewStatus ) {
              rtcp_log(LOG_DEBUG,"getUpdates() segment (%d,%s) status updated %d -> %d\n",
                       file->filereq.tape_fseq,file->filereq.file_path,
                       segmOldStatus,segmNewStatus);
              segmIterator->oldStatus = segmNewStatus;
              switch (segmNewStatus) {
              case SEGMENT_FILECOPIED:
                file->filereq.proc_status = RTCP_FINISHED;
                file->filereq.TEndTransferTape = 
                  file->filereq.TEndTransferDisk = (int)time(NULL);
                Cstager_Segment_bytes_in(
                                         segmIterator->segment,
                                         &file->filereq.bytes_in
                                         );
                Cstager_Segment_bytes_out(
                                          segmIterator->segment,
                                          &file->filereq.bytes_out
                                          );
                Cstager_Segment_host_bytes(
                                           segmIterator->segment,
                                           &file->filereq.host_bytes
                                           );
                Cstager_Segment_segmCksumAlgorithm(
                                                   segmIterator->segment,
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
                                          segmIterator->segment,
                                          &file->filereq.castorSegAttr.segmCksum
                                          );
              case SEGMENT_COPYRUNNING:
                if ( segmNewStatus == SEGMENT_COPYRUNNING ) {
                  file->filereq.proc_status = RTCP_POSITIONED;
                  file->filereq.TStartPosition = file->filereq.TEndPosition = 
                    file->filereq.TStartTransferTape = 
                    file->filereq.TStartTransferDisk = (int)time(NULL);
                }
                Cstager_Segment_blockid(
                                        segmIterator->segment,
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
                                     segmIterator->segment,
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
                break;
              case SEGMENT_FAILED:
                Cstager_Segment_errMsgTxt(
                                          segmIterator->segment,
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
                                          segmIterator->segment,
                                          &file->filereq.err.errorcode
                                          );
                Cstager_Segment_severity(
                                         segmIterator->segment,
                                         &file->filereq.err.severity
                                         );
                file->filereq.cprc = -1;
                save_serrno = file->filereq.err.errorcode;
                (void)rtcpc_GiveInfo(tape,file,0);
                rc = updateCaller(tape,file);
                break;
              case SEGMENT_WAITFSEQ:
              case SEGMENT_WAITPATH:
                rc = getMoreInfo(tape);
                if ( rc == -1 ) {
                  save_serrno = serrno;
                } else {
                  rc = doFullUpdate(tpIterator,tape);
                  if ( rc == -1 ) save_serrno = serrno;
                }
                if ( rc == -1 ) serrno = save_serrno;
                return(rc);
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
              if ( rc == -1 ) {
                serrno = save_serrno;
                return(-1);
              }
            }
          }
        }
      CLIST_ITERATE_END(tpIterator->segments,segmIterator);
    }
  CLIST_ITERATE_END(tpList,tpIterator);
  return(0);
}

static int findTpReqMap(tape,tpList,killed)
     tape_list_t *tape;
     RtcpcldTapeList_t **tpList;
     int *killed;
{
  int rc;
  TpReqMap_t *iterator = NULL;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  if ( tpList != NULL ) *tpList = NULL;
  rc = Cmutex_lock(&runningReqsMap,-1);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cmutex_lock()");
    return(-1);
  }
  
  rc = 0;
  CLIST_ITERATE_BEGIN(runningReqsMap,iterator) 
    {
      if ( iterator->tape == tape ) {
        if ( tpList != NULL ) *tpList = iterator->tpList;
        if ( killed != NULL ) {
          if ( *killed > 0 ) iterator->killed = *killed;
          *killed = iterator->killed;
        }
        rc = 1;
        break;
      }
    }
  CLIST_ITERATE_END(runningReqsMap,iterator);
  (void)Cmutex_unlock(&runningReqsMap);
  return(rc);
}

int rtcpcld_findTpReqMap(tape,tpList)
     tape_list_t *tape;
     RtcpcldTapeList_t **tpList;
{
  return(findTpReqMap(tape,tpList,NULL));
}

int rtcpcld_killed(tape)
     tape_list_t *tape;
{
  int killed = 0, rc;

  rc = findTpReqMap(tape,NULL,&killed);
  if ( rc == -1 ) return(-1);
  if ( killed > 0 ) return(1);
  return(0);
}

int rtcpcld_setKilled(tape)
     tape_list_t *tape;
{
  int killed = 1, rc;

  rc = findTpReqMap(tape,NULL,&killed);
  if ( rc == -1 ) return(-1);
  return(0);
}

int rtcpcld_addTpReqMap(tape,tpList)
     tape_list_t *tape;
     RtcpcldTapeList_t *tpList;
{
  int rc;
  RtcpcldTapeList_t *tpListTmp = NULL;
  TpReqMap_t *runningReqMap = NULL;

  if ( tape == NULL || tpList == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  rc = rtcpcld_findTpReqMap(tape,&tpListTmp);
  if ( rc == 1 ) {
    if ( tpList != tpListTmp ) {
      serrno = SEINTERNAL;
      return(-1);
    }
    return(0);
  }
  
  runningReqMap = (TpReqMap_t *)calloc(1,sizeof(TpReqMap_t));
  if ( runningReqMap == NULL ) {
    LOG_FAILED_CALL("calloc()");
    serrno = SESYSERR;
    return(-1);
  }
  runningReqMap->tape = tape;
  runningReqMap->tpList = tpList;

  rc = Cmutex_lock(&runningReqsMap,-1);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cmutex_lock()");
    return(-1);
  }
  
  CLIST_INSERT(runningReqsMap,runningReqMap);
  (void)Cmutex_unlock(&runningReqsMap);
  return(0);
}

int rtcpcldc_appendFileReqs(tape,file)
     tape_list_t *tape;
     file_list_t *file;
{
  int rc, save_serrno;
  file_list_t *fl, *flCp;
  RtcpcldTapeList_t *tpList = NULL;
  RtcpcldSegmentList_t *rtcpcldSegm = NULL;
  struct C_Services_t **dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *segmIObj;
  
  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  rc = Cmutex_lock(tape,-1);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cmutex_lock()");
    return(-1);
  }
  
  rc = rtcpcld_findTpReqMap(tape,&tpList);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("rtcpcld_findTpReqMap()");
    save_serrno = serrno;
    (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }
  if ( rc == 0 || tpList == NULL ) {
    (void)Cmutex_unlock(tape);
    serrno = ENOENT;
    return(-1);
  }
  CLIST_ITERATE_BEGIN(file,fl) 
    {
      fl->filereq.err.severity = fl->filereq.err.severity &
        ~RTCP_RESELECT_SERV;
      fl->filereq.cprc = 0;
      if ( fl->filereq.proc_status <= RTCP_WAITING )
        fl->filereq.proc_status = RTCP_WAITING;
      fl->tape = tape;
      rc = addSegment(
                      tpList,
                      fl,
                      NULL,
					  NULL
                      );
      if ( rc == -1 ) {
        LOG_FAILED_CALL("addSegment()");
        save_serrno = serrno;
        (void)Cmutex_unlock(tape);
        serrno = save_serrno;
        return(-1);
      }
    }
  CLIST_ITERATE_END(file,fl);

  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()");
    save_serrno = serrno;
    if ( serrno == 0 ) save_serrno = errno;
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
    (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("C_Services_create()");
    (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }
  flCp = file;
  while ( (fl = flCp) != NULL ) {
    CLIST_DELETE(flCp,fl);
    CLIST_INSERT(tape->file,fl);
  }

  /*
   * Assume it is more efficient to create the new segment one by one
   * rather than updating the whole tape + all segments.
   */
  fl = file;
  CLIST_ITERATE_BEGIN(tpList->segments,rtcpcldSegm) 
    {
      if ( rtcpcldSegm->file == fl ) {
        segmIObj = Cstager_Segment_getIObject(rtcpcldSegm->segment);
        rc = C_Services_createRep(*dbSvc,iAddr,segmIObj,1);
        if ( rc != 0 ) {
          LOG_FAILED_CALL("C_Services_createRep()");
          save_serrno = serrno;
          fl->filereq.err.errorcode = serrno;
          strcpy(fl->filereq.err.errmsgtxt,
                 C_Services_errorMsg(*dbSvc));
          fl->filereq.err.severity = RTCP_FAILED|RTCP_SYERR;
          C_IAddress_delete(iAddr);
          C_Services_delete(*dbSvc);
          *dbSvc = NULL;
          (void)Cmutex_unlock(tape);
          rtcp_log(LOG_ERR,"DB error: %s\n",fl->filereq.err.errmsgtxt);
          serrno = save_serrno;
          return(-1);
        }
        /* Make sure to not go beyond the first file */
        if ( fl == tpList->segments->file->prev ) break;
        fl = fl->next;
      }
    }
  CLIST_ITERATE_END(tpList->segments,rtcpcldSegm);
  (void)Cmutex_unlock(tape);

  C_IAddress_delete(iAddr);
  return(0);
}

static int deleteFromDB(segmItem)
     RtcpcldSegmentList_t *segmItem;
{
  int rc, save_serrno;
  struct C_Services_t **dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *segmIObj;
  
  if ( segmItem == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()");
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 || dbSvc == NULL || *dbSvc == NULL ) {
    LOG_FAILED_CALL("rtcpcld_getDbSvc()");
    return(-1);
  }

  segmIObj = Cstager_Segment_getIObject(segmItem->segment);
  rc = C_Services_deleteRep(*dbSvc,iAddr,segmIObj,1);
  if ( rc != 0 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("C_Services_deleteRep()");
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

int rtcpcldc_kill(
                  tape
                  )
     tape_list_t *tape;
{
  RtcpcldTapeList_t *tpList;
  rtcpHdr_t hdr;
  char *server, *p;
  int rc, save_serrno, port, noLock = 0;
  SOCKET abortSocket = INVALID_SOCKET;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  /* Try to get the lock but don't give up if not acquired */
  rc = Cmutex_lock(tape,0); 
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cmutex_lock()");
    noLock = 1;
  }

  (void)rtcpcld_setKilled(tape);
  
  rc = rtcpcld_findTpReqMap(tape,&tpList);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcpcld_findTpReqMap()");
    if ( noLock == 0 ) (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }
  if ( rc == 0 || tpList == NULL ) {
    if ( noLock == 0 ) (void)Cmutex_unlock(tape);
    serrno = ENOENT;
    return(-1);
  }

  p = strstr(tpList->vwAddress,":");
  if ( p == NULL ) {
    rtcp_log(LOG_ERR,"Incorrect format for VidWorker address: %s\n",
             tpList->vwAddress);
    if ( noLock == 0 ) (void)Cmutex_unlock(tape);
    serrno = EINVAL;
    return(-1);
  }
  *p = '\0';
  server = tpList->vwAddress;
  port = atoi(++p);

  rc = rtcp_Connect(&abortSocket,server,&port);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcp_Connect()");
    if ( noLock == 0 ) (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }
  hdr.magic = RTCOPY_MAGIC;
  hdr.reqtype = RTCP_KILLJID_REQ;
  hdr.len = 0;

  rc = rtcp_SendReq(&abortSocket,&hdr,NULL,NULL,NULL);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcp_SendReq(RTCP_KILLJID_REQ)");
    if ( noLock == 0 ) (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }

  rc = rtcp_RecvAckn(&abortSocket,hdr.reqtype);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcp_RecvAckn(RTCP_KILLJID_REQ)");
    if ( noLock == 0 ) (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }
  if ( noLock == 0 ) (void)Cmutex_unlock(tape);
  (void)rtcp_CloseConnection(&abortSocket);
  return(0);
}

void rtcpcldc_cleanup(tape)
     tape_list_t *tape;
{
  struct C_Services_t **dbSvc;
  RtcpcldTapeList_t *tpList = NULL, *tpItem;
  RtcpcldSegmentList_t *segmItem;
  int rc;
  
  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == 0 && dbSvc != NULL && *dbSvc != NULL ) {
    C_Services_delete(*dbSvc);
    *dbSvc = NULL;
  }
  
  if ( tape == NULL ) return;
  rc = rtcpcld_findTpReqMap(tape,&tpList);
  if ( rc == 1 ) {
    while ( (tpItem = tpList) != NULL ) {
      while ( (segmItem = tpItem->segments) != NULL ) {
        CLIST_DELETE(tpItem->segments,segmItem);
        (void)deleteFromDB(segmItem);
        segmItem->segment = NULL;
        free(segmItem);
      }
      CLIST_DELETE(tpList,tpItem);
      if ( tpItem->tp != NULL ) Cstager_Tape_delete(tpItem->tp);
      tpItem->tp = NULL;
      free(tpItem);
    }
  }
  C_Services_delete(*dbSvc);
  *dbSvc = NULL;
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
  enum Cstager_TapeStatusCodes_t currentStatus;
  tape_list_t *tl;
  file_list_t *fl;
  RtcpcldTapeList_t *rtcpcldTpList = NULL, *rtcpcldTp;
  int rc, save_serrno, notificationPort = 0, maxfd = 0;
  SOCKET *notificationSocket = NULL;
  char myHost[CA_MAXHOSTNAMELEN+1], myNotificationAddr[CA_MAXHOSTNAMELEN+12], *p;
  struct timeval timeout;
  time_t notificationTimeout;
  fd_set rd_set, rd_setcp;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  dontLog = 1;

  rc = rtcpcld_initNotifyByPort(&notificationSocket,&notificationPort);
  if ( (rc == -1) || (notificationSocket == NULL) ) {
    LOG_FAILED_CALL("rtcpcld_initNotifyByPort()");
    return(-1);
  }
  
  rc = gethostname(myHost,sizeof(myHost)-1);
  sprintf(myNotificationAddr,"%s:%d",myHost,notificationPort);
  notificationTimeout = RTCPCLD_NOTIFYTIMEOUT;
  if ( (p = (char *)getconfent("RTCPCLD","NOTIFYTIMEOUT",0)) != NULL ) {
    notificationTimeout = atoi(p);
  } else if ( (p = getenv("RTCPCLD_NOTIFYTIMEOUT")) != NULL ) {
    notificationTimeout = atoi(p);
  }
  rtcp_log(
           LOG_DEBUG,
           "Notification address = %s, timeout = %d\n",
           myNotificationAddr,
           notificationTimeout
           );
  
  rc = Cmutex_lock(tape, -1);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("Cmutex_lock()");
    return(-1);
  }

  (void)initOldStgUpdc(tape);

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);
  if ( rc == -1 || dbSvc == NULL || *dbSvc == NULL ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcpcld_getDbSvc()");
    *dbSvc = NULL;
    strncpy(tape->tapereq.err.errmsgtxt,
            sstrerror(save_serrno),
            sizeof(tape->tapereq.err.errmsgtxt)-1);
    tape->tapereq.err.errorcode = save_serrno;
    tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
    (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }

  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    LOG_FAILED_CALL("C_BaseAddress_create()");
    save_serrno = serrno;
    strncpy(tape->tapereq.err.errmsgtxt,
            sstrerror(save_serrno),
            sizeof(tape->tapereq.err.errmsgtxt)-1);
    tape->tapereq.err.errorcode = save_serrno;
    tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
    rtcpcldc_cleanup(tape);
    (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);

  rc = rtcpcld_getStgDbSvc(&stgSvc);
  if ( rc == -1 || stgSvc == NULL ) {
    save_serrno = serrno;
    LOG_FAILED_CALL("rtcpcld_getStgDbSvc()");
    stgSvc = NULL;
    C_IAddress_delete(iAddr);
    rtcpcldc_cleanup(tape);
    (void)Cmutex_unlock(tape);
    serrno = save_serrno;
    return(-1);
  }
  
  CLIST_ITERATE_BEGIN(tape,tl) 
    {
      rtcpcldTp = (RtcpcldTapeList_t *)calloc(1,sizeof(RtcpcldTapeList_t));
      if ( rtcpcldTp == NULL ) {
        LOG_FAILED_CALL("calloc()");
        C_IAddress_delete(iAddr);
        (void)C_Services_rollback(*dbSvc,iAddr);
        rtcpcldc_cleanup(tape);
        (void)Cmutex_unlock(tape);
        serrno = SESYSERR;
        return(-1);
      }
      rc = rtcpcld_addTpReqMap(tl,rtcpcldTp);
      if ( rc == -1 ) {
        LOG_FAILED_CALL("rtcpcld_addTpReqMap()");
        save_serrno = serrno;
        (void)C_Services_rollback(*dbSvc,iAddr);
        C_IAddress_delete(iAddr);
        rtcpcldc_cleanup(tape);
        (void)Cmutex_unlock(tape);
        serrno = save_serrno;
        return(-1);
      }
      rtcpcldTp->tape = tl;
      serrno = 0;
      /*      rc = Cstager_Tape_create(&rtcpcldTp->tp);*/
      rc = Cstager_IStagerSvc_selectTape(
                                         stgSvc,
                                         &rtcpcldTp->tp,
                                         tl->tapereq.vid,
                                         tl->tapereq.side,
                                         tl->tapereq.mode
                                         );
      if ( rc == -1 ) {
        LOG_FAILED_CALL("Cstager_IStagerSvc_selectTape()");
        if ( serrno == 0 ) serrno = errno;
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"Cstager_IStagerSvc_selectTape() DB error: %s\n",
                 Cstager_IStagerSvc_errorMsg(stgSvc));
        strncpy(tl->tapereq.err.errmsgtxt,
                Cstager_IStagerSvc_errorMsg(stgSvc),
                sizeof(tl->tapereq.err.errmsgtxt)-1);
        tl->tapereq.err.errorcode = save_serrno;
        tl->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
        (void)C_Services_rollback(*dbSvc,iAddr);
        C_IAddress_delete(iAddr);
        rtcpcldc_cleanup(tape);
        (void)Cmutex_unlock(tape);
        serrno = save_serrno;
        return(-1);
      }
      CLIST_INSERT(rtcpcldTpList,rtcpcldTp);
      Cstager_Tape_setVid(rtcpcldTp->tp,tape->tapereq.vid);
      Cstager_Tape_setSide(rtcpcldTp->tp,tape->tapereq.side);
      Cstager_Tape_setTpmode(rtcpcldTp->tp,tape->tapereq.mode);
      Cstager_Tape_status(rtcpcldTp->tp,&currentStatus);
      if ( currentStatus != TAPE_PENDING &&
           currentStatus != TAPE_WAITVDQM &&
           currentStatus != TAPE_WAITMOUNT &&
           currentStatus != TAPE_MOUNTED ) {
        Cstager_Tape_setStatus(rtcpcldTp->tp,TAPE_PENDING);
        rtcpcldTp->oldStatus = TAPE_PENDING;
      } else {
        rtcpcldTp->oldStatus = currentStatus;
      }

      rtcp_log(
               LOG_DEBUG,
               "Tape req: vid=%s, side=%d, mode=%d\n",
               tape->tapereq.vid,
               tape->tapereq.side,
               tape->tapereq.mode
               );
      
      CLIST_ITERATE_BEGIN(tl->file,fl) 
        {
          fl->filereq.err.severity = fl->filereq.err.severity &
            ~RTCP_RESELECT_SERV;
          fl->filereq.cprc = 0;
          if ( fl->filereq.proc_status <= RTCP_WAITING )
            fl->filereq.proc_status = RTCP_WAITING;
          rc = addSegment(
                          rtcpcldTp,
                          fl,
                          myNotificationAddr,
						  NULL
                          );
          if ( rc == -1 ) {
            LOG_FAILED_CALL("addSegment()");
            save_serrno = serrno;
            (void)C_Services_rollback(*dbSvc,iAddr);
            C_IAddress_delete(iAddr);
            rtcpcldc_cleanup(tape);
            (void)Cmutex_unlock(tape);
            serrno = save_serrno;
            return(-1);
          }
        }
      CLIST_ITERATE_END(tl->file,fl);
    }
  CLIST_ITERATE_END(tape,tl);
  
  CLIST_ITERATE_BEGIN(rtcpcldTpList,rtcpcldTp) 
    {
      tapeIObj = Cstager_Tape_getIObject(rtcpcldTp->tp);
      rc = C_Services_updateRep(*dbSvc,iAddr,tapeIObj,1);
      if ( rc != 0 ) {
        LOG_FAILED_CALL("C_Services_updateRep()");
        save_serrno = serrno;
        (void)C_Services_rollback(*dbSvc,iAddr);
        rtcpcldTp->tape->tapereq.err.errorcode = save_serrno;
        strncpy(rtcpcldTp->tape->tapereq.err.errmsgtxt,
               C_Services_errorMsg(*dbSvc),
               sizeof(rtcpcldTp->tape->tapereq.err.errmsgtxt)-1);
        rtcpcldTp->tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
        C_IAddress_delete(iAddr);
        rtcpcldc_cleanup(tape);
        (void)Cmutex_unlock(tape);
        rtcp_log(LOG_ERR,"DB error: %s\n",
                 rtcpcldTp->tape->tapereq.err.errmsgtxt);
        serrno = save_serrno;
        return(-1);
      }
    }
  CLIST_ITERATE_END(rtcpcldTpList,rtcpcldTp);
  (void)Cmutex_unlock(tape);
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
      if ( rtcpcld_killed(tape) == 1 ) {
        rc = -1;
        save_serrno = ERTUSINTR;
        break;
      }
      
      if ( rc > 0 ) (void)rtcpcld_getNotify(*notificationSocket);
      rc = Cmutex_lock(tape,-1);
      if ( rc == -1 ) {
        LOG_FAILED_CALL("Cmutex_lock()");
        save_serrno = serrno;
        break;
      }
      rc = getUpdates(*dbSvc,rtcpcldTpList);
      save_serrno = serrno;
      (void)Cmutex_unlock(tape);
      if ( rc == -1 ) break;
    } else {
      LOG_FAILED_CALL("select()");
      /** Test for appending requests */
      /*       if ( errno == EINTR ) { */
      /*         printf("Add more requests?\n"); */
      /*         fflush(stdout); */
      /*         scanf("%s",tmp); */
      /*         if ( strstr(tmp,"Y") != NULL || strstr(tmp,"y") != NULL ) { */
      /*           fl = (file_list_t *)calloc(1,sizeof(file_list_t)); */
      /*           printf("fseq diskpath?\n"); */
      /*           scanf("%d %s",&fl->filereq.tape_fseq,fl->filereq.file_path); */
      /*           fl->filereq.concat = NOCONCAT; */
      /*           CLIST_INSERT(fl,fl); */
      /*           rc = rtcpcldc_appendFileReqs(tape,fl); */
      /*           if ( rc == -1 ) { */
      /*             save_serrno = serrno; */
      /*             fprintf(stderr,"rtcpcldc_appendFileReqs(): %s, %s\n", */
      /*                     sstrerror(serrno), */
      /*                     fl->filereq.err.errmsgtxt); */
      /*             break; */
      /*           } */
      /*           continue; */
      /*         } */
      /*         save_serrno = EINTR; */
      /*         break; */
      /*       } */
      save_serrno = EINTR;
      break;
    }
    rc = Cmutex_lock(tape,-1);
    if ( rc == -1 ) {
      LOG_FAILED_CALL("Cmutex_lock()");
      save_serrno = serrno;
      break;
    }
    rc = rtcpc_EndOfRequest(tape);
    if ( rc == -1 ) {
      LOG_FAILED_CALL("rtcpc_EndOfRequest()");
      save_serrno = serrno;
    }
    (void)Cmutex_unlock(tape);
    if ( rc == 1 ) break;
  }

  rtcpcldc_cleanup(tape);

  if ( rc == -1 ) {
    serrno = save_serrno;
    return(-1);
  }
  return(0);
}
