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
 * @(#)$RCSfile: rtcpcldapi.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/05/18 15:23:52 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldapi.c,v $ $Revision: 1.1 $ $Date: 2004/05/18 15:23:52 $ CERN-IT/ADC Olof Barring";
#endif /* not lint */


#include <errno.h>
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
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld_messages.h>

typedef struct rtcpcldTapeList 
{
  tape_list_t *tape;
  struct Cstager_Tape_t *tp;
  struct rtcpcldSegmentList *segments;
  struct rtcpcldTapeList *next;
  struct rtcpcldTapeList *prev;
}
rtcpcldTapeList_t;

typedef struct rtcpcldSegmentList 
{
  file_list_t *file;
  struct Cstager_Segment_t *segment;
  struct rtcpcldTapeList *tp;
  struct rtcpcldSegmentList *next;
  struct rtcpcldSegmentList *prev;
}
rtcpcldSegmentList_t;
  

static int ENOSPCkey = -1;

static int dbSvcKey = -1;

int (*rtcpcld_ClientCallback) _PROTO((
                                      rtcpTapeRequest_t *,
                                      rtcpFileRequest_t *
                                      )) = NULL;

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

  rc = Cglobals_get(&ENOSPCkey,&ENOSPC_occurred,sizeof(int));
  if ( rc == -1 || ENOSPC_occurred == NULL ) return(-1);
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
  
  rc = Cglobals_get(&ENOSPCkey,&ENOSPC_occurred,sizeof(int));
  if ( rc == -1 || ENOSPC_occurred == NULL ) return(-1);

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
      rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() stage_updc_tppos(%s,%d,%d,%d,%s,%s,%d,%d,%s,%s)\n",
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
                            newpath);
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
  }
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
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq;
  int rc;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  tapereq = &tape->tapereq;
  filereq = &file->filereq;

  if ( rtcpcld_ClientCallback != (int (*) _PROTO((rtcpTapeRequest_t *,
                                                  rtcpFileRequest_t *)))NULL ) {
    rc = rtcpcld_ClientCallback(tapereq,filereq);
    if ( rc == -1 ) return(-1);
  }

  rc = updateOldStgCat(tape,file);
  
  return(0);
}

int rtcpcldc(tape)
     tape_list_t *tape;
{
  struct C_IObject_t *segmentIObj = NULL, *tapeIObj = NULL;
  struct C_Services_t **dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  tape_list_t *tl;
  file_list_t *fl;
  rtcpcldTapeList_t *rtcpcldTpList = NULL, *rtcpcldTp;
  rtcpcldSegmentList_t *rtcpcldSegm;
  int rc, save_serrno;

  CLIST_ITERATE_BEGIN(tape,tl) 
    {
      rtcpcldTp = (rtcpcldTapeList_t *)calloc(1,sizeof(rtcpcldTapeList_t));
      if ( rtcpcldTp == NULL ) {
        serrno = SESYSERR;
        return(-1);
      }
      rtcpcldTp->tape = tl;
      serrno = 0;
      rc = Cstager_Tape_create(&rtcpcldTp->tp);
      if ( rtcpcldTp->tp == NULL ) {
        if ( serrno == 0 ) serrno = errno;
        return(-1);
      }
      CLIST_INSERT(rtcpcldTpList,rtcpcldTp);
      Cstager_Tape_setVid(rtcpcldTp->tp,tape->tapereq.vid);
      Cstager_Tape_setSide(rtcpcldTp->tp,tape->tapereq.side);
      Cstager_Tape_setTpmode(rtcpcldTp->tp,tape->tapereq.mode);
      Cstager_Tape_setStatus(rtcpcldTp->tp,TAPE_PENDING);
      
      CLIST_ITERATE_BEGIN(tl->file,fl) 
        {
          rtcpcldSegm = (rtcpcldSegmentList_t *)
            calloc(1,sizeof(rtcpcldSegmentList_t));
          if ( rtcpcldSegm == NULL ) {
            serrno = SESYSERR;
            return(-1);
          }
          rtcpcldSegm->file = fl;
          rtcpcldSegm->tp = rtcpcldTp;
          serrno = 0;
          rc = Cstager_Segment_create(&rtcpcldSegm->segment);
          if ( rc == -1 ) {
            if ( serrno == 0 ) serrno = errno;
            return(-1);
          }
          CLIST_INSERT(rtcpcldTp->segments, rtcpcldSegm);
          Cstager_Segment_setFseq(
                                  rtcpcldSegm->segment,
                                  fl->filereq.tape_fseq
                                  );
          Cstager_Segment_setDiskPath(
                                      rtcpcldSegm->segment,
                                      fl->filereq.file_path
                                      );
          Cstager_Segment_setCastorNsHost(
                                          rtcpcldSegm->segment,
                                          fl->filereq.castorSegAttr.nameServerHostName
                                          );
          Cstager_Segment_setCastorFileId(
                                          rtcpcldSegm->segment,
                                          fl->filereq.castorSegAttr.castorFileId
                                          );
          Cstager_Segment_setOffset(
                                    rtcpcldSegm->segment,
                                    fl->filereq.offset
                                    );
          Cstager_Segment_setBlockid(
                                     rtcpcldSegm->segment,
                                     fl->filereq.blockid
                                     );
          Cstager_Segment_setStgReqId(
                                      rtcpcldSegm->segment,
                                      fl->filereq.stgReqId
                                      );
          Cstager_Segment_setTape(
                                  rtcpcldSegm->segment,
                                  rtcpcldTp->tp
                                  );
          if ( fl->filereq.proc_status == RTCP_WAITING ||
               fl->filereq.proc_status == RTCP_POSITIONED ) {
            Cstager_Segment_setStatus(
                                      rtcpcldSegm->segment,
                                      SEGMENT_UNPROCESSED
                                      );
          } else {
            Cstager_Segment_setStatus(
                                      rtcpcldSegm->segment,
                                      SEGMENT_FILECOPIED
                                      );
          }
        }
      CLIST_ITERATE_END(tl->file,fl);
    }
  CLIST_ITERATE_END(tape,tl);

  dbSvc = NULL;
  rc = Cglobals_get(&dbSvcKey,(void **)&dbSvc,sizeof(struct C_Services_t **));
  if ( rc == -1 || dbSvc == NULL ) return(-1);
  
  serrno = 0;
  rc = C_BaseAddress_create("OraCnvSvc", SVC_ORACNV, &baseAddr);
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( serrno == 0 ) save_serrno = errno;
    C_Services_delete(*dbSvc);
    serrno = save_serrno;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  serrno = 0;
  rc = C_Services_create(dbSvc);
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( serrno == 0 ) save_serrno = errno;
    C_Services_delete(*dbSvc);
    serrno = save_serrno;
    return(-1);
  }  

  CLIST_ITERATE_BEGIN(rtcpcldTpList,rtcpcldTp) 
    {
      tapeIObj = Cstager_Tape_getIObject(rtcpcldTp->tp);
      rc = C_Services_createRep(*dbSvc,iAddr,tapeIObj,1);
      if ( rc != 0 ) {
        save_serrno = serrno;
        rtcpcldTp->tape->tapereq.err.errorcode = serrno;
        //        strcpy(rtcpcldTp->tape->tapereq.err.errmsgtxt,
        //       C_Services_errorMsg(*dbSvc));
        rtcpcldTp->tape->tapereq.err.severity = RTCP_FAILED|RTCP_SYERR;
        C_IAddress_delete(iAddr);
        C_Services_delete(*dbSvc);
        serrno = save_serrno;
        return(-1);
      }
      CLIST_ITERATE_BEGIN(rtcpcldTp->segments,rtcpcldSegm) 
        {
          segmentIObj = Cstager_Segment_getIObject(rtcpcldSegm->segment);
          rc = C_Services_createRep(*dbSvc,iAddr,segmentIObj,1);
          if ( rc != 0 ) {
            save_serrno = serrno;
            rtcpcldSegm->file->filereq.err.errorcode = serrno;
            //strcpy(rtcpcldSegm->file->filereq.err.errmsgtxt,
            //     C_Services_errorMsg(*dbSvc));
            rtcpcldSegm->file->filereq.err.severity = RTCP_FAILED|RTCP_SYERR;
            C_IAddress_delete(iAddr);
            C_Services_delete(*dbSvc);
            serrno = save_serrno;
            return(-1);
          }
        }
      CLIST_ITERATE_END(rtcpcldTp->segments,rtcpcldSegm);
    }
  CLIST_ITERATE_END(rtcpcldTpList,rtcpcldTp);
  C_IAddress_delete(iAddr);

  C_Services_delete(*dbSvc);
  return(0);
}
