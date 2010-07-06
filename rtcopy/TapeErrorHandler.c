/******************************************************************************
 *                      TapeErrorHandler.c
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
 * @(#)$RCSfile: TapeErrorHandler.c,v $ $Revision: 1.28 $ $Release$ $Date: 2009/08/18 09:43:01 $ $Author: waldron $
 *
 *
 *
 * @author Olof Barring
 *****************************************************************************/

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>                  /* Standard data types          */
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <Cgetopt.h>
#include <Cpwd.h>
#include <Cns_api.h>
#include <expert_api.h>
#include <dlf_api.h>
#include <Cnetdb.h>
#include <Cuuid.h>
#include <Cmutex.h>
#include <u64subr.h>
#include <Cglobals.h>
#include <serrno.h>
#include <vmgr_api.h>
#include <Ctape_constants.h>
#include <castor/Constants.h>
#include <castor/stager/Tape.h>
#include <castor/stager/Segment.h>
#include <castor/stager/SvcClass.h>
#include <castor/stager/TapePool.h>
#include <castor/stager/TapeCopy.h>
#include <castor/stager/CastorFile.h>
#include <castor/stager/FileClass.h>
#include <castor/stager/TapeCopyStatusCodes.h>
#include <castor/stager/DiskCopy.h>
#include <castor/stager/FileSystem.h>
#include <castor/stager/FileSystemStatusCodes.h>
#include <castor/stager/DiskServer.h>
#include <castor/stager/DiskServerStatusCode.h>
#include <castor/stager/ITapeSvc.h>
#include <castor/Services.h>
#include <castor/BaseAddress.h>
#include <castor/IAddress.h>
#include <castor/IObject.h>
#include <castor/IClient.h>
#include <castor/Constants.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld_messages.h>
#include <rtcpcld.h>
#include <rfio_api.h>
#include <Cns_api.h>
extern char *getconfent (const char *, const char *, int);
/** Default retry policy names
 */
static char *migratorRetryPolicy = MIGRATOR_RETRY_POLICY_NAME;
static char *recallerRetryPolicy = RECALLER_RETRY_POLICY_NAME;
/** Global needed for determine which uuid to log
 */
int inChild;
/** uuid's set by calling process (rtcpclientd:mainUuid, VidWorker:childUuid)
 */
Cuuid_t childUuid, mainUuid;

static int prepareForDBAccess(
                              _dbSvc,
                              _tpSvc,
                              _iAddr
                              )
  struct Cstager_ITapeSvc_t **_tpSvc;
  struct C_Services_t **_dbSvc;
  struct C_IAddress_t **_iAddr;
{
  struct Cstager_ITapeSvc_t **tpSvc;
  struct C_Services_t **dbSvc;
  struct C_IAddress_t *iAddr;
  struct C_BaseAddress_t *baseAddr;
  int rc;

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);

  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("getDbSvc()");
    return(-1);
  }

  tpSvc = NULL;
  rc = rtcpcld_getStgSvc(&tpSvc);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("getStgSvc()");
    return(-1);
  }

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("C_BaseAddress_create()");
    return(-11);
  }

  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);

  if ( _dbSvc != NULL ) *_dbSvc = *dbSvc;
  if ( _tpSvc != NULL ) *_tpSvc = *tpSvc;
  if ( _iAddr != NULL ) *_iAddr = iAddr;

  return(0);
}

static int cleanupSegment(
                          segment
                          )
     struct Cstager_Segment_t *segment;
{
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  int rc;

  if ( segment == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) return(-1);

  iObj = Cstager_Segment_getIObject(segment);

  rc = C_Services_deleteRep(
                            dbSvc,
                            iAddr,
                            iObj,
                            1
                            );
  if ( rc == -1 ) {
    LOG_DBCALL_ERR("C_Services_delRep(segment)",
                     C_Services_errorMsg(dbSvc));
    C_IAddress_delete(iAddr);
    return(-1);
  }

  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

static int callExpert(
                      mode,
                      expertMessage
                      )
     int mode;
     char *expertMessage;
{
  int msgLen, rc, fd = -1;
  int timeout = 30;
  char answer[21]; /* boolean: 0 -> PUT_FAILED, 1 -> do retry */

  if ( expertMessage == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  msgLen = strlen(expertMessage);
  if ( mode == WRITE_ENABLE ) {
    rc = expert_send_request(&fd,EXP_RQ_MIGRATOR);
  } else {
    rc = expert_send_request(&fd,EXP_RQ_RECALLER);
  }

  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("expert_send_request()");
    return(-1);
  }

  rc = expert_send_data(fd,expertMessage,msgLen);
  if ( rc != msgLen ) {
    LOG_SYSCALL_ERR("expert_send_data()");
    return(-1);
  }

  memset(answer,'\0',sizeof(answer));
  rc = expert_receive_data(fd,answer,sizeof(answer)-1,timeout);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("expert_receive_data()");
    return(-1);
  }

  rc = atoi(answer);
  return(rc);
}

/*
 * Trigger a recall retry:
 *  - set the failed segment status to SEGMENT_RETRIED
 *  - create a new segment with identical tape position and status
 *    set to SEGMENT_UNPROCESSED
 *  - update the Tape status to TAPE_PENDING unless the tape
 *    is already active (TAPE_WAITDRIVE, TAPE_WAITMOUNT, TAPE_MOUNTED)
 */
static int doRecallRetry(
                         segment,
                         tapeCopy
                         )
     struct Cstager_Segment_t *segment;
     struct Cstager_TapeCopy_t *tapeCopy;
{
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  struct Cstager_Tape_t *tape = NULL;
  enum Cstager_TapeStatusCodes_t tapeStatus;
  struct Cstager_Segment_t *newSegment = NULL;
  unsigned char blockid[4];
  u_signed64 creationTime;
  u_signed64 priority;
  u_signed64 offset;
  int rc, fseq;
  ID_TYPE key;

  if ( (segment == NULL) || (tapeCopy == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) return(-1);

  iObj = Cstager_Segment_getIObject(segment);
  Cstager_Segment_setStatus(segment,SEGMENT_RETRIED);
  rc = C_Services_updateRep(
                            dbSvc,
                            iAddr,
                            iObj,
                            0
                            );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_updateRep(segment)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }

  Cstager_Segment_create(&newSegment);
  if ( newSegment == NULL ) {
    LOG_SYSCALL_ERR("Cstager_Segment_create()");
    C_Services_rollback(dbSvc,iAddr);
    C_IAddress_delete(iAddr);
    return(-1);
  }
  /*
   * Clone the tape position and disk position (file offset)
   * information from the failed segment
   */
  Cstager_Segment_fseq(segment,&fseq);
  Cstager_Segment_setFseq(newSegment,fseq);

  Cstager_Segment_blockId0(segment,&blockid[0]);
  Cstager_Segment_setBlockId0(newSegment,blockid[0]);
  Cstager_Segment_blockId1(segment,&blockid[1]);
  Cstager_Segment_setBlockId1(newSegment,blockid[1]);
  Cstager_Segment_blockId2(segment,&blockid[2]);
  Cstager_Segment_setBlockId2(newSegment,blockid[2]);
  Cstager_Segment_blockId3(segment,&blockid[3]);
  Cstager_Segment_setBlockId3(newSegment,blockid[3]);

  /* creation time of the old one */

  Cstager_Segment_creationTime(segment,&creationTime);
  Cstager_Segment_setCreationTime(newSegment,creationTime);

  /* get the old priority */

  Cstager_Segment_priority(segment,&priority);
  Cstager_Segment_setPriority(newSegment,priority);

  Cstager_Segment_offset(segment,&offset);
  Cstager_Segment_setOffset(newSegment,offset);
  Cstager_Segment_setStatus(newSegment,SEGMENT_UNPROCESSED);

  Cstager_Segment_tape(segment,&tape);
  if ( tape == NULL ) {
    iObj = Cstager_Segment_getIObject(segment);
    Cstager_Segment_id(segment,&key);
    rc = C_Services_fillObj(
                            dbSvc,
                            iAddr,
                            iObj,
                            OBJ_Tape,
                            0
                            );
    if ( rc == -1 ) {
      LOG_DBCALLANDKEY_ERR("C_Services_fillObj(segment,OBJ_Tape)",
                           C_Services_errorMsg(dbSvc),
                           key);
      (void)C_Services_rollback(dbSvc,iAddr);
      C_IAddress_delete(iAddr);
      return(-1);
    }
    Cstager_Segment_tape(segment,&tape);
  }
  Cstager_Tape_status(tape,&tapeStatus);
  switch (tapeStatus) {
  case TAPE_UNUSED:
  case TAPE_FINISHED:
  case TAPE_FAILED:
  case TAPE_UNKNOWN:
    Cstager_Tape_setStatus(tape,TAPE_PENDING);
    Cstager_Tape_id(tape,&key);
    iObj = Cstager_Tape_getIObject(tape);
    rc = C_Services_updateRep(
                              dbSvc,
                              iAddr,
                              iObj,
                              0
                              );
    if ( rc == -1 ) {
      LOG_DBCALLANDKEY_ERR("C_Services_updateRep(tape)",
                           C_Services_errorMsg(dbSvc),
                           key);
      (void)C_Services_rollback(dbSvc,iAddr);
      C_IAddress_delete(iAddr);
      return(-1);
    }
  default:
    break;
  }

  Cstager_Segment_setTape(newSegment,tape);
  Cstager_Tape_addSegments(tape,newSegment);

  Cstager_Segment_setCopy(newSegment,tapeCopy);
  Cstager_TapeCopy_addSegments(tapeCopy,newSegment);

  /*
   * Create Segment in DataBase
   */
  iObj = Cstager_TapeCopy_getIObject(tapeCopy);
  rc = C_Services_fillRep(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_Segment,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALL_ERR("C_Services_fillRep(tapeCopy,OBJ_Segment)",
                   C_Services_errorMsg(dbSvc));
    (void)C_Services_rollback(dbSvc,iAddr);
    C_IAddress_delete(iAddr);
    return(-1);
  }
  /*
   * Link Segment to Tape
   */
  iObj = Cstager_Segment_getIObject(newSegment);
  rc = C_Services_fillRep(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_Tape,
                          1
                          );
  if ( rc == -1 ) {
    LOG_DBCALL_ERR("C_Services_fillRep(tapeCopy,OBJ_Segment)",
                   C_Services_errorMsg(dbSvc));
    (void)C_Services_rollback(dbSvc,iAddr);
    C_IAddress_delete(iAddr);
    return(-1);
  }

  /*
   * Cleanup
   */
  Cstager_Tape_delete(tape);
  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

static int validNsSegment(
                          nsSegment
                          )
     struct Cns_segattrs *nsSegment;
{
  struct vmgr_tape_info vmgrTapeInfo;
  int rc;

  if ( (nsSegment == NULL) || (*nsSegment->vid == '\0') ||
       (nsSegment->s_status != '-') ) {
    return(0);
  }

  rc = vmgr_querytape(
                      nsSegment->vid,
                      nsSegment->side,
                      &vmgrTapeInfo,
                      0
                      );
  if ( rc == -1 ) return(0);
  if ( ((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
       ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ||
       ((vmgrTapeInfo.status & EXPORTED) == EXPORTED) ) {
    return(0);
  }
  return(1);
}

static int checkRecallRetry(
                            segment,
                            tapeCopy
                            )
     struct Cstager_Segment_t *segment;
     struct Cstager_TapeCopy_t *tapeCopy;
{
  enum Cstager_SegmentStatusCodes_t segmentStatus;
  enum Cstager_DiskCopyStatusCodes_t diskCopyStatus;
  struct Cstager_CastorFile_t *castorFile = NULL;
  struct Cstager_Tape_t *tape = NULL;
  struct Cstager_DiskCopy_t **diskCopies = NULL;
  struct Cstager_Segment_t **segments = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  struct Cns_fileid fileid;
  struct Cns_segattrs *nsSegmentAttrs = NULL;
  int nbSegments = 0, nbNsSegments = 0, i, rc, errorCode, severity, fseq = -1;
  char *expertBuffer = NULL, *errMsgTxt, *tmpBuf, *p, *nsHost = NULL;
  char *vid = NULL;
  unsigned char blockid[4];
  int expertBufferLen = 0, nsSegmentOK = 0, nbDiskCopies = 0, anyStagedSegment;
  int save_serrno = 0;
  ID_TYPE key;

  if ( (tapeCopy == NULL) || (segment == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  /*
   * Tape should already have been read from DB
   */
  Cstager_Segment_tape(segment,&tape);
  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  Cstager_Tape_vid(tape,(const char **)&vid);
  if ( vid == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  Cstager_TapeCopy_id(tapeCopy,&key);

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) return(-1);

  /*
   * Get castor file
   */
  iObj = Cstager_TapeCopy_getIObject(tapeCopy);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_CastorFile,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_fillObj(tapeCopy,OBJ_CastorFile)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }

  Cstager_TapeCopy_castorFile(tapeCopy,&castorFile);
  if ( castorFile == NULL ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    "REASON",
                    DLF_MSG_PARAM_STR,
                    "TapeCopy not associated with a CastorFile",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = ENOENT;
    return(-1);
  }
  memset(&fileid,'\0',sizeof(fileid));
  Cstager_CastorFile_fileId(
                            castorFile,
                            &fileid.fileid
                            );
  Cstager_CastorFile_nsHost(
                            castorFile,
                            (const char **)&nsHost
                            );
  if ( nsHost != NULL ) {
    strncpy(
            fileid.server,
            nsHost,
            sizeof(fileid.server)
            );
  }
  /*
   * Get name server segments to verify that the segment is still OK
   */
  rc = Cns_getsegattrs(
                       NULL,
                       &fileid,
                       &nbNsSegments,
                       &nsSegmentAttrs
                       );
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_SYSCALL_ERR("Cns_getsegattrs()");
    /*
     * Not fatal unless ENOENT when checking a retry. Assume the segment is valid
     */
    if ( save_serrno != ENOENT ) nsSegmentOK = 1;
  } else {
    Cstager_Segment_fseq(segment,&fseq);
    Cstager_Segment_blockId0(segment,&blockid[0]);
    Cstager_Segment_blockId1(segment,&blockid[1]);
    Cstager_Segment_blockId2(segment,&blockid[2]);
    Cstager_Segment_blockId3(segment,&blockid[3]);
    for ( i=0; i<nbNsSegments; i++ ) {
      if ( validNsSegment(&nsSegmentAttrs[i]) == 1 ) {
        if ( (strncmp(vid,nsSegmentAttrs[i].vid,CA_MAXVIDLEN) == 0) &&
             (nsSegmentAttrs[i].fseq == fseq) &&
             (memcmp(nsSegmentAttrs[i].blockid,blockid,sizeof(blockid)) == 0) ) {
          nsSegmentOK = 1;
          break;
        }
      }
    }
  }
  if ( nsSegmentOK == 0 ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INVALSEGM),
                    (struct Cns_fileid *)&fileid,
                    1,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key
                    );
    serrno = SERTYEXHAUST;
    return(-1);
  }
  /*
   * Get disk copies
   */
  iObj = Cstager_CastorFile_getIObject(castorFile);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_DiskCopy,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_fillObj(castorFile,OBJ_DiskCopy)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }
  Cstager_CastorFile_diskCopies(castorFile,&diskCopies,&nbDiskCopies);

  /*
   * Get all segments
   */
  iObj = Cstager_TapeCopy_getIObject(tapeCopy);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_Segment,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_fillObj(tapeCopy,OBJ_Segment)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }


  Cstager_TapeCopy_segments(tapeCopy,&segments,&nbSegments);
  if ( (segments == NULL) || (nbSegments <= 0) ) {
    /*
     * This is not possible! our TapeCopy is retrieved from a failed Segment
     * so we should at least find that one.
     */
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                    (struct Cns_fileid *)&fileid,
                    RTCPCLD_NB_PARAMS+2,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    "REASON",
                    DLF_MSG_PARAM_STR,
                    "Inconsistent catalog: no Segment found for TapeCopy",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = SEINTERNAL;
    if ( iAddr != NULL ) C_IAddress_delete(iAddr);
    return(-1);
  }
  /*
   * Disconnect DiskCopies from FileSystem in case there are no other STAGED segments.
   * If we don't do this, the retry will retain the selected filesystem, which is an
   * unnecessary constraint
   */
  anyStagedSegment = 0;
  for ( i=0; i<nbSegments; i++ ) {
    Cstager_Segment_status(segments[i],&segmentStatus);
    if ( segmentStatus == SEGMENT_FILECOPIED ) {
      anyStagedSegment = 1;
      break;
    }
  }
  if ( anyStagedSegment == 0 ) {
    for ( i=0; i<nbDiskCopies; i++ ) {
      Cstager_DiskCopy_status(diskCopies[i],&diskCopyStatus);
      if ( diskCopyStatus == DISKCOPY_WAITTAPERECALL ) {
        Cstager_DiskCopy_setFileSystem(diskCopies[i],NULL);
        iObj = Cstager_DiskCopy_getIObject(diskCopies[i]);
        rc = C_Services_fillRep(
                                dbSvc,
                                iAddr,
                                iObj,
                                OBJ_FileSystem,
                                1
                                );
        if ( rc == -1 ) {
          LOG_DBCALLANDKEY_ERR("C_Services_fillRep(tapeCopy,OBJ_FileSystem)",
                               C_Services_errorMsg(dbSvc),
                               key);
        }
      }
    }
  }
  if ( diskCopies != NULL ) free(diskCopies);
  if ( iAddr != NULL ) C_IAddress_delete(iAddr);

  /*
   * Arguments for retry policy are:
   *  - policy name: e.g. "recallerRetryPolicy.pl"
   *  - list ofh retries: "errorCode rtcopySeverity 'errormessage'"
   */
  expertBufferLen += (strlen(recallerRetryPolicy)+2);
  for ( i=0; i<nbSegments; i++ ) {
    errMsgTxt = NULL;
    Cstager_Segment_errMsgTxt(
                              segments[i],
                              (const char **)&errMsgTxt
                              );
    /*
     * Error message text is passed within qoutes ('')
     */
    if ( (errMsgTxt == NULL) || (*errMsgTxt == '\0') ) {
      expertBufferLen += (strlen("NO MESSAGE")+3);
    } else {
      expertBufferLen += (strlen(errMsgTxt)+3);
    }
    expertBufferLen += 22; /* Max len of text representation for two 32 bit numbers */
    expertBufferLen += 3;  /* Added space for token delimiter (' ') */
  }
  expertBuffer = (char *)calloc(1,expertBufferLen);
  if ( expertBuffer == NULL ) {
    LOG_SYSCALL_ERR("calloc()");
    free(segments);
    serrno = SESYSERR;
    return(-1);
  }
  p = expertBuffer;
  strcpy(p,recallerRetryPolicy);
  p += strlen(p);
  for ( i=0; i<nbSegments; i++ ) {
    errMsgTxt = tmpBuf = NULL;
    errorCode = 0;
    severity = 0;
    Cstager_Segment_errMsgTxt(
                              segments[i],
                              (const char **)&errMsgTxt
                              );
    errMsgTxt = tmpBuf = rtcpcld_fixStr(errMsgTxt);
    if ( (errMsgTxt == NULL) || (*errMsgTxt == '\0') ) {
      errMsgTxt = "NO MESSAGE";
    }
    Cstager_Segment_errorCode(
                              segments[i],
                              &errorCode
                              );
    Cstager_Segment_severity(
                             segments[i],
                             &severity
                             );
    /*
     * Error message text is passed within qoutes ('')
     */
    sprintf(p," %d %d '%s'",errorCode,severity,errMsgTxt);
    p += strlen(p);
    if ( tmpBuf != NULL ) free(tmpBuf);
  }
  strcat(expertBuffer,"\n");
  rc = callExpert(WRITE_DISABLE,expertBuffer);
  if ( rc == 0 ) {
    /*
     * Recall retry was refused. GET_FAILED !
     */
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_GETFAILED),
                    (struct Cns_fileid *)&fileid,
                    RTCPCLD_NB_PARAMS+1,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    RTCPCLD_LOG_WHERE
                    );
    serrno = SERTYEXHAUST;
    rc = -1;
  } else if ( rc < 0 ) {
    /*
     * The call to the expert system failed. Log it and let the next tperrhandler pick
     * up the segment with the hope that the error was temporary.
     */
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                    (struct Cns_fileid *)&fileid,
                    RTCPCLD_NB_PARAMS+3,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "callExpert(WRITE_DISABLE)",
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    rc = -1;
  } else {
    /*
     * Do retry
     */
    rc = 0;
  }
  free(expertBuffer);
  free(segments);
  return(rc);
}

/*
 * Trigger a migration retry:
 *  - update status of failed Segment to SEGMENT_RETRY
 *  - update status of associated TapeCopy to TAPECOPY_CREATED allowing
 *    it to be picked up at next mighunter iteration.
 */
static int doMigrationRetry(
                            segment,
                            tapeCopy
                            )
     struct Cstager_Segment_t *segment;
     struct Cstager_TapeCopy_t *tapeCopy;
{
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_Stream_t **streamArray = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  int rc, nbStreams = 0;
  ID_TYPE key = 0;

  if ( (segment == NULL) || (tapeCopy == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) return(-1);

  iObj = Cstager_Segment_getIObject(segment);
  Cstager_Segment_setStatus(segment,SEGMENT_RETRIED);
  rc = C_Services_updateRep(
                            dbSvc,
                            iAddr,
                            iObj,
                            0
                            );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_updateRep(segment)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }

  iObj = Cstager_TapeCopy_getIObject(tapeCopy);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_Stream,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_fillObj(tapeCopy,OBJ_Stream)",
                         C_Services_errorMsg(dbSvc),
                         key);
    (void)C_Services_rollback(dbSvc,iAddr);
    C_IAddress_delete(iAddr);
    return(-1);
  }
  Cstager_TapeCopy_stream(tapeCopy,&streamArray,&nbStreams);
  if ( (nbStreams > 0) && (streamArray != NULL) ) {
    /*
     * If already attached to stream(s), leave it
     */
    Cstager_TapeCopy_setStatus(tapeCopy,TAPECOPY_WAITINSTREAMS);
    free(streamArray);
  } else {
    /*
     * Else, the mighunter must re-attach it
     */
    Cstager_TapeCopy_setStatus(tapeCopy,TAPECOPY_CREATED);
  }

  rc = C_Services_updateRep(
                            dbSvc,
                            iAddr,
                            iObj,
                            1
                            );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_updateRep(tapeCopy)",
                         C_Services_errorMsg(dbSvc),
                         key);
    (void)C_Services_rollback(dbSvc,iAddr);
    C_IAddress_delete(iAddr);
    return(-1);
  }

  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

/*
 * Check if the failed migration candidate is retryable:
 *  - castor file still exists
 *  - disk copy on an available diskserver/filesystem
 *  - catalog associations and status attributes are OK
 * If any of these checks fail, the routine returns -1 and
 * sets serrno as follows:
 *  - ENOENT -> truely invalid migration candidate. All
 *              associated objects can be removed without damage
 *  - SEINTERNAL -> migration candidate may still be valid but a
 *                  retry is currently not possible because of
 *                  inconsistent status
 *  - EAGAIN -> migration candidate is still valid but retry is
 *              useless for the moment because disk file is not
 *              accessible
 *  - SERTYEXHAUST -> retry policy denies further retries. PUT_FAILED!
 *
 * The routine returns 0 if migration candidate is OK and retry policy
 * admits another retry.
 *
 */
static int checkMigrationRetry(
                               tapeCopy,
                               deleteDiskCopy,
                               _fileid
                               )
     struct Cstager_TapeCopy_t *tapeCopy;
     int *deleteDiskCopy;
     struct Cns_fileid *_fileid;
{
  enum Cstager_TapeCopyStatusCodes_t tapeCopyStatus;
  enum Cstager_DiskCopyStatusCodes_t diskCopyStatus;
  enum Cstager_FileSystemStatusCodes_t fileSystemStatus;
  enum Cstager_DiskServerStatusCode_t diskServerStatus;
  struct Cstager_CastorFile_t *castorFile = NULL;
  struct Cstager_Segment_t **segments = NULL;
  struct Cstager_DiskCopy_t **diskCopies = NULL, *diskCopy = NULL;
  struct Cstager_FileSystem_t *fileSystem = NULL;
  struct Cstager_DiskServer_t *diskServer = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  struct Cns_fileid fileid;
  struct Cns_filestat statbuf;
  int nbSegments = 0, nbDiskCopies = 0, i, rc, errorCode, severity;
  int fileSystemAvailable, diskServerAvailable;
  char *expertBuffer = NULL, *tmpBuf, *errMsgTxt, *p, *nsHost = NULL;
  char castorFileName[CA_MAXPATHLEN+1];
  int expertBufferLen = 0, save_serrno = 0;
  ID_TYPE key;

  if ( deleteDiskCopy != NULL ) *deleteDiskCopy = 0;
  if ( _fileid != NULL ) memset(_fileid,'\0',sizeof(struct Cns_fileid));
  if ( tapeCopy == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  Cstager_TapeCopy_id(tapeCopy,&key);

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) return(-1);

  /*
   * Get castor file
   */
  iObj = Cstager_TapeCopy_getIObject(tapeCopy);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_CastorFile,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_fillObj(tapeCopy,OBJ_CastorFile)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }

  Cstager_TapeCopy_castorFile(tapeCopy,&castorFile);
  if ( castorFile == NULL ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    "REASON",
                    DLF_MSG_PARAM_STR,
                    "TapeCopy not associated with a CastorFile",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = ENOENT;
    return(-1);
  }
  memset(&fileid,'\0',sizeof(fileid));
  Cstager_CastorFile_fileId(
                            castorFile,
                            &fileid.fileid
                            );
  Cstager_CastorFile_nsHost(
                            castorFile,
                            (const char **)&nsHost
                            );
  if ( nsHost != NULL ) {
    strncpy(
            fileid.server,
            nsHost,
            sizeof(fileid.server)
            );
  }
  if ( _fileid != NULL ) memcpy(_fileid,&fileid,sizeof(fileid));

  /*
   * If castor file has been removed, the Cns_statx() returns ENOENT
   */
  castorFileName[0] = '\0';

  memset(&statbuf,'\0',sizeof(statbuf));
  rc = Cns_statx(castorFileName,&fileid,&statbuf);
  if ( rc == -1 ) {
    save_serrno = serrno;
    C_IAddress_delete(iAddr);

    if ( save_serrno == ENOENT ) {
      /*
       * CASTOR file doesn't exist anymore. We can then safely delete the DiskCopies,
       * but only in that case!
       */
      if ( deleteDiskCopy != NULL ) *deleteDiskCopy = 1;
      save_serrno = SERTYEXHAUST;
    }

    serrno = save_serrno;
    return(-1);
  }
  if ( deleteDiskCopy != NULL ) *deleteDiskCopy = 0;

  /*
   * Check if any disk copy is accessible
   */
  iObj = Cstager_CastorFile_getIObject(castorFile);
  Cstager_CastorFile_id(castorFile,&key);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_DiskCopy,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_fillObj(castorFile,OBJ_DiskCopy)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }
  Cstager_CastorFile_diskCopies(
                                castorFile,
                                &diskCopies,
                                &nbDiskCopies
                                );
  if ( (diskCopies == NULL) || (nbDiskCopies <= 0) ) {
    serrno = ENOENT;
    C_IAddress_delete(iAddr);
    return(-1);
  }

  fileSystemAvailable = diskServerAvailable = 0;
  diskCopy = NULL;
  fileSystem = NULL;
  diskServer = NULL;
  for ( i=0; i<nbDiskCopies; i++ ) {
    if ( diskCopy != NULL ) {
      Cstager_DiskCopy_delete(diskCopy);
      diskCopy = NULL;
    }
    if ( fileSystem != NULL ) {
      Cstager_FileSystem_delete(fileSystem);
      fileSystem = NULL;
    }
    if ( diskServer != NULL ) {
      Cstager_DiskServer_delete(diskServer);
      diskServer = NULL;
    }
    diskCopy = diskCopies[i];
    Cstager_DiskCopy_status(diskCopy,&diskCopyStatus);
    if ( diskCopyStatus != DISKCOPY_CANBEMIGR ) continue;
    iObj = Cstager_DiskCopy_getIObject(diskCopy);
    Cstager_DiskCopy_id(diskCopy,&key);
    rc = C_Services_fillObj(
                            dbSvc,
                            iAddr,
                            iObj,
                            OBJ_FileSystem,
                            0
                            );

    if ( rc == -1 ) {
      LOG_DBCALLANDKEY_ERR("C_Services_fillObj(diskCopy,OBJ_FileSystem)",
                           C_Services_errorMsg(dbSvc),
                           key);
      C_IAddress_delete(iAddr);
      return(-1);
    }
    Cstager_DiskCopy_fileSystem(diskCopy,&fileSystem);
    if ( fileSystem == NULL ) continue;
    Cstager_FileSystem_status(fileSystem,&fileSystemStatus);
    if ( (fileSystemStatus != FILESYSTEM_PRODUCTION) &&
         (fileSystemStatus != FILESYSTEM_DRAINING) ) {
      continue;
    }
    fileSystemAvailable++;
    iObj = Cstager_FileSystem_getIObject(fileSystem);
    Cstager_FileSystem_id(fileSystem,&key);
    rc = C_Services_fillObj(
                            dbSvc,
                            iAddr,
                            iObj,
                            OBJ_DiskServer,
                            0
                            );

    if ( rc == -1 ) {
      LOG_DBCALLANDKEY_ERR("C_Services_fillObj(fileSystem,OBJ_DiskServer)",
                           C_Services_errorMsg(dbSvc),
                           key);
      C_IAddress_delete(iAddr);
      return(-1);
    }
    Cstager_FileSystem_diskserver(fileSystem,&diskServer);
    if ( diskServer == NULL ) continue;
    Cstager_DiskServer_status(diskServer,&diskServerStatus);
    if ( (diskServerStatus != DISKSERVER_PRODUCTION) &&
         (diskServerStatus != DISKSERVER_DRAINING) ) {
      continue;
    }
    diskServerAvailable++;
  }
  free(diskCopies);
  if ( diskCopy != NULL ) Cstager_DiskCopy_delete(diskCopy);
  if ( fileSystem != NULL ) Cstager_FileSystem_delete(fileSystem);
  if ( diskServer != NULL ) Cstager_DiskServer_delete(diskServer);

  if ( (fileSystemAvailable == 0) || (diskServerAvailable == 0) ) {
    C_IAddress_delete(iAddr);
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_POSTPONERETRY),
                    (struct Cns_fileid *)&fileid,
                    1,
                    "REASON",
                    DLF_MSG_PARAM_STR,
                    (fileSystemAvailable == 0 ?
                     "DiskCopy(s) on unavailable file system" :
                     "DiskCopy(s) on unavailable disk server")
                    );
    serrno = EAGAIN;
    return(-1);
  }

  /*
   * A failed tapeCopy from a migration must be in TAPECOPY_SELECTED status
   */
  Cstager_TapeCopy_id(tapeCopy,&key);
  Cstager_TapeCopy_status(tapeCopy,&tapeCopyStatus);
  if ( tapeCopyStatus != TAPECOPY_SELECTED ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                    (struct Cns_fileid *)&fileid,
                    RTCPCLD_NB_PARAMS+3,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    "REASON",
                    DLF_MSG_PARAM_STR,
                    "TapeCopy not in TAPECOPY_SELECTED status",
                    "STATUS",
                    DLF_MSG_PARAM_INT,
                    (int)tapeCopyStatus,
                    RTCPCLD_LOG_WHERE
                    );
    if ( tapeCopyStatus == TAPECOPY_FAILED ) serrno = SERTYEXHAUST;
    else serrno = SEINTERNAL;
    return(-1);
  }

  /*
   * Get all segments
   */
  iObj = Cstager_TapeCopy_getIObject(tapeCopy);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_Segment,
                          0
                          );
  if ( rc == -1 ) {
    LOG_DBCALLANDKEY_ERR("C_Services_fillObj(tapeCopy,OBJ_Segment)",
                         C_Services_errorMsg(dbSvc),
                         key);
    C_IAddress_delete(iAddr);
    return(-1);
  }

  if ( iAddr != NULL ) C_IAddress_delete(iAddr);

  Cstager_TapeCopy_segments(tapeCopy,&segments,&nbSegments);
  if ( (segments == NULL) || (nbSegments <= 0) ) {
    /*
     * This is not possible! our TapeCopy is retrieved from a failed Segment
     * so we should at least find that one.
     */
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                    (struct Cns_fileid *)&fileid,
                    RTCPCLD_NB_PARAMS+2,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    "REASON",
                    DLF_MSG_PARAM_STR,
                    "Inconsistent catalog: no Segment found for TapeCopy",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = SEINTERNAL;
    return(-1);
  }

  /*
   * Arguments for retry policy are:
   *  - policy name: e.g. "migratorRetryPolicy.pl"
   *  - list of retries: "errorCode rtcopySeverity 'errormessage'"
   */
  expertBufferLen += (strlen(migratorRetryPolicy)+2);
  for ( i=0; i<nbSegments; i++ ) {
    errMsgTxt = NULL;
    Cstager_Segment_errMsgTxt(
                              segments[i],
                              (const char **)&errMsgTxt
                              );
    /*
     * Error message text is passed within qoutes ('')
     */
    if ( (errMsgTxt == NULL) || (*errMsgTxt == '\0') ) {
      expertBufferLen += (strlen("NO MESSAGE")+3);
    } else {
      expertBufferLen += (strlen(errMsgTxt)+3);
    }
    expertBufferLen += 22; /* Max len of text representation for two 32 bit numbers */
    expertBufferLen += 3;  /* Added space for token delimiter (' ') */
  }
  expertBuffer = (char *)calloc(1,expertBufferLen);
  if ( expertBuffer == NULL ) {
    LOG_SYSCALL_ERR("calloc()");
    free(segments);
    serrno = SESYSERR;
    return(-1);
  }
  p = expertBuffer;
  strcpy(p,migratorRetryPolicy);
  p += strlen(p);
  for ( i=0; i<nbSegments; i++ ) {
    errMsgTxt = tmpBuf = NULL;
    errorCode = 0;
    severity = 0;
    Cstager_Segment_errMsgTxt(
                              segments[i],
                              (const char **)&errMsgTxt
                              );
    errMsgTxt = tmpBuf = rtcpcld_fixStr(errMsgTxt);
    if ( (errMsgTxt == NULL) || (*errMsgTxt == '\0') ) {
      errMsgTxt = "NO MESSAGE";
    }
    Cstager_Segment_errorCode(
                              segments[i],
                              &errorCode
                              );
    Cstager_Segment_severity(
                             segments[i],
                             &severity
                             );
    /*
     * Error message text is passed within qoutes ('')
     */
    sprintf(p," %d %d '%s'",errorCode,severity,errMsgTxt);
    p += strlen(p);
    if ( tmpBuf != NULL ) free(tmpBuf);
  }
  strcat(expertBuffer,"\n");
  rc = callExpert(WRITE_ENABLE,expertBuffer);
  if ( rc == 0 ) {
    /*
     * PUT_FAILED
     */
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_PUTFAILED),
                    (struct Cns_fileid *)&fileid,
                    RTCPCLD_NB_PARAMS+1,
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    RTCPCLD_LOG_WHERE
                    );
    serrno = SERTYEXHAUST;
    rc = -1;
  } else if ( rc < 0 ) {
    /*
     * The call to the expert system failed. Log it and let the next tperrhandler pick
     * up the segment with the hope that the error was temporary.
     */
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                    (struct Cns_fileid *)&fileid,
                    RTCPCLD_NB_PARAMS+3,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "callExpert(WRITE_ENABLE)",
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    "DBKEY",
                    DLF_MSG_PARAM_INT64,
                    key,
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    rc = -1;
  } else {
    /*
     * Do retry
     */
    rc = 0;
  }
  free(expertBuffer);
  free(segments);
  return(rc);
}

void checkConfiguredPolicy()
{
  char *p;

  p = getconfent("TAPEERRORHANDLER","MIGRATOR_RETRY_POLICY",0);
  if ( p != NULL ) migratorRetryPolicy = strdup(p);
  p = getconfent("TAPEERRORHANDLER","RECALLER_RETRY_POLICY",0);
  if ( p != NULL ) recallerRetryPolicy = strdup(p);
  return;
}

int main() {
  struct Cstager_Segment_t **failedSegments = NULL, *segm, **segms;
  struct Cstager_CastorFile_t *castorFile = NULL;
  struct Cstager_Tape_t *tp;
  struct Cstager_TapeCopy_t *tapeCopy;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  char *tapeErrorHunterFacilityName = TAPEERRORHANDLER_FACILITY;
  int nbFailedSegments = 0, nbSegms, i, fseq, rc;
  int tpErrorCode, tpSeverity, segErrorCode, segSeverity, mode;
  char *vid, *tpErrMsgTxt, *segErrMsgTxt, cns_error_buffer[512];
  int deleteDiskCopy = 0;
  struct Cns_fileid fileid;
  ID_TYPE key;

  Cuuid_create(
               &childUuid
               );
  (void)rtcpcld_initLogging(tapeErrorHunterFacilityName);

  (void)Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer));

  checkConfiguredPolicy();

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    dlf_shutdown();
    return(1);
  }

  rc = Cstager_ITapeSvc_failedSegments(
                                         tpSvc,
                                         &failedSegments,
                                         &nbFailedSegments
                                         );

  if ( rc == -1 ) {
    if ( rc == -1 ) {
      LOG_DBCALL_ERR("C_Services_failedSegments()",
                     Cstager_ITapeSvc_errorMsg(tpSvc));
      C_IAddress_delete(iAddr);
      dlf_shutdown();
      return(1);
    }
  }
  if ( (nbFailedSegments == 0) || (failedSegments == NULL) ) {
    dlf_shutdown();
    return(0);
  }

  for ( i=0; i<nbFailedSegments; i++ ) {
    nbSegms = 0;
    segms = NULL;
    segm = failedSegments[i];
    Cstager_Segment_id(segm,&key);
    Cstager_Segment_tape(segm,&tp);
    if ( tp == NULL ) {
      iObj = Cstager_Segment_getIObject(segm);
      rc = C_Services_fillObj(
                              dbSvc,
                              iAddr,
                              iObj,
                              OBJ_Tape,
                              0
                              );
      if ( rc == -1 ) {
        LOG_DBCALL_ERR("C_Services_fillObj(segment,OBJ_Tape)",
                       C_Services_errorMsg(dbSvc));
        continue;
      }
    }
    Cstager_Segment_tape(segm,&tp);
    if ( tp == NULL ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "DBKEY",
                      DLF_MSG_PARAM_INT64,
                      key,
                      "REASON",
                      DLF_MSG_PARAM_STR,
                      "Tape not found for segment. Running cleanup",
                      RTCPCLD_LOG_WHERE
                      );
      (void)cleanupSegment(segm);
      continue;
    }
    fseq = tpErrorCode = tpSeverity = segErrorCode = segSeverity = 0;
    vid = tpErrMsgTxt = segErrMsgTxt = NULL;
    Cstager_Tape_vid(tp,(const char **)&vid);
    Cstager_Tape_tpmode(tp,&mode);
    Cstager_Tape_errorCode(tp,&tpErrorCode);
    Cstager_Tape_severity(tp,&tpSeverity);
    Cstager_Tape_errMsgTxt(tp,(const char **)&tpErrMsgTxt);
    Cstager_Segment_fseq(segm,&fseq);
    Cstager_Segment_errorCode(segm,&segErrorCode);
    Cstager_Segment_severity(segm,&segSeverity);
    Cstager_Segment_errMsgTxt(segm,(const char **)&segErrMsgTxt);
    iObj = Cstager_Segment_getIObject(segm);
    rc = C_Services_fillObj(
                            dbSvc,
                            iAddr,
                            iObj,
                            OBJ_TapeCopy,
                            0
                            );
    if ( rc == -1 ) {
      LOG_DBCALL_ERR("C_Services_fillObj(segment,OBJ_TapeCopy)",
                     C_Services_errorMsg(dbSvc));
      if ( mode == WRITE_ENABLE ) (void)cleanupSegment(segm);
      continue;
    }
    tapeCopy = NULL;
    Cstager_Segment_copy(segm,&tapeCopy);
    if ( tapeCopy != NULL ) {
      if ( mode == WRITE_ENABLE ) {
        deleteDiskCopy = 0;
        rc = checkMigrationRetry(tapeCopy,&deleteDiskCopy,&fileid);
        /*
         * We can allow for immediate retry on EAGAIN because streamsToDo()
         * does now also check that there is at least one migration candidate
         * on an available diskserver/filesystem.
         */
        if ( (rc == 0) || ((rc == -1) && (serrno == EAGAIN)) ) {
          rc = doMigrationRetry(segm,tapeCopy);
          if ( rc == -1 ) {
            LOG_SYSCALL_ERR("doMigrationRetry()");
          }
        } else if (serrno == ENOENT ) {
          (void)cleanupSegment(segm);
        } else if (( serrno = SERTYEXHAUST )) {
          Cstager_TapeCopy_id(tapeCopy,&key);
          /*
           * PUT_FAILED
           */
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_PUTFAILED),
                          (struct Cns_fileid *)&fileid,
                          RTCPCLD_NB_PARAMS+2,
                          "GC_DCP",
                          DLF_MSG_PARAM_INT,
                          deleteDiskCopy,
                          "DBKEY",
                          DLF_MSG_PARAM_INT64,
                          key,
                          RTCPCLD_LOG_WHERE
                          );
          rc = rtcpcld_putFailed(tapeCopy, deleteDiskCopy);
          if ( rc == -1 ) {
            LOG_SYSCALL_ERR("rtcpcld_putFailed()");
          }
        }
      } else {
        rc = checkRecallRetry(segm,tapeCopy);
        if ( rc == 0 ) {
          rc = doRecallRetry(segm,tapeCopy);
          if ( rc == -1 ) {
            LOG_SYSCALL_ERR("doRecallRetry()");
          }
        } else if ( serrno == ENOENT ) {
          (void)cleanupSegment(segm);
        } else if ( serrno == SERTYEXHAUST ) {
          Cstager_TapeCopy_id(tapeCopy,&key);
          rc = Cstager_ITapeSvc_fileRecallFailed(
                                                 tpSvc,
                                                 tapeCopy
                                                 );
          if ( rc == -1 ) {
            LOG_DBCALLANDKEY_ERR("Cstager_ITapeSvc_fileRecallFailed()",
                                 Cstager_ITapeSvc_errorMsg(tpSvc),
                                 key);
          }
          (void)cleanupSegment(segm);
        }
      }
      castorFile = NULL;
      Cstager_TapeCopy_castorFile(tapeCopy,&castorFile);
      if ( castorFile != NULL ) Cstager_CastorFile_delete(castorFile);
      else Cstager_TapeCopy_delete(tapeCopy);
    } else {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "DBKEY",
                      DLF_MSG_PARAM_INT64,
                      key,
                      "REASON",
                      DLF_MSG_PARAM_STR,
                      "TapeCopy not found for segment. Running cleanup",
                      RTCPCLD_LOG_WHERE
                      );
      (void)cleanupSegment(segm);
      Cstager_Segment_delete(segm);
    }
  }
  if ( failedSegments != NULL ) free(failedSegments);
  dlf_shutdown();
  return(0);
}
