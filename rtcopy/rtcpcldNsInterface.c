/******************************************************************************
 *                      rtcpcldNsInterface.c
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
 * @(#)$RCSfile: rtcpcldNsInterface.c,v $ $Revision: 1.52 $ $Release$ $Date: 2009/07/23 12:22:05 $ $Author: waldron $
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
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <Cgetopt.h>
#include <Cpwd.h>
#include <dlf_api.h>
#include <Cnetdb.h>
#include <Cuuid.h>
#include <Cmutex.h>
#include <u64subr.h>
#include <Cglobals.h>
#include <serrno.h>
#include <Cthread_api.h>
#include <Cns_api.h>
#include <vmgr_api.h>

#include <Ctape_constants.h>
#include <castor/stager/Tape.h>
#include <castor/stager/Segment.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <castor/stager/CastorFile.h>
#include <castor/stager/TapeCopy.h>
#include <castor/stager/SubRequest.h>
#include <castor/stager/ITapeSvc.h>
#include <castor/Constants.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld_messages.h>
#include <rtcpcld.h>


char *getconfent (char *, char *, int);

/** Global needed for determine which uuid to log
 */
int inChild;
/** uuid's set by calling process (rtcpclientd:mainUuid, VidWorker:childUuid)
 */
Cuuid_t childUuid, mainUuid;

static int use_checksum = 1, change_checksum_name = 0;

static int getNsErrBuf(char **nsErrBuf)
{
  static int nsErrBufKey = -1;
  void *tmpBuf = NULL;
  int rc;
  
  rc = Cglobals_get(
                    &nsErrBufKey,
                    &tmpBuf,
                    512
                    );
  if ( rc == -1 ) return(-1);
  rc = Cns_seterrbuf(tmpBuf,512);
  if ( rc == -1 ) return(-1);
  if ( nsErrBuf != NULL ) *nsErrBuf = (char *)tmpBuf;
  return(0);
}

int rtcpcld_initNsInterface() 
{
  char *p;

  if ( ((p = getenv("RTCPCLD_USE_CHECKSUM")) != NULL) ||
       ((p = getconfent("rtcpcld","USE_CHECKSUM",0)) != NULL) ) {
    if ( strcmp(p,"NO") == 0 ) use_checksum = 0;
  }
  if ( ((p = getenv("RTCPCLD_CHANGE_CHECKSUM_NAME")) != NULL) ||
       ((p = getconfent("rtcpcld","CHANGE_CHECKSUM_NAME",0)) != NULL) ) {
    if ( strcmp(p,"YES") == 0 ) change_checksum_name = 1;
  }

  return(0);
}


static int checkSegment(tape_list_t *tape,
                        file_list_t *file)
{
  struct Cstager_Segment_t *segment;
  struct Cstager_TapeCopy_t *tapeCopy;
  struct Cstager_CastorFile_t *castorFile;
  u_signed64 fileSize = 0;

  if ( (tape == NULL) || (file == NULL) ||
       (tape->dbRef == NULL) || (file->dbRef == NULL) ||
       (tape->dbRef->row == NULL) || (file->dbRef->row == NULL ) ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( (file->filereq.proc_status != RTCP_FINISHED) ||
       (file->filereq.cprc == -1) || 
       ((file->filereq.err.severity & RTCP_OK) != RTCP_OK) ) {
    if ( (serrno = file->filereq.err.errorcode) <= 0 ) {
      serrno = SEINTERNAL;
    }
    return(-1);
  }

  segment = (struct Cstager_Segment_t *)file->dbRef->row;
  Cstager_Segment_copy(segment,&tapeCopy);
  
  if ( tapeCopy == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  Cstager_TapeCopy_castorFile(tapeCopy,&castorFile);
  if ( castorFile == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  Cstager_CastorFile_fileSize(castorFile,&fileSize);
  if ( fileSize != file->filereq.bytes_in ) {
    serrno = SEINTERNAL;
    return(-1);
  }
  
  return(0);
}

int rtcpcld_updateNsSegmentAttributes(tape_list_t *tape,
                                      file_list_t *file,
                                      int tapeCopyNb,
                                      struct Cns_fileid *castorFile,
                                      time_t last_mod_time)
{
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq;
  int i, rc, stat_rc, save_serrno = 0, nbSegms = 0, compressionFactor;
  int retryNsUpdate = 0, maxRetryNsUpdate = 5;
  struct Cns_fileid castorFileId;
  struct Cns_segattrs *nsSegAttrs = NULL;
  struct Cns_filestat statbuf;
  char *blkid = NULL, *nsErrMsg = NULL;
  char castorFileName[CA_MAXPATHLEN+1];
 
  struct  Cns_segattrs * oldSegattrs = NULL;
  int oldNbSegms=0;

  if ( (tape == NULL) || (file == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  tapereq = &(tape->tapereq);
  filereq = &(file->filereq);
  (void)getNsErrBuf(&nsErrMsg);
  
  memset(&castorFileId,'\0',sizeof(castorFileId));
  strncpy(
          castorFileId.server,
          filereq->castorSegAttr.nameServerHostName,
          sizeof(castorFileId.server)-1
          );
  castorFileId.fileid = filereq->castorSegAttr.castorFileId;
  
  rc = checkSegment(
                    tape,
                    file
                    );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SEGMCHECK),
                    (struct Cns_fileid *)&castorFileId,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }

  /**
   * \internal
   * The file has been fully and correctly copied to tape. Go ahead
   * and create the segment in the name server. Note that for migration
   * there is only one segment per castor file since file segmentation
   * is not allowed (feature request #5179).
   */
  nbSegms = 1;
  nsSegAttrs = (struct Cns_segattrs *)calloc(
                                             nbSegms,
                                             sizeof(struct Cns_segattrs)
                                             );
  if ( nsSegAttrs == NULL ) {
    save_serrno = errno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                    (struct Cns_fileid *)&castorFileId,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "calloc()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }

  nsSegAttrs->copyno = tapeCopyNb;
  nsSegAttrs->fsec = 1;
  nsSegAttrs->segsize = filereq->bytes_in;
  if ( filereq->bytes_out > 0 ) {
    compressionFactor = (filereq->host_bytes * 100) / filereq->bytes_out;
    if ( (compressionFactor < 95) && (strcmp(tapereq->devtype,"3592") == 0)  )
      compressionFactor = 100;
  } else if ( (filereq->bytes_out == 0) && (strcmp(tapereq->devtype,"3592") == 0) ) {
    compressionFactor = 100;
  } else {
    compressionFactor = 0;
  }
  nsSegAttrs->compression = compressionFactor;
  nsSegAttrs->s_status = '-';
  if ( tapereq->mode != WRITE_ENABLE ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_WRONGMODE),
                    (struct Cns_fileid *)&castorFileId,
                    RTCPCLD_NB_PARAMS+3,
                    "",
                    DLF_MSG_PARAM_TPVID,
                    tapereq->vid,
                    "SIDE",
                    DLF_MSG_PARAM_INT,
                    tapereq->side,
                    "MODE",
                    DLF_MSG_PARAM_INT,
                    tapereq->mode,
                    RTCPCLD_LOG_WHERE
                    );
    if ( nsSegAttrs != NULL ) free(nsSegAttrs);
    serrno = EINVAL;
    return(-1);
  }
  strncpy(nsSegAttrs->vid,tapereq->vid,CA_MAXVIDLEN);
  nsSegAttrs->side = tapereq->side;
  
  memcpy(
         nsSegAttrs->blockid,
         filereq->blockid,
         sizeof(nsSegAttrs->blockid)
         );

  nsSegAttrs->fseq = filereq->tape_fseq;
  if ( use_checksum == 1 ) {
    strncpy(
            nsSegAttrs->checksum_name,
            filereq->castorSegAttr.segmCksumAlgorithm,
            CA_MAXCKSUMNAMELEN
            );
    nsSegAttrs->checksum = filereq->castorSegAttr.segmCksum;
  }
  blkid = rtcp_voidToString(
                            (void *)nsSegAttrs->blockid,
                            sizeof(nsSegAttrs->blockid)
                            );
  if ( blkid == NULL ) blkid = strdup("unknown");
  
  (void)dlf_write(
                  (inChild == 0 ? mainUuid : childUuid),
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_ADDSEGM),
                  (struct Cns_fileid *)&castorFileId,
                  10,
                  "",
                  DLF_MSG_PARAM_TPVID,
                  nsSegAttrs->vid,
                  "SIDE",
                  DLF_MSG_PARAM_INT,
                  nsSegAttrs->side,
                  "COPYNB",
                  DLF_MSG_PARAM_INT,
                  nsSegAttrs->copyno,
                  "FSEC",
                  DLF_MSG_PARAM_INT,
                  nsSegAttrs->fsec,
                  "SEGSIZE",
                  DLF_MSG_PARAM_INT64,
                  nsSegAttrs->segsize,
                  "COMPRFAC",
                  DLF_MSG_PARAM_INT,
                  nsSegAttrs->compression,
                  "FSEQ",
                  DLF_MSG_PARAM_INT,
                  nsSegAttrs->fseq,
                  "BLOCKID",
                  DLF_MSG_PARAM_STR,
                  blkid,
                  "CKSUMALG",
                  DLF_MSG_PARAM_STR,
                  nsSegAttrs->checksum_name,
                  "CKSUM",
                  DLF_MSG_PARAM_INT,
                  (int)nsSegAttrs->checksum
                  );
  free(blkid);



  /** For repacking purpose we have to decide what CNS function we have
      to call. In one case we update (replace), and in the normal case we
      add (setsegattr)
   */
  char* repackvid = NULL;
  serrno = rc = 0;
  
  /** check the stager catalogue, the diskcopy in STAGEOUT and a
      StageRepackRequest is found 
   */
  rc = rtcpcld_checkFileForRepack( castorFile, &repackvid );

  if (rc == -1){
   (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                      (struct Cns_fileid *)&castorFileId,
                      1,
                      "CHECKFORREPACK",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno));
    return (-1);
  }

  /*
   * Protect against false ENOENT from name server. This is the last
   * chance for this file, so let's be sure. There have been cases when
   * the nsd reports ENOENT but in fact the file is there...
   */
  for ( retryNsUpdate = 0; retryNsUpdate<maxRetryNsUpdate; retryNsUpdate++ ) {
    if ( repackvid != NULL ) {
       blkid = rtcp_voidToString(
                            (void *)nsSegAttrs->blockid,
                            sizeof(nsSegAttrs->blockid)
                            );
       (void)dlf_write(
                       (inChild == 0 ? mainUuid : childUuid),
                       RTCPCLD_LOG_MSG(RTCPCLD_MSG_REPACK),
                       (struct Cns_fileid *)&castorFileId,
                       1,
                       "OldVid",
                       DLF_MSG_PARAM_STR,
                       repackvid);

   
      /* we found a repackfile initiate different NS behavior*/
      /* replace the old tapecopy. Note that the old segments are deleted!
         and the old tapecopyno is assigned to the new tapecopy.*/

       rc = Cns_getsegattrs(
                       NULL,
                       (struct Cns_fileid *)&castorFileId,
                       &oldNbSegms,
                       &oldSegattrs
                       );

       if ( (rc == -1) ||
            (oldNbSegms <= 0) ||
            (oldSegattrs == NULL) ) {
          LOG_SYSCALL_ERR("Cns_getsegattrs()");  
          return(-1);
       }
       
       if ( use_checksum && oldNbSegms == 1 && oldSegattrs->checksum != nsSegAttrs->checksum){
         /* log error checksum problems for repack */
         /* checksum error I don't replace the segment */

         (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_WRONGCKSUM),
                          (struct Cns_fileid *)&castorFileId,
                          RTCPCLD_NB_PARAMS+7,
                          "",
                          DLF_MSG_PARAM_TPVID,
                          nsSegAttrs->vid,
                          "SIDE",
                          DLF_MSG_PARAM_INT,
                          nsSegAttrs->side,
                          "FSEQ",
                          DLF_MSG_PARAM_INT,
                          nsSegAttrs->fseq,
                          "BLOCKID",
                          DLF_MSG_PARAM_STR,
                          blkid,
                          "OLDCKSUM",
                          DLF_MSG_PARAM_INT,
                          (int)oldSegattrs->checksum,
                          "NEWCKSUM",
                          DLF_MSG_PARAM_INT,
                          (int)nsSegAttrs->checksum,
                          RTCPCLD_LOG_WHERE
                          );

         save_serrno = SECHECKSUM;
         free(oldSegattrs);
         oldSegattrs = NULL;
         rc=-1;
         break;
       }
       else {
         /* look for the previous copyno in the repacked tape */
         for(i = 0; i < oldNbSegms; i++) {
           if(!strcmp(oldSegattrs[i].vid, repackvid)) {
             nsSegAttrs->copyno = oldSegattrs[i].copyno;
             break;
           }
         }
         /* perform the replacement */
         rc = Cns_replacetapecopy(
                               &castorFileId,
                               repackvid,
                               nsSegAttrs->vid,
                               nbSegms,
                               nsSegAttrs,
                               last_mod_time
                               );

         free(oldSegattrs);
         oldSegattrs = NULL;
         free(repackvid);
         repackvid = NULL;
       }

    } else { 
      /* the normal Case */
            rc = Cns_setsegattrs(
                          (char *)NULL, // CASTOR file name 
                          &castorFileId,
                          nbSegms,
                          nsSegAttrs,
                          last_mod_time
                          );
    }
    if ( rc == 0 ) {
      break;
    } else {
      save_serrno = serrno;
      if ( save_serrno == ENOENT ) {
        /*
         * We ignore ENOENT. This means that the user has removed the
         * file before migrated to tape. We should double check however...
         */
        *castorFileName = '\0';
        stat_rc = Cns_statx(castorFileName,&castorFileId,&statbuf);
        if ( stat_rc == 0 )  {
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_FALSE_ENOENT),
                          (struct Cns_fileid *)&castorFileId,
                          2,
                          "SYSCALL",
                           DLF_MSG_PARAM_STR,
                          "Cns_setsegattrs()/Cns_replacetapecopy()",
                          "RETRY",
                          DLF_MSG_PARAM_INT,
                          retryNsUpdate
                          );
          rc = -1;
          serrno = SEINTERNAL;
          break;
        } else {
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_IGNORE_ENOENT),
                          (struct Cns_fileid *)&castorFileId,
                          3,
                          "SYSCALL",
                          DLF_MSG_PARAM_STR,
                          "Cns_setsegattrs()/Cns_replacetapecopy()",
                          "RETRY",
                          DLF_MSG_PARAM_INT,
                          retryNsUpdate,
                          "ERROR_STR",
                          DLF_MSG_PARAM_STR,
                          sstrerror(save_serrno)
                          );
        }
        rc = 0;
      } else if ( save_serrno == ENSFILECHG ) {
        /*
         * We ignore ENSFILECHG. This means that the file had alrady be
         * overwritten by the user before it was migrated.
         */
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_IGNORE_ENSFILECHG),
                        (struct Cns_fileid *)&castorFileId,
                        3,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "Cns_setsegattrs()/Cns_replacetapecopy()",
                        "RETRY",
                        DLF_MSG_PARAM_INT,
                        retryNsUpdate,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(save_serrno)
                        );
        rc = 0;
        // no need to retry here. We believe the nameserver, not like for ENOENT
        break;
      } else if ( save_serrno == ENSNOSEG ) {
        /*
         * We ignore ENSNOSEG. This means that the segment we are replacing
         * had alrady been removed. Probably a concurrent overwrite of the file
         */
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_IGNORE_ENSNOSEG),
                        (struct Cns_fileid *)&castorFileId,
                        3,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "Cns_setsegattrs()/Cns_replacetapecopy()",
                        "RETRY",
                        DLF_MSG_PARAM_INT,
                        retryNsUpdate,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(save_serrno)
                        );
        rc = 0;
        // no need to retry here. We believe the nameserver, not like for ENOENT
        break;
      } else if ( save_serrno == ENSCLASSNOSEGS ) {
        /*
         * We ignore ENSCLASSNOSEGS. This means that the segment created belongs
         * to a file that should not be on tape. Thus migration can be considered
	 * as completed.
         */
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_IGNORE_ENSCLASSNOSEGS),
                        (struct Cns_fileid *)&castorFileId,
                        3,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "Cns_setsegattrs()/Cns_replacetapecopy()",
                        "RETRY",
                        DLF_MSG_PARAM_INT,
                        retryNsUpdate,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(save_serrno)
                        );
        rc = 0;
        break;
      } else if ( save_serrno == ENSTOOMANYSEGS ) {
        /*
         * We ignore ENSCLASSNOSEGS. This means that the segment created belongs
         * to a file that already has enough copy on tape. So this segment is one too
	 * much. Thus migration can be considered as completed.
         */
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_IGNORE_ENSTOOMANYSEGS),
                        (struct Cns_fileid *)&castorFileId,
                        3,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "Cns_setsegattrs()/Cns_replacetapecopy()",
                        "RETRY",
                        DLF_MSG_PARAM_INT,
                        retryNsUpdate,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(save_serrno)
                        );
        rc = 0;
        break;
      } else {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                        (struct Cns_fileid *)&castorFileId,
                        RTCPCLD_NB_PARAMS+2,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "Cns_setsegattrs()/Cns_replacetapecopy()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(save_serrno),
                        RTCPCLD_LOG_WHERE
                        );
      }
      serrno = save_serrno;
      if ( rc == -1 ) break;
    }
    sleep(1);
  }
  if ( nsSegAttrs != NULL ) free(nsSegAttrs);
  return(rc);
}

int rtcpcld_checkNsSegment(tape_list_t *tape,
                           file_list_t *file)
{
  int rc = 0, nbSegms = 0, i, found = 0, save_serrno;
  struct Cns_segattrs *currentSegattrs = NULL, newSegattrs;
  struct Cns_fileid *castorFileId = NULL;
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq;
  char *blkid = NULL, *nsErrMsg = NULL;
  
  if ( (tape == NULL) || (file == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  tapereq = &(tape->tapereq);
  filereq = &(file->filereq);
  (void)getNsErrBuf(&nsErrMsg);

  rc = rtcpcld_getFileId(file,&castorFileId);
  if ( (rc == -1) || (castorFileId == NULL) ) {
    LOG_SYSCALL_ERR("rtcpcld_findTape()");
    return(-1);
  }

  rc = Cns_getsegattrs(
                       NULL,
                       castorFileId,
                       &nbSegms,
                       &currentSegattrs
                       );

  if ( (rc == -1) ||
       (nbSegms <= 0) ||
       (currentSegattrs == NULL) ) {
    dlf_write((inChild == 0 ? mainUuid : childUuid),
              RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
              castorFileId,
              RTCPCLD_NB_PARAMS+2,
              "SYSCALL",
              DLF_MSG_PARAM_STR,
              "Cns_getsegattrs()",
              "ERROR_STRING",
              DLF_MSG_PARAM_STR,
              sstrerror(serrno),
              RTCPCLD_LOG_WHERE);
    if ( castorFileId != NULL ) free(castorFileId);  
    return(-1);
  }

  /*
   * Find this segment
   */
  for ( i=0; i<nbSegms; i++ ) {
    if ( strcmp(currentSegattrs[i].vid,tapereq->vid) != 0 ) continue;
    if ( currentSegattrs[i].side != tapereq->side ) continue;
    if ( currentSegattrs[i].fseq != filereq->tape_fseq ) continue;
    if ( (filereq->position_method == TPPOSIT_BLKID) &&
         (memcmp(currentSegattrs[i].blockid,
                 filereq->blockid,
                 sizeof(filereq->blockid)) != 0) ) continue;
    found = 1;
    break;
  }

  /* 
   * String representation of the blockid used for logging purpose only
   */
  blkid = rtcp_voidToString(
                            (void *)filereq->blockid,
                            sizeof(filereq->blockid)
                            );
  if ( blkid == NULL ) blkid = strdup("unknown");

  if ( found == 0 ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_NSSEGNOTFOUND),
                    (struct Cns_fileid *)castorFileId,
                    RTCPCLD_NB_PARAMS+5,
                    "",
                    DLF_MSG_PARAM_TPVID,
                    tapereq->vid,
                    "SIDE",
                    DLF_MSG_PARAM_INT,
                    tapereq->side,
                    "MODE",
                    DLF_MSG_PARAM_INT,
                    tapereq->mode,
                    "FSEQ",
                    DLF_MSG_PARAM_INT,
                    filereq->tape_fseq,
                    "BLOCKID",
                    DLF_MSG_PARAM_STR,
                    blkid,
                    RTCPCLD_LOG_WHERE
                    );
    if ( blkid != NULL ) free(blkid);
    if ( currentSegattrs != NULL ) free(currentSegattrs);
    if ( castorFileId != NULL ) free(castorFileId);  
    serrno = ENOENT;
    return(-1);
  }
  
  if ( (use_checksum == 1) &&
       (filereq->castorSegAttr.segmCksumAlgorithm[0] != '\0') ) {
    if ( strlen(
                filereq->castorSegAttr.segmCksumAlgorithm
                ) > CA_MAXCKSUMNAMELEN ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_CKSUMNAMETOOLONG),
                      (struct Cns_fileid *)castorFileId,
                      1,
                      "CKSUMNAM",
                      DLF_MSG_PARAM_STR,
                      filereq->castorSegAttr.segmCksumAlgorithm
                      );
    } else if ( (currentSegattrs[i].checksum_name[0] == '\0') ||
                ((change_checksum_name == 1) && 
                 (strcmp(currentSegattrs[i].checksum_name,
                         filereq->castorSegAttr.segmCksumAlgorithm) != 0 )) ) {
      /*
       * The checksum is not set for this segment, or it is set but the
       * algorithm has changed since. If it is not set it normally means that
       * it has been created with an old CASTOR version and this is the
       * first time it is staged in since segment checksum is supported.
       */
      memcpy(&newSegattrs,&currentSegattrs[i],sizeof(newSegattrs));
      strncpy(
              newSegattrs.checksum_name,
              filereq->castorSegAttr.segmCksumAlgorithm,
              CA_MAXCKSUMNAMELEN
              );
      newSegattrs.checksum_name[CA_MAXCKSUMNAMELEN] = '\0';
      newSegattrs.checksum = filereq->castorSegAttr.segmCksum;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                       RTCPCLD_LOG_MSG(RTCPCLD_MSG_UPDCKSUM),
                      (struct Cns_fileid *)castorFileId,
                      9,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                      "SIDE",
                      DLF_MSG_PARAM_INT,
                      tapereq->side,
                      "MODE",
                      DLF_MSG_PARAM_INT,
                      tapereq->mode,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      filereq->tape_fseq,
                      "BLOCKID",
                      DLF_MSG_PARAM_STR,
                      blkid,
                      "OLDALG",
                      DLF_MSG_PARAM_STR,
                      currentSegattrs[i].checksum_name,
                      "OLDCKSUM",
                      DLF_MSG_PARAM_INT,
                      (int)currentSegattrs[i].checksum,
                      "NEWALG",
                      DLF_MSG_PARAM_STR,
                      newSegattrs.checksum_name,
                      "NEWCKSUM",
                      DLF_MSG_PARAM_INT,
                      (int)newSegattrs.checksum
                      );
      rc = Cns_updateseg_checksum(
                                  castorFileId->server,
                                  castorFileId->fileid,
                                  &currentSegattrs[i],
                                  &newSegattrs
                                  );
      if ( rc < 0 ) {
        LOG_SYSCALL_ERR("Cns_updateseg_checksum()");
        if ( blkid != NULL ) free(blkid);
        if ( currentSegattrs != NULL ) free(currentSegattrs);
        if ( castorFileId != NULL ) free(castorFileId);  
        return(-1);
      }
    } else {
      if ( strcmp(currentSegattrs[i].checksum_name,
                  filereq->castorSegAttr.segmCksumAlgorithm) == 0 ) {
        if ( currentSegattrs[i].checksum != filereq->castorSegAttr.segmCksum ) {
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_WRONGCKSUM),
                          (struct Cns_fileid *)castorFileId,
                          RTCPCLD_NB_PARAMS+7,
                          "",
                          DLF_MSG_PARAM_TPVID,
                          tapereq->vid,
                          "SIDE",
                          DLF_MSG_PARAM_INT,
                          tapereq->side,
                          "MODE",
                          DLF_MSG_PARAM_INT,
                          tapereq->mode,
                          "FSEQ",
                          DLF_MSG_PARAM_INT,
                          filereq->tape_fseq,
                          "BLOCKID",
                          DLF_MSG_PARAM_STR,
                          blkid,
                          "NSCKSUM",
                          DLF_MSG_PARAM_INT,
                          (int)currentSegattrs[i].checksum,
                          "SEGCKSUM",
                          DLF_MSG_PARAM_INT,
                          (int)filereq->castorSegAttr.segmCksum,
                          RTCPCLD_LOG_WHERE
                          );
          save_serrno = SECHECKSUM;
          rc = -1;
        } else {
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_CKSUMOK),
                          (struct Cns_fileid *)castorFileId,
                          6,
                          "",
                          DLF_MSG_PARAM_TPVID,
                          tapereq->vid,
                          "SIDE",
                          DLF_MSG_PARAM_INT,
                          tapereq->side,
                          "MODE",
                          DLF_MSG_PARAM_INT,
                          tapereq->mode,
                          "FSEQ",
                          DLF_MSG_PARAM_INT,
                          filereq->tape_fseq,
                          "BLOCKID",
                          DLF_MSG_PARAM_STR,
                          blkid,
                          "SEGCKSUM",
                          DLF_MSG_PARAM_INT,
                          (int)filereq->castorSegAttr.segmCksum
                          );
        }
      } else {
        /*
         * Not same checksum algorithm and we are not allowed to change it
         */
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_WRONGALG),
                        (struct Cns_fileid *)castorFileId,
                        9,
                        "",
                        DLF_MSG_PARAM_TPVID,
                        tapereq->vid,
                        "SIDE",
                        DLF_MSG_PARAM_INT,
                        tapereq->side,
                        "MODE",
                        DLF_MSG_PARAM_INT,
                        tapereq->mode,
                        "FSEQ",
                        DLF_MSG_PARAM_INT,
                        filereq->tape_fseq,
                        "BLOCKID",
                        DLF_MSG_PARAM_STR,
                        blkid,
                        "OLDALG",
                        DLF_MSG_PARAM_STR,
                        currentSegattrs[i].checksum_name,
                        "OLDCKSUM",
                        DLF_MSG_PARAM_INT,
                        (int)currentSegattrs[i].checksum,
                        "NEWALG",
                        DLF_MSG_PARAM_STR,
                        filereq->castorSegAttr.segmCksumAlgorithm,
                        "NEWCKSUM",
                        DLF_MSG_PARAM_INT,
                        (int)filereq->castorSegAttr.segmCksum
                        );
      }
    }
  }

  if ( castorFileId != NULL ) free(castorFileId);  
  if ( blkid != NULL ) free(blkid);
  if ( currentSegattrs != NULL ) free(currentSegattrs);
  if ( rc == -1 ) serrno = save_serrno;
  return(rc);
}

/** assure that all copies are on different tapes
 */
int rtcpcld_checkDualCopies(file_list_t *file)
{
  tape_list_t *tape;
  int rc, i, nbSegs = 0, dualCopyFound = 0;
  struct Cns_segattrs *segArray = NULL;
  struct Cns_fileid *fileId = NULL;
  struct Cns_filestat statbuf;
  struct Cns_fileclass fileClass;
  char castorFileName[CA_MAXPATHLEN+1], *nsErrMsg = NULL;
  
  if ( (file == NULL) || ((tape = file->tape) == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  (void)getNsErrBuf(&nsErrMsg);
  
  rc = rtcpcld_getFileId(file,&fileId);
  if ( fileId == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  rc = Cns_getsegattrs(NULL,fileId,&nbSegs,&segArray);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cns_getsegattrs()");
    free(fileId);
    return(-1);
  }

  /*
   * We need to check, if there is already a tapecopy of a file in the same 
   * migrator queue. Usually this is avoided by a migration policy, but if there
   * is none, it can then happen that >1 tapecopies are written to the same
   * tape.This is avoided here.
   */
   file_list_t* fl = NULL;
   /* go through the list of files and compare the fileids 

          iteration ->      file
                   |         |
                   V         V
     fileid:   1 2 3 4 5 6 7 3
   */
   if (file != NULL && file != file->next ){
   CLIST_ITERATE_BEGIN(file->next, fl)
     {
        
        if ( file->filereq.castorSegAttr.castorFileId == fl->filereq.castorSegAttr.castorFileId )
        { // fileid is the same
           dualCopyFound = 1;
           break;
        }
     }
   CLIST_ITERATE_END(file, fl);
   }

  for ( i=0; i<nbSegs; i++ ) {
    if ( strncmp(tape->tapereq.vid,segArray[i].vid,CA_MAXVIDLEN) == 0 ) {
      dualCopyFound = 1;
      break;
    }
  }
  if ( dualCopyFound == 1 ) {
    /*
     * We ignore dual copies if it is a single copy file class
     */
    *castorFileName = '\0';
    rc = Cns_statx(castorFileName,fileId,&statbuf);
    if ( rc == -1 ) {
      free(fileId);
      return(-1);
    }
    memset(&fileClass,'\0',sizeof(fileClass));
    rc = Cns_queryclass(
                        fileId->server,
                        statbuf.fileclass,
                        NULL,
                        &fileClass
                        );
    free(fileId);
    if ( rc == -1 ) return(-1);
    if ( fileClass.tppools != NULL ) free(fileClass.tppools);
    if ( fileClass.nbcopies == 1 ) return(0);
    serrno = EEXIST;
    return(-1);
  } else {
    free(fileId);
  }
  return(0);
}

int rtcpcld_getOwner(struct Cns_fileid *fileId,
                     int *uid,
                     int *gid)
{
  int rc;
  struct Cns_filestat statbuf;
  char castorFileName[CA_MAXPATHLEN+1], *nsErrMsg = NULL;
  
  if ( fileId == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  (void)getNsErrBuf(&nsErrMsg);
  *castorFileName = '\0';
  rc = Cns_statx(castorFileName,fileId,&statbuf);
  if ( rc == -1 ) {
    return(-1);
  }
  if ( uid != NULL ) *uid = (int)statbuf.uid;
  if ( gid != NULL ) *gid = (int)statbuf.gid;
  return(0);
}

int rtcpcld_getFileId(file_list_t *file,
                      struct Cns_fileid **fileId)
{
  rtcpFileRequest_t *filereq;

  if ( (file == NULL) || (fileId == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  filereq = &(file->filereq);
  *fileId = (struct Cns_fileid *)calloc(1,sizeof(struct Cns_fileid));
  if ( *fileId == NULL ) {
    serrno = SESYSERR;
    return(-1);
  }
  (*fileId)->fileid = filereq->castorSegAttr.castorFileId;
  strncpy(
          (*fileId)->server,
          filereq->castorSegAttr.nameServerHostName,
          sizeof((*fileId)->server)-1
          );
  return(0);
}

