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
 * @(#)$RCSfile: rtcpcldNsInterface.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/05 16:04:56 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldNsInterface.c,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/05 16:04:56 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>                  /* Standard data types          */
#include <sys/stat.h>
#include <errno.h>
#if defined(_WIN32)
#include <io.h>
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <unistd.h>
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#endif /* _WIN32 */
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
#include <Cns_api.h>
#include <vmgr_api.h>

#include <Ctape_constants.h>
#include <castor/stager/Tape.h>
#include <castor/stager/Segment.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <castor/stager/IStagerSvc.h>
#include <castor/stager/CastorFile.h>
#include <castor/stager/TapeCopy.h>
#include <castor/Services.h>
#include <castor/BaseAddress.h>
#include <castor/db/DbAddress.h>
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

char *getconfent _PROTO((char *, char *, int));

/** Global for switching off logging when called from rtcpcldapi.c
 */
int dontLog = 0;
/** Global needed for determine which uuid to log
 */
int inChild;
/** uuid's set by calling process (rtcpclientd:mainUuid, VidWorker:childUuid)
 */
Cuuid_t childUuid, mainUuid;

static unsigned char nullblkid[4] = {'\0', '\0', '\0', '\0'};

static int use_checksum = 1;

static int orderSegmByOffset(
                             arg1,
                             arg2
                             )
     CONST void *arg1, *arg2;
{
  int rc;
  struct Cstager_Segment_t **segm1, **segm2;
  u_signed64 offset1, offset2;

  segm1 = (struct Cstager_Segment_t **)arg1;
  segm2 = (struct Cstager_Segment_t **)arg2;
  if ( segm1 == NULL || segm2 == NULL || *segm1 == NULL || *segm2 == NULL ) {
    return(0);
  }
  
  Cstager_Segment_offset(*segm1,&offset1);
  Cstager_Segment_offset(*segm2,&offset2);
  rc = 0;

  if ( offset1 < offset2 ) rc = -1;
  if ( offset1 > offset2 ) rc = 1;
  return(rc);
}

void rtcpcld_initNsInterface() 
{
  char *p;
  
  p = getenv("STAGE_USE_CHECKSUM");
  if ( p == NULL ) {
    p = getconfent("STG","USE_CHECKSUM",0);
  }
  if ( (p != NULL) && (strcmp(p,"NO") == 0) ) use_checksum = 0;

  return;
}


int rtcpcld_checkSegments(
                          segmArray,
                          nbSegms,
                          castorFileSize,
                          errorStr
                          )
     struct Cstager_Segment_t **segmArray;
     int nbSegms;
     u_signed64 castorFileSize;
     char **errorStr;
{
  int rc = 0, i, save_serrno, errorCode, severity;
  char *failedTestName = NULL;
  enum Cstager_SegmentStatusCodes_t segmStatus;
  u_signed64 totSize = 0;
  u_signed64 segmSize, segmOffset, nextSegmOffset;
  u_signed64 lastSegmOffset = 0, lastSegmSize = 0;

  /*
   * Sanity checks:
   *  Test 1: accumulated segment size must match the castor file size
   *  Test 2: all but last segment must have hit EOT (errorCode = ENOSPC)
   *  Test 3: last segment must have no error
   *  Test 4: offset + bytes_in of last segment must match the castor file size
   *  Test 5: offset + bytes_in of each but last segment must match the offset
   *          of the next segment (paranoid...)
   */
  for ( i=0; i<nbSegms; i++ ) {
    segmSize = segmOffset = nextSegmOffset = 0;
    Cstager_Segment_bytes_in(segmArray[i],&segmSize);
    Cstager_Segment_offset(segmArray[i],&segmOffset);
    if ( i < (nbSegms-1) ) Cstager_Segment_offset(
                                                  segmArray[i+1],
                                                  &nextSegmOffset
                                                  );
    /*
     * Calculate accumulated segment size for test 1
     */
    totSize += segmSize;

    Cstager_Segment_status(segmArray[i],&segmStatus);
    Cstager_Segment_severity(segmArray[i],&severity);
    Cstager_Segment_errorCode(segmArray[i],&errorCode);
    if ( i < (nbSegms-1) ) {
      /*
       * Test 2: If not last segment, check that EOT was hit
       */
      if ( (segmStatus != SEGMENT_FAILED) ||
           ((severity & RTCP_FAILED) != RTCP_FAILED) ||
           (errorCode != ENOSPC) ) {
        failedTestName = strdup("EOT was not hit for intermediate segment");
        rc = -1;
        break;
      }
      /*
       * Test 5: If not last segment, check that offset + bytes_in == offset of
       * next segment
       */
      if ( (segmOffset + segmSize) != nextSegmOffset ) {
        failedTestName = strdup("Hole detected between two intermediate segments");
        rc = -1;
        break;
      }
    }
  }
  /*
   * Test 1: accumulated size must match castor file size
   */
  if ( totSize != castorFileSize ) {
    failedTestName = strdup("Accumulated segment size do not match file size");
    rc = -1;
  }
  /*
   * Test 3: last segment must be successful
   */
  if ( (segmStatus != SEGMENT_FILECOPIED) ||
       (errorCode != 0) || (severity != RTCP_OK) ) {
    failedTestName = strdup("Last segment not successfully copied");
    rc = -1;
  }
  /*
   * Test 4: last segment offset + bytes_in must match
   * castor file size
   */
  if ( (segmOffset + segmSize) != castorFileSize ) {
    failedTestName = strdup("Last segment offset+size != file size");
    rc = -1;
  }

  if ( errorStr != NULL ) *errorStr = failedTestName;

  return(rc);
}

int rtcpcld_updateSegmentAttr(
                              tpCopy
                              )

     struct Cstager_TapeCopy_t *tpCopy;
{
  int rc, i, save_serrno, nbSegms = 0, errorCode, severity, compressionFactor;
  int mode, side;
  struct Cstager_Segment_t **segmArray = NULL, *segm;
  struct Cstager_Tape_t *tp;
  struct Cstager_CastorFile_t *castorFile = NULL;
  struct Cns_fileid castorFileId;
  struct Cns_segattrs *nsSegAttrs = NULL;
  unsigned char *blockid;
  char *nsHost = NULL, *errorStr = NULL, *cksumName = NULL, *vid, *blkid;
  unsigned long cksumValue;
  ID_TYPE key;
  u_signed64 fileId = 0, hostBytes, bytesOut, castorFileSize;

  if ( tpCopy == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  Cstager_TapeCopy_segments(tpCopy,&segmArray,&nbSegms);
  if ( (segmArray == NULL) || (nbSegms <= 0) ) {
    Cstager_TapeCopy_id(tpCopy,&key);
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_TPCPNOSEGM,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "KEY",
                      DLF_MSG_PARAM_INT64,
                      key,
                      RTCPCLD_LOG_WHERE
                      );
    }
    serrno = ENOENT;
    return(-1);
  }

  qsort(
        (void *)segmArray,
        (size_t)nbSegms,
        sizeof(struct Cstager_Segment_t *),
        orderSegmByOffset
        );

  Cstager_TapeCopy_castorFile(tpCopy,&castorFile);
  if ( castorFile == NULL ) {
    Cstager_TapeCopy_id(tpCopy,&key);
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_TPCPNOFILE,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "KEY",
                      DLF_MSG_PARAM_INT64,
                      key,
                      RTCPCLD_LOG_WHERE
                      );
    }
    
    free(segmArray);
    serrno = ENOENT;
    return(-1);
  }

  nsHost = NULL;
  memset(&castorFileId,'\0',sizeof(castorFileId));
  Cstager_CastorFile_nsHost(castorFile,(CONST char **)&nsHost);
  Cstager_CastorFile_fileId(castorFile,&fileId);
  strncpy(castorFileId.server,nsHost,sizeof(castorFileId.server)-1);
  castorFileId.fileid = fileId;
  
  Cstager_CastorFile_size(castorFile,&castorFileSize);
  if ( castorFileSize <= 0 ){
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_ZEROSIZE,
                      (struct Cns_fileid *)&castorFileId,
                      RTCPCLD_NB_PARAMS,
                      RTCPCLD_LOG_WHERE
                      );
    }
    free(segmArray);
    serrno = EINVAL;
    return(-1);
  }

  rc = rtcpcld_checkSegments(
                             segmArray,
                             nbSegms,
                             castorFileSize,
                             &errorStr
                             );
  if ( rc == -1 ) {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SEGMCHKERROR,
                      (struct Cns_fileid *)&castorFileId,
                      RTCPCLD_NB_PARAMS+1,
                      "ERRORSTR",
                      DLF_MSG_PARAM_STR,
                      errorStr,
                      RTCPCLD_LOG_WHERE
                      );
    }
    free(segmArray);
    free(errorStr);
    serrno = EINVAL;
    return(-1);
  }

  /*
   * The file has been fully and correctly copied to tape. Go ahead
   * and create the segments in the name server
   */
  nsSegAttrs = (struct Cns_segattrs *)calloc(
                                             nbSegms,
                                             sizeof(struct Cns_segattrs)
                                             );
  if ( nsSegAttrs == NULL ) {
    save_serrno = errno;
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
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
    }
    free(segmArray);
    serrno = save_serrno;
    return(-1);
  }

  for ( i=0; i<nbSegms; i++ ) {
    nsSegAttrs[i].copyno = 0;
    nsSegAttrs[i].fsec = i+1;
    Cstager_Segment_bytes_in(segmArray[i],&(nsSegAttrs[i].segsize));
    Cstager_Segment_bytes_out(segmArray[i],&bytesOut);
    if ( bytesOut > 0 ) {
      Cstager_Segment_host_bytes(segmArray[i],&hostBytes);
      compressionFactor = (hostBytes * 100) / bytesOut;
    } else {
      compressionFactor = 0;
    }
    nsSegAttrs[i].compression = compressionFactor;
    Cstager_Segment_tape(segmArray[i],&tp);
    if ( tp == NULL ) {
      if ( dontLog == 0 ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_NOTAPE,
                        (struct Cns_fileid *)&castorFileId,
                        RTCPCLD_NB_PARAMS,
                        RTCPCLD_LOG_WHERE
                        );
      }
      serrno = EINVAL;
      free(segmArray);
      return(-1);
    }
    nsSegAttrs[i].s_status = '-';

    vid = NULL;
    Cstager_Tape_vid(tp,(CONST char **)&vid);
    Cstager_Tape_side(tp,&side);
    Cstager_Tape_tpmode(tp,&mode);
    if ( mode != WRITE_ENABLE ) {
      if ( dontLog == 0 ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_WRONGMODE,
                        (struct Cns_fileid *)&castorFileId,
                        RTCPCLD_NB_PARAMS+3,
                        "",
                        DLF_MSG_PARAM_TPVID,
                        vid,
                        "SIDE",
                        DLF_MSG_PARAM_INT,
                        side,
                        "MODE",
                        DLF_MSG_PARAM_INT,
                        mode,
                        RTCPCLD_LOG_WHERE
                        );
      }
      serrno = EINVAL;
      free(segmArray);
      return(-1);
    }
    strncpy(nsSegAttrs[i].vid,vid,CA_MAXVIDLEN);
    nsSegAttrs[i].side = side;

    blockid = NULL;
    Cstager_Segment_blockid(segmArray[i],(CONST unsigned char **)&blockid);
    if ( blockid != NULL ) {
      memcpy(nsSegAttrs[i].blockid,blockid,sizeof(nsSegAttrs[i].blockid));
    }
    Cstager_Segment_fseq(segmArray[i],&(nsSegAttrs[i].fseq));
    if ( use_checksum == 1 ) {
      Cstager_Segment_segmCksumAlgorithm(
                                         segmArray[i],
                                         (CONST char **)&cksumName
                                         );
      Cstager_Segment_segmCksum(
                                segmArray[i],
                                &cksumValue
                                );
      strncpy(
              nsSegAttrs[i].checksum_name,
              cksumName,
              CA_MAXCKSUMNAMELEN
              );
      nsSegAttrs[i].checksum = cksumValue;
    }
    blkid = rtcp_voidToString(
                              (void *)nsSegAttrs[i].blockid,
                              sizeof(nsSegAttrs[i].blockid)
                              );
    if ( blkid == NULL ) blkid = strdup("unknown");

    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_DEBUG,
                      RTCPCLD_MSG_ADDSEGM,
                      (struct Cns_fileid *)&castorFileId,
                      9,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      nsSegAttrs[i].vid,
                      "SIDE",
                      DLF_MSG_PARAM_INT,
                      nsSegAttrs[i].side,
                      "FSEC",
                      DLF_MSG_PARAM_INT,
                      nsSegAttrs[i].fsec,
                      "SEGSIZE",
                      DLF_MSG_PARAM_INT64,
                      nsSegAttrs[i].segsize,
                      "COMPRFAC",
                      DLF_MSG_PARAM_INT,
                      nsSegAttrs[i].compression,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      nsSegAttrs[i].fseq,
                      "BLOCKID",
                      DLF_MSG_PARAM_STR,
                      blkid,
                      "CKSUMALG",
                      DLF_MSG_PARAM_STR,
                      nsSegAttrs[i].checksum_name,
                      "CKSUM",
                      DLF_MSG_PARAM_INT,
                      nsSegAttrs[i].checksum
                      );
      free(blkid);
    }
  }
  rc = Cns_setsegattrs(
                       (char *)NULL, /* CASTOR file name */
                       &castorFileId,
                       nbSegms,
                       nsSegAttrs
                       );
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)&castorFileId,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "Cns_setsegattrs()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(save_serrno),
                      RTCPCLD_LOG_WHERE
                      );
    }

    serrno = save_serrno;
  }
  free(segmArray);

  return(rc);
}
