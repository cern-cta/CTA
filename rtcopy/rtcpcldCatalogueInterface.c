/******************************************************************************
 *                      rtcpcldCatalogueInterface.c
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
 * @(#)$RCSfile: rtcpcldCatalogueInterface.c,v $ $Revision: 1.49 $ $Release$ $Date: 2004/09/27 11:02:29 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldCatalogueInterface.c,v $ $Revision: 1.49 $ $Release$ $Date: 2004/09/27 11:02:29 $ Olof Barring";
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
#include <Cns_api.h>
#include <dlf_api.h>
#include <Cnetdb.h>
#include <Cuuid.h>
#include <Cmutex.h>
#include <u64subr.h>
#include <Cglobals.h>
#include <serrno.h>
#if defined(VMGR)
#include <vmgr_api.h>
#endif /* VMGR */
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
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld_messages.h>
#include <rtcpcld.h>

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

/** VidChild only: the tape processed by this VidChild
 */
static struct Cstager_Tape_t *currentTape = NULL;

/** rtcpclientd only: the list of active tapes
 */
static RtcpcldTapeList_t *tpList = NULL;

/*
 * Macros for synchronizing access to global currentTape in VidWorker
 * (and other multithreaded clients)
 */
#define LOCKTP { \
  int __save_serrno = serrno; \
  (void)Cmutex_lock(&currentTape,-1); \
  serrno = __save_serrno; \
}
#define UNLOCKTP { \
  int __save_serrno = serrno; \
  (void)Cmutex_unlock(&currentTape); \
  serrno = __save_serrno; \
}

/*
 * We can affort this to be thread-unsafe since the
 * key will only be set once, just in the beginning of
 * the VidWorker process
 */
static ID_TYPE tapeKey(
                       _key
                       )
     ID_TYPE *_key;
{
  static ID_TYPE key;
  
  if ( _key != NULL ) key = *_key;
  return(key);
}

void rtcpcld_setTapeKey(
                        key
                        )
     ID_TYPE key;
{
  (void)tapeKey(&key);
  return;
}

ID_TYPE rtcpcld_getTapeKey() 
{
  return(tapeKey(NULL));
}

/**
 * Search active tapes for an entry matching the provided tapereq.
 * This routine is called from various places both in parent (rtcpclientd)
 * and child (VidWorker) mode. In the latter case it will always return
 * the current tape.
 */
static int findTape(
                    tapereq,
                    tp,
                    tpItem
                    )
     rtcpTapeRequest_t *tapereq;
     struct Cstager_Tape_t **tp;
     RtcpcldTapeList_t **tpItem;
{
  RtcpcldTapeList_t *tpIterator;
  tape_list_t *tl;
  int found = 0;

  if ( tapereq == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  if ( (*tapereq->vid == '\0') ||
       (tapereq->mode != 0 && tapereq->mode != 1) ) {
    serrno = ENOENT;
    return(-1);
  }
  if ( tpList == NULL ) {
    if ( currentTape == NULL ) return(0);
    if ( tp != NULL ) *tp = currentTape;
    if ( tpItem != NULL ) *tpItem = NULL;
    return(1);
  }
  
  CLIST_ITERATE_BEGIN(tpList,tpIterator) 
    {
      tl = tpIterator->tape;
      if ( (tl->tapereq.mode == tapereq->mode) &&
           (tl->tapereq.side == tapereq->side) &&
           (strcmp(tl->tapereq.vid,tapereq->vid) == 0) ) {
        found = 1;
        break;
      }
    }
  CLIST_ITERATE_END(tpList,tpIterator);
  if ( found == 1 && tp != NULL ) *tp = tpIterator->tp;
  if ( found == 1 && tpItem != NULL ) *tpItem = tpIterator;
  return(found);
}

/**
 * Add a new tape to the list of active tapes. This routine is only
 * called in parent (rtcpclientd) mode from the method
 * rtcpcld_getVIDsToDo().
 */
static int addTape(
                   tape,
                   tp
                   )
     tape_list_t *tape;
     struct Cstager_Tape_t *tp;
{
  RtcpcldTapeList_t *tpIterator;
  int save_serrno;

  if ( tape == NULL && tp == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  tpIterator = (RtcpcldTapeList_t *)calloc(1,sizeof(RtcpcldTapeList_t));
  if ( tpIterator == NULL ) {
    save_serrno = errno;
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
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
    serrno = save_serrno;
    return(-1);
  }
  tpIterator->tape = tape;
  tpIterator->tp = tp;
  CLIST_INSERT(tpList,tpIterator);
  return(0);
}

/**
 * Remove a tape from the list of active tapes. This routine is called
 * (via the externalised rtcpcld_delTape()) from rtcpcpclientd.
 */
static int delTape(
                   tape
                   )
     tape_list_t **tape;
{
  struct Cstager_Tape_t *tp;
  RtcpcldTapeList_t *tpItem = NULL;
  int found = 0;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  found = findTape(&((*tape)->tapereq),&tp,&tpItem);
  if ( found == 1 ) {
    Cstager_Tape_delete(tp);
    rtcpc_FreeReqLists(tape);
    if ( tpItem != NULL ) CLIST_DELETE(tpList,tpItem);
  }
  return(0);
}

/**
 * Create/get the DB services. The call is wrapped in this routine to
 * avoid repeating the same log statment upon failure.
 *
 * \note Thread safeness and concurrent access is managed by C_Services
 */
static int getDbSvc(
                    dbSvc
                    )
     struct C_Services_t ***dbSvc;
{
  static int svcsKey = -1;
  struct C_Services_t **svc;
  int rc, save_serrno;

  if ( dbSvc == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  svc = NULL;
  rc = Cglobals_get(&svcsKey,(void **)&svc,sizeof(struct C_Services_t **));
  if ( rc == -1 || svc == NULL ) {
    save_serrno = serrno;
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "Cglobals_get()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      RTCPCLD_LOG_WHERE
                      );
    }
    serrno = save_serrno;
    return(-1);
  }

  if ( *svc == NULL ) {
    rc = C_Services_create(svc);
    if ( rc == -1 ) {
      save_serrno = serrno;
      if ( dontLog == 0 ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_DBSVC,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "DBSVCCALL",
                        DLF_MSG_PARAM_STR,
                        "C_Services_create()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
      }
      serrno = save_serrno;
      return(-1);
    }
  }
  *dbSvc = svc;
  
  return(0);
}

/**
 * Create/get the stager DB services. The call is wrapped in this routine to
 * avoid repeating the same log statment upon failure.
 *
 * \note Thread safeness and concurrent access is managed by Cstager_IStagerSvc
 */
static int getStgSvc(
                     stgSvc
                     )
     struct Cstager_IStagerSvc_t **stgSvc;
{
  struct C_Services_t **svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  int rc, save_serrno;  

  if ( stgSvc == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = getDbSvc(&svcs);
  if ( rc == -1 ) return(-1);

  rc = C_Services_service(*svcs,"OraStagerSvc",SVC_ORASTAGERSVC, &iSvc);
  if ( rc == -1 || iSvc == NULL ) {
    save_serrno = serrno;
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+3,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "C_IServices_service()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
    }
    serrno = save_serrno;
    return(-1);
  }

  *stgSvc = Cstager_IStagerSvc_fromIService(iSvc);
  return(0);
}

/**
 * Update the memory copy of the tape and all attached segments from the database.
 * If no tape is passed (tp == NULL), the currentTape is updated (or created if
 * it not yet exists).
 */
static int updateTapeFromDB(
                            tp
                            )
     struct Cstager_Tape_t *tp;
{
  struct Cdb_DbAddress_t *dbAddr;
  struct C_Services_t **svcs = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  int rc = 0, save_serrno;
  ID_TYPE key = 0;

  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);

  if ( tp == NULL ) tp = currentTape;

  if ( tp == NULL ) {
    key = rtcpcld_getTapeKey();
    rc = Cdb_DbAddress_create(key,"OraCnvSvc",SVC_ORACNV,&dbAddr);
    if ( rc == -1 ) {
      save_serrno = serrno;
      if ( dontLog == 0 ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_SYSCALL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "Cdb_DbAddress_create()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(save_serrno),
                        RTCPCLD_LOG_WHERE
                        );
      }
      serrno = save_serrno;
      return(-1);
    }
    baseAddr = Cdb_DbAddress_getBaseAddress(dbAddr);
  } else {
    Cstager_Tape_id(tp,&key);
    rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
    if ( rc == -1 ) return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  if ( tp == NULL ) {
    rc = C_Services_createObj(*svcs,iAddr,&iObj);
  } else {
    iObj = Cstager_Tape_getIObject(tp);
    rc = C_Services_updateObj(*svcs,iAddr,iObj);
  }
  
  if ( rc == -1 ) {
    save_serrno = serrno;
    C_IAddress_delete(iAddr);
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+4,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      (tp == NULL ? "C_Services_createObj()" : "C_Services_updateObj()"),
                      "DBKEY",
                      DLF_MSG_PARAM_INT,
                      (int)key,
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(save_serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
    }
    serrno = save_serrno;
    return(-1);
  }
  C_IAddress_delete(iAddr);
  if ( tp == NULL ) currentTape = Cstager_Tape_fromIObject(iObj);
  return(0);
}

int rtcpcld_updateTapeFromDB(
                             tp
                             )
     struct Cstager_Tape_t *tp;
{
  return(updateTapeFromDB(tp));
}

/**
 * Update the memory copy of the segment from the database.
 */
static int updateSegmentFromDB(
                               segm
                               )
     struct Cstager_Segment_t *segm;
{
  struct C_Services_t **svcs = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  int rc = 0, save_serrno;
  ID_TYPE key = 0;

  if ( segm == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);

  rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
  if ( rc == -1 ) return(-1);

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_Segment_getIObject(segm);

  Cstager_Segment_id(segm,&key);

  rc = C_Services_updateObj(*svcs,iAddr,iObj);  
  if ( rc == -1 ) {
    save_serrno = serrno;
    C_IAddress_delete(iAddr);
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+4,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "C_Services_updateObj()",
                      "DBKEY",
                      DLF_MSG_PARAM_INT,
                      (int)key,
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(save_serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
    }
    serrno = save_serrno;
    return(-1);
  }
  C_IAddress_delete(iAddr);
  return(0);
}
/**
 * Send notification (UDP) to all clients that submitted segment requests for this tape.
 * Only one notification message is sent per client, even if the client owns several
 * segments.
 */
static int notifyTape(
                      tp
                      )
     struct Cstager_Tape_t *tp;
{
  char *currentNotifyAddr = NULL;
  char *notifyAddr = NULL;
  struct Cstager_Segment_t **segments = NULL, *segm;
  int moreToDo = 1, rc, i, nbItems;
  int *notified, save_serrno;

  Cstager_Tape_segments(tp,&segments,&nbItems);

  if ( nbItems > 0 ){
    notified = (int *)calloc(nbItems,sizeof(int));
    if ( notified == NULL ) {
      serrno = errno;
      free(segments);
      return(-1);
    }
  } else return(0);
  /*
   * Only send one notification per client
   */
  while (moreToDo == 1) {
    moreToDo = 0;
    currentNotifyAddr = NULL;
    for (i=0; i<nbItems; i++) {
      segm = segments[i];
      if ( notified[i] == 0 ) {
        moreToDo = 1;
        Cstager_Segment_clientAddress(
                                      segm,
                                      (CONST char **)&notifyAddr
                                      );
        if ( currentNotifyAddr == NULL && notifyAddr != NULL ) {
          currentNotifyAddr = notifyAddr;
          save_serrno = serrno;
          rc = rtcpcld_sendNotify(notifyAddr);
          if ( rc == -1 ) {
            /* Notification failed: not fatal but must be logged */
            if ( dontLog == 0 ) {
              (void)dlf_write(
                              (inChild == 0 ? mainUuid : childUuid),
                              DLF_LVL_ERROR,
                              RTCPCLD_MSG_SYSCALL,
                              (struct Cns_fileid *)NULL,
                              RTCPCLD_NB_PARAMS+3,
                              "SYSCALL",
                              DLF_MSG_PARAM_STR,
                              "rtcpcld_sendNotify()",
                              "TOADDR",
                              DLF_MSG_PARAM_STR,
                              (notifyAddr != NULL ? notifyAddr : "(null)"),
                              "ERROR_STR",
                              DLF_MSG_PARAM_STR,
                              sstrerror(serrno),
                              RTCPCLD_LOG_WHERE
                              );
            }
          }
          serrno = save_serrno;
          notified[i] = 1;
        } else if ( (notifyAddr != NULL) &&
                    (strcmp(notifyAddr,currentNotifyAddr) == 0) ) {
          /*
           *  Client already notified
           */
          notified[i] = 1;
        }
      }
    }
  }
  free(notified);
  free(segments);

  return(0);
}

/**
 * Notify (UDP) the client that submitted this segment
 */
static int notifySegment(
                         segm
                         )
     struct Cstager_Segment_t *segm;
{
  char *notifyAddr = NULL;
  int rc, save_serrno;

  if ( segm == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  Cstager_Segment_clientAddress(
                                segm,
                                (CONST char **)&notifyAddr
                                );
  save_serrno = serrno;
  rc = rtcpcld_sendNotify(notifyAddr);
  if ( rc == -1 ) {
    /* Notification failed: not fatal but must be logged */
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+3,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "rtcpcld_sendNotify()",
                      "TOADDR",
                      DLF_MSG_PARAM_STR,
                      (notifyAddr != NULL ? notifyAddr : "(null)"),
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      RTCPCLD_LOG_WHERE
                      );
    }
  }
  serrno = save_serrno;

  return(0);
}
/**
 * Find the segment matching the passed filereq. This routine is only
 * called from methods used by the VidWorker. Match criteria is either that
 *   - the client address (if specified) must match that of the segment
 *   - the tape fseq or blockid are valid and match
 *   - the disk paths (not '\0' or '.') are valid and match
 */
int rtcpcld_findSegmentFromFile(
                                file,
                                tp,
                                segment,
                                clientAddr
                                )
     file_list_t *file;
     struct Cstager_Tape_t *tp;
     struct Cstager_Segment_t **segment;
     char *clientAddr;
{
  int found = 0, i, nbItems, fseq;
  unsigned char *blockid;
  char *diskPath, *cAddr;
  struct Cstager_Segment_t **segments = NULL, *segm;
  rtcpFileRequest_t *filereq = NULL;

  if ( segment != NULL ) *segment = NULL;

  if ( file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  filereq = &file->filereq;

  if ( (filereq->tape_fseq <= 0) && 
       (memcmp(filereq->blockid,nullblkid,4) == 0) &&
       ((*filereq->file_path == '\0') || 
        (strcmp(filereq->file_path,".") == 0)) ) {
    serrno = ENOENT;
    return(-1);
  }

  Cstager_Tape_segments(tp,&segments,&nbItems);

  for (i=0; i<nbItems; i++) {
    segm = segments[i];
    diskPath = cAddr = NULL;
    blockid = NULL;
    fseq = -1;
    Cstager_Segment_fseq(
                         segm,
                         &fseq
                         );
    Cstager_Segment_blockid(
                            segm,
                            (CONST unsigned char **)&blockid
                            );
    Cstager_Segment_diskPath(
                             segm,
                             (CONST char **)&diskPath
                             );
    if ( clientAddr != NULL ) Cstager_Segment_clientAddress(
                                                            segm,
                                                            (CONST char **)&cAddr
                                                            );
    if ( blockid == NULL || diskPath == NULL ) {
      continue;
    }

    if ( clientAddr != NULL ) {
      if ( (cAddr == NULL) || (strcmp(clientAddr,cAddr) != 0) ) continue;
    }

    if ( (fseq > 0) ||
         (memcmp(blockid,nullblkid,4) != 0) ||
         ((*diskPath != '\0') &&
          (strcmp(diskPath,".") != 0)) ) {
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
                  (memcmp(blockid,nullblkid,4) == 0) &&
                  (strcmp(diskPath,filereq->file_path) == 0) ) {
        /*
         * ... or, the tape position is not known and the disk file paths match
         */
        found = 1;
        break;
      }
    }
  }

  if ( segments != NULL ) free(segments);
  if ( (found == 1) && (segment != NULL) ) *segment = segm;
  return(found);
}

static int findSegment(
                       filereq,
                       segment
                       )
     rtcpFileRequest_t *filereq;
     struct Cstager_Segment_t **segment;
{
  int rc;
  file_list_t file;
  if ( filereq == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  file.filereq = *filereq;

  if ( currentTape == NULL ) {
    rc = updateTapeFromDB(NULL);
    if ( rc == -1 ) return(-1);
  }

  return(rtcpcld_findSegmentFromFile(&file,currentTape,segment,NULL));
}

/**
 * This method is only called from methods used by the VidWorker childs.
 * Search the passed tape request for an entry matching the passed segment.
 * Match criteria is either that
 *   - the client address (if specified) must match that of the segment
 *   - the tape fseq or blockid are valid and match
 *   - the disk paths (not '\0' or '.') are valid and match
 */
int rtcpcld_findFileFromSegment(
                                segment,
                                tape,
                                clientAddr,
                                file
                                )
     struct Cstager_Segment_t *segment;
     tape_list_t *tape;
     char *clientAddr;
     file_list_t **file;
{
  unsigned char  *blockid = NULL;
  file_list_t *fl;
  char *diskPath = NULL, *cAddr = NULL;
  int found = 0, fseq = -1;

  if ( segment == NULL || tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  if ( file != NULL ) *file = NULL;

  if ( clientAddr != NULL ) {
    Cstager_Segment_clientAddress(
                                  segment,
                                  (CONST char **)&cAddr
                                  );
    if ( (cAddr == NULL) || (strcmp(clientAddr,cAddr) != 0) ) return(0);
  }
  
  Cstager_Segment_fseq(
                       segment,
                       &fseq
                       );
  Cstager_Segment_blockid(
                          segment,
                          (CONST unsigned char **)&blockid
                          );
  Cstager_Segment_diskPath(
                           segment,
                           (CONST char **)&diskPath
                           );
  if ( blockid == NULL || diskPath == NULL ) {
    serrno = SEINTERNAL;
    return(-1);
  }
    
  CLIST_ITERATE_BEGIN(tape->file,fl) 
    {
      if ( (fseq > 0) ||
           (memcmp(blockid,nullblkid,4) != 0) ||
           ((*diskPath != '\0') &&
            (strcmp(diskPath,".") != 0)) ) {
        /*
         * Either the tape position MUST correspond ...
         */
        if ( ((fseq > 0) && 
              (fseq == fl->filereq.tape_fseq)) ||
             ((memcmp(blockid,nullblkid,4) != 0) &&
              (memcmp(blockid,fl->filereq.blockid,4) == 0)) ) {
          found = 1;
          break;
        } else if ( (fseq <= 0) &&
                    (memcmp(blockid,nullblkid,4) == 0) &&
                    (strcmp(diskPath,fl->filereq.file_path) == 0) ) {
          /*
           * ... or, the tape position is not known and the disk file paths match
           */
          found = 1;
          break;
        }
      }
    }
  CLIST_ITERATE_END(tape->file,fl);
  if ( (found == 1) && (file != NULL) ) *file = fl;
  return(found);
}

int findFileFromSegment(
                        segment,
                        tape,
                        file
                        )
     struct Cstager_Segment_t *segment;
     tape_list_t *tape;
     file_list_t **file;
{
  return(rtcpcld_findFileFromSegment(segment,tape,NULL,file));
}

/**
 * Verify that currentTape really match the requested tape entry. If * the tape doesn't match it means that the rtcpclientd or somebody
 * else has started the VidWorker with an incorrect database key specified
 * using the -k option.
 */
static int verifyTape(
                      tape
                      )
     tape_list_t *tape;
{
  char *vid;
  int rc, mode, side;
  
  if ( currentTape == NULL ) {
    rc = updateTapeFromDB(NULL);
    if ( rc == -1 ) return(-1);
  }

  /*
   * Cross check that we really got hold of the correct request
   */
  Cstager_Tape_vid(currentTape,(CONST char **)&vid);
  Cstager_Tape_tpmode(currentTape,&mode);
  Cstager_Tape_side(currentTape,&side);
  if ( (tape->tapereq.mode != mode) ||
       (tape->tapereq.side != side) ||
       (strcmp(tape->tapereq.vid,vid) != 0) ) {
    /*
     * This is really quite serious...
     */
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ALERT,
                      RTCPCLD_MSG_WRONG_TAPE,
                      (struct Cns_fileid *)NULL,
                      2,
                      "VID",
                      DLF_MSG_PARAM_TPVID,
                      tape->tapereq.vid,
                      "DBENTRY",
                      DLF_MSG_PARAM_INT64,
                      (u_signed64)rtcpcld_getTapeKey()
                      );
    }
    serrno = SEINTERNAL;
    return(-1);
  }
  return(0);
}

/**
 * Find the database key associated with this tape. Needed by the rtcpclientd
 * for correctly specifying the -k option when starting the VidWorker.
 */
int rtcpcld_findTapeKey(
                        tape,
                        key
                        )
     tape_list_t *tape;
     ID_TYPE *key;
{
  int rc;
  struct Cstager_Tape_t *tp = NULL;
  
  if ( tape == NULL || key == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = findTape(&(tape->tapereq),&tp,NULL);
  if ( rc != 1 || tp == NULL ) {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_INTERNAL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "REASON",
                      DLF_MSG_PARAM_STR,
                      "Tape request could not be found in internal list",
                      RTCPCLD_LOG_WHERE
                      );
    }
    serrno = SEINTERNAL;
    return(-1);
  }

  Cstager_Tape_id(tp,key);
  return(0);
}

/**
 * Externalised version of getDbSvc(). Used in rtcpcldapi.c
 */
int rtcpcld_getDbSvc(
                     svcs
                     )
     struct C_Services_t ***svcs;
{
  return(getDbSvc(svcs));
}

/**
 * Externalised version of getStgDbSvc(). Used in rtcpcldapi.c
 */
int rtcpcld_getStgDbSvc(
                        stgSvc
                        )
     struct Cstager_IStagerSvc_t **stgSvc;
{
  return(getStgSvc(stgSvc));
}

/**
 * Externalised version of delTape(). Called by rtcpclientd to remove
 * a tape after it has been completely processed (VidWorker finished)
 */
int rtcpcld_delTape(
                    tape
                    )
     tape_list_t **tape;
{
  return(delTape(tape));
}

/**
 * This method is only called from methods used by the rtcpclientd parent.
 * Called by rtcpclientd to find out work to do. The internal active tapes
 * list is updated with new entries if any. It is the caller's responsability
 * to free the returned tapeArray and all its tape_list_t members.
 */
int rtcpcld_getVIDsToDo(
                        tapeArray, 
                        cnt
                        )
     tape_list_t ***tapeArray;
     int *cnt;
{
  struct Cstager_Tape_t **tpArray = NULL;
  struct Cstager_IStagerSvc_t *stgsvc = NULL;
  rtcpTapeRequest_t tapereq;
#if defined(VMGR)
  char vmgr_error_buffer[512];
  struct vmgr_tape_info vmgr_tapeinfo;
#endif /* VMGR */
  tape_list_t *tape;
  char *vid;
  int i, rc, nbItems = 0, save_serrno, mode;
  int side = 0;

  if ( tapeArray == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = getStgSvc(&stgsvc);
  if ( rc == -1 || stgsvc == NULL ) return(-1);
  rc = Cstager_IStagerSvc_tapesToDo(
                                    stgsvc,
                                    &tpArray,
                                    &nbItems
                                    );
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+3,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "Cstager_IStagerSvc_tapesTodo()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(Cstager_IStagerSvc_errorMsg(stgsvc))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
    }
    serrno = save_serrno;
    return(-1);
  }

  if ( nbItems <= 0 || tpArray == NULL ) return(0);
  *tapeArray = (tape_list_t **)calloc(nbItems,sizeof(tape_list_t *));
  if ( *tapeArray != NULL ) {
#if defined(VMGR)
    vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer));
#endif /* VMGR */
    i=0;
    for (i=0; i<nbItems; i++) {
      tape = NULL;
      Cstager_Tape_vid(tpArray[i],(CONST char **)&vid);
      if ( vid == NULL ) continue;
      Cstager_Tape_tpmode(tpArray[i],&mode);
      tapereq.mode = mode;
      tapereq.side = 0;
      strncpy(tapereq.vid,vid,sizeof(tapereq.vid)-1);
      rc = findTape(&tapereq,NULL,NULL);
      if ( rc == 1 ) continue;
      rc = rtcp_NewTapeList(&tape,NULL,mode);
      if ( rc == -1 ) return(-1);
      (*tapeArray)[i] = tape;
      strcpy(tape->tapereq.vid,vid);
#if defined(VMGR)
      memset(&vmgr_tapeinfo,'\0',sizeof(vmgr_tapeinfo));
      rc = vmgr_querytape (
                           tape->tapereq.vid,
                           side,
                           &vmgr_tapeinfo,
                           tape->tapereq.dgn
                           );
      if ( rc == -1 ) {
        if ( dontLog == 0 ) {
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          DLF_LVL_ERROR,
                          RTCPCLD_MSG_SYSCALL,
                          (struct Cns_fileid *)NULL,
                          RTCPCLD_NB_PARAMS+5,
                          "SYSCALL",
                          DLF_MSG_PARAM_STR,
                          "vmgr_querytape()",
                          "ERROR_STR",
                          "",
                          DLF_MSG_PARAM_TPVID,
                          tape->tapereq.vid,
                          "TPSIDE",
                          DLF_MSG_PARAM_INT,
                          tape->tapereq.side,
                          "TPMODE",
                          DLF_MSG_PARAM_INT,
                          tape->tapereq.mode,
                          DLF_MSG_PARAM_STR,
                          sstrerror(serrno),
                          RTCPCLD_LOG_WHERE
                          );
        }
      }
      
      if ( ((vmgr_tapeinfo.status & DISABLED) == DISABLED) ||
           ((vmgr_tapeinfo.status & EXPORTED) == EXPORTED) ||
           ((vmgr_tapeinfo.status & ARCHIVED) == ARCHIVED) ) {
        errno = EACCES;
        return(-1);
      }
      if ( rc != -1 ) {
        strcpy(tape->tapereq.density, vmgr_tapeinfo.density);
        strcpy(tape->tapereq.label, vmgr_tapeinfo.lbltype);
        tape->tapereq.side = side;
      }
#endif /* VMGR */
      rc = addTape(tape,tpArray[i]);
      if ( rc == -1 ) return(-1);
    };
    rc = 0;
    if ( cnt != NULL ) *cnt = nbItems;
  } else {
    serrno = errno;
    rc = -1;
  }
  return(0);
}

static int validPosition(
                         segment
                         )
     struct Cstager_Segment_t *segment;
{                         
  int fseq;
  unsigned char *blockid;

  Cstager_Segment_fseq(segment,&fseq);
  Cstager_Segment_blockid(segment,(CONST unsigned char **)&blockid);

  if ( (fseq <= 0) &&
       (blockid == NULL || memcmp(blockid,nullblkid,sizeof(nullblkid)) == 0) ) {
    return(0);
  } else {
    return(1);
  }
}

static int validPath(
                     segment
                     )
     struct Cstager_Segment_t *segment;
{
  char *path;

  Cstager_Segment_diskPath(segment,(CONST char **)&path);
  if ( (path == NULL) || (*path == '\0') || (strcmp(path,".") == 0) ) {
    return(0);
  } else {
    return(1);
  }
}

static int validSegment(
                        segment
                        )
     struct Cstager_Segment_t *segment;
{
  if ( segment == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  if ( (validPosition(segment) == 1) &&
       (validPath(segment) == 1) ) {
    return(1);
  } else {
    return(0);
  }
}

/*
 * Externalised version for rtcpcldapi.c
 */
int rtcpcld_validSegment(
                         segment
                         )
     struct Cstager_Segment_t *segment;
{
  return(validSegment(segment));
}

static int compareSegments(
                           arg1,
                           arg2
                           )
     CONST void *arg1, *arg2;
{
  struct Cstager_Segment_t **segm1, **segm2;
  int fseq1, fseq2, rc;
  
  segm1 = (struct Cstager_Segment_t **)arg1;
  segm2 = (struct Cstager_Segment_t **)arg2;
  if ( segm1 == NULL || segm2 == NULL ||
       *segm1 == NULL || *segm2 == NULL ) return(0);
  Cstager_Segment_fseq(*segm1,&fseq1);
  Cstager_Segment_fseq(*segm2,&fseq2);
  rc = 0;
  if ( fseq1 < fseq2 ) rc = -1;
  if ( fseq1 > fseq2 ) rc = 1;

  return(rc);
}

/**
 * This method is only called from methods used by the VidWorker childs.
 * Query database for segments to be processed for a tape. The tape
 * request is updated with new segments (duplicates are checked before
 * adding a new filereq). It is the caller's responsability to free
 * the new file_list_t members.
 */
static int procReqsForVID(
                          tape
                          )
     tape_list_t *tape;
{
  struct Cstager_Tape_t *dummyTp = NULL;
  struct Cstager_IStagerSvc_t *stgsvc = NULL;
  struct C_Services_t **svcs = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  struct Cstager_Segment_t **segmArray = NULL;
  enum Cstager_SegmentStatusCodes_t cmpStatus;
  RtcpcldSegmentList_t *segmIterator = NULL;
  file_list_t *fl = NULL;
  tape_list_t *tl = NULL;
  char *diskPath, *nsHost, *fid;
  unsigned char *blockid;
  struct Cns_fileid fileid;
  int rc, i, nbItems = 0, save_serrno, fseq, updated = 0, segmUpdated;
  int newFileReqs = 0, incompleteSegments = 0, prevFseq = -1;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = getStgSvc(&stgsvc);
  if ( rc == -1 || stgsvc == NULL ) return(-1);

  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);
  rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
  if ( rc == -1 ) {
    return(-1);
  }
  iAddr = C_BaseAddress_getIAddress(baseAddr);

  LOCKTP;
  rc = verifyTape(tape);
  if ( rc == -1 ) {
    C_IAddress_delete(iAddr);
    UNLOCKTP;
    return(-1);
  }
  
  rc = Cstager_IStagerSvc_segmentsForTape(
                                          stgsvc,
                                          currentTape,
                                          &segmArray,
                                          &nbItems
                                          );
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+3,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "Cstager_IStagerSvc_segmentsForTape()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(Cstager_IStagerSvc_errorMsg(stgsvc))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);      
    }
    C_IAddress_delete(iAddr);
    UNLOCKTP;
    serrno = save_serrno;
    return(-1);
  }

  if ( nbItems == 0 ) {
    C_IAddress_delete(iAddr);
    UNLOCKTP;
    serrno = ENOENT;
    return(-1);
  }

  /*
   * BEGIN Hack until client can use a createRepNoRec() to add segments
   */
/*   rc = Cstager_IStagerSvc_selectTape( */
/*                                      stgsvc, */
/*                                      &dummyTp, */
/*                                      tape->tapereq.vid, */
/*                                      tape->tapereq.side, */
/*                                      tape->tapereq.mode */
/*                                      ); */
/*   if ( rc == -1 ) { */
/*     save_serrno = serrno; */
/*     if ( dontLog == 0 ) { */
/*       char *_dbErr = NULL; */
/*       (void)dlf_write( */
/*                       (inChild == 0 ? mainUuid : childUuid), */
/*                       DLF_LVL_ERROR, */
/*                       RTCPCLD_MSG_DBSVC, */
/*                       (struct Cns_fileid *)NULL, */
/*                       RTCPCLD_NB_PARAMS+3, */
/*                       "DBSVCCALL", */
/*                       DLF_MSG_PARAM_STR, */
/*                       "Cstager_IStagerSvc_selectTape()", */
/*                       "ERROR_STR", */
/*                       DLF_MSG_PARAM_STR, */
/*                       sstrerror(serrno), */
/*                       "DB_ERROR", */
/*                       DLF_MSG_PARAM_STR, */
/*                       (_dbErr = rtcpcld_fixStr(Cstager_IStagerSvc_errorMsg(stgsvc))), */
/*                       RTCPCLD_LOG_WHERE */
/*                       ); */
/*       if ( _dbErr != NULL ) free(_dbErr);       */
/*     } */
/*     C_IAddress_delete(iAddr); */
/*     UNLOCKTP; */
/*     serrno = save_serrno; */
/*     return(-1); */
/*   } */
/*   Cstager_Tape_delete(dummyTp); */

  /*
   * END Hack until client can use a createRepNoRec() to add segments
   */

  qsort(
        (void *)segmArray,
        (size_t)nbItems,
        sizeof(struct Cstager_Segment_t *),
        compareSegments
        );

  tl = tape;
  for ( i=0; i<nbItems; i++ ) {
    segmUpdated = 0;
    Cstager_Segment_status(segmArray[i],&cmpStatus);
    if ( (cmpStatus == SEGMENT_UNPROCESSED) ||
         (cmpStatus == SEGMENT_WAITFSEQ) ||
         (cmpStatus == SEGMENT_WAITPATH) ||
         (cmpStatus == SEGMENT_WAITCOPY) ) {
      if ( validSegment(segmArray[i]) == 1 ) {
        Cstager_Segment_blockid(
                                segmArray[i],
                                (CONST unsigned char **)&blockid
                                );
        Cstager_Segment_fseq(
                             segmArray[i],
                             &fseq
                             );
        if ( (tape->tapereq.mode == WRITE_ENABLE) &&
             (prevFseq > 0) &&
             (fseq != prevFseq+1) ) {
          if ( dontLog == 0 ) {
            (void)dlf_write(
                            (inChild == 0 ? mainUuid : childUuid),
                            DLF_LVL_SYSTEM,
                            RTCPCLD_MSG_OUTOFSEQ,
                            (struct Cns_fileid *)NULL,
                            2,
                            "FSEQ",
                            DLF_MSG_PARAM_INT,
                            fseq,
                            "PREV_FSEQ",
                            DLF_MSG_PARAM_INT,
                            prevFseq
                            );
          }
          break;
        }
        prevFseq = fseq;
        
        Cstager_Segment_diskPath(
                                 segmArray[i],
                                 (CONST char **)&diskPath
                                 );
        Cstager_Segment_castorFileId(
                                     segmArray[i],
                                     &(fileid.fileid)
                                     );
        nsHost = NULL;
        Cstager_Segment_castorNsHost(
                                     segmArray[i],
                                     (CONST char **)&nsHost
                                     );
        if ( nsHost != NULL ) strncpy(
                                      fileid.server,
                                      nsHost,
                                      sizeof(fileid.server)-1
                                      );
        rc = findFileFromSegment(segmArray[i],tl,&fl);
        if ( rc != 1 || fl == NULL ) {
          rc = rtcp_NewFileList(&tl,&fl,tl->tapereq.mode);
          if ( rc == -1 ) {
            save_serrno = serrno;
            free(segmArray);
            if ( dontLog == 0 ) {
              (void)dlf_write(
                              (inChild == 0 ? mainUuid : childUuid),
                              DLF_LVL_ERROR,
                              RTCPCLD_MSG_SYSCALL,
                              (struct Cns_fileid *)&fileid,
                              RTCPCLD_NB_PARAMS+2,
                              "SYSCALL",
                              DLF_MSG_PARAM_STR,
                              "rtcp_NewFileList()",
                              "ERROR_STR",
                              DLF_MSG_PARAM_STR,
                              sstrerror(save_serrno),
                              RTCPCLD_LOG_WHERE
                              );
            }
            rc = C_Services_rollback(*svcs,iAddr);
            C_IAddress_delete(iAddr);
            UNLOCKTP;
            serrno = save_serrno;
            return(-1);
          }
          if ( dontLog == 0 ) {
            (void)dlf_write(
                            childUuid,
                            DLF_LVL_SYSTEM,
                            RTCPCLD_MSG_FILEREQ,
                            (struct Cns_fileid *)&fileid,
                            2,
                            "FSEQ",
                            DLF_MSG_PARAM_INT,
                            fseq,
                            "PATH",
                            DLF_MSG_PARAM_STR,
                            diskPath
                            );
          }
          newFileReqs = 1;
          fl->filereq.concat = NOCONCAT;
          fl->filereq.convert = ASCCONV;
          strcpy(fl->filereq.recfm,"F");
          fl->filereq.tape_fseq = fseq;
          fl->filereq.def_alloc = 0;
          fl->filereq.disk_fseq = ++(fl->prev->filereq.disk_fseq);
          fl->filereq.castorSegAttr.castorFileId = fileid.fileid;
          strcpy(fl->filereq.castorSegAttr.nameServerHostName,fileid.server);
        } else {
          fl = segmIterator->file;
        }
        memcpy(fl->filereq.blockid,blockid,sizeof(fl->filereq.blockid));
        if ( memcmp(fl->filereq.blockid,nullblkid,sizeof(nullblkid)) == 0 ) 
          fl->filereq.position_method = TPPOSIT_FSEQ;
        else fl->filereq.position_method = TPPOSIT_BLKID;
        fl->filereq.proc_status = RTCP_WAITING;
        /*
         * Temporary hack until rtcpd_MainCntl.c fix has been deployed
         */
        fl->filereq.blocksize = 32760;

        strcpy(fl->filereq.file_path,".");
        if ( (diskPath != NULL) && (*diskPath != '\0') &&
             (strcmp(diskPath,".") != 0) ) {
          strncpy(fl->filereq.file_path,
                  diskPath,
                  sizeof(fl->filereq.file_path)-1);
        }
        fid = NULL;
        Cstager_Segment_fid(
                            segmArray[i],
                            (CONST char **)&fid
                            );
        if ( fid != NULL ) {
          strncpy(fl->filereq.fid,fid,sizeof(fl->filereq.fid)-1);
        }

        Cstager_Segment_bytes_in(
                                 segmArray[i],
                                 &(fl->filereq.bytes_in)
                                 );
        Cstager_Segment_bytes_out(
                                  segmArray[i],
                                  &(fl->filereq.bytes_out)
                                  );
        Cstager_Segment_offset(
                               segmArray[i],
                               &(fl->filereq.offset)
                               );
        Cstager_Segment_stgReqId(
                                 segmArray[i],
                                 &(fl->filereq.stgReqId)
                                 );
        if ( cmpStatus != SEGMENT_COPYRUNNING ) {
          updated = 1;
          segmUpdated = 1;
          Cstager_Segment_setStatus(segmArray[i],SEGMENT_COPYRUNNING);
        }
      } else if ( validPosition(segmArray[i]) == 1 ) {
        incompleteSegments = 1;
        if ( cmpStatus != SEGMENT_WAITPATH ) {
          updated = 1;
          segmUpdated = 1;
          Cstager_Segment_setStatus(segmArray[i],SEGMENT_WAITPATH);
        }
      } else {
        incompleteSegments = 1;
        if ( cmpStatus != SEGMENT_WAITFSEQ ) {
          updated = 1;
          segmUpdated = 1;
          Cstager_Segment_setStatus(segmArray[i],SEGMENT_WAITFSEQ);
        }
      }

      if ( segmUpdated == 1 ) {
        ID_TYPE _key = 0;
        Cstager_Segment_id(segmArray[i],&_key);
        iObj = Cstager_Segment_getIObject(segmArray[i]);
        rc = C_Services_updateRepNoRec(*svcs,iAddr,iObj,0);
        if ( rc == -1 ) {
          save_serrno = serrno;
          if ( dontLog == 0 ) {
            (void)dlf_write(
                            (inChild == 0 ? mainUuid : childUuid),
                            DLF_LVL_ERROR,
                            RTCPCLD_MSG_DBSVC,
                            (struct Cns_fileid *)NULL,
                            RTCPCLD_NB_PARAMS+4,
                            "DBSVCCALL",
                            DLF_MSG_PARAM_STR,
                            "C_Services_updateRepNoRec()",
                            "DBKEY",
                            DLF_MSG_PARAM_INT,
                            (int)_key,
                            "ERROR_STR",
                            DLF_MSG_PARAM_STR,
                            sstrerror(serrno),
                            "DB_ERROR",
                            DLF_MSG_PARAM_STR,
                            C_Services_errorMsg(*svcs),
                            RTCPCLD_LOG_WHERE
                            );
          }
          rc = C_Services_rollback(*svcs,iAddr);
          C_IAddress_delete(iAddr);
          if ( segmArray != NULL ) free(segmArray);
          UNLOCKTP;
          serrno = save_serrno;
          return(-1);
        }
      }

    }
  }

  /*
   * BEGIN Hack: commit all new segment updates
   */
  rc = C_Services_commit(*svcs,iAddr);
  /*
   * END Hack: commit all new segment updates
   */
  
/*   if ( updated == 1 ) { */
/*     rc = C_Services_commit(*svcs,iAddr); */
/*     if ( rc == -1 ) { */
/*       save_serrno = serrno; */
/*       C_IAddress_delete(iAddr); */
/*       if ( dontLog == 0 ) { */
/*         char *_dbErr = NULL; */
/*         (void)dlf_write( */
/*                         (inChild == 0 ? mainUuid : childUuid), */
/*                         DLF_LVL_ERROR, */
/*                         RTCPCLD_MSG_DBSVC, */
/*                         (struct Cns_fileid *)NULL, */
/*                         RTCPCLD_NB_PARAMS+3, */
/*                         "DBSVCCALL", */
/*                         DLF_MSG_PARAM_STR, */
/*                         "C_Services_commit()", */
/*                         "ERROR_STR", */
/*                         DLF_MSG_PARAM_STR, */
/*                         sstrerror(serrno), */
/*                         "DB_ERROR", */
/*                         DLF_MSG_PARAM_STR, */
/*                         (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))), */
/*                         RTCPCLD_LOG_WHERE */
/*                         ); */
/*         if ( _dbErr != NULL ) free(_dbErr); */
/*       } */
/*       if ( segmArray != NULL ) free(segmArray); */
/*       C_Services_rollback(*svcs,iAddr); */
/*       serrno = save_serrno; */
/*       return(-1); */
/*     } */
/*   } else C_Services_rollback(*svcs,iAddr); */

  C_IAddress_delete(iAddr);
  UNLOCKTP;
  
  if ( segmArray != NULL ) free(segmArray);
  if ( (updated == 1) || (incompleteSegments == 1) ) (void)notifyTape(currentTape);
  if ( newFileReqs == 0 ) {
    serrno = EAGAIN;
    return(-1);
  }
  return(0);
}

/**
 * Externalised entry to procReqsForVID()
 */
int rtcpcld_getReqsForVID(
                          tape
                          )
     tape_list_t *tape;
{
  return(procReqsForVID(tape));
}

/**
 * This method is only called from methods used by the VidWorker childs.
 * Query database to check if there are any segments to process for this
 * tape. This method is called by the VidWorker at startup to avoid unnecessary
 * tape mounts.
 */
int rtcpcld_anyReqsForVID(
                          tape
                          )
     tape_list_t *tape;
{
  struct Cstager_IStagerSvc_t *stgsvc = NULL;
  int rc, nbItems, save_serrno;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = getStgSvc(&stgsvc);
  if ( rc == -1 || stgsvc == NULL ) return(-1);

  LOCKTP;
  rc = verifyTape(tape);
  if ( rc == -1 ) {
    UNLOCKTP;
    return(-1);
  }
  
  rc = Cstager_IStagerSvc_anySegmentsForTape(stgsvc,currentTape);
  UNLOCKTP;
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+3,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "Cstager_anySegmentsForTape()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(Cstager_IStagerSvc_errorMsg(stgsvc))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
    }
    serrno = save_serrno;
    return(-1);
  }
  nbItems = rc;

  return(nbItems);
}

/**
 * This method is called from methods used by both the VidWorker childs
 * and the rtcpclientd parent. Update the status of a tape (not its
 * segments). Mostly used to mark FAILED status since most other tape
 * statuses are atomically updated by Cstager_IStagerSvc methods.
 */
int rtcpcld_updateVIDStatus(
                            tape, 
                            fromStatus,
                            toStatus
                            )
     tape_list_t *tape;
     enum Cstager_TapeStatusCodes_t fromStatus;
     enum Cstager_TapeStatusCodes_t toStatus;
{
  struct C_Services_t **svcs = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  struct Cstager_Tape_t *tapeItem = NULL;
  enum Cstager_TapeStatusCodes_t cmpStatus;  
  struct Cstager_IStagerSvc_t *stgsvc = NULL;
  int rc = 0, save_serrno;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);

  LOCKTP;
  rc = findTape(&(tape->tapereq),&tapeItem,NULL);
  if ( tapeItem == NULL ) {
    (void)updateTapeFromDB(NULL);
    rc = findTape(&(tape->tapereq),&tapeItem,NULL);
  } else if ( rc == 1 ) {
    rc = updateTapeFromDB(tapeItem);
    if ( rc == -1 ) {
      UNLOCKTP;
      return(-1);
    } else rc = 1;
  }

  if ( rc != 1 || tapeItem == NULL ) {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_INTERNAL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "REASON",
                      DLF_MSG_PARAM_STR,
                      "Tape request could not be found in internal list",
                      RTCPCLD_LOG_WHERE
                      );
    }
    UNLOCKTP;
    serrno = SEINTERNAL;
    return(-1);
  }
  
  Cstager_Tape_status(tapeItem,&cmpStatus);
  if ( fromStatus == cmpStatus ) {
    ID_TYPE _key = 0;
    rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
    if ( rc == -1 ) {
      UNLOCKTP;
      return(-1);
    }

    rc = getStgSvc(&stgsvc);
    if ( rc == -1 || stgsvc == NULL ) {
      UNLOCKTP;
      return(-1);
    }

    iAddr = C_BaseAddress_getIAddress(baseAddr);
    iObj = Cstager_Tape_getIObject(tapeItem);
    Cstager_Tape_id(tapeItem,&_key);
    Cstager_Tape_setStatus(tapeItem,toStatus);
    /*
     * Set error info, if TAPE_FAILED status is requested
     */
    if ( toStatus == TAPE_FAILED ) {
      if ( tape->tapereq.err.errorcode > 0 )
        Cstager_Tape_setErrorCode(tapeItem,tape->tapereq.err.errorcode);
      if ( *tape->tapereq.err.errmsgtxt != '\0' )
        Cstager_Tape_setErrMsgTxt(tapeItem,tape->tapereq.err.errmsgtxt);
      if ( tape->tapereq.err.severity != RTCP_OK )
        Cstager_Tape_setSeverity(tapeItem,tape->tapereq.err.severity);
    }
    
    if ( toStatus == TAPE_FAILED || toStatus == TAPE_FINISHED ) {
      char *vwAddress = "";
      Cstager_Tape_setVwAddress(tapeItem,vwAddress);
    }
    
    rc = C_Services_updateRepNoRec(*svcs,iAddr,iObj,1);
    save_serrno = serrno;
    if ( rc == -1 ) {
      save_serrno = serrno;
      C_IAddress_delete(iAddr);
      if ( dontLog == 0 ) {
        char *_dbErr = NULL;
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_DBSVC,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+4,
                        "DBSVCCALL",
                        DLF_MSG_PARAM_STR,
                        "DBKEY",
                        DLF_MSG_PARAM_INT,
                        (int)_key,
                        "C_Services_updateRepNoRec()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        "DB_ERROR",
                        DLF_MSG_PARAM_STR,
                        (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))),
                        RTCPCLD_LOG_WHERE
                        );
        if ( _dbErr != NULL ) free(_dbErr);
      }
      UNLOCKTP;
      serrno = save_serrno;
      return(-1);
    }
    C_IAddress_delete(iAddr);
    (void)notifyTape(tapeItem);
  }
  UNLOCKTP;
  return(0);
}

/**
 * This method is called from methods used by the VidWorker childs
 * Update the vwAddress (VidWorker address) for receiving RTCOPY kill
 * requests. 
 */
int rtcpcld_setVidWorkerAddress(
                                tape,
                                port
                                )
     tape_list_t *tape;
     int port;
{
  struct C_Services_t **svcs = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  struct Cstager_Tape_t *tapeItem = NULL;
  char myHost[CA_MAXHOSTNAMELEN+1], vwAddress[CA_MAXHOSTNAMELEN+12];
  int rc = 0, save_serrno;
  ID_TYPE _key = 0;

  if ( tape == NULL || port < 0 ) {
    serrno = EINVAL;
    return(-1);
  }
  (void)gethostname(myHost,sizeof(myHost)-1);
  sprintf(vwAddress,"%s:%d",myHost,port);
  
  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);

  LOCKTP;
  rc = findTape(&(tape->tapereq),&tapeItem,NULL);
  if ( tapeItem == NULL ) {
    (void)updateTapeFromDB(NULL);
    rc = findTape(&(tape->tapereq),&tapeItem,NULL);
  }

  if ( rc != 1 || tapeItem == NULL ) {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_INTERNAL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "REASON",
                      DLF_MSG_PARAM_STR,
                      "Tape request could not be found in internal list",
                      RTCPCLD_LOG_WHERE
                      );
    }
    UNLOCKTP;
    serrno = SEINTERNAL;
    return(-1);
  }
  
  rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
  if ( rc == -1 ) {
    UNLOCKTP;
    return(-1);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_Tape_getIObject(tapeItem);
  Cstager_Tape_id(tapeItem,&_key);
  rc = C_Services_updateObj(*svcs,iAddr,iObj);
  if ( rc == -1 ) {
    save_serrno = serrno;
    C_IAddress_delete(iAddr);
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+4,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "C_Services_updateObj()",
                      "DBKEY",
                      DLF_MSG_PARAM_INT,
                      (int)_key,
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
    }
    UNLOCKTP;
    serrno = save_serrno;
    return(-1);
  }
  Cstager_Tape_setVwAddress(tapeItem,vwAddress);
  Cstager_Tape_id(tapeItem,&_key);
  rc = C_Services_updateRepNoRec(*svcs,iAddr,iObj,1);
  if ( rc == -1 ) {
    save_serrno = serrno;
    C_IAddress_delete(iAddr);
    if ( dontLog == 0 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+4,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "C_Services_updateRepNoRec()",
                      "DBKEY",
                      DLF_MSG_PARAM_INT,
                      (int)_key,
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
    }
    UNLOCKTP;
    serrno = save_serrno;
    return(-1);
  }
  UNLOCKTP;
  C_IAddress_delete(iAddr);
  (void)notifyTape(currentTape);
  return(0);
}

/**
 * This method is called from methods used by both the VidWorker childs
 * and the rtcpclientd parent. Update the status of all segments with the
 * current status == fromStatus. Mostly used to mark unfinished segments
 * with SEGMENT_FAILED status after a partial request failure.
 */
int rtcpcld_updateVIDFileStatus(
                                tape, 
                                fromStatus, 
                                toStatus
                                )
     tape_list_t *tape;
     enum Cstager_SegmentStatusCodes_t fromStatus;
     enum Cstager_SegmentStatusCodes_t toStatus;
{
  struct Cstager_IStagerSvc_t *stgsvc = NULL;
  struct C_Services_t **svcs = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  struct Cstager_Tape_t *tapeItem = NULL;
  struct Cstager_Segment_t **segments, *segmItem;
  struct Cns_fileid fileid;
  char *nsHost;
  Cuuid_t stgUuid;
  file_list_t *fl;
  rtcpFileRequest_t *filereq;
  enum Cstager_SegmentStatusCodes_t cmpStatus;  
  int rc = 0, updated = 0, i, save_serrno, nbItems;
  ID_TYPE _key = 0;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);

  LOCKTP;
  rc = findTape(&(tape->tapereq),&tapeItem,NULL);
  if ( tapeItem == NULL ) {
    (void)updateTapeFromDB(NULL);
    rc = findTape(&(tape->tapereq),&tapeItem,NULL);
  }
  if ( rc != 1 || tapeItem == NULL ) {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_INTERNAL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "REASON",
                      DLF_MSG_PARAM_STR,
                      "Tape request could not be found in internal list",
                      RTCPCLD_LOG_WHERE
                      );
    }
    UNLOCKTP;
    serrno = SEINTERNAL;
    return(-1);
  }

  rc = updateTapeFromDB(tapeItem);
  if ( rc == -1 ) {
    UNLOCKTP;
    return(-1);
  }
  
  rc = getStgSvc(&stgsvc);
  if ( rc == -1 || stgsvc == NULL ) {
    UNLOCKTP;
    return(-1);  
  }
  
  rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
  if ( rc == -1 ) {
    UNLOCKTP;
    return(-1);
  }
  iAddr = C_BaseAddress_getIAddress(baseAddr);

  segments = NULL;
  Cstager_Tape_segments(tapeItem,&segments,&nbItems);
  for ( i=0; i<nbItems; i++ ) {
    segmItem = segments[i];
    Cstager_Segment_status(
                           segmItem,
                           &cmpStatus
                           );
    Cstager_Segment_castorNsHost(
                                 segmItem,
                                 (CONST char **)&nsHost
                                 );
    memset(&fileid,'\0',sizeof(fileid));
    if ( nsHost != NULL ) strncpy(fileid.server,nsHost,sizeof(fileid.server)-1);
    Cstager_Segment_castorFileId(
                                 segmItem,
                                 &(fileid.fileid)
                                 );
    Cstager_Segment_stgReqId(
                             segmItem,
                             &stgUuid
                             );
    if ( cmpStatus == fromStatus ) {
      Cstager_Segment_setStatus(segmItem,toStatus);
      rc = findFileFromSegment(segmItem,tape,&fl);
      if ( (rc != 1) || (fl == NULL) ) {
        if ( dontLog == 0 ) {
          if ( rc == -1 ) {
            (void)dlf_write(
                            (inChild == 0 ? mainUuid : childUuid),
                            DLF_LVL_ERROR,
                            RTCPCLD_MSG_SYSCALL,
                            (struct Cns_fileid *)&fileid,
                            RTCPCLD_NB_PARAMS+3,
                            "SYSCALL",
                            DLF_MSG_PARAM_STR,
                            "findFileFromSegment",
                            "ERROR_STR",
                            DLF_MSG_PARAM_STR,
                            sstrerror(serrno),
                            "",
                            DLF_MSG_PARAM_UUID,
                            stgUuid,
                            RTCPCLD_LOG_WHERE
                            );
          } else {
            (void)dlf_write(
                            (inChild == 0 ? mainUuid : childUuid),
                            DLF_LVL_ERROR,
                            RTCPCLD_MSG_UNEXPECTED_NEWSEGM,
                            (struct Cns_fileid *)&fileid,
                            RTCPCLD_NB_PARAMS+1,
                            "",
                            DLF_MSG_PARAM_UUID,
                            stgUuid,
                            RTCPCLD_LOG_WHERE
                            );
          }
        }
      } else {
        filereq = &(fl->filereq);
        if ( toStatus == SEGMENT_FILECOPIED ) {
          Cstager_Segment_setBlockid(segmItem,
                                     filereq->blockid);
          Cstager_Segment_setFid(segmItem,
                                 filereq->fid);
          Cstager_Segment_setBytes_in(segmItem,
                                      filereq->bytes_in);
          Cstager_Segment_setBytes_out(segmItem,
                                       filereq->bytes_out);
          Cstager_Segment_setHost_bytes(segmItem,
                                        filereq->host_bytes);
          Cstager_Segment_setSegmCksumAlgorithm(segmItem,
                                                filereq->castorSegAttr.segmCksumAlgorithm);
          Cstager_Segment_setSegmCksum(segmItem,
                                       filereq->castorSegAttr.segmCksum);
        }
        if ( toStatus == SEGMENT_FAILED ) {
          if (filereq->err.errorcode <= 0)
            filereq->err.errorcode = SEINTERNAL;
          Cstager_Segment_setErrorCode(segmItem,
                                       filereq->err.errorcode);

          if (filereq->err.severity == RTCP_OK)
            filereq->err.severity = RTCP_FAILED|RTCP_UNERR;
          Cstager_Segment_setSeverity(segmItem,
                                      filereq->err.severity);

          if (*filereq->err.errmsgtxt == '\0')
            strncpy(filereq->err.errmsgtxt,
                    sstrerror(fl->filereq.err.errorcode),
                    sizeof(fl->filereq.err.errmsgtxt)-1);
          Cstager_Segment_setErrMsgTxt(segmItem,
                                       filereq->err.errmsgtxt);
        }
      }
      iObj = Cstager_Segment_getIObject(segmItem);
      Cstager_Segment_id(segmItem,&_key);
      rc = C_Services_updateRepNoRec(*svcs,iAddr,iObj,1);
      if ( rc == -1 ) {
        save_serrno = serrno;
        C_IAddress_delete(iAddr);
        if ( dontLog == 0 ) {
          char *_dbErr = NULL;
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          DLF_LVL_ERROR,
                          RTCPCLD_MSG_DBSVC,
                          (struct Cns_fileid *)NULL,
                          RTCPCLD_NB_PARAMS+4,
                          "DBSVCCALL",
                          DLF_MSG_PARAM_STR,
                          "C_Services_updateRepNoRec()",
                          "DBKEY",
                          DLF_MSG_PARAM_INT,
                          (int)_key,
                          "ERROR_STR",
                          DLF_MSG_PARAM_STR,
                          sstrerror(serrno),
                          "DB_ERROR",
                          DLF_MSG_PARAM_STR,
                          (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs))),
                          RTCPCLD_LOG_WHERE
                          );
          if ( _dbErr != NULL ) free(_dbErr);
        }
      } else updated = 1;
    }
  }
  if ( segments != NULL ) free(segments);
  if ( updated == 1 ) {
    (void)notifyTape(tapeItem);
  }
  C_IAddress_delete(iAddr);
  UNLOCKTP;
  return(0);
}

/**
 * This method is only called from methods used by the VidWorker childs.
 * Update the status of a segments matching the passed filereq. If no
 * matching entry is found, the method returns -1 and sets serrno == ENOENT.
 */
int rtcpcld_setFileStatus(
                          filereq, 
                          newStatus,
                          notify
                          )
     rtcpFileRequest_t *filereq;
     enum Cstager_SegmentStatusCodes_t newStatus;
     int notify;
{
  struct C_Services_t **svcs = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  struct Cstager_Segment_t *segmItem;
  enum Cstager_SegmentStatusCodes_t currentStatus;
  struct Cstager_Tape_t *dummyTp = NULL;
  struct Cstager_IStagerSvc_t *stgsvc = NULL;
  ID_TYPE _key = 0;
  char *vid;
  int mode, side;
  struct Cns_fileid fileid;
  char *nsHost;
  Cuuid_t stgUuid;
  char *diskPath;
  unsigned char *blockid = NULL;
  int rc = 0, rcTmp, save_serrno, fseq;

  if ( filereq == NULL || 
       *filereq->file_path == '\0' || 
       *filereq->file_path == '.' ) {
    serrno = EINVAL;
    return(-1);
  }

  LOCKTP;
  rc = findSegment(filereq,&segmItem);
  if ( rc == -1 ) {
    UNLOCKTP;
    return(-1);
  }

  svcs = NULL;
  rc = getDbSvc(&svcs);

  if ( rc == 0 || segmItem == NULL ) {
    (void)updateTapeFromDB(NULL);
    rc = findSegment(filereq,&segmItem);
    if ( rc != 1 || segmItem == NULL ) {
      UNLOCKTP;
      serrno = ENOENT;
      return(-1);
    }
  } else {
    rc = updateSegmentFromDB(segmItem);
    if ( rc == -1 ) {
      UNLOCKTP;
      return(-1);
    }
  }

  Cstager_Segment_status(
                         segmItem,
                         &currentStatus
                         );
  Cstager_Segment_castorNsHost(
                               segmItem,
                               (CONST char **)&nsHost
                               );
  memset(&fileid,'\0',sizeof(fileid));
  if ( nsHost != NULL ) strncpy(fileid.server,nsHost,sizeof(fileid.server)-1);
  Cstager_Segment_castorFileId(
                               segmItem,
                               &(fileid.fileid)
                               );
  Cstager_Segment_stgReqId(
                           segmItem,
                           &stgUuid
                           );

  if ( (currentStatus == SEGMENT_FAILED) &&
       (newStatus != SEGMENT_FAILED) ) {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SEGMFAILED,
                      (struct Cns_fileid *)&fileid,
                      RTCPCLD_NB_PARAMS+1,
                      "",
                      DLF_MSG_PARAM_UUID,
                      stgUuid,
                      RTCPCLD_LOG_WHERE
                      );
    }
    UNLOCKTP;
    serrno = EPERM;
    return(-1);
  }

  diskPath = NULL;
  Cstager_Segment_diskPath(
                           segmItem,
                           (CONST char **)&diskPath
                           );
  Cstager_Segment_fseq(
                       segmItem,
                       &fseq
                       );
  Cstager_Segment_blockid(
                          segmItem,
                          (CONST unsigned char **)&blockid
                          );

  if ( ((diskPath != NULL) && 
        (strcmp(filereq->file_path,diskPath) == 0)) &&
       (((filereq->position_method == TPPOSIT_FSEQ) &&
         (filereq->tape_fseq == fseq)) ||
        ((filereq->position_method == TPPOSIT_BLKID) &&
         (memcmp(filereq->blockid,blockid,sizeof(filereq->blockid))==0))) ) {
    rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
    if ( rc == -1 ) {
      UNLOCKTP;
      return(-1);
    }
    
    iAddr = C_BaseAddress_getIAddress(baseAddr);
    /*
     * BEGIN Hack until client can use a createRepNoRec() to add segments
     */
    if ( currentTape != NULL ) {
      Cstager_Tape_vid(currentTape,(CONST char **)&vid);
      Cstager_Tape_tpmode(currentTape,&mode);
      Cstager_Tape_side(currentTape,&side);
      
      rc = getStgSvc(&stgsvc);
      if ( rc == -1 || stgsvc == NULL ) {
        UNLOCKTP;
        return(-1);
      }
/*       rc = Cstager_IStagerSvc_selectTape( */
/*                                          stgsvc, */
/*                                          &dummyTp, */
/*                                          vid, */
/*                                          side, */
/*                                          mode */
/*                                          ); */
/*       if ( rc == -1 ) { */
/*         save_serrno = serrno; */
/*         if ( dontLog == 0 ) { */
/*           char *_dbErr = NULL; */
/*           (void)dlf_write( */
/*                           (inChild == 0 ? mainUuid : childUuid), */
/*                           DLF_LVL_ERROR, */
/*                           RTCPCLD_MSG_DBSVC, */
/*                           (struct Cns_fileid *)NULL, */
/*                           RTCPCLD_NB_PARAMS+3, */
/*                           "DBSVCCALL", */
/*                           DLF_MSG_PARAM_STR, */
/*                           "Cstager_IStagerSvc_selectTape()", */
/*                           "ERROR_STR", */
/*                           DLF_MSG_PARAM_STR, */
/*                           sstrerror(serrno), */
/*                           "DB_ERROR", */
/*                           DLF_MSG_PARAM_STR, */
/*                           (_dbErr = rtcpcld_fixStr(Cstager_IStagerSvc_errorMsg(stgsvc))), */
/*                           RTCPCLD_LOG_WHERE */
/*                           ); */
/*           if ( _dbErr != NULL ) free(_dbErr);       */
/*         } */
/*         UNLOCKTP; */
/*         serrno = save_serrno; */
/*         return(-1); */
/*       } */
    }
    /*
     * END Hack until client can use a createRepNoRec() to add segments
     */
    Cstager_Segment_setStatus(
                              segmItem,
                              newStatus
                              );
    if ( newStatus == SEGMENT_FILECOPIED ) {
      Cstager_Segment_setBlockid(
                                 segmItem,
                                 filereq->blockid
                                 );
      Cstager_Segment_setFid(
                             segmItem,
                             filereq->fid
                             );
      Cstager_Segment_setBytes_in(
                                  segmItem,
                                  filereq->bytes_in
                                  );
      Cstager_Segment_setBytes_out(
                                   segmItem,
                                   filereq->bytes_out
                                   );
      Cstager_Segment_setHost_bytes(
                                    segmItem,
                                    filereq->host_bytes
                                    );
      Cstager_Segment_setSegmCksumAlgorithm(
                                            segmItem,
                                            filereq->castorSegAttr.segmCksumAlgorithm
                                            );
      Cstager_Segment_setSegmCksum(
                                   segmItem,
                                   filereq->castorSegAttr.segmCksum
                                   );
    }
    if ( newStatus == SEGMENT_FAILED ) {
      if (filereq->err.errorcode <= 0)
        filereq->err.errorcode = SEINTERNAL;
      Cstager_Segment_setErrorCode(segmItem,
                                   filereq->err.errorcode);
      
      if (filereq->err.severity == RTCP_OK)
        filereq->err.severity = RTCP_FAILED|RTCP_UNERR;
      Cstager_Segment_setSeverity(segmItem,
                                  filereq->err.severity);
      
      if (*filereq->err.errmsgtxt == '\0')
        strncpy(filereq->err.errmsgtxt,
                sstrerror(filereq->err.errorcode),
                sizeof(filereq->err.errmsgtxt)-1);
      Cstager_Segment_setErrMsgTxt(segmItem,
                                   filereq->err.errmsgtxt);
    }

    iObj = Cstager_Segment_getIObject(segmItem);
    Cstager_Segment_id(segmItem,&_key);
    if ( rc != -1 && svcs != NULL && *svcs != NULL ) {
      rc = C_Services_updateRepNoRec(*svcs,iAddr,iObj,1);
      save_serrno = serrno;
    }
    
    /*
     * BEGIN Hack: commit updated segment
     */
/*     rcTmp = C_Services_commit(*svcs,iAddr); */
/*     Cstager_Tape_delete(dummyTp); */
/*    serrno = save_serrno;*/
    /*
     * END Hack: commit updated segment
     */
    if ( rc == -1 ) {
      save_serrno = serrno;
      if ( dontLog == 0 ) {
        char *_dbErr = NULL;
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_DBSVC,
                        (struct Cns_fileid *)&fileid,
                        RTCPCLD_NB_PARAMS+5,
                        "DBSVCCALL",
                        DLF_MSG_PARAM_STR,
                        (*svcs == NULL ? "getDbSvcs()" : "C_Services_updateRepNoRec()"),
                        "DBKEY",
                        DLF_MSG_PARAM_INT,
                        (int)_key,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        "DB_ERROR",
                        DLF_MSG_PARAM_STR,
                        (*svcs == NULL ? "(null)" : 
                         (_dbErr = rtcpcld_fixStr(C_Services_errorMsg(*svcs)))),
                        "",
                        DLF_MSG_PARAM_UUID,
                        stgUuid,
                        RTCPCLD_LOG_WHERE
                        );
        if ( _dbErr != NULL ) free(_dbErr);
      }
      C_IAddress_delete(iAddr);
      UNLOCKTP;
      serrno = save_serrno;
      return(-1);
    }
    C_IAddress_delete(iAddr);
    if ( notify == 1 ) (void)notifySegment(segmItem);
  } else {
    UNLOCKTP;
    serrno = ENOENT;
    return(-1);
  }
  UNLOCKTP;

  return(0);
}

