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
 * @(#)$RCSfile: rtcpcldCatalogueInterface.c,v $ $Revision: 1.50 $ $Release$ $Date: 2004/10/11 15:55:28 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldCatalogueInterface.c,v $ $Revision: 1.50 $ $Release$ $Date: 2004/10/11 15:55:28 $ Olof Barring";
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
#include <castor/stager/TapeCopy.h>
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
#include <Cthread_api.h>
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
                            tape
                            )
     tape_list_t *tape;
{
  struct Cdb_DbAddress_t *dbAddr;
  struct C_Services_t **svcs = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  struct Cstager_Tape_t *tp;
  int rc = 0, save_serrno;
  ID_TYPE key;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);

  rc = getStgSvc(&stgSvc);
  if ( rc == -1 || stgSvc == NULL ) return(-1);

  if ( tape->dbRef == NULL ) {
    tape->dbRef = (RtcpDBRef_t *)calloc(1,sizeof(RtcpDBRef_t));
    if ( tape->dbRef == NULL ) {
      serrno = errno;
      LOG_SYSCALL_ERR("calloc()");
      return(-1);
    }
  }
  tp = (struct Cstager_Tape_t *)tape->dbRef->row;
  key = tape->dbRef->key;

  if ( (tp == NULL) && (key != 0) ) {
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
    rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
    if ( rc == -1 ) return(-1);
    if ( tp != NULL ) Cstager_Tape_id(tp,&key);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  if ( (tp == NULL) && (key != 0) ) {
    rc = C_Services_createObj(*svcs,iAddr,&iObj);
  } else if ( tp != NULL ) { 
    iObj = Cstager_Tape_getIObject(tp);
    rc = C_Services_updateObj(*svcs,iAddr,iObj);
  } else {
    rc = Cstager_IStagerSvc_selectTape(
                                       stgSvc,
                                       &tp,
                                       tape->tapereq.vid,
                                       tape->tapereq.side,
                                       tape->tapereq.mode
                                       );
    if ( rc == -1 ) {
      char *_dbErr = NULL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_DBSVC,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+3,
                      "DBSVCCALL",
                      DLF_MSG_PARAM_STR,
                      "Cstager_IStagerSvc_selectTape()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(save_serrno),
                      "DB_ERROR",
                      DLF_MSG_PARAM_STR,
                      (_dbErr = rtcpcld_fixStr(Cstager_IStagerSvc_errorMsg(stgSvc))),
                      RTCPCLD_LOG_WHERE
                      );
      if ( _dbErr != NULL ) free(_dbErr);
      return(-1);
    }
    /*
     * selectTape() locks the Tape in DB. We rollback immediately since
     * we don't intend to update the DB here.
     */
    (void)C_Services_rollback(*svcs,iAddr);
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
  Cstager_Tape_id(tp,&key);
  tape->dbRef->key = key;
  tape->dbRef->row = (void *)tp;

  C_IAddress_delete(iAddr);
  return(0);
}

int rtcpcld_updateTapeFromDB(
                             tape
                             )
     tape_list_t *tape;
{
  return(updateTapeFromDB(tape));
}

/**
 * Update the memory copy of the segment from the database.
 */
static int updateSegmentFromDB(
                               file
                               )
     file_list_t *file;
{
  struct Cdb_DbAddress_t *dbAddr;
  struct C_Services_t **svcs = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj;
  struct Cstager_Segment_t *segm;
  int rc = 0, save_serrno;
  ID_TYPE key = 0;

  if ( (file == NULL) || (file->dbRef == NULL) ||
         ((file->dbRef->row == NULL) && (file->dbRef->key == 0)) ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = getDbSvc(&svcs);
  if ( rc == -1 || svcs == NULL || *svcs == NULL ) return(-1);

  rc = getStgSvc(&stgSvc);
  if ( rc == -1 || stgSvc == NULL ) return(-1);

  key = file->dbRef->key;
  segm = (struct Cstager_Segment_t *)file->dbRef->row;

  if ( (segm == NULL) && (key != 0) ) {
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
    rc = C_BaseAddress_create("OraCnvSvc",SVC_ORACNV,&baseAddr);
    if ( rc == -1 ) return(-1);
    if ( segm != NULL ) Cstager_Segment_id(segm,&key);
  }

  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_Segment_getIObject(segm);

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
  file->dbRef->row = (void *)segm;
  file->dbRef->key = key;
  C_IAddress_delete(iAddr);
  return(0);
}

int rtcpcld_updateSegmentFromDB(
                                file
                                )
     file_list_t *file;
{
  return(updateSegmentFromDB(file));
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
  struct Cstager_Tape_t *tp = NULL;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  if ( (tape->dbRef == NULL) || (tape->dbRef->row == NULL) ) {
    rc = updateTapeFromDB(tape);
    if ( rc == -1 ) return(-1);
  }
  
  if ( (tape->dbRef == NULL) || (tape->dbRef->row == NULL) ) {
    serrno = ENOENT;
    return(-1);
  }

  tp = tape->dbRef->row;
  /*
   * Cross check that we really got hold of the correct request
   */
  Cstager_Tape_vid(tp,(CONST char **)&vid);
  Cstager_Tape_tpmode(tp,&mode);
  Cstager_Tape_side(tp,&side);
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
                      tape->dbRef->key
                      );
    }
    serrno = SEINTERNAL;
    return(-1);
  }
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
  struct Cstager_Tape_t **tpArray = NULL, *tp;
  struct Cstager_Segment_t *segm;
  struct Cstager_Stream_t **streamArray = NULL;
  struct Cstager_IStagerSvc_t *stgsvc = NULL;
  struct Cstager_TapePool_t *tapePool;
  rtcpTapeRequest_t tapereq;
  tape_list_t *tmpTapeArray, *tape = NULL, *tl;
  char *vid, *tapePoolName;
  int i, rc = 0, nbTpItems = 0, nbStreamItems = 0, nbItems = 0;
  int save_serrno, mode, side = 0;
  ID_TYPE key = 0;
  u_signed64 sizeToTransfer = 0;

  if ( (tapeArray == NULL) || (cnt == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( cnt != NULL ) *cnt = 0;

  rc = getStgSvc(&stgsvc);
  if ( rc == -1 || stgsvc == NULL ) return(-1);
  rc = Cstager_IStagerSvc_tapesToDo(
                                    stgsvc,
                                    &tpArray,
                                    &nbTpItems
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

  if ( nbTpItems > 0 ) {
    for (i=0; i<nbTpItems; i++) {
      tl = NULL;
      Cstager_Tape_vid(tpArray[i],(CONST char **)&vid);
      if ( vid == NULL ) continue;
      Cstager_Tape_tpmode(tpArray[i],&mode);
      tapereq.mode = mode;
      tapereq.side = 0;
      strncpy(tapereq.vid,vid,sizeof(tapereq.vid)-1);
      rc = rtcp_NewTapeList(&tape,&tl,mode);
      if ( rc == -1 ) break;
      tl->dbRef = (RtcpDBRef_t *)calloc(1,sizeof(RtcpDBRef_t *));
      if ( tl->dbRef == NULL ) {
        serrno = errno;
        rc = -1;
        break;
      }
      Cstager_Tape_id(tpArray[i],&(tl->dbRef->key));
      tl->dbRef->row = (void *)tpArray[i];
      tl->tapereq = tapereq;
      rc = rtcpcld_tapeOK(tl);
      if ( rc == -1 ) break;
    }
    save_serrno = serrno;
    if ( rc != -1) nbItems = nbTpItems;
    free(tpArray);
  }

  if ( rc != -1 ) {
    /*
     * Now do the streams. For each stream we need to call
     * vmgr_gettape() to get the VID to be used for the stream.
     */
    rc = Cstager_IStagerSvc_streamsToDo(
                                        stgsvc,
                                        &streamArray,
                                        &nbStreamItems
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
                        "Cstager_IStagerSvc_streamsTodo()",
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
    }
  }

  if ( (rc != -1 ) && (nbStreamItems > 0) ) {
    for (i=0; i<nbStreamItems; i++) {
      Cstager_Stream_tapePool(streamArray[i],&tapePool);
      if ( tapePool == NULL ) {
        Cstager_Stream_id(streamArray[i],&key);
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_NOTAPEPOOL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+1,
                        "DBKEY",
                        DLF_MSG_PARAM_INT64,
                        key,
                        RTCPCLD_LOG_WHERE
                        );
        continue;
      }
      Cstager_Stream_initialSizeToTransfer(streamArray[i],&sizeToTransfer);
      Cstager_TapePool_name(tapePool,(CONST char **)&tapePoolName);
      tl = NULL;
      rc = rtcpcld_gettape(
                           tapePoolName,
                           sizeToTransfer,
                           &tl
                           );
      /*
       * OK, we got the tape. The tape is now BUSY in VMGR.
       * Need to create the Tape in DB to facilitate the cleanup
       * in case something goes wrong.
       */
      rc = updateTapeFromDB(tl);
      if ( rc == -1 ) {
        save_serrno = serrno;
        (void)rtcpcld_updateTape(&(tl->tapereq),NULL,1,0);
        serrno = save_serrno;
        break;
      }
      CLIST_INSERT(tape,tl);
      tl->dbRef = (RtcpDBRef_t *)calloc(1,sizeof(RtcpDBRef_t *));
      if ( tape->dbRef == NULL ) {
        serrno = errno;
        rc = -1;
        break;
      }
      Cstager_Tape_id(tpArray[i],&(tl->dbRef->key));
      tl->dbRef->row = (void *)tp;
      rc = rtcpcld_tapeOK(tape);
      if ( rc == -1 ) break;
    }
    save_serrno = serrno;
    if ( rc != -1) nbItems += nbStreamItems;
    free(streamArray);
  }

  if ( rc != -1 ) {
    *tapeArray = (tape_list_t **)calloc(nbItems,sizeof(tape_list_t *));
    if ( *tapeArray == NULL ) {
      save_serrno = errno;
      LOG_SYSCALL_ERR("Cthread_mutex_lock(currentTapeFseq)");
      rc = -1;
    }
  }
  
  while ( (tl = tape) != NULL ) {
    CLIST_DELETE(tape,tl);
    if ( rc == -1 ) {
      if ( tl->file != NULL ) free(tl->file);
      if ( tl->dbRef != NULL ) tp = tl->dbRef->row;
      if ( tp != NULL ) Cstager_Tape_delete(tp);
      free(tl);
    } else {
      (*tapeArray)[i] = tl;
    }
  }

  if ( rc == -1 ) {
    serrno = save_serrno;
  } else {
    *cnt = nbItems;
  }
  return(rc);
}

void rtcpcld_cleanupTape(
                         tape
                         )
     tape_list_t *tape;
{
  struct Cstager_Tape_t *tp;
  
  if ( tape == NULL ) return;
  if ( tape->dbRef != NULL ) {
    tp = (struct Cstager_Tape_t *)tape->dbRef->row;
    if ( tp != NULL ) Cstager_Tape_delete(tp);
    free(tape->dbRef);
  }
  if ( tape->file != NULL ) free(tape->file);
  free(tape);
  return;
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
  char *path = NULL;

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
    if ( cmpStatus == SEGMENT_UNPROCESSED ) {
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
      } else if ( validPosition(segmArray[i]) == 1 ) {
        incompleteSegments = 1;
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
   * Commit all new segment updates
   */
  rc = C_Services_commit(*svcs,iAddr);

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
  if ( (tape->dbRef == NULL) || (tape->dbRef->row == NULL) ) {
    rc = updateTapeFromDB(tape);
    if ( (rc == -1) || (tape->dbRef == NULL) || (tape->dbRef->row == NULL) ) {
      UNLOCKTP;
      return(-1);
    }
  }
  tapeItem = (struct Cstager_Tape_t *)tape->dbRef->row;

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
  if ( (tape->dbRef == NULL) || (tape->dbRef->row == NULL) ) {
    rc = updateTapeFromDB(tape);
    if ( (rc == 0) && (tape->dbRef != NULL) ) {
      tapeItem = (struct Cstager_Tape_t *)tape->dbRef->row;
    }
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
  if ( (tape->dbRef == NULL) || (tape->dbRef->row == NULL) ) {
    rc = updateTapeFromDB(tape);
    if ( (rc == 0) && (tape->dbRef != NULL) ) {
      tapeItem = (struct Cstager_Tape_t *)tape->dbRef->row;
    }
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

  rc = updateTapeFromDB(tape);
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
                          tape,
                          file, 
                          newStatus,
                          notify
                          )
     tape_list_t *tape;
     file_list_t *file;
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
  rtcpFileRequest_t *filereq;
  ID_TYPE _key = 0;
  char *vid;
  int mode, side;
  struct Cns_fileid fileid;
  char *nsHost;
  Cuuid_t stgUuid;
  char *diskPath;
  unsigned char *blockid = NULL;
  int rc = 0, rcTmp, save_serrno, fseq;

  if ( (file == NULL) || (file->dbRef == NULL) || (file->dbRef->row == NULL) ||
       (*(file->filereq.file_path) == '\0') ||
       (*(file->filereq.file_path) == '.') ) {
    serrno = EINVAL;
    return(-1);
  }
  filereq = &(file->filereq);

  segmItem = (struct Cstager_Segment_t *)file->dbRef->row;

  LOCKTP;

  svcs = NULL;
  rc = getDbSvc(&svcs);

  rc = updateSegmentFromDB(file);
  if ( rc == -1 ) {
    UNLOCKTP;
    return(-1);
  }

  Cstager_Segment_status(
                         segmItem,
                         &currentStatus
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

