
/******************************************************************************
 *                      MigHunter.c
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
 * @(#)$RCSfile: MigHunter.c,v $ $Revision: 1.27 $ $Release$ $Date: 2005/08/03 10:48:06 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: MigHunter.c,v $ $Revision: 1.27 $ $Release$ $Date: 2005/08/03 10:48:06 $ Olof Barring";
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
#include <sys/stat.h>
#include <patchlevel.h>
#include <Castor_limits.h>
#include <Cinit.h>
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
#include <castor/Constants.h>
#include <castor/stager/Stream.h>
#include <castor/stager/TapeCopy.h>
#include <castor/stager/TapePool.h>
#include <castor/stager/CastorFile.h>
#include <castor/stager/FileClass.h>
#include <castor/stager/StreamStatusCodes.h>
#include <castor/stager/TapeCopyStatusCodes.h>
#include <castor/stager/SvcClass.h>
#include <castor/stager/ITapeSvc.h>
#include <castor/Services.h>
#include <castor/BaseObject.h>
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
#include <expert_api.h>
static int runAsDaemon = 0;
static int legacyMode = 0;
static int doCloneStream = 0;
static char *cmdName = NULL;
static struct Cstager_TapeCopy_t ***_migrCandidates = NULL;
static int *_nbMigrCandidates = NULL;
/** Global needed for determine which uuid to log
 */
int inChild;
/** uuid's set by calling process (rtcpclientd:mainUuid, VidWorker:childUuid)
 */
Cuuid_t childUuid, mainUuid;

static int prepareForDBAccess _PROTO((
                                      struct C_Services_t **,
                                      struct Cstager_ITapeSvc_t **,
                                      struct C_IAddress_t **
                                      ));
static int getSvcClass _PROTO((
                               char *,
                               struct Cstager_SvcClass_t **
                               ));
static int getStreamsFromDB _PROTO((
                                    struct Cstager_TapePool_t *,
                                    int *
                                    ));
static int findTapePoolInSvcClass _PROTO((
                                          struct Cstager_SvcClass_t *,
                                          char *
                                          ));
static int findTapePoolInFileClass _PROTO((
                                           struct Cns_fileclass *,
                                           char *
                                           ));
static int tapePoolCreator _PROTO((
                                   struct Cstager_SvcClass_t *,
                                   struct Cstager_TapeCopy_t **,
                                   struct Cns_fileclass **,
                                   int
                                   ));
static int getStreamsForSvcClass _PROTO((
                                         struct Cstager_SvcClass_t *,
                                         int *
                                         ));
static void restoreMigrCandidates _PROTO((
                                          struct Cstager_TapeCopy_t **,
                                          int
                                          ));
static int cloneStream _PROTO((
                               struct Cstager_Stream_t *, 
                               struct Cstager_Stream_t *
                               ));
static int streamCreator _PROTO((
                                 struct Cstager_SvcClass_t *,
                                 u_signed64,
                                 int *
                                 ));
static int startStreams _PROTO((
                                struct Cstager_SvcClass_t *,
                                u_signed64
                                ));
static int addTapeCopyToStreams _PROTO((
                                        struct Cstager_SvcClass_t *,
                                        struct Cstager_TapeCopy_t *,
                                        struct Cns_fileclass *
                                        ));
static int callExpert _PROTO((
                              char *,
                              char *,
                              int,
                              struct Cns_filestat *,
                              char *
                              ));
static int getMigrCandidates _PROTO((
                                     struct Cstager_SvcClass_t *,
                                     struct Cstager_TapeCopy_t ***,
                                     struct Cns_fileclass ***,
                                     int *,
                                     u_signed64 *
                                     ));
static void freeFileClassArray _PROTO((
                                       struct Cns_fileclass **,
                                       int
                                       ));
static void freeMigrCandidates _PROTO((
                                       struct Cstager_TapeCopy_t **,
                                       int
                                       ));
static int addMigrationCandidatesToStreams _PROTO((
                                                   struct Cstager_SvcClass_t *,
                                                   struct Cstager_TapeCopy_t **,
                                                   struct Cns_fileclass **,
                                                   int
                                                   ));
static void shutdownService _PROTO((
                                    int
                                    ));
static void usage _PROTO((
                          char *
                          ));


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
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getDbSvc(): %s\n",sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("getDbSvc()");
    return(-1);
  }

  tpSvc = NULL;
  rc = rtcpcld_getStgSvc(&tpSvc);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getStgSvc(): %s\n",sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("getStgSvc()");
    return(-1);
  }

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"C_BaseAddress_create(): %s\n",sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("C_BaseAddress_create()");
    return(-1);
  }
  
  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);

  if ( _dbSvc != NULL ) *_dbSvc = *dbSvc;
  if ( _tpSvc != NULL ) *_tpSvc = *tpSvc;
  if ( _iAddr != NULL ) *_iAddr = iAddr;
  
  return(0);
}
     

static int getSvcClass(
                       svcClassName,
                       svcClass
                       )
     char *svcClassName;
     struct Cstager_SvcClass_t **svcClass;
{
  struct Cstager_SvcClass_t *svcClassTmp = NULL;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  struct C_Services_t *dbSvc = NULL;
  struct C_IAddress_t *iAddr = NULL;
  int rc;
  
  if ( svcClassName == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass() called without SvcClass name\n");
    }
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("getSvcClass()");
    return(-1);
  }

  LOG_CALL_TRACE((rc = Cstager_ITapeSvc_selectSvcClass(
                                                         tpSvc,
                                                         &svcClassTmp,
                                                         svcClassName
                                                         )));
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"Cstager_ITapeSvc_selectSvcClass(%s): %s, %s\n",
              svcClassName,
              sstrerror(serrno),
              Cstager_ITapeSvc_errorMsg(tpSvc));
    }
    LOG_DBCALL_ERR("Cstager_ITapeSvc_selectSvcClass()",
                   Cstager_ITapeSvc_errorMsg(tpSvc));
    C_IAddress_delete(iAddr);
    return(-1);
  }
  if ( svcClassTmp == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"SvcClass %s not found???\n",svcClassName);
    }
    serrno = ENOENT;
    C_IAddress_delete(iAddr);
    return(-1);
  }

  iObj = Cstager_SvcClass_getIObject(svcClassTmp);
  rc = C_Services_fillObj(
                       dbSvc,
                       iAddr,
                       iObj,
                       OBJ_TapePool
                       );
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"C_Services_fillObj(svcClass,OBJ_TapePool) %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(dbSvc));
    }
    LOG_DBCALL_ERR("C_Services_fillObj(svcClass,OBJ_TapePool)",
                   C_Services_errorMsg(dbSvc));
    C_IAddress_delete(iAddr);
    return(-1);
  }

  if ( svcClass != NULL ) *svcClass = svcClassTmp;
  C_IAddress_delete(iAddr);
  return(0);
}

static int getStreamsFromDB(
                            tapePool,
                            nbStreams
                            )
     struct Cstager_TapePool_t *tapePool;
     int *nbStreams;
{
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  struct Cstager_Stream_t **streamArray = NULL;
  int rc, _nbStreams = 0;

  if ( tapePool == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getStreamsFromDB() called without TapePool\n");
    }
    serrno = EINVAL;
    return(-1);
  }
  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return(-1);
  }
  iObj = Cstager_TapePool_getIObject(tapePool);
  LOG_CALL_TRACE((rc = Cstager_ITapeSvc_streamsForTapePool(tpSvc,tapePool)));
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"Cstager_ITapeSvc_streamsForTapePool %s, %s\n",
              sstrerror(serrno),
              Cstager_ITapeSvc_errorMsg(tpSvc));
    }
    LOG_DBCALL_ERR("Cstager_ITapeSvc_streamsForTapePool()",
                   Cstager_ITapeSvc_errorMsg(tpSvc));
  }
  Cstager_TapePool_streams(tapePool,&streamArray,&_nbStreams);
  if ( nbStreams != NULL ) *nbStreams = _nbStreams;
  if ( streamArray != NULL ) free(streamArray);
  
  C_IAddress_delete(iAddr);
  return(rc);
}

static int findTapePoolInSvcClass(
                                  svcClass,
                                  tapePoolName
                                  )
     struct Cstager_SvcClass_t *svcClass;
     char *tapePoolName;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  int i, nbTapePools = 0;
  char *name = NULL;
  
  if ( svcClass == NULL || tapePoolName == NULL ) return(0);
  
  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  if ( tapePoolArray == NULL || nbTapePools <= 0 ) return(0);
  
  for ( i=0; i<nbTapePools; i++ ) {
    name = NULL;
    Cstager_TapePool_name(tapePoolArray[i],(CONST char **)&name);
    if ( strcmp(tapePoolName,name) == 0 ) {
      free(tapePoolArray);
      return(1);
    }
  }
  if ( tapePoolArray != NULL ) free(tapePoolArray);
  return(0);
}  

static int findTapePoolInFileClass(
                                   fileClass,
                                   tapePoolName
                                   )
     struct Cns_fileclass *fileClass;
     char *tapePoolName;
{
  int i;
  char *name;

  if ( fileClass == NULL || tapePoolName == NULL ) return(0);
  
  for ( i=0; i<fileClass->nbtppools; i++ ) {
    name = fileClass->tppools+i*(CA_MAXCLASNAMELEN+1);
    if ( strcmp(name,tapePoolName) == 0 ) return(1);
  }
  return(0);
}

static int tapePoolCreator(
                           svcClass,
                           tapeCopyArray,
                           fileClass,
                           nbTapeCopies
                           )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t **tapeCopyArray;
     struct Cns_fileclass **fileClass;
     int nbTapeCopies;
{
  struct Cstager_TapePool_t *newTapePool;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  char  *tapePoolName;
  int rc, i, j;

  if ( (tapeCopyArray == NULL) || (fileClass == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"tapePoolCreator(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return(-1);
  }

  for ( i=0; i<nbTapeCopies; i++ ) {
    iObj = Cstager_TapeCopy_getIObject(tapeCopyArray[i]);
    for ( j=0; j<fileClass[i]->nbtppools; j++ ) {
      tapePoolName = fileClass[i]->tppools+j*(CA_MAXCLASNAMELEN+1);
      rc = findTapePoolInSvcClass(svcClass,tapePoolName);
      if ( rc == 0 ) {
        newTapePool = NULL;
        Cstager_TapePool_create(&newTapePool);
        Cstager_TapePool_setName(newTapePool,tapePoolName);
        Cstager_TapePool_addSvcClasses(newTapePool,svcClass);
        Cstager_SvcClass_addTapePools(svcClass,newTapePool);
        iObj = Cstager_SvcClass_getIObject(svcClass);        
        rc = C_Services_fillRep(
                                dbSvc,
                                iAddr,
                                iObj,
                                OBJ_TapePool,
                                0
                                );
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_TapePool) %s, %s\n",
                    sstrerror(serrno),
                    C_Services_errorMsg(dbSvc));
          }
          LOG_DBCALL_ERR("C_Services_fillRep()",
                         C_Services_errorMsg(dbSvc));
          (void)C_Services_rollback(dbSvc,iAddr);
          C_IAddress_delete(iAddr);
          return(-1);
        }
      }
    }
  }
  rc = C_Services_commit(dbSvc,iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"C_Services_commit() %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(dbSvc));
    }
    LOG_DBCALL_ERR("C_Services_commit()",
                   C_Services_errorMsg(dbSvc));
    C_IAddress_delete(iAddr);
    return(-1);
  }
  C_IAddress_delete(iAddr);
  return(0);
}

static int getStreamsForSvcClass(
                                 svcClass,
                                 nbStreams
                                 )
     struct Cstager_SvcClass_t *svcClass;
     int *nbStreams;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  char *svcClassName = NULL;
  int rc, i, nbTapePools, _nbStreams, totNbStreams = 0;

  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  if ( (nbTapePools <= 0) || (tapePoolArray == NULL) ){
    Cstager_SvcClass_name(svcClass,(CONST char **)&svcClassName);
    if ( runAsDaemon == 0 ) {
      fprintf(stdout,
              "getStreamsForSvcClass(): no tape pools associated with SvcClass %s\n",
              (svcClassName != NULL ? svcClassName : "(null)"));
    }
    serrno = ENOENT;
    return(-1);
  }

  for ( i=0; i<nbTapePools; i++ ) {
    _nbStreams = 0;
    rc = getStreamsFromDB(tapePoolArray[i],&_nbStreams);
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"getStreamsFromDB(): %s\n",sstrerror(serrno));
      }
      LOG_SYSCALL_ERR("getStreamsFromDB()");
      free(tapePoolArray);
      return(-1);
    }
    totNbStreams += _nbStreams;
  }
  if ( tapePoolArray != NULL ) free(tapePoolArray);
  if ( nbStreams != NULL ) *nbStreams = totNbStreams;
  return(0);
}

/*
 * Restore the status of all migration candidates that have not been
 * attached to any streams.
 */
static void restoreMigrCandidates(
                                  migrCandidates,
                                  nbMigrCandidates
                                  )
     struct Cstager_TapeCopy_t **migrCandidates;
     int nbMigrCandidates;
{
  enum Cstager_TapeCopyStatusCodes_t tapeCopyStatus;
  struct Cstager_TapeCopy_t *tapeCopy;
  struct Cstager_Stream_t **streamArray;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  int rc, i, nbStreams;
  char buf[21];
  ID_TYPE key;
  
  if ( (migrCandidates == NULL) || (nbMigrCandidates <= 0) ) return;
  if ( runAsDaemon == 0 ) {
    fprintf(stdout,"Restoring unattached migration candidates. Check %d candidates\n",
            nbMigrCandidates);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"restoreMigrCandidates(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return;
  }
  for ( i=0; i<nbMigrCandidates; i++ ) {
    tapeCopy = migrCandidates[i];
    streamArray = NULL;
    nbStreams = 0;
    Cstager_TapeCopy_stream(tapeCopy,&streamArray,&nbStreams);
    if ( (streamArray == NULL) || (nbStreams <= 0) ) {
      Cstager_TapeCopy_status(tapeCopy,&tapeCopyStatus);
      if ( tapeCopyStatus == TAPECOPY_WAITINSTREAMS ) {
        Cstager_TapeCopy_id(tapeCopy,&key);
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,
                  "Restore unassigned tapeCopy %d/%d (id=%s)\n",
                  i,nbMigrCandidates,
                  u64tostr(key,buf,0));
        }
        Cstager_TapeCopy_setStatus(tapeCopy,TAPECOPY_TOBEMIGRATED);
        iObj = Cstager_TapeCopy_getIObject(tapeCopy);
        LOG_CALL_TRACE((rc = C_Services_updateRep(
                                                  dbSvc,
                                                  iAddr,
                                                  iObj,
                                                  1
                                                  )));
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"C_Services_updateRep(tapeCopy(id=%s)): %s, %s\n",
                    u64tostr(key,buf,0),
                    sstrerror(serrno),
                    C_Services_errorMsg(dbSvc));
          }
          LOG_DBCALLANDKEY_ERR("C_Services_updateRep(tapeCopy)",
                               C_Services_errorMsg(dbSvc),
                               key);
        }
      }
    }
    if ( streamArray != NULL ) free(streamArray);
  }
  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  if ( runAsDaemon == 0 ) fprintf(stdout,"Restore procedure finished\n");
  return;
}
     

/*
 * Add all old migration candidates from an existing to a new stream
 */
static int cloneStream(
                       oldStream,
                       newStream
                       )
     struct Cstager_Stream_t *oldStream, *newStream;
{
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  struct Cstager_TapeCopy_t **tapeCopyArray = NULL;
  int rc, i, nbTapeCopies = 0;

  if ( oldStream == NULL || newStream == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"closeStream(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return(-1);
  }

  iObj = Cstager_Stream_getIObject(oldStream);
  LOG_CALL_TRACE((rc = C_Services_fillObj(
                                          dbSvc,
                                          iAddr,
                                          iObj,
                                          OBJ_TapeCopy
                                          )));
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"C_Services_fillObj(stream,OBJ_TapeCopy): %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(dbSvc));
    }
    LOG_DBCALL_ERR("C_Services_fillObj(stream,OBJ_TapeCopy)",
                   C_Services_errorMsg(dbSvc));
    C_IAddress_delete(iAddr);
    return(-1);
  }

  Cstager_Stream_tapeCopy(oldStream,&tapeCopyArray,&nbTapeCopies);
  if ( runAsDaemon == 0 ) {
    fprintf(stdout,"cloneStream() copy %d TapeCopies to new stream\n",
            nbTapeCopies);
  }

  if ( tapeCopyArray != NULL ) {
    for ( i=0; i<nbTapeCopies; i++ ) {
      Cstager_Stream_addTapeCopy(newStream,tapeCopyArray[i]);
      Cstager_TapeCopy_addStream(tapeCopyArray[i],newStream);
    }
  }
  iObj = Cstager_Stream_getIObject(newStream);
  LOG_CALL_TRACE((rc = C_Services_fillRep(
                                          dbSvc,
                                          iAddr,
                                          iObj,
                                          OBJ_TapeCopy,
                                          1
                                          )));
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"C_Services_fillRep(stream,OBJ_TapeCopy): %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(dbSvc));
    }
    LOG_DBCALL_ERR("C_Services_fillRep(stream,OBJ_TapeCopy)",
                   C_Services_errorMsg(dbSvc));
    C_IAddress_delete(iAddr);
    return(-1);
  }
  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  if ( tapeCopyArray != NULL ) free(tapeCopyArray);
  return(0);
}

static int streamCreator(
                         svcClass,
                         initialSizeToTransfer,
                         nbNewStreams
                         )
     struct Cstager_SvcClass_t *svcClass;
     u_signed64 initialSizeToTransfer;
     int *nbNewStreams;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  struct Cstager_Stream_t *newStream, **streamArray = NULL, **streamsToClone = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  char *svcClassName = NULL;
  int rc, nbTapePools, nbStreams, i, j;
  unsigned int nbDrives, nbStreamsTotal;
  int *streamsPerTapePool = NULL, _nbNewStreams = 0;

  if ( nbNewStreams != NULL ) *nbNewStreams = 0;
  if ( svcClass == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"streamCreator() called without SvcClass\n");
    }
    serrno = EINVAL;
    return(-1);
  }

  Cstager_SvcClass_nbDrives(svcClass,&nbDrives);
  
  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  if ( (nbTapePools <= 0) || (tapePoolArray == NULL) ){
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"streamCreator(): no tape pools associated with SvcClass\n");
    }
    serrno = ENOENT;
    return(-1);
  }

  streamsPerTapePool = (int *)calloc(nbTapePools,sizeof(int));
  if ( streamsPerTapePool == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"calloc(): %s\n",strerror(errno));
    }
    LOG_SYSCALL_ERR("calloc()");
    serrno = SESYSERR;
    return(-1);
  }

  streamsToClone = (struct Cstager_Stream_t **)calloc(
                                                      nbTapePools,
                                                      sizeof(struct Cstager_stream_t *)
                                                      );
  if ( streamsToClone == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"calloc(): %s\n",strerror(errno));
    }
    LOG_SYSCALL_ERR("calloc()");
    serrno = SESYSERR;
    return(-1);
  }
    
  nbStreamsTotal = 0;
  for ( i=0; i<nbTapePools; i++ ) {
    streamArray = NULL;
    Cstager_TapePool_streams(tapePoolArray[i],&streamArray,&nbStreams);
    if ( (nbStreams > 0) && (streamArray != NULL) ) {
      nbStreamsTotal += nbStreams;
      streamsPerTapePool[i] = nbStreams;
      streamsToClone[i] = streamArray[0];
      free(streamArray);
    }
  }
  
  if ( nbStreamsTotal != nbDrives ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stdout,"SvcClass allows for %d streams, found %d\n",
              nbDrives,nbStreamsTotal);
    }
    rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
                sstrerror(serrno));
      }
      LOG_SYSCALL_ERR("prepareForDBAccess()");
      return(-1);
    }
  }

  if ( nbStreamsTotal < nbDrives ) {
    fprintf(stdout,"Create %d new streams\n",nbDrives-nbStreamsTotal);
    while ( nbStreamsTotal < nbDrives ) {
      j = 0;
      for ( i=1; i<nbTapePools; i++ ) {
        if ( streamsPerTapePool[i] < streamsPerTapePool[j] ) {
          j = i;
        }
      }
      newStream = NULL;
      Cstager_Stream_create(&newStream);
      /*
       * Set state to STREAM_CREATED to block it from being
       * immediately swallowed by streamsToDo() before we had
       * a chance to throw TapeCopies into it.
       */
      Cstager_Stream_setStatus(newStream,STREAM_CREATED);
      Cstager_Stream_setInitialSizeToTransfer(newStream,initialSizeToTransfer);
      Cstager_TapePool_addStreams(tapePoolArray[j],newStream);
      Cstager_Stream_setTapePool(newStream,tapePoolArray[j]);
      streamsPerTapePool[j]++;
      nbStreamsTotal++;
      iObj = Cstager_TapePool_getIObject(tapePoolArray[j]);
      rc = C_Services_fillRep(
                              dbSvc,
                              iAddr,
                              iObj,
                              OBJ_Stream,
                              1
                              );
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_fillRep(tapePool,OBJ_Stream) %s, %s\n",
                  sstrerror(serrno),
                  C_Services_errorMsg(dbSvc));
        }
        LOG_DBCALL_ERR("C_Services_fillRep(tapePool,OBJ_Stream)",
                       C_Services_errorMsg(dbSvc));
        C_IAddress_delete(iAddr);
        return(-1);
      }
      if ( doCloneStream == 1 ) {
        rc = cloneStream(streamsToClone[j],newStream);
        if ( rc == -1 ) return(-1);
      }
      _nbNewStreams++;
    }
  } else if ( nbStreamsTotal < nbDrives ) {
    Cstager_SvcClass_name(svcClass,(CONST char **)&svcClassName);
    (void)dlf_write(
                    mainUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_TOOMANYSTRS),
                    (struct Cns_fileid *)NULL,
                    3,
                    "SVCCLASS",
                    DLF_MSG_PARAM_STR,
                    (svcClassName != NULL ? svcClassName : "(unknown)"),
                    "NBDRIVES",
                    DLF_MSG_PARAM_INT,
                    nbDrives,
                    "NBSTREAMS",
                    DLF_MSG_PARAM_INT,
                    nbStreamsTotal
                    );
  }
  

  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  if ( streamsPerTapePool != NULL ) free(streamsPerTapePool);
  if ( streamsToClone != NULL ) free(streamsToClone);
  if ( nbNewStreams != NULL ) *nbNewStreams = _nbNewStreams;
  return(0);
}

static int startStreams(
                        svcClass,
                        initialSizeToTransfer
                        )
     struct Cstager_SvcClass_t *svcClass;
     u_signed64 initialSizeToTransfer;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  struct Cstager_Stream_t **streamArray = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  u_signed64 sz;
  enum Cstager_StreamStatusCodes_t streamStatus;
  char *tapePoolName;
  int rc, i, j, nbTapePools = 0, nbStreams = 0;
  
  if ( svcClass == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"startStreams() called with NULL argument\n");
    }
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"startStreams(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return(-1);
  }

  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  for ( i=0; i<nbTapePools; i++ ) {
    streamArray = NULL;
    Cstager_TapePool_streams(tapePoolArray[i],&streamArray,&nbStreams);
    tapePoolName = NULL;
    Cstager_TapePool_name(tapePoolArray[i],(CONST char **)&tapePoolName);
    for ( j=0; j<nbStreams; j++ ) {
      Cstager_Stream_status(streamArray[j],&streamStatus);
      Cstager_Stream_initialSizeToTransfer(streamArray[j],&sz);
      if ( (streamStatus == STREAM_CREATED) ||
           ((sz == 0) && (initialSizeToTransfer>0)) ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Start %d stream for tape pool %s\n",
                  j,tapePoolName);
        }
        if ( streamStatus == STREAM_CREATED ) {
          Cstager_Stream_setStatus(streamArray[j],STREAM_PENDING);
        }
        /*
         * We don't know how much has been migrated so far because
         * initialSizeToTransfer is never decremented. However, giving
         * the initialSizeToTransfer for the new migr candidates should
         * be a reasonable guess for the next vmgr_gettape()
         */
        if ( initialSizeToTransfer > 0 ) {
          Cstager_Stream_setInitialSizeToTransfer(
                                                  streamArray[j],
                                                  initialSizeToTransfer
                                                  );
        }
        iObj = Cstager_Stream_getIObject(streamArray[j]);
        rc = C_Services_updateRep(
                                  dbSvc,
                                  iAddr,
                                  iObj,
                                  0
                                  );
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ){
            fprintf(stderr,"C_Services_updateRep(Stream) %s, %s\n",
                    sstrerror(serrno),
                    C_Services_errorMsg(dbSvc));
          }
          LOG_DBCALL_ERR("C_Services_updateRep()",
                         C_Services_errorMsg(dbSvc));
          C_IAddress_delete(iAddr);
          return(-1);
        }
      }
    }
    if ( streamArray != NULL ) free(streamArray);
  }
  if ( tapePoolArray != NULL ) free(tapePoolArray);
  C_IAddress_delete(iAddr);
  return(0);
}

static int addTapeCopyToStreams(
                                svcClass,
                                tapeCopy,
                                fileClass
                                )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t *tapeCopy;
     struct Cns_fileclass *fileClass;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  struct Cstager_Stream_t **streamArray = NULL;
  struct Cstager_CastorFile_t *castorFile = NULL;
  char *tapePoolName = NULL, *migratorPolicyName = NULL, *nsHost = NULL;
  char castorFileName[CA_MAXPATHLEN+1];
  struct Cns_filestat statbuf;
  struct Cns_fileid fileId;
  int rc, i, j, nbTapePools = 0, nbStreams = 0, addedToStream = 0;
  unsigned int cpNb = 0;

  if ( (svcClass == NULL) || (tapeCopy == NULL) ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addTapeCopyToStream() called with NULL argument\n");
    }
    serrno = EINVAL;
    return(-1);
  }
  Cstager_SvcClass_migratorPolicy(
                                  svcClass,
                                  (CONST char **)&migratorPolicyName
                                  );
  if ( (migratorPolicyName != NULL) && (*migratorPolicyName != '\0') ) {
    Cstager_TapeCopy_copyNb(tapeCopy,&cpNb);
    Cstager_TapeCopy_castorFile(tapeCopy,&castorFile);
    memset(&fileId,'\0',sizeof(fileId));
    Cstager_CastorFile_fileId(castorFile,&(fileId.fileid));
    nsHost = NULL;
    Cstager_CastorFile_nsHost(castorFile,(CONST char **)&nsHost);
    if ( nsHost != NULL ) {
      strncpy(fileId.server,nsHost,sizeof(fileId.server)-1);
    }
    *castorFileName = '\0';
    rc = Cns_statx(castorFileName,&fileId,&statbuf);
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"Cns_statx(): %s\n",sstrerror(serrno));
      }
      LOG_SYSCALL_ERR("Cns_statx()");
      return(-1);
    }
    rc = Cns_getpath(fileId.server,fileId.fileid,castorFileName);
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"Cns_getpath(): %s\n",sstrerror(serrno));
      }
      LOG_SYSCALL_ERR("Cns_getpath()");
      return(-1);
    }
  }

  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  for ( i=0; i<nbTapePools; i++ ) {
    Cstager_TapePool_name(tapePoolArray[i],(CONST char **)&tapePoolName);
    if ( ((legacyMode == 1) &&
          (findTapePoolInFileClass(fileClass,tapePoolName) == 1)) ||
         ((legacyMode == 0) &&
          (callExpert(migratorPolicyName,
                      castorFileName,
                      cpNb,
                      &statbuf,
                      tapePoolName) == 1)) ) {
      addedToStream = 1;
      streamArray = NULL;
      Cstager_TapePool_streams(tapePoolArray[i],&streamArray,&nbStreams);
      for ( j=0; j<nbStreams; j++ ) {
        Cstager_Stream_addTapeCopy(streamArray[j],tapeCopy);
        Cstager_TapeCopy_addStream(tapeCopy,streamArray[j]);
      }
      if ( streamArray != NULL ) free(streamArray);
    }
  }
  if ( tapePoolArray != NULL ) free(tapePoolArray);
  return(addedToStream);
}

static int callExpert(
                      migratorPolicyName,
                      castorFileName,
                      copyNb,
                      statbuf,
                      tapePoolName
                      )
     char *migratorPolicyName, *castorFileName, *tapePoolName;
     int copyNb;
     struct Cns_filestat *statbuf;
{
  char expertBuffer[2*(CA_MAXPATHLEN+1)+1*(CA_MAXPOOLNAMELEN+1)+2*22+8*11+1];
  char *p, u64buf[22];
  int msgLen, rc = 0, fd = -1;
  int timeout = 30;
  char answer[21]; /* migrate this file -> 1, don't migrate now -> 0 */

  /*
   * Arguments for migrator polic are:
   * - migration policy name, e.g. "userFilePolicy.pl" (1024 chars)
   * - tape pool name (17 chars)
   * - castor file name, e.g. "/castor/cern.ch/user/m/me/myfile" (1024 chars)
   * - copy number (dual tapecopy files) (11 chars)
   * - fileid, 64bit castor bit-file id (22 chars)
   * - filesize (64bit) (22 chars)
   * - filemode, file permission mode, e.g. 0644 (11 chars)
   * - uid, owner uid (11 chars)
   * - gid, owner gid
   * - atime, last access to file (11 chars)
   * - mtime, last file modification (11 chars)
   * - ctime, last file metadata modification (11 chars)
   * - fileclass, castor file class identifier (11 chars)
   */

  /*
   * If no policy, always migrate
   */
  if ( (migratorPolicyName == NULL) || (*migratorPolicyName == '\0') ) return(1);
  memset(expertBuffer,'\0',sizeof(expertBuffer));
  p = expertBuffer;
  sprintf(p,"%s %s %s %d ",migratorPolicyName,tapePoolName,castorFileName,copyNb);
  p += strlen(p);
  sprintf(p,"%s ",u64tostr(statbuf->fileid,u64buf,-1));
  p += strlen(p);
  sprintf(p,"%s ",u64tostr(statbuf->filesize,u64buf,-1));
  p += strlen(p);
  sprintf(p,"%d %d %d %d %d %d %d\n",
          statbuf->filemode,
          statbuf->uid,
          statbuf->gid,
          (int)statbuf->atime,
          (int)statbuf->mtime,
          (int)statbuf->ctime,
          statbuf->fileclass
          );

  
  msgLen = strlen(expertBuffer);
  rc = expert_send_request(&fd,EXP_RQ_MIGRATOR);

  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("expert_send_request()");
    return(-1);
  }

  rc = expert_send_data(fd,expertBuffer,msgLen);
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
  if ( rc == 0 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stdout,"Policy returns skip from migration, %s\n",
              expertBuffer);
    }
  }
  
  return(rc);
}    

static int getMigrCandidates(
                             svcClass,
                             migrCandidates,
                             nsFileClassArray,
                             nbMigrCandidates,
                             initialSizeToTransfer
                             )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t ***migrCandidates;
     struct Cns_fileclass ***nsFileClassArray;
     int *nbMigrCandidates;
     u_signed64 *initialSizeToTransfer;
{
  struct Cstager_TapeCopy_t **tapeCopyArray = NULL;
  struct Cstager_CastorFile_t *castorFile;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  char *nsHost, castorFileName[CA_MAXPATHLEN+1];
  struct Cns_fileid fileId;
  struct Cns_fileclass fileClass, **fileClassArray = NULL;
  struct Cns_filestat statbuf;
  int rc, nbTapeCopies, i;
  u_signed64 sz = 0;

  if ( svcClass == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getMigrCandidates() called without SvcClass\n");
    }
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getMigrCandidates(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return(-1);
  }

  LOG_CALL_TRACE((rc = Cstager_ITapeSvc_selectTapeCopiesForMigration(
                                                                       tpSvc,
                                                                       svcClass,
                                                                       &tapeCopyArray,
                                                                       &nbTapeCopies
                                                                       )));
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"Cstager_ITapeSvc_selectTapeCopiesForMigration(): %s, %s\n",
              sstrerror(serrno),
              Cstager_ITapeSvc_errorMsg(tpSvc));
    }
    LOG_DBCALL_ERR("Cstager_ITapeSvc_selectTapeCopiesForMigration()",
                   Cstager_ITapeSvc_errorMsg(tpSvc));
    C_IAddress_delete(iAddr);
    return(-1);
  }

  if ( legacyMode == 1 ) {
    fileClassArray = (struct Cns_fileclass **)calloc(nbTapeCopies,
                                                     sizeof(struct Cns_fileclass *));
  }
  for ( i=0; i<nbTapeCopies; i++ ) {
    iObj = Cstager_TapeCopy_getIObject(tapeCopyArray[i]);
    rc = C_Services_fillObj(
                            dbSvc,
                            iAddr,
                            iObj,
                            OBJ_CastorFile
                            );
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"C_Services_fillObj(tapeCopy,OBJ_CastorFile): %s, %s\n",
                sstrerror(serrno),
                C_Services_errorMsg(dbSvc));
      }
      LOG_DBCALL_ERR("C_Services_fillObj()",
                     C_Services_errorMsg(dbSvc));
      continue;
    }
    castorFile = NULL;
    castorFileName[0] = '\0';
    Cstager_TapeCopy_castorFile(tapeCopyArray[i],&castorFile);
    memset(&fileId,'\0',sizeof(fileId));
    Cstager_CastorFile_fileId(castorFile,&(fileId.fileid));
    nsHost = NULL;
    Cstager_CastorFile_nsHost(castorFile,(CONST char **)&nsHost);
    if ( nsHost != NULL ) {
      strncpy(fileId.server,nsHost,sizeof(fileId.server)-1);
    }
    Cstager_CastorFile_fileSize(castorFile,&sz);
    *initialSizeToTransfer += sz;
    if ( legacyMode == 1 ) {
      rc = Cns_statx(castorFileName,&fileId,&statbuf);
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"Cns_statx(): %s\n",sstrerror(serrno));
        }
        LOG_SYSCALL_ERR("Cns_statx()");
        continue;
      }
      if ( legacyMode == 1 ) {
        memset(&fileClass,'\0',sizeof(fileClass));
        rc = Cns_queryclass(
                            fileId.server,
                            statbuf.fileclass,
                            NULL,
                            &fileClass
                            );
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"Cns_queryclass(%d): %s\n",
                    statbuf.fileclass,sstrerror(serrno));
          }
          LOG_SYSCALL_ERR("Cns_Queryclass()");
          continue;
        }
        fileClassArray[i] = (struct Cns_fileclass *)calloc(1,sizeof(struct Cns_fileclass));
        memcpy(fileClassArray[i],&fileClass,sizeof(struct Cns_fileclass));
      }
    }
  }
  *nsFileClassArray = fileClassArray;
  *migrCandidates = tapeCopyArray;
  *nbMigrCandidates = nbTapeCopies;
  C_IAddress_delete(iAddr);
  
  return(0);
}

static void freeFileClassArray(
                               nsFileClassArray,
                               nbItems
                               )
     struct Cns_fileclass **nsFileClassArray;
     int nbItems;
{
  int i;
  if ( nsFileClassArray == NULL ) return;
  
  for ( i=0; i<nbItems; i++ ) {
    if ( nsFileClassArray[i] != NULL ) {
      if ( nsFileClassArray[i]->tppools != NULL ) {
        free(nsFileClassArray[i]->tppools);
      }
      free(nsFileClassArray[i]);
      nsFileClassArray[i] = NULL;
    }
  }
  free(nsFileClassArray);
  return;
}

static void freeMigrCandidates(
                               migrCandidates,
                               nbMigrCandidates
                               )
     struct Cstager_TapeCopy_t **migrCandidates;
     int nbMigrCandidates;
{
  int i;
  struct Cstager_CastorFile_t *castorFile;
  
  if ( migrCandidates == NULL ) return;
  
  for ( i=0; i<nbMigrCandidates; i++ ) {
    castorFile = NULL;
    Cstager_TapeCopy_castorFile(migrCandidates[i],&castorFile);
    if ( castorFile != NULL ) Cstager_CastorFile_delete(castorFile);
    else Cstager_TapeCopy_delete(migrCandidates[i]);
  }
  free(migrCandidates);
  return;
}

static int addMigrationCandidatesToStreams(
                                           svcClass,
                                           tapeCopyArray,
                                           nsFileClassArray,
                                           nbTapeCopies
                                           )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t **tapeCopyArray;
     struct Cns_fileclass **nsFileClassArray;
     int nbTapeCopies;
{
  struct Cstager_CastorFile_t *castorFile;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  ID_TYPE key;
  int rc, i;
  char *nsHost, buf[22];
  struct Cns_fileid fileid;

  if ( svcClass == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addMigrationCandidatesToStreams() called without SvcClass\n");
    }
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addMigrationCandidatesToStreams(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return(-1);
  }

  for ( i=0; i<nbTapeCopies; i++ ) {
    rc = addTapeCopyToStreams(
                              svcClass,
                              tapeCopyArray[i],
                              (nsFileClassArray == NULL ? NULL : nsFileClassArray[i])
                              );
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"addTapeCopyToStreams(): %s\n",sstrerror(serrno));
      }
      LOG_SYSCALL_ERR("addTapeCopyToStreams()");
    }

    iObj = Cstager_TapeCopy_getIObject(tapeCopyArray[i]);
    if ( rc == 1 ) {
      rc = C_Services_fillRep(
                              dbSvc,
                              iAddr,
                              iObj,
                              OBJ_Stream,
                              1
                              );
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_fillRep(tapeCopy,OBJ_Stream): %s, %s\n",
                  sstrerror(serrno),
                  C_Services_errorMsg(dbSvc));
        }
        LOG_DBCALL_ERR("C_Services_fillRep(tapeCopy,OBJ_Stream)",
                       C_Services_errorMsg(dbSvc));
        C_IAddress_delete(iAddr);
        return(-1);
      }
    } else {
      /*
       * The tape copy was not added to a stream. Maybe because
       * of an error when calling policy engine or because
       * the number of TapePools exceeds the nbDrives in SvcClass.
       * Note that selectTapeCopiesForMigration() has set the
       * atomically set the status to TAPECOPY_WAITINSTREAMS. 
       * We should therefore reset the TapeCopy status to 
       * TAPECOPY_TOBEMIGRATED so that it is picked up next time
       *
       * In case of an error, we better set PUT_FAILED and log
       * an alert for manual check.
       */
      Cstager_TapeCopy_id(tapeCopyArray[i],&key);
      Cstager_TapeCopy_setStatus(tapeCopyArray[i],TAPECOPY_FAILED);
      Cstager_TapeCopy_castorFile(tapeCopyArray[i],&castorFile);
      memset(&fileid,'\0',sizeof(fileid));
      nsHost = NULL;
      Cstager_CastorFile_fileId(castorFile,&(fileid.fileid));
      Cstager_CastorFile_nsHost(castorFile,(CONST char **)&nsHost);
      strcpy(fileid.server,nsHost);
      if ( rc == -1 ) {
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
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"PUT_FAILED: %s@%s\n",u64tostr(fileid.fileid,buf,-1),
                  fileid.server);
        }
      } else {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"Reset to TAPECOPY_TOBEMIGRATED: %s@%s\n",
                  u64tostr(fileid.fileid,buf,-1),
                  fileid.server);
        }
        Cstager_TapeCopy_setStatus(tapeCopyArray[i],TAPECOPY_TOBEMIGRATED);
      }
      
      rc = C_Services_updateRep(
                                dbSvc,
                                iAddr,
                                iObj,
                                1
                                );
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_updateRep(tapeCopy): %s, %s\n",
                  sstrerror(serrno),
                  C_Services_errorMsg(dbSvc));
        }
        LOG_DBCALL_ERR("C_Services_updateRep(tapeCopy)",
                       C_Services_errorMsg(dbSvc));
        C_IAddress_delete(iAddr);
        return(-1);
      }
    }
  }
  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

static void shutdownService(
                            signo
                            )
     int signo;
{
  (void)dlf_write(
                  mainUuid,
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_SHUTDOWN),
                  (struct Cns_fileid *)NULL,
                  1,
                  "SIGNAL",
                  DLF_MSG_PARAM_INT,
                  signo
                  );
  if ( (_migrCandidates != NULL) && (_nbMigrCandidates != NULL) ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stdout,"Trapped signal %d: Restore %d migration candidates\n",
              signo,*_nbMigrCandidates);
    }
    restoreMigrCandidates(*_migrCandidates,*_nbMigrCandidates);
  }
  exit(0);
  return;
}

static void usage(
                  cmd
                  )
     char *cmd;
     
{
  fprintf(stdout,"Usage: %s [-d] [-L] [-t sleepTime(seconds)] svcClass1 svcClass2 svcClass3 ...\n"
          "-C                     : clone tapecopies from existing to new streams (very slow!)\n"
          "-d                     : run as runAsDaemon\n"
          "-L                     : Legacy mode, emulate old stgdaemon taking information from Cns\n"
          "-t sleepTime(seconds)  : sleep time (in seconds) between two checks. Default=300\n"
          "-v volume(bytes)       : data volume threshold below migration will not start\n"
          "For instance:\n"
          "%s default\n",cmd,cmd);
  return;
}

int main(int argc, char *argv[]) 
{
  struct Cstager_SvcClass_t *svcClass = NULL;
  struct Cstager_TapeCopy_t **migrCandidates = NULL;
  struct Cns_fileclass **nsFileClassArray = NULL;
  struct C_Services_t *dbSvc;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_ITapeSvc_t *tpSvc = NULL;
  char *migHunterFacilityName = "MigHunter";
  u_signed64 initialSizeToTransfer, volumeThreshold = 0;
  char buf[32];
  int c, rc, i, nbMigrCandidates = 0, nbOldStreams = 0, nbNewStreams = 0;
  time_t sleepTime = 0;
#if !defined(_WIN32)
  struct sigaction saOther;
#endif /* _WIN32 */

  _migrCandidates = &migrCandidates;
  _nbMigrCandidates = &nbMigrCandidates;
#if !defined(_WIN32)
  memset(&saOther,'\0',sizeof(saOther));
  saOther.sa_handler = SIG_IGN;
  sigaction(SIGPIPE,&saOther,NULL);
  sigaction(SIGXFSZ,&saOther,NULL);
  saOther.sa_handler = shutdownService;
  sigaction(SIGINT,&saOther,NULL);
  sigaction(SIGTERM,&saOther,NULL);
  sigaction(SIGABRT,&saOther,NULL);
#endif /* _WIN32 */
  
  cmdName = argv[0];
  if ( argc <= 1 ) {
    usage(argv[0]);
    return(1);
  }

  Coptind = 1;
  Copterr = 1;
  
  while ( (c = Cgetopt(argc,argv,"CdLt:v:")) != -1 ) {
    switch (c) {
    case 'C':
      doCloneStream = 1;
      break;
    case 'd':
      runAsDaemon = 1;
      break;
    case 'L':
      legacyMode = 1;
      break;
    case 't':
      sleepTime = atoi(Coptarg);
      break;
    case 'v':
      volumeThreshold = strutou64(Coptarg);
      break;
    default:
      usage(argv[0]);
      return(2);
    }
  }
  
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);
  Cuuid_create(
               &mainUuid
               );
  (void)rtcpcld_initLogging(migHunterFacilityName);

#if defined(__DATE__) && defined (__TIME__)
  (void)dlf_write(
                  mainUuid,
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_STARTUP),
                  (struct Cns_fileid *)NULL,
                  3,
                  "GENERATED_DATE",DLF_MSG_PARAM_STR,__DATE__,
                  "GENERATED_TIME",DLF_MSG_PARAM_STR,__TIME__,
                  "SERVICE",DLF_MSG_PARAM_STR,
                  (cmdName != NULL ? cmdName : "(null)")
                  );
#endif /* __DATE__ && __TIME__ */
  
  if ( runAsDaemon == 1 ) {
    if ( (Cinitdaemon("MigHunter",SIG_IGN) == -1) ) exit(1);
    if ( sleepTime == 0 ) sleepTime = 300;
  }
  
  rc = prepareForDBAccess(&dbSvc,&tpSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    }
    LOG_SYSCALL_ERR("prepareForDBAccess()");
    return(1);
  }
  do {
    for ( i=Coptind; i<argc; i++ ) {
      (void)C_Services_rollback(dbSvc,iAddr);
      if ( svcClass != NULL ) Cstager_SvcClass_delete(svcClass);
      svcClass = NULL;
      if ( runAsDaemon == 0 ) {
        fprintf(stdout,"Get service class %s\n",argv[i]);
      }
      rc = getSvcClass(argv[i],&svcClass);
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"getSvcClass(): %s\n",sstrerror(serrno));
        }
        LOG_SYSCALL_ERR("getSvcClass()");
        continue;
      }
      freeMigrCandidates(migrCandidates,nbMigrCandidates);
      freeFileClassArray(nsFileClassArray,nbMigrCandidates);
      migrCandidates = NULL;
      nsFileClassArray = NULL;
      nbMigrCandidates = 0;
      initialSizeToTransfer = 0;
      
      if ( runAsDaemon == 0 ) {
        fprintf(stdout,"Get migration candidates for %s\n",argv[i]);
      }
      rc = getMigrCandidates(
                             svcClass,
                             &migrCandidates,
                             &nsFileClassArray,
                             &nbMigrCandidates,
                             &initialSizeToTransfer
                             );
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"getMigrCandidates(%s): %s\n",argv[i],sstrerror(serrno));
        }
        LOG_SYSCALL_ERR("getMigrCandidates()");
        continue;
      }
      if ( runAsDaemon == 0 ) {
        fprintf(stdout,"Found %d candidates for %s\n",nbMigrCandidates,argv[i]);
      }
      (void)dlf_write(
                      mainUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_MIGRCANDS),
                      (struct Cns_fileid *)NULL,
                      2,
                      "SVCCLASS",
                      DLF_MSG_PARAM_STR,
                      argv[i],
                      "NBCANDS",
                      DLF_MSG_PARAM_INT,
                      nbMigrCandidates
                      );
      
      if ( runAsDaemon == 0 ) {
        fprintf(stdout,"Create new tape pools for %s if necessary\n",argv[i]);
      }
      if ( legacyMode == 1 ) {
        rc = tapePoolCreator(
                             svcClass,
                             migrCandidates,
                             nsFileClassArray,
                             nbMigrCandidates
                             );
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"tapePoolCreator(%s): %s\n",
                    argv[i],sstrerror(serrno));
          }
          LOG_SYSCALL_ERR("tapePoolCreator()");
          restoreMigrCandidates(migrCandidates,nbMigrCandidates);
          continue;
        }
      }
      if ( runAsDaemon == 0 ) {
        fprintf(stdout,"Initial size to transfer: %s\n",
                u64tostr(initialSizeToTransfer,buf,(int)(-sizeof(buf))));
        fprintf(stdout,"Get existing streams for %s\n",argv[i]);
      }
      
      /*
       * This one takes a lock on all Streams associated
       * with the SvcClass
       */
      nbOldStreams = nbNewStreams = 0;
      rc = getStreamsForSvcClass(svcClass,&nbOldStreams);
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"getStreamsForSvcClass(%s): %s\n",
                  argv[i],sstrerror(serrno));
        }
        if ( (serrno != ENOENT) && (nbMigrCandidates>0) ) {
          (void)dlf_write(
                          mainUuid,
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_NOTPPOOLS),
                          (struct Cns_fileid *)NULL,
                          2,
                          "SVCCLASS",
                          DLF_MSG_PARAM_STR,
                          argv[i],
                          "NBCANDS",
                          DLF_MSG_PARAM_INT,
                          nbMigrCandidates
                          );
        }
        restoreMigrCandidates(migrCandidates,nbMigrCandidates);
        continue;
      }
      if ( (nbOldStreams <= 0) && (initialSizeToTransfer<volumeThreshold) ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Not enough data to migrate: %s < %s\n",
                  u64tostr(initialSizeToTransfer,buf,(int)(-sizeof(buf))),
                  u64tostr(volumeThreshold,buf,(int)(-sizeof(buf))));
        }
        (void)dlf_write(
                        mainUuid,
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_DATALIMIT),
                        (struct Cns_fileid *)NULL,
                        3,
                        "SVCCLASS",
                        DLF_MSG_PARAM_STR,
                        argv[i],
                        "MIGRVOLUM",
                        DLF_MSG_PARAM_INT64,
                        initialSizeToTransfer,
                        "THRESHOLD",
                        DLF_MSG_PARAM_INT64,
                        volumeThreshold
                        );
        (void)C_Services_rollback(dbSvc,iAddr);
        restoreMigrCandidates(migrCandidates,nbMigrCandidates);
      }
      
      if ( (nbOldStreams >= 0) &&
           ((doCloneStream == 1) || (nbMigrCandidates > 0)) ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Found %d streams for %s. Create new streams if necessary\n",
                  nbOldStreams,argv[i]);
        }
        rc = streamCreator(svcClass,initialSizeToTransfer,&nbNewStreams);
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"streamCreator(%s): %s\n",
                    argv[i],sstrerror(serrno));
          }
          LOG_SYSCALL_ERR("streamCreator()");
          (void)C_Services_rollback(dbSvc,iAddr);
          restoreMigrCandidates(migrCandidates,nbMigrCandidates);
          continue;
        }
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"%d new streams created for %s\n",nbNewStreams,argv[i]);
        }
        (void)dlf_write(
                        mainUuid,
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_NBSTREAMS),
                        (struct Cns_fileid *)NULL,
                        3,
                        "SVCCLASS",
                        DLF_MSG_PARAM_STR,
                        argv[i],
                        "NBOLDSTR",
                        DLF_MSG_PARAM_INT,
                        nbOldStreams,
                        "NBNEWSTR",
                        DLF_MSG_PARAM_INT,
                        nbNewStreams
                        );
      }
      /*
       * Release the lock on streams. The adding of migration
       * candidates can be a lenghty process...
       */
      rc = C_Services_commit(dbSvc,iAddr);
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_commit(): %s\n",
                  sstrerror(serrno));
        }
        LOG_SYSCALL_ERR("C_Services_commit()");
      }
      if ( nbMigrCandidates > 0 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Add migration candidates to streams for %s\n",
                  argv[i]);
        }
        rc = addMigrationCandidatesToStreams(
                                             svcClass,
                                             migrCandidates,
                                             nsFileClassArray,
                                             nbMigrCandidates
                                             );
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"addMigrationCandidatesToStreams(%s): %s\n",
                    argv[i],sstrerror(serrno));
          }
          LOG_SYSCALL_ERR("addMigrationCandidatesToStreams()");
          (void)C_Services_rollback(dbSvc,iAddr);
          restoreMigrCandidates(migrCandidates,nbMigrCandidates);
          continue;
        }
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Start the streams for %s\n",argv[i]);
        }
      }
      rc = startStreams(svcClass,initialSizeToTransfer);
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"startStreams(%s): %s\n",
                  argv[i],sstrerror(serrno));
        }
        LOG_SYSCALL_ERR("startStreams()");
        (void)C_Services_rollback(dbSvc,iAddr);
        continue;
      }
      rc = C_Services_commit(dbSvc,iAddr);
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_commit(): %s\n",
                  sstrerror(serrno));
        }
        LOG_SYSCALL_ERR("C_Services_commit()");
      }
    }
    sleep(sleepTime);
  } while (sleepTime > 0);

  shutdownService(0);
  return(0);
}
