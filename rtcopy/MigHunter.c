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
 * @(#)$RCSfile: MigHunter.c,v $ $Revision: 1.7 $ $Release$ $Date: 2004/12/09 17:49:44 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: MigHunter.c,v $ $Revision: 1.7 $ $Release$ $Date: 2004/12/09 17:49:44 $ Olof Barring";
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
#include <castor/stager/IStagerSvc.h>
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
static int runAsDaemon = 0;
/** Global needed for determine which uuid to log
 */
int inChild;
/** uuid's set by calling process (rtcpclientd:mainUuid, VidWorker:childUuid)
 */
Cuuid_t childUuid, mainUuid;

static int prepareForDBAccess(
                              _dbSvc,
                              _stgSvc,
                              _iAddr
                              )
  struct Cstager_IStagerSvc_t **_stgSvc;
  struct C_Services_t **_dbSvc;
  struct C_IAddress_t **_iAddr;
{
  struct Cstager_IStagerSvc_t **stgSvc;
  struct C_Services_t **dbSvc;
  struct C_IAddress_t *iAddr;
  struct C_BaseAddress_t *baseAddr;
  int rc;

  dbSvc = NULL;
  rc = rtcpcld_getDbSvc(&dbSvc);

  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getDbSvc(): %s\n",sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("getDbSvc()");
    }
    return(-1);
  }

  stgSvc = NULL;
  rc = rtcpcld_getStgSvc(&stgSvc);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getStgSvc(): %s\n",sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("getStgSvc()");
    }
    return(-1);
  }

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"C_BaseAddress_create(): %s\n",sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("C_BaseAddress_create()");
    }
    return(-11);
  }
  
  C_BaseAddress_setCnvSvcName(baseAddr,"OraCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_ORACNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);

  if ( _dbSvc != NULL ) *_dbSvc = *dbSvc;
  if ( _stgSvc != NULL ) *_stgSvc = *stgSvc;
  if ( _iAddr != NULL ) *_iAddr = iAddr;
  
  return(0);
}
     

int getSvcClass(
                svcClassName,
                svcClass
                )
     char *svcClassName;
     struct Cstager_SvcClass_t **svcClass;
{
  struct Cstager_SvcClass_t *svcClassTmp = NULL;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  struct C_Services_t *dbSvc = NULL;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  int rc;
  
  if ( svcClassName == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass() called without SvcClass name\n");
    }
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("getSvcClass()");
    }
    return(-1);
  }

  rc = Cstager_IStagerSvc_selectSvcClass(
                                         stgSvc,
                                         &svcClassTmp,
                                         svcClassName
                                         );
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"Cstager_IStagerSvc_selectSvcClass(%s): %s, %s\n",
              svcClassName,
              sstrerror(serrno),
              Cstager_IStagerSvc_errorMsg(stgSvc));
    } else {
      LOG_DBCALL_ERR("Cstager_IStagerSvc_selectSvcClass()",
                     Cstager_IStagerSvc_errorMsg(stgSvc));
    }
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
    } else {
      LOG_DBCALL_ERR("C_Services_fillObj(svcClass,OBJ_TapePool)",
                     C_Services_errorMsg(dbSvc));
    }
    C_IAddress_delete(iAddr);
    return(-1);
  }

  if ( svcClass != NULL ) *svcClass = svcClassTmp;
  C_IAddress_delete(iAddr);
  return(0);
}

int getStreamsFromDB(
                     tapePool
                     )
     struct Cstager_TapePool_t *tapePool;
{
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  int rc;

  if ( tapePool == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getStreamsFromDB() called without TapePool\n");
    }
    serrno = EINVAL;
    return(-1);
  }
  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("prepareForDBAccess()");
    }
    return(-1);
  }
  iObj = Cstager_TapePool_getIObject(tapePool);
  rc = Cstager_IStagerSvc_streamsForTapePool(stgSvc,tapePool);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"Cstager_IStagerSvc_streamsForTapePool %s, %s\n",
              sstrerror(serrno),
              Cstager_IStagerSvc_errorMsg(stgSvc));
    } else {
      LOG_DBCALL_ERR("Cstager_IStagerSvc_streamsForTapePool()",
                     Cstager_IStagerSvc_errorMsg(stgSvc));
    }
  }

  C_IAddress_delete(iAddr);
  return(rc);
}

int findTapePoolInSvcClass(
                           svcClass,
                           tapePoolName
                           )
     struct Cstager_SvcClass_t *svcClass;
     char *tapePoolName;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  int rc, i, nbTapePools = 0;
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

int findTapePoolInFileClass(
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

int tapePoolCreator(
                    svcClass,
                    tapeCopyArray,
                    fileClass,
                    nbTapeCopies
                    )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t **tapeCopyArray;
     struct Cns_fileclass *fileClass;
     int nbTapeCopies;
{
  struct Cstager_TapePool_t *newTapePool;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  char  *tapePoolName;
  u_signed64 sz;
  int rc, i, j;

  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addMigrationCandidatesToStreams(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("prepareForDBAccess()");
    }
    return(-1);
  }

  for ( i=0; i<nbTapeCopies; i++ ) {
    iObj = Cstager_TapeCopy_getIObject(tapeCopyArray[i]);
    for ( j=0; j<fileClass->nbtppools; j++ ) {
      tapePoolName = fileClass->tppools+j*(CA_MAXCLASNAMELEN+1);
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
          } else {
            LOG_DBCALL_ERR("C_Services_fillRep()",
                           C_Services_errorMsg(dbSvc));
          }
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
    } else {
      LOG_DBCALL_ERR("C_Services_commit()",
                     C_Services_errorMsg(dbSvc));
    }
    C_IAddress_delete(iAddr);
    return(-1);
  }
  C_IAddress_delete(iAddr);
  return(0);
}

int getStreamsForSvcClass(
                          svcClass
                          )
     struct Cstager_SvcClass_t *svcClass;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  int rc, i, nbTapePools;

  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  if ( (nbTapePools <= 0) || (tapePoolArray == NULL) ){
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getStreamsForSvcClass(): no tape pools associated with SvcClass\n");
    }
    serrno = ENOENT;
    return(-1);
  }

  for ( i=0; i<nbTapePools; i++ ) {
    rc = getStreamsFromDB(tapePoolArray[i]);
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"getStreamsFromDB(): %s\n",sstrerror(serrno));
      } else {
        LOG_SYSCALL_ERR("getStreamsFromDB()");
      }
      free(tapePoolArray);
      return(-1);
    }
  }
  if ( tapePoolArray != NULL ) free(tapePoolArray);
  return(0);
}

int streamCreator(
                  svcClass,
                  initialSizeToTransfer
                  )
     struct Cstager_SvcClass_t *svcClass;
     u_signed64 initialSizeToTransfer;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  struct Cstager_Stream_t *newStream, **streamArray = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  int rc, nbTapePools, nbStreams, nbStreamsTotal, nbDrives, i, j;
  int *streamsPerTapePool = NULL;
  
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
    } else {
      LOG_SYSCALL_ERR("calloc()");
    }
    serrno = SESYSERR;
    return(-1);
  }
  
  nbStreamsTotal = 0;
  for ( i=0; i<nbTapePools; i++ ) {
    Cstager_TapePool_streams(tapePoolArray[i],&streamArray,&nbStreams);
    if ( (nbStreams > 0) && (streamArray != NULL) ) {
      nbStreamsTotal += nbStreams;
      streamsPerTapePool[i] = nbStreams;
      free(streamArray);
    }
  }
  
  if ( nbStreamsTotal != nbDrives ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stdout,"SvcClass allows for %d streams, found %d\n",
              nbDrives,nbStreamsTotal);
    }
    rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
                sstrerror(serrno));
      } else {
        LOG_SYSCALL_ERR("prepareForDBAccess()");
      }
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
       * Set state to STREAM_WAITSPACE just to block it from being
       * immediately swallowed by streamsToDo() before we had
       * a chance to throw TapeCopies into it. Later a more proper
       * status like STREAM_CREATED should be used but for the moment
       * that status does not exist.
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
                              0
                              );
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_fillRep(tapePool,OBJ_Stream) %s, %s\n",
                  sstrerror(serrno),
                  C_Services_errorMsg(dbSvc));
        } else {
          LOG_DBCALL_ERR("C_Services_fillRep(tapePool,OBJ_Stream)",
                         C_Services_errorMsg(dbSvc));
        }
        C_IAddress_delete(iAddr);
        return(-1);
      }
    }
  }

  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

int startStreams(
                 svcClass
                 )
     struct Cstager_SvcClass_t *svcClass;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL, *newTapePool;
  struct Cstager_Stream_t **streamArray = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  enum Cstager_StreamStatusCodes_t streamStatus;
  char *tapePoolName;
  int rc, i, j, nbTapePools = 0, nbStreams = 0;
  
  if ( svcClass == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"startStreams(%p) called with NULL argument\n",
              svcClass);
    }
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addMigrationCandidatesToStreams(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("prepareForDBAccess()");
    }
    return(-1);
  }

  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  for ( i=0; i<nbTapePools; i++ ) {
    Cstager_TapePool_streams(tapePoolArray[i],&streamArray,&nbStreams);
    tapePoolName = NULL;
    Cstager_TapePool_name(tapePoolArray[i],(CONST char **)&tapePoolName);
    for ( j=0; j<nbStreams; j++ ) {
      Cstager_Stream_status(streamArray[j],&streamStatus);
      if ( streamStatus == STREAM_CREATED ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Start %d stream for tape pool %s\n",
                  j,tapePoolName);
        }
        Cstager_Stream_setStatus(streamArray[j],STREAM_PENDING);
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
          } else {
            LOG_DBCALL_ERR("C_Services_updateRep()",
                           C_Services_errorMsg(dbSvc));
          }
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

int addTapeCopyToStreams(
                         svcClass,
                         tapeCopy,
                         fileClass
                         )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t *tapeCopy;
     struct Cns_fileclass *fileClass;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL, *newTapePool;
  struct Cstager_Stream_t **streamArray = NULL;
  char *tapePoolName;
  int rc, i, j, nbTapePools = 0, nbStreams = 0, addedToStream = 0;

  if ( (svcClass == NULL) || (tapeCopy == NULL) ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addTapeCopyToStream(%p,%p) called with NULL argument\n",
              svcClass,tapeCopy);
    }
    serrno = EINVAL;
    return(-1);
  }

  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  for ( i=0; i<nbTapePools; i++ ) {
    Cstager_TapePool_name(tapePoolArray[i],(CONST char **)&tapePoolName);
    if ( findTapePoolInFileClass(fileClass,tapePoolName) == 1 ) {
      addedToStream = 1;
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

int getMigrCandidates(
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
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  char *nsHost, *tapePoolName, castorFileName[CA_MAXPATHLEN+1];
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

  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getMigrCandidates(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("prepareForDBAccess()");
    }
    return(-1);
  }

  rc = Cstager_IStagerSvc_selectTapeCopiesForMigration(
                                                       stgSvc,
                                                       svcClass,
                                                       &tapeCopyArray,
                                                       &nbTapeCopies
                                                       );
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"Cstager_IStagerSvc_selectTapeCopiesForMigration(): %s, %s\n",
              sstrerror(serrno),
              Cstager_IStagerSvc_errorMsg(stgSvc));
    } else {
      LOG_DBCALL_ERR("Cstager_IStagerSvc_selectTapeCopiesForMigration()",
                     Cstager_IStagerSvc_errorMsg(stgSvc));
    }
    C_IAddress_delete(iAddr);
    return(-1);
  }

  fileClassArray = (struct Cns_fileclass **)calloc(nbTapeCopies,
                                                   sizeof(struct Cns_fileclass *));
  for ( i=0; i<nbTapeCopies; i++ ) {
    memset(&fileClass,'\0',sizeof(fileClass));
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
      } else {
        LOG_DBCALL_ERR("C_Services_fillObj()",
                       C_Services_errorMsg(dbSvc));
      }
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
    rc = Cns_statx(castorFileName,&fileId,&statbuf);
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"Cns_statx(): %s\n",sstrerror(serrno));
      } else {
        LOG_SYSCALL_ERR("Cns_statx()");
      }
      continue;
    }
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
      } else {
        LOG_SYSCALL_ERR("Cns_Queryclass()");
      }
      continue;
    }
    fileClassArray[i] = (struct Cns_fileclass *)calloc(1,sizeof(struct Cns_fileclass));
    memcpy(fileClassArray[i],&fileClass,sizeof(struct Cns_fileclass));
  }
  *migrCandidates = tapeCopyArray;
  *nsFileClassArray = fileClassArray;
  *nbMigrCandidates = nbTapeCopies;
  C_IAddress_delete(iAddr);
  return(0);
}

void freeFileClassArray(
                        nsFileClassArray,
                        nbItems
                        )
     struct Cns_fileclass **nsFileClassArray;
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

int addMigrationCandidatesToStreams(
                                    svcClass,
                                    tapeCopyArray,
                                    nbTapeCopies
                                    )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t **tapeCopyArray;
     int nbTapeCopies;
{
  struct Cstager_CastorFile_t *castorFile;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  int rc, i;

  if ( svcClass == NULL ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addMigrationCandidatesToStreams() called without SvcClass\n");
    }
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"addMigrationCandidatesToStreams(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("prepareForDBAccess()");
    }
    return(-1);
  }

  for ( i=0; i<nbTapeCopies; i++ ) {
    rc = addTapeCopyToStreams(
                              svcClass,
                              tapeCopyArray[i]
                              );
    if ( rc == -1 ) {
      if ( runAsDaemon == 0 ) {
        fprintf(stderr,"addTapeCopyToStreams(): %s\n",sstrerror(serrno));
      } else {
        LOG_SYSCALL_ERR("addTapeCopyToStreams()");
      }
      C_IAddress_delete(iAddr);
      return(-1);
    }

    iObj = Cstager_TapeCopy_getIObject(tapeCopyArray[i]);
    if ( rc == 1 ) {
      rc = C_Services_fillRep(
                              dbSvc,
                              iAddr,
                              iObj,
                              OBJ_Stream,
                              0
                              );
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_fillRep(tapeCopy,OBJ_Stream): %s, %s\n",
                  sstrerror(serrno),
                  C_Services_errorMsg(dbSvc));
        } else {
          LOG_DBCALL_ERR("C_Services_fillRep(tapeCopy,OBJ_Stream)",
                         C_Services_errorMsg(dbSvc));
        }
        C_IAddress_delete(iAddr);
        return(-1);
      }
    } else {
      /*
       * The tape copy was not added to a stream. Probably because
       * the number of TapePools exceeds the nbDrives in SvcClass.
       * Note that selectTapeCopiesForMigration() has set the
       * atomically set the status to TAPECOPY_WAITINSTREAMS. 
       * We should therefore reset the TapeCopy status to 
       * TAPECOPY_TOBEMIGRATED so that it is picked up next time
       */
      Cstager_TapeCopy_setStatus(tapeCopyArray[i],TAPECOPY_TOBEMIGRATED);
      rc = C_Services_updateRep(
                                dbSvc,
                                iAddr,
                                iObj,
                                0
                                );
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"C_Services_updateRep(tapeCopy): %s, %s\n",
                  sstrerror(serrno),
                  C_Services_errorMsg(dbSvc));
        } else {
          LOG_DBCALL_ERR("C_Services_updateRep(tapeCopy)",
                         C_Services_errorMsg(dbSvc));
        }
        C_IAddress_delete(iAddr);
        return(-1);
      }
    }
  }
  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

void usage(char *cmd) 
{
  fprintf(stdout,"Usage: %s [-d] [-t sleepTime(seconds)] svcClass1 svcClass2 svcClass3 ...\n"
          "-d                     : run as runAsDaemon\n"
          "-t sleepTime(seconds)  : sleep time (in seconds) between two checks. Default=300\n"
          "For instance:\n"
          "%s default\n",cmd,cmd);
  return;
}

int main(int argc, char *argv[]) 
{
  struct Cstager_SvcClass_t *svcClass = NULL;
  struct Cstager_TapeCopy_t **migrCandidates = NULL;
  struct Cns_fileclass **nsFileClassArray = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  char *migHunterFacilityName = "MigHunter";
  u_signed64 initialSizeToTransfer;
  char buf[32];
  int c, rc, i, nbMigrCandidates = 0;
  time_t sleepTime = 0;

  if ( argc <= 1 ) {
    usage(argv[0]);
    return(1);
  }

  Coptind = 1;
  Copterr = 1;
  
  while ( (c = Cgetopt(argc,argv,"dt:")) != -1 ) {
    switch (c) {
    case 'd':
      runAsDaemon = 1;
      break;
    case 't':
      sleepTime = atoi(Coptarg);
      break;
    default:
      usage(argv[0]);
      return(2);
    }
  }
  
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);
  
  if ( runAsDaemon == 1 ) {
    if ( (Cinitdaemon("MigHunter",SIG_IGN) == -1) ) exit(1);
    (void)rtcpcld_initLogging(migHunterFacilityName);
    if ( sleepTime == 0 ) sleepTime = 300;
  }
  
  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    if ( runAsDaemon == 0 ) {
      fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
    } else {
      LOG_SYSCALL_ERR("prepareForDBAccess()");
    }
    return(1);
  }
  do {
    for ( i=Coptind; i<argc; i++ ) {
      if ( svcClass != NULL ) Cstager_SvcClass_delete(svcClass);
      svcClass = NULL;
      if ( runAsDaemon == 0 ) {
        fprintf(stdout,"Get service class %s\n",argv[i]);
      }
      rc = getSvcClass(argv[i],&svcClass);
      if ( rc == -1 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stderr,"getSvcClass(): %s\n",sstrerror(serrno));
        } else {
          LOG_SYSCALL_ERR("getSvcClass()");
        }
        continue;
      }
      if ( migrCandidates != NULL ) free(migrCandidates);
      freeFileClassArray(nsFileClassArray);
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
        } else {
          LOG_SYSCALL_ERR("getMigrCandidates()");
        }
        continue;
      }
      if ( runAsDaemon == 0 ) {
        fprintf(stdout,"Found %d candidates for %s\n",nbMigrCandidates,argv[i]);
      }
      if ( nbMigrCandidates > 0 ) {
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Create new tape pools for %s if necessary\n",argv[i]);
        }
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
          } else {
            LOG_SYSCALL_ERR("tapePoolCreator()");
          }
          continue;
        }
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Initial size to transfer: %s\n",
                  u64tostr(initialSizeToTransfer,buf,-sizeof(buf)));
          fprintf(stdout,"Get existing streams for %s\n",argv[i]);
        }

        /*
         * This one takes a lock on all Streams associated
         * with the SvcClass
         */
        rc = getStreamsForSvcClass(svcClass);
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"getStreamsForSvcClass(%s): %s\n",
                    argv[i],sstrerror(serrno));
          } else {
            LOG_SYSCALL_ERR("getStreamsForSvcClass()");
          }
          continue;
        }
        
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Create new streams for %s if necessary\n",argv[i]);
        }
        rc = streamCreator(svcClass,initialSizeToTransfer);
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"streamCreator(%s): %s\n",
                    argv[i],sstrerror(serrno));
          } else {
            LOG_SYSCALL_ERR("streamCreator()");
          }
          (void)C_Services_rollback(dbSvc,iAddr);
          continue;
        }
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
          } else {
            LOG_SYSCALL_ERR("addMigrationCandidatesToStreams()");
          }
          (void)C_Services_rollback(dbSvc,iAddr);
          continue;
        }
        if ( runAsDaemon == 0 ) {
          fprintf(stdout,"Start the streams for %s\n",argv[i]);
        }
        rc = startStreams(svcClass);
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"startStreams(%s): %s\n",
                    argv[i],sstrerror(serrno));
          } else {
            LOG_SYSCALL_ERR("startStreams()");
          }
          (void)C_Services_rollback(dbSvc,iAddr);
          continue;
        }
        rc = C_Services_commit(dbSvc,iAddr);
        if ( rc == -1 ) {
          if ( runAsDaemon == 0 ) {
            fprintf(stderr,"C_Services_commit(): %s\n",
                    sstrerror(serrno));
          } else {
            LOG_SYSCALL_ERR("C_Services_commit()");
          }
        }
      }
    }
    sleep(sleepTime);
  } while (sleepTime > 0);
  return(0);
}
