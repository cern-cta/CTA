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
 * @(#)$RCSfile: MigHunter.c,v $ $Revision: 1.2 $ $Release$ $Date: 2004/12/06 13:23:26 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: MigHunter.c,v $ $Revision: 1.2 $ $Release$ $Date: 2004/12/06 13:23:26 $ Olof Barring";
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
    fprintf(stderr,"getDbSvc(): %s\n",sstrerror(serrno));
    return(-1);
  }

  stgSvc = NULL;
  rc = rtcpcld_getStgSvc(&stgSvc);
  if ( rc == -1 ) {
    fprintf(stderr,"getStgSvc()\n",sstrerror(serrno));
    return(-1);
  }

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) {
    fprintf(stderr,"C_BaseAddress_create(): %s\n",sstrerror(serrno));
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
    fprintf(stderr,"getSvcClass() called without SvcClass name\n");
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
            sstrerror(serrno));
    return(-1);
  }

  rc = Cstager_IStagerSvc_selectSvcClass(
                                         stgSvc,
                                         &svcClassTmp,
                                         svcClassName
                                         );
  if ( rc == -1 ) {
    fprintf(stderr,"Cstager_IStagerSvc_selectSvcClass(%s): %s, %s\n",
            svcClassName,
            sstrerror(serrno),
            Cstager_IStagerSvc_errorMsg(stgSvc));
    return(-1);
  }
  if ( svcClassTmp == NULL ) {
    fprintf(stderr,"SvcClass %s not found???\n",svcClassName);
    serrno = ENOENT;
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
    fprintf(stderr,"C_Services_fillObj(svcClass,OBJ_TapePool) %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(dbSvc));
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
    fprintf(stderr,"getStreamsFromDB() called without TapePool\n");
    serrno = EINVAL;
    return(-1);
  }
  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
            sstrerror(serrno));
    return(-1);
  }
  iObj = Cstager_TapePool_getIObject(tapePool);
  rc = C_Services_fillObj(
                          dbSvc,
                          iAddr,
                          iObj,
                          OBJ_Stream
                          );
  
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_fillObj(tapePool,OBJ_Stream) %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(dbSvc));
    return(-1);
  }

  C_IAddress_delete(iAddr);
  return(0);
}

int streamCreator(
                  svcClass
                  )
     struct Cstager_SvcClass_t *svcClass;
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
    fprintf(stderr,"streamCreator() called without SvcClass\n");
    serrno = EINVAL;
    return(-1);
  }

  Cstager_SvcClass_nbDrives(svcClass,&nbDrives);
  
  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  if ( (nbTapePools <= 0) || (tapePoolArray == NULL) ){
    fprintf(stderr,"streamCreator(): no tape pools associated with SvcClass\n");
    serrno = ENOENT;
    return(-1);
  }

  streamsPerTapePool = (int *)calloc(nbTapePools,sizeof(int));
  if ( streamsPerTapePool == NULL ) {
    fprintf(stderr,"calloc(): %s\n",strerror(errno));
    serrno = SESYSERR;
    return(-1);
  }
  
  nbStreamsTotal = 0;
  for ( i=0; i<nbTapePools; i++ ) {
    rc = getStreamsFromDB(tapePoolArray[i]);
    if ( rc == -1 ) {
      fprintf(stderr,"getStreamsFromDB(): %s\n",sstrerror(serrno));
      return(-1);
    }
    Cstager_TapePool_streams(tapePoolArray[i],&streamArray,&nbStreams);
    if ( (nbStreams > 0) && (streamArray != NULL) ) {
      nbStreamsTotal += nbStreams;
      streamsPerTapePool[i] = nbStreams;
      free(streamArray);
    }
  }
  
  if ( nbStreamsTotal != nbDrives ) {
    fprintf(stdout,"SvcClass allows for %d streams, found %d\n",
            nbDrives,nbStreamsTotal);
    rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
    if ( rc == -1 ) {
      fprintf(stderr,"getSvcClass(): prepareForDBAccess(): %s\n",
              sstrerror(serrno));
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
      Cstager_Stream_setStatus(newStream,STREAM_WAITSPACE);
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
        fprintf(stderr,"C_Services_fillRep(tapePool,OBJ_Stream) %s, %s\n",
                sstrerror(serrno),
                C_Services_errorMsg(dbSvc));
        return(-1);
      }
    }
    rc = C_Services_commit(
                           dbSvc,
                           iAddr
                           );
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_commit() %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(dbSvc));
      return(-1);
    }
  }

  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

int addTapeCopyToStreams(
                         svcClass,
                         tapeCopy
                         )
     struct Cstager_SvcClass_t *svcClass;
     struct Cstager_TapeCopy_t *tapeCopy;
{
  struct Cstager_TapePool_t **tapePoolArray = NULL;
  struct Cstager_Stream_t **streamArray = NULL;
  int rc, i, j, nbTapePools = 0, nbStreams = 0;

  if ( (svcClass == NULL) || (tapeCopy == NULL) ) {
    fprintf(stderr,"addTapeCopyToStream(%p,%p) called with NULL argument\n",
            svcClass,tapeCopy);
    serrno = EINVAL;
    return(-1);
  }
  Cstager_SvcClass_tapePools(svcClass,&tapePoolArray,&nbTapePools);
  for ( i=0; i<nbTapePools; i++ ) {
    Cstager_TapePool_streams(tapePoolArray[i],&streamArray,&nbStreams);
    for ( j=0; j<nbStreams; j++ ) {
      Cstager_Stream_addTapeCopy(streamArray[j],tapeCopy);
      Cstager_TapeCopy_addStream(tapeCopy,streamArray[j]);
    }
    if ( streamArray != NULL ) free(streamArray);
  }
  if ( tapePoolArray != NULL ) free(tapePoolArray);
  return(0);
}

int addMigrationCandidatesToStreams(
                                    svcClass
                                    )
     struct Cstager_SvcClass_t *svcClass;
{
  struct Cstager_TapeCopy_t **tapeCopyArray = NULL;
  struct C_IObject_t *iObj = NULL;
  struct C_Services_t *dbSvc;
  struct C_BaseAddress_t *baseAddr;
  struct C_IAddress_t *iAddr = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  int rc, nbTapeCopies, i;

  if ( svcClass == NULL ) {
    fprintf(stderr,"addMigrationCandidatesToStreams() called without SvcClass\n");
    serrno = EINVAL;
    return(-1);
  }

  rc = prepareForDBAccess(&dbSvc,&stgSvc,&iAddr);
  if ( rc == -1 ) {
    fprintf(stderr,"addMigrationCandidatesToStreams(): prepareForDBAccess(): %s\n",
            sstrerror(serrno));
    return(-1);
  }

  rc = Cstager_IStagerSvc_selectTapeCopiesForMigration(
                                                       stgSvc,
                                                       svcClass,
                                                       &tapeCopyArray,
                                                       &nbTapeCopies
                                                       );
  if ( rc == -1 ) {
    fprintf(stderr,"Cstager_IStagerSvc_selectTapeCopiesForMigration(): %s, %s\n",
            sstrerror(serrno),
            Cstager_IStagerSvc_errorMsg(stgSvc));
    return(-1);
  }

  for ( i=0; i<nbTapeCopies; i++ ) {
    rc = addTapeCopyToStreams(
                              svcClass,
                              tapeCopyArray[i]
                              );
    if ( rc == -1 ) {
      fprintf(stderr,"addTapeCopyToStreams(): %s\n",sstrerror(serrno));
      return(-1);
    }
    iObj = Cstager_TapeCopy_getIObject(tapeCopyArray[i]);
    rc = C_Services_fillRep(
                            dbSvc,
                            iAddr,
                            iObj,
                            OBJ_Stream,
                            0
                            );
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_fillRep(tapeCopy,OBJ_Stream): %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(dbSvc));
      return(-1);
    }
  }
  if ( tapeCopyArray != NULL ) {
    free(tapeCopyArray);
    rc = C_Services_commit(
                           dbSvc,
                           iAddr
                           );
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_commit() %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(dbSvc));
      return(-1);
    }
  }
  if ( iAddr != NULL ) C_IAddress_delete(iAddr);
  return(0);
}

void usage(char *cmd) 
{
  fprintf(stdout,"Usage: %s svcClass1 svcClass2 svcClass3 ...\n"
          "For instance:\n"
          "%s default\n",cmd,cmd);
  return;
}

int main(int argc, char *argv[]) 
{
  struct Cstager_SvcClass_t *svcClass = NULL;
  int rc, i;

  if ( argc <= 1 ) {
    usage(argv[0]);
    return(1);
  }
  
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);
  
  for ( i=1; i<argc; i++ ) {
    fprintf(stdout,"Get service class %s\n",argv[i]);
    rc = getSvcClass(argv[i],&svcClass);
    if ( rc == -1 ) {
      fprintf(stderr,"getSvcClass(): %s\n",sstrerror(serrno));
    } else {
      fprintf(stdout,"Create streams (if necessary) for service class %s\n",
              argv[i]);
      rc = streamCreator(svcClass);
      if ( rc == -1 ) {
        fprintf(stderr,"streamCreator(): %s\n",sstrerror(serrno));
      } 
    }
    rc = addMigrationCandidatesToStreams(svcClass);
    if ( rc == -1 ) {
      fprintf(stderr,"addMigrationCandidatesToStreams(): %s\n",
              sstrerror(serrno));
    }
    Cstager_SvcClass_delete(svcClass);
  }
  return(0);
}
