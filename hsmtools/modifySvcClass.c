/******************************************************************************
 *                      modifySvcClass.c
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
 * @(#)$RCSfile: modifySvcClass.c,v $ $Revision: 1.1 $ $Release$ $Date: 2005/03/07 14:44:11 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: modifySvcClass.c,v $ $Revision: 1.1 $ $Release$ $Date: 2005/03/07 14:44:11 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <serrno.h>
#include <Cgetopt.h>
#include <castor/stager/SvcClass.h>
#include <castor/stager/FileClass.h>
#include <castor/stager/TapeCopy.h>
#include <castor/stager/IStagerSvc.h>
#include <castor/Services.h>
#include <castor/BaseAddress.h>
#include <castor/IAddress.h>
#include <castor/IObject.h>
#include <castor/IClient.h>
#include <castor/Constants.h>

int help_flag =0;
static char *itemDelimiter = ":";

enum SvcClassAttributes {
  Name = 1,
  NbDrives,
  DefaultFileSize,
  MaxReplicaNb,
  ReplicationPolicy,
  GcPolicy,
  MigratorPolicy,
  RecallerPolicy,
  AddTapePools,
  AddDiskPools,
  RemoveTapePools,
  RemoveDiskPools
} svcClassAttributes;

static struct Coptions longopts[] = {
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {"Name",REQUIRED_ARGUMENT,0,Name},
  {"DefaultFileSize",REQUIRED_ARGUMENT,0,DefaultFileSize},
  {"MaxReplicaNb",REQUIRED_ARGUMENT,0,MaxReplicaNb},
  {"NbDrives",REQUIRED_ARGUMENT,0,NbDrives},
  {"ReplicationPolicy",REQUIRED_ARGUMENT,0,ReplicationPolicy},
  {"GcPolicy",REQUIRED_ARGUMENT,0,GcPolicy},
  {"MigratorPolicy",REQUIRED_ARGUMENT,0,MigratorPolicy},
  {"RecallerPolicy",REQUIRED_ARGUMENT,0,RecallerPolicy},
  {"AddTapePools",REQUIRED_ARGUMENT,0,AddTapePools},
  {"AddDiskPools",REQUIRED_ARGUMENT,0,AddDiskPools},
  {"RemoveTapePools",REQUIRED_ARGUMENT,0,RemoveTapePools},
  {"RemoveDiskPools",REQUIRED_ARGUMENT,0,RemoveDiskPools},
  {NULL, 0, NULL, 0}
};

void usage(
           cmd
           )
     char *cmd;
{
  int i;
  fprintf(stdout,"Usage: %s \n",cmd);
  for (i=0; longopts[i].name != NULL; i++) {
    fprintf(stdout,"\t--%s %s\n",longopts[i].name,
            (longopts[i].has_arg == NO_ARGUMENT ? "" : longopts[i].name));
  }
  return;
}

int countItems(
               itemStr
               )
     char *itemStr;
{
  char *p, *q;
  int nbItems = 0;
  if ( itemStr == NULL ) return(0);
  
  p = itemStr;
  while ( p != NULL ) {
    nbItems++;
    p = strstr(p+1,itemDelimiter);
  }
  return(nbItems);
}

int splitItemStr(
                 itemStr,
                 itemArray,
                 nbItems
                 )
     char *itemStr;
     char ***itemArray;
     int *nbItems;
{
  char *p, *tmpStr;
  int tmpNbItems, i;

  if ( itemStr == NULL || itemArray == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  tmpNbItems = countItems(itemStr);
  if ( tmpNbItems == 0 ) {
    if ( nbItems != NULL ) *nbItems = tmpNbItems;
    *itemArray = NULL;
    return(0);
  }
  tmpStr = strdup(itemStr);
  if ( itemArray == NULL ) {
    serrno = errno;
    return(-1);
  }
  *itemArray = (char **)calloc(tmpNbItems,sizeof(char *));
  if ( itemArray == NULL ) {
    serrno = errno;
    return(-1);
  }
  *nbItems = tmpNbItems;
  p = strtok(tmpStr,itemDelimiter);
  for (i=0; i<tmpNbItems; i++) {
    if ( p == NULL ) break;
    (*itemArray)[i] = strdup(p);
    p = strtok(NULL,itemDelimiter);
  }
  free(tmpStr);
  return(0);
}

int main(int argc, char *argv[]) 
{
  int ch, rc, i;
  char *cmd, buf[32], *name = NULL;
  char *addTapePoolsStr = NULL, *addDiskPoolsStr = NULL;
  char **addTapePoolsArray = NULL, **addDiskPoolsArray = NULL;
  char *removeTapePoolsStr = NULL, *removeDiskPoolsStr = NULL;
  char **removeTapePoolsArray = NULL, **removeDiskPoolsArray = NULL;
  int nbDiskPools = 0, nbTapePools = 0;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  struct C_Services_t *svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_SvcClass_t *svcClass = NULL, *svcClassOld = NULL;
  struct Cstager_TapePool_t *tapePool = NULL;
  struct Cstager_DiskPool_t *diskPool = NULL;
  u_signed64 u64, defaultFileSize = 0;
  int maxReplicaNb = -1, nbDrives = -1;
  char *replicationPolicy = NULL, *migratorPolicy = NULL, *gcPolicy = NULL;
  char *recallerPolicy = NULL;
  
  Coptind = 1;
  Copterr = 1;
  cmd = argv[0];

  rc = C_Services_create(&svcs);
  if ( rc == -1 ) {
    fprintf(stderr,"Cannot create catalogue DB services: %s\n",
            sstrerror(serrno));
    return(1);
  }
  rc = C_Services_service(svcs,"OraStagerSvc",SVC_ORASTAGERSVC,&iSvc);
  if ( rc == -1 ) {
    fprintf(stderr,"Cannot create stager svc: %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  stgSvc = Cstager_IStagerSvc_fromIService(iSvc);
    
  Cstager_SvcClass_create(&svcClass);
  while ((ch = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (ch) {
    case Name:
      name = strdup(Coptarg);
      Cstager_SvcClass_setName(svcClass,Coptarg);
      break;
    case DefaultFileSize:
      defaultFileSize = strtou64(Coptarg);
      break;
    case MaxReplicaNb:
      maxReplicaNb = atoi(Coptarg);
      break;
    case NbDrives:
      nbDrives = atoi(Coptarg);
      break;
    case ReplicationPolicy:
      replicationPolicy = strdup(Coptarg);
      break;
    case GcPolicy:
      gcPolicy = strdup(Coptarg);
      break;
    case MigratorPolicy:
      migratorPolicy = strdup(Coptarg);
      break;
    case RecallerPolicy:
      recallerPolicy = strdup(Coptarg);
      break;
    case AddTapePools:
      addTapePoolsStr = strdup(Coptarg);
      break;
    case AddDiskPools:
      addDiskPoolsStr = strdup(Coptarg);
      break;
    case RemoveTapePools:
      removeTapePoolsStr = strdup(Coptarg);
      break;
    case RemoveDiskPools:
      removeDiskPoolsStr = strdup(Coptarg);
      break;
    default:
      break;
    }
  }
  if ( help_flag != 0 || name == NULL ) {
    if ( name == NULL ) fprintf(stderr,"SvcClass 'name' is required\n");
    usage(cmd);
    return(0);
  }

  rc = Cstager_IStagerSvc_selectSvcClass(stgSvc,&svcClassOld,name);
  if ( (rc == -1) || (svcClassOld == NULL) ) {
    fprintf(stdout,
            "SvcClass %s does exists\n",name);
    return(1);
  }

  Cstager_SvcClass_print(svcClass);

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) return(-1);

  C_BaseAddress_setCnvSvcName(baseAddr,"OraCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_ORACNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_SvcClass_getIObject(svcClass);
  rc = C_Services_createRep(svcs,iAddr,iObj,1);
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_createRep(): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  if ( tapePoolsStr != NULL ) {
    rc = splitItemStr(tapePoolsStr,&tapePoolsArray,&nbTapePools);
    if ( rc == -1 ) {
      fprintf(stderr,"Error parsing tape pools string: %s\n",sstrerror(serrno));
      return(1);
    }
    for ( i=0; i<nbTapePools; i++ ) {
      fprintf(stdout,"Add tape pool %s to SvcClass %s\n",tapePoolsArray[i],name);
      tapePool = NULL;
      Cstager_TapePool_create(&tapePool);
      if ( tapePool == NULL ) {
        fprintf(stderr,"Cstager_TapePool_create(): %s\n",sstrerror(serrno));
        return(1);
      }
      Cstager_TapePool_setName(tapePool,tapePoolsArray[i]);
      Cstager_TapePool_addSvcClasses(tapePool,svcClass);
      Cstager_SvcClass_addTapePools(svcClass,tapePool);
    }
    rc = C_Services_fillRep(
                            svcs,
                            iAddr,
                            iObj,
                            OBJ_TapePool,
                            1
                            );
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_TapePool): %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(svcs));
      return(1);
    }
  }

  if ( diskPoolsStr != NULL ) {
    rc = splitItemStr(diskPoolsStr,&diskPoolsArray,&nbDiskPools);
    if ( rc == -1 ) {
      fprintf(stderr,"Error parsing disk pools string: %s\n",sstrerror(serrno));
      return(1);
    }
    for ( i=0; i<nbDiskPools; i++ ) {
      fprintf(stdout,"Add disk pool %s to SvcClass %s\n",diskPoolsArray[i],name);
      diskPool = NULL;
      Cstager_DiskPool_create(&diskPool);
      if ( diskPool == NULL ) {
        fprintf(stderr,"Cstager_DiskPool_create(): %s\n",sstrerror(serrno));
        return(1);
      }
      Cstager_DiskPool_setName(diskPool,diskPoolsArray[i]);
      Cstager_DiskPool_addSvcClasses(diskPool,svcClass);
      Cstager_SvcClass_addDiskPools(svcClass,diskPool);
    }
       rc = C_Services_fillRep(
                            svcs,
                            iAddr,
                            iObj,
                            OBJ_DiskPool,
                            1
                            );
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_DiskPool): %s, %s\n",
              sstrerror(serrno),
              C_Services_errorMsg(svcs));
      return(1);
    }
  }
  
  return(0);
}

