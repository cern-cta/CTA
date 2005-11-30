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
 * @(#)$RCSfile: modifySvcClass.c,v $ $Revision: 1.7 $ $Release$ $Date: 2005/11/30 17:28:19 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: modifySvcClass.c,v $ $Revision: 1.7 $ $Release$ $Date: 2005/11/30 17:28:19 $ Olof Barring";
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
#include <castor/stager/IFSSvc.h>
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
int findTapePool(
                 tapePoolArray,
                 nbTapePools,
                 foundPool,
                 name
                 )
  struct Cstager_TapePool_t **tapePoolArray, **foundPool;
  int nbTapePools;
  char *name;
{
  char *tmpName;
  int i;

  if ( (tapePoolArray == NULL) || (name == NULL) ) return(0);
  for ( i=0; i<nbTapePools; i++ ) {
    tmpName = NULL;
    Cstager_TapePool_name(tapePoolArray[i],(CONST char **)&tmpName);
    if ( strcmp(tmpName,name) == 0 ) {
      if ( foundPool != NULL ) *foundPool = tapePoolArray[i];
      return(1);
    }
  }
  return(0);
}

int findDiskPool(
                 diskPoolArray,
                 nbDiskPools,
                 foundPool,
                 name
                 )
  struct Cstager_DiskPool_t **diskPoolArray, **foundPool;
  int nbDiskPools;
  char *name;
{
  char *tmpName;
  int i;

  if ( (diskPoolArray == NULL) || (name == NULL) ) return(0);
  for ( i=0; i<nbDiskPools; i++ ) {
    tmpName = NULL;
    Cstager_DiskPool_name(diskPoolArray[i],(CONST char **)&tmpName);
    if ( strcmp(tmpName,name) == 0 ) {
      if ( foundPool != NULL ) *foundPool = diskPoolArray[i];
      return(1);
    }
  }
  return(0);
}

int addTapePools(
                 fsSvc,
                 svcClass,
                 tapePoolsArray,
                 nbTapePools,
                 addTapePoolsArray,
                 nbAddTapePools
                 )
  struct Cstager_IFSSvc_t *fsSvc;
  struct Cstager_SvcClass_t *svcClass;
  char **tapePoolsArray, **addTapePoolsArray;
  int nbTapePools, nbAddTapePools;
{
  struct Cstager_TapePool_t *tapePool;
  int i, rc;

  if ( addTapePoolsArray == NULL ) {
    fprintf(stderr,"addTapePools(): empty tape pool array\n");
    return(-1);
  }
  
  for ( i=0; i<nbAddTapePools; i++ ) {
    fprintf(stdout,"Add tape pool %s\n",addTapePoolsArray[i]);
    tapePool = NULL;
    if ( findTapePool(tapePoolsArray,nbTapePools,&tapePool,addTapePoolsArray[i]) == 0 ) {
      rc = Cstager_IFSSvc_selectTapePool(
                                             fsSvc,
                                             &tapePool,
                                             addTapePoolsArray[i]
                                             );
      if ( rc == -1 ) {
        fprintf(stderr,"Cstager_IFSSvc_selectTapePool(%s): %s, %s\n",
          addTapePoolsArray[i],
          sstrerror(serrno),
          Cstager_IFSSvc_errorMsg(fsSvc));
        return(1);
      }
      if ( tapePool == NULL ) {
        Cstager_TapePool_create(&tapePool);
        if ( tapePool == NULL ) {
          fprintf(stderr,"Cstager_TapePool_create(): %s\n",sstrerror(serrno));
          return(1);
        }
        Cstager_TapePool_setName(tapePool,addTapePoolsArray[i]);
      }
      Cstager_TapePool_addSvcClasses(tapePool,svcClass);
      Cstager_SvcClass_addTapePools(svcClass,tapePool);
    } else {
      fprintf(stdout,"Add tape pool ignored: %s already linked\n",addTapePoolsArray[i]);
    }
  }
  return(0);
}

int removeTapePools(
                    svcClass,
                    tapePoolsArray,
                    nbTapePools,
                    removeTapePoolsArray,
                    nbRemoveTapePools
                    )
  struct Cstager_SvcClass_t *svcClass;
  char **tapePoolsArray, **removeTapePoolsArray;
  int nbTapePools, nbRemoveTapePools;
{
  struct Cstager_TapePool_t *tapePool;
  int i, rc;

  if ( removeTapePoolsArray == NULL ) {
    fprintf(stderr,"removeTapePools(): empty tape pool array\n");
    return(-1);
  }

  for ( i=0; i<nbRemoveTapePools; i++ ) {
    fprintf(stdout,"Remove tape pool %s\n",removeTapePoolsArray[i]);
    tapePool = NULL;
    if ( findTapePool(tapePoolsArray,nbTapePools,&tapePool,removeTapePoolsArray[i]) == 1 ) {
      Cstager_TapePool_removeSvcClasses(tapePool,svcClass);
      Cstager_SvcClass_removeTapePools(svcClass,tapePool);
    } else {
      fprintf(stdout,"Remove tape pool ignored: %s not found\n",removeTapePoolsArray[i]);
    }
  }
  return(0);
}

int addDiskPools(
                 fsSvc,
                 svcClass,
                 diskPoolsArray,
                 nbDiskPools,
                 addDiskPoolsArray,
                 nbAddDiskPools
                 )
  struct Cstager_IFSSvc_t *fsSvc;
  struct Cstager_SvcClass_t *svcClass;
  char **diskPoolsArray, **addDiskPoolsArray;
  int nbDiskPools, nbAddDiskPools;
{
  struct Cstager_DiskPool_t *diskPool;
  int i, rc;

  if ( addDiskPoolsArray == NULL ) {
    fprintf(stderr,"addDiskePools(): empty disk pool array\n");
    return(-1);
  }
  for ( i=0; i<nbAddDiskPools; i++ ) {
    fprintf(stdout,"Add disk pool %s\n",addDiskPoolsArray[i]);
    diskPool = NULL;
    if ( findDiskPool(diskPoolsArray,nbDiskPools,&diskPool,addDiskPoolsArray[i]) == 0 ) {
      rc = Cstager_IFSSvc_selectDiskPool(
                                             fsSvc,
                                             &diskPool,
                                             addDiskPoolsArray[i]
                                             );
      if ( rc == -1 ) {
        fprintf(stderr,"Cstager_IFSSvc_selectDiskPool(%s): %s, %s\n",
          addDiskPoolsArray[i],
          sstrerror(serrno),
          Cstager_IFSSvc_errorMsg(fsSvc));
        return(1);
      }
      if ( diskPool == NULL ) {
        Cstager_DiskPool_create(&diskPool);
        if ( diskPool == NULL ) {
          fprintf(stderr,"Cstager_DiskPool_create(): %s\n",sstrerror(serrno));
          return(1);
        }
        Cstager_DiskPool_setName(diskPool,addDiskPoolsArray[i]);
      }
      Cstager_DiskPool_addSvcClasses(diskPool,svcClass);
      Cstager_SvcClass_addDiskPools(svcClass,diskPool);
    } else {
      fprintf(stdout,"Add disk pool ignored: %s already linked\n",addDiskPoolsArray[i]);
    }
  }
  return(0);
}

int removeDiskPools(
                    svcClass,
                    diskPoolsArray,
                    nbDiskPools,
                    removeDiskPoolsArray,
                    nbRemoveDiskPools
                    )
  struct Cstager_SvcClass_t *svcClass;
  char **diskPoolsArray, **removeDiskPoolsArray;
  int nbDiskPools, nbRemoveDiskPools;
{
  struct Cstager_DiskPool_t *diskPool;
  int i, rc;

  if ( removeDiskPoolsArray == NULL ) {
    fprintf(stderr,"removeDiskPools(): empty disk pool array\n");
    return(-1);
  }

  for ( i=0; i<nbRemoveDiskPools; i++ ) {
    fprintf(stdout,"Remove disk pool %s\n",removeDiskPoolsArray[i]);
    diskPool = NULL;
    if ( findDiskPool(diskPoolsArray,nbDiskPools,&diskPool,removeDiskPoolsArray[i]) == 1 ) {
      Cstager_DiskPool_removeSvcClasses(diskPool,svcClass);
      Cstager_SvcClass_removeDiskPools(svcClass,diskPool);
    } else {
      fprintf(stdout,"Remove disk pool ignored: %s not found\n",removeDiskPoolsArray[i]);
    }
  }
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
  int nbAddTapePools = 0, nbRemoveTapePools = 0, nbAddDiskPools = 0, nbRemoveDiskPools = 0;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_IFSSvc_t *fsSvc = NULL;
  struct C_Services_t *svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_SvcClass_t *svcClass = NULL;
  struct Cstager_TapePool_t *tapePool = NULL, **tapePoolsArray = NULL;
  struct Cstager_DiskPool_t *diskPool = NULL, **diskPoolsArray = NULL;
  u_signed64 u64, defaultFileSize = 0;
  int maxReplicaNb = -1, nbDrives = -1;
  char *replicationPolicy = NULL, *migratorPolicy = NULL, *gcPolicy = NULL;
  char *recallerPolicy = NULL;
  
  Coptind = 1;
  Copterr = 1;
  cmd = argv[0];

  /* Initializing the C++ log */
  /* Necessary at start of program and after any fork */
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);

  rc = C_Services_create(&svcs);
  if ( rc == -1 ) {
    fprintf(stderr,"Cannot create catalogue DB services: %s\n",
            sstrerror(serrno));
    return(1);
  }
  rc = C_Services_service(svcs,"DbFSSvc",SVC_DBFSSVC,&iSvc);
  if ( rc == -1 ) {
    fprintf(stderr,"Cannot create fs svc: %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  fsSvc = Cstager_IFSSvc_fromIService(iSvc);
    
  while ((ch = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (ch) {
    case Name:
      name = strdup(Coptarg);
      break;
    case DefaultFileSize:
      defaultFileSize = strutou64(Coptarg);
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

  rc = Cstager_IFSSvc_selectSvcClass(fsSvc,&svcClass,name);
  if ( (rc == -1) || (svcClass == NULL) ) {
    fprintf(stdout,
            "SvcClass %s does not exists, %s, %s\n",name,
            sstrerror(serrno),
            Cstager_IFSSvc_errorMsg(fsSvc));
    return(1);
  }
  if ( nbDrives >= 0 ) Cstager_SvcClass_setNbDrives(svcClass,nbDrives);
  if ( maxReplicaNb >= 0 ) Cstager_SvcClass_setMaxReplicaNb(svcClass,maxReplicaNb);
  if ( defaultFileSize > 0 ) Cstager_SvcClass_setDefaultFileSize(svcClass,defaultFileSize);
  if ( replicationPolicy != NULL ) {
    Cstager_SvcClass_setReplicationPolicy(svcClass,replicationPolicy);
  }
  if ( gcPolicy != NULL ) {
    Cstager_SvcClass_setGcPolicy(svcClass,gcPolicy);
  }
  if ( migratorPolicy != NULL ) {
    Cstager_SvcClass_setMigratorPolicy(svcClass,migratorPolicy);
  }
  if ( recallerPolicy != NULL ) {
    Cstager_SvcClass_setRecallerPolicy(svcClass,recallerPolicy);
  }

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) return(-1);

  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_SvcClass_getIObject(svcClass);

  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_SvcClass_getIObject(svcClass);
  rc = C_Services_updateRep(
                            svcs,
                            iAddr,
                            iObj,
                            0
                            );
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_updateRep(svcClass): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  

  rc = C_Services_fillObj(
                          svcs,
                          iAddr,
                          iObj,
                          OBJ_TapePool
                          );
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_fillObj(svcClass,OBJ_TapePool): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  rc = C_Services_fillObj(
                          svcs,
                          iAddr,
                          iObj,
                          OBJ_DiskPool
                          );
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_fillObj(svcClass,OBJ_DiskPool): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  Cstager_SvcClass_tapePools(svcClass,&tapePoolsArray,&nbTapePools);
  Cstager_SvcClass_diskPools(svcClass,&diskPoolsArray,&nbDiskPools);

  if ( addTapePoolsStr != NULL ) {
    rc = splitItemStr(addTapePoolsStr,&addTapePoolsArray,&nbAddTapePools);
    if ( rc == -1 ) {
      fprintf(stderr,"Error parsing tape pools string: %s\n",sstrerror(serrno));
      return(1);
    }
    rc = addTapePools(
                      fsSvc,
                      svcClass,
                      tapePoolsArray,
                      nbTapePools,
                      addTapePoolsArray,
                      nbAddTapePools
                      );
    if ( rc == -1 ) {
      fprintf(stderr,"addTapePools(): failed\n");
      return(1);
    }
  }
  if ( removeTapePoolsStr != NULL ) {
    rc = splitItemStr(removeTapePoolsStr,&removeTapePoolsArray,&nbRemoveTapePools);
    if ( rc == -1 ) {
      fprintf(stderr,"Error parsing tape pools string: %s\n",sstrerror(serrno));
      return(1);
    }
    rc = removeTapePools(
                         svcClass,
                         tapePoolsArray,
                         nbTapePools,
                         removeTapePoolsArray,
                         nbRemoveTapePools
                         );
    if ( rc == -1 ) {
      fprintf(stderr,"removeTapePools(): failed\n");
      return(1);
    }
  }

  if ( (addTapePoolsStr != NULL) || (removeTapePoolsStr != NULL) ) {
    rc = C_Services_fillRep(
                            svcs,
                            iAddr,
                            iObj,
                            OBJ_TapePool,
                            0
                            );
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_TapePool): %s, %s\n",
        sstrerror(serrno),
        C_Services_errorMsg(svcs));
      return(1);
    }
  }

  if ( addDiskPoolsStr != NULL ) {
    rc = splitItemStr(addDiskPoolsStr,&addDiskPoolsArray,&nbAddDiskPools);
    if ( rc == -1 ) {
      fprintf(stderr,"Error parsing disk pools string: %s\n",sstrerror(serrno));
      return(1);
    }
    rc = addDiskPools(
                      fsSvc,
                      svcClass,
                      diskPoolsArray,
                      nbDiskPools,
                      addDiskPoolsArray,
                      nbAddDiskPools
                      );
    if ( rc == -1 ) {
      fprintf(stderr,"addDiskPools(): failed\n");
      return(1);
    }
  }
  if ( removeDiskPoolsStr != NULL ) {
    rc = splitItemStr(removeDiskPoolsStr,&removeDiskPoolsArray,&nbRemoveDiskPools);
    if ( rc == -1 ) {
      fprintf(stderr,"Error parsing disk pools string: %s\n",sstrerror(serrno));
      return(1);
    }
    rc = removeDiskPools(
                         svcClass,
                         diskPoolsArray,
                         nbDiskPools,
                         removeDiskPoolsArray,
                         nbRemoveDiskPools
                         );
    if ( rc == -1 ) {
      fprintf(stderr,"removeDiskPools(): failed\n");
      return(1);
    }
  }

  if ( (addDiskPoolsStr != NULL) || (removeDiskPoolsStr != NULL ) ) {
    rc = C_Services_fillRep(
                            svcs,
                            iAddr,
                            iObj,
                            OBJ_DiskPool,
                            0
                            );
    if ( rc == -1 ) {
      fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_DiskPool): %s, %s\n",
        sstrerror(serrno),
        C_Services_errorMsg(svcs));
      return(1);
    }
  }

  rc = C_Services_commit(svcs,iAddr);
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_commit(): %s, %s\n",
      sstrerror(serrno),
      C_Services_errorMsg(svcs));
    return(1);
  }
  
  return(0);
}

