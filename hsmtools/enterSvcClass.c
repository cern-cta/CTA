/******************************************************************************
 *                      enterSvcClass.c
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
 * @(#)$RCSfile: enterSvcClass.c,v $ $Revision: 1.7 $ $Release$ $Date: 2007/05/07 06:49:17 $ $Author: waldron $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <common.h>
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
#include <castor/BaseObject.h>
#include <castor/stager/TapePool.h>
#include <castor/stager/DiskPool.h>
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
  TapePools,
  DiskPools
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
  {"TapePools",REQUIRED_ARGUMENT,0,TapePools},
  {"DiskPools",REQUIRED_ARGUMENT,0,DiskPools},
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
  char *p;
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
  char *cmd, *name = NULL;
  char *tapePoolsStr = NULL, *diskPoolsStr = NULL;
  char **tapePoolsArray = NULL, **diskPoolsArray = NULL;
  char *gcPolicy = NULL;
  int nbDiskPools = 0, nbTapePools = 0;
  int defaultReplicaNb = 1, maxReplicaNb = -1;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_IStagerSvc_t *fsSvc = NULL;
  struct C_Services_t *svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_SvcClass_t *svcClass = NULL, *svcClassOld = NULL;
  struct Cstager_TapePool_t *tapePool = NULL;
  struct Cstager_DiskPool_t *diskPool = NULL;
  const char* gcPolicyConst = NULL;
  
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
  rc = C_Services_service(svcs,"DbStagerSvc",SVC_DBSTAGERSVC,&iSvc);
  if ( rc == -1 ) {
    fprintf(stderr,"Cannot create fs svc: %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  fsSvc = Cstager_IStagerSvc_fromIService(iSvc);
    
  Cstager_SvcClass_create(&svcClass);
  while ((ch = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (ch) {
    case Name:
      name = strdup(Coptarg);
      Cstager_SvcClass_setName(svcClass,Coptarg);
      break;
    case DefaultFileSize:
      Cstager_SvcClass_setDefaultFileSize(svcClass,strtou64(Coptarg));
      break;
    case MaxReplicaNb:
      maxReplicaNb = atoi(Coptarg);
      Cstager_SvcClass_setMaxReplicaNb(svcClass,maxReplicaNb);
      break;
    case NbDrives:
      Cstager_SvcClass_setNbDrives(svcClass,atoi(Coptarg));
      break;
    case ReplicationPolicy:
      Cstager_SvcClass_setReplicationPolicy(svcClass,Coptarg);
      break;
    case GcPolicy:
      Cstager_SvcClass_setGcPolicy(svcClass,Coptarg);
      break;
    case MigratorPolicy:
      Cstager_SvcClass_setMigratorPolicy(svcClass,Coptarg);
      break;
    case RecallerPolicy:
      Cstager_SvcClass_setRecallerPolicy(svcClass,Coptarg);
      break;
    case TapePools:
      tapePoolsStr = strdup(Coptarg);
      break;
    case DiskPools:
      diskPoolsStr = strdup(Coptarg);
      break;
    default:
      break;
    }
  }
  /* Check the arguments */
  argc -= Coptind;
  argv += Coptind;
  if (argc != 1) {
    return(1);
  }
  if ( help_flag != 0 || name == NULL ) {
    if ( name == NULL ) fprintf(stderr,"SvcClass 'name' is required\n");
    usage(cmd);
    return(0);
  }

  svcClassOld = NULL;
  rc = Cstager_IStagerSvc_selectSvcClass(fsSvc,&svcClassOld,name);
  if ( (rc == 0) && (svcClassOld != NULL) ) {
    fprintf(stdout,
            "SvcClass %s already exists, please use 'modifySvcClass' command\n"
            "to change any attribute of an existing SvcClass\n",name);
    Cstager_SvcClass_print(svcClassOld);
    return(1);
  }
  if ( maxReplicaNb < 0 ) {
    Cstager_SvcClass_setMaxReplicaNb(
                                     svcClass,
                                     defaultReplicaNb
                                     );
  }
  gcPolicyConst = gcPolicy;
  Cstager_SvcClass_gcPolicy(svcClass, &gcPolicyConst);
  if ( NULL == gcPolicy ) {
    Cstager_SvcClass_setGcPolicy(
                                 svcClass,
                                 "defaultGCPolicy"
                                 );
    fprintf(stdout,"No gc policy given, using default\n");    
  }

  fprintf(stdout,"Adding SvcClass: %s\n",name);
  Cstager_SvcClass_print(svcClass);

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) return(-1);

  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
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
      rc = Cstager_IStagerSvc_selectTapePool(
                                             fsSvc,
                                             &tapePool,
                                             tapePoolsArray[i]
                                             );
      if ( rc == -1 ) {
        fprintf(stderr,"Cstager_IStagerSvc_selectTapePool(%s): %s, %s\n",
                tapePoolsArray[i],
                sstrerror(serrno),
                Cstager_IStagerSvc_errorMsg(fsSvc));
        return(1);
      }
      if ( tapePool == NULL ) {
        Cstager_TapePool_create(&tapePool);
        if ( tapePool == NULL ) {
          fprintf(stderr,"Cstager_TapePool_create(): %s\n",sstrerror(serrno));
          return(1);
        }
        Cstager_TapePool_setName(tapePool,tapePoolsArray[i]);
      }
      Cstager_TapePool_addSvcClasses(tapePool,svcClass);
      Cstager_SvcClass_addTapePools(svcClass,tapePool);
    }
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

  if ( diskPoolsStr != NULL ) {
    rc = splitItemStr(diskPoolsStr,&diskPoolsArray,&nbDiskPools);
    if ( rc == -1 ) {
      fprintf(stderr,"Error parsing disk pools string: %s\n",sstrerror(serrno));
      return(1);
    }
    for ( i=0; i<nbDiskPools; i++ ) {
      fprintf(stdout,"Add disk pool %s to SvcClass %s\n",diskPoolsArray[i],name);
      diskPool = NULL;
      rc = Cstager_IStagerSvc_selectDiskPool(
                                             fsSvc,
                                             &diskPool,
                                             diskPoolsArray[i]
                                             );
      if ( rc == -1 ) {
        fprintf(stderr,"Cstager_IStagerSvc_selectDiskPool(%s): %s, %s\n",
                diskPoolsArray[i],
                sstrerror(serrno),
                Cstager_IStagerSvc_errorMsg(fsSvc));
        return(1);
      }
      if ( diskPool == NULL ) {
        Cstager_DiskPool_create(&diskPool);
        if ( diskPool == NULL ) {
          fprintf(stderr,"Cstager_DiskPool_create(): %s\n",sstrerror(serrno));
          return(1);
        }
        Cstager_DiskPool_setName(diskPool,diskPoolsArray[i]);
      }
      Cstager_DiskPool_addSvcClasses(diskPool,svcClass);
      Cstager_SvcClass_addDiskPools(svcClass,diskPool);
    }  
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
    fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_DiskPool): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  
  return(0);
}

