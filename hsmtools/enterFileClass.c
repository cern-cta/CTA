/******************************************************************************
 *                      enterFileClass.c
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
 * @(#)$RCSfile: enterFileClass.c,v $ $Revision: 1.2 $ $Release$ $Date: 2005/01/20 13:31:36 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: enterFileClass.c,v $ $Revision: 1.2 $ $Release$ $Date: 2005/01/20 13:31:36 $ Olof Barring";
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
#include <castor/BaseObject.h>
#include <castor/stager/FileClass.h>
#include <castor/stager/TapeCopy.h>
#include <castor/stager/IStagerSvc.h>
#include <castor/Services.h>
#include <castor/BaseAddress.h>
#include <castor/IAddress.h>
#include <castor/IObject.h>
#include <castor/IClient.h>
#include <castor/Constants.h>
#include <u64subr.h>
#include <Cns_api.h>

int help_flag =0;
int cns_flag = 0;

enum FileClassAttributes {
  Name = 1,
  NsHost,
  GetFromCns,
  NbCopies,
  MinFileSize,
  MaxFileSize,
};

static struct Coptions longopts[] = {
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {"name",REQUIRED_ARGUMENT,0,Name},
  {"nsHost",REQUIRED_ARGUMENT,0,NsHost},
  {"getFromCns",NO_ARGUMENT,&cns_flag,GetFromCns},
  {"nbCopies",REQUIRED_ARGUMENT,0,NbCopies},
  {"minFileSize",REQUIRED_ARGUMENT,0,MinFileSize},
  {"maxFileSize",REQUIRED_ARGUMENT,0,MaxFileSize},
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

int main(int argc, char *argv[]) 
{
  int ch, rc, nbCopiesSet = 0, minFileSizeSet = 0, maxFileSizeSet = 0;
  char *cmd, *name = NULL, *nsHost = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_IStagerSvc_t *stgSvc = NULL;
  struct C_Services_t *svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_FileClass_t *fileClass = NULL, *fileClassOld = NULL;
  struct Cns_fileclass nsFileClass;
  
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
  rc = C_Services_service(svcs,"OraStagerSvc",SVC_ORASTAGERSVC,&iSvc);
  if ( rc == -1 ) {
    fprintf(stderr,"Cannot create stager svc: %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  stgSvc = Cstager_IStagerSvc_fromIService(iSvc);
    
  Cstager_FileClass_create(&fileClass);
  while ((ch = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (ch) {
    case Name:
      name = strdup(Coptarg);
      Cstager_FileClass_setName(fileClass,Coptarg);
      break;
    case NbCopies:
      nbCopiesSet++;
      Cstager_FileClass_setNbCopies(fileClass,atoi(Coptarg));
      break;
    case NsHost:
      nsHost = strdup(Coptarg);
      break;
    case MinFileSize:
      minFileSizeSet++;
      Cstager_FileClass_setMinFileSize(fileClass,strtou64(Coptarg));
      break;
    case MaxFileSize:
      maxFileSizeSet++;
      Cstager_FileClass_setMaxFileSize(fileClass,strtou64(Coptarg));
      break;
    default:
      break;
    }
  }
  if ( help_flag != 0 || name == NULL ) {
    if ( name == NULL ) fprintf(stderr,"FileClass 'name' is required\n");
    usage(cmd);
    return(0);
  }

  if ( cns_flag != 0 ) {
    memset(&nsFileClass,'\0',sizeof(nsFileClass));
    fprintf(stdout,"Retrieve information from fileclass %s from Cns\n",
            name);
    rc = Cns_queryclass(nsHost,-1,name,&nsFileClass);
    if ( rc == -1 ) {
      fprintf(stderr,"Cns_queryclass(%s,%s): %s\n",
              (nsHost != NULL ? nsHost : "(null)"),
              name,sstrerror(serrno));
      return(1);
    }
    if ( nbCopiesSet == 0 ) {
      Cstager_FileClass_setNbCopies(fileClass,nsFileClass.nbcopies);
    }
    if ( minFileSizeSet == 0 ) {
      Cstager_FileClass_setMinFileSize(fileClass,
                                       (u_signed64)(nsFileClass.min_filesize)*1024*1024);
    }
    
    if ( maxFileSizeSet == 0 ) {
      Cstager_FileClass_setMaxFileSize(fileClass,
                                       (u_signed64)(nsFileClass.max_filesize)*1024*1024);
    }
  }

  rc = Cstager_IStagerSvc_selectFileClass(stgSvc,&fileClassOld,name);
  if ( fileClassOld != NULL ) {
    fprintf(stdout,
            "FileClass %s already exists, please use 'modifyFileClass' command\n"
            "to change any attribute of an existing FileClass\n",name);
    Cstager_FileClass_print(fileClassOld);
    return(1);
  }

  fprintf(stdout,"Adding FileClass: %s\n",name);
  Cstager_FileClass_print(fileClass);

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) return(-1);

  C_BaseAddress_setCnvSvcName(baseAddr,"OraCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_ORACNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_FileClass_getIObject(fileClass);
  rc = C_Services_createRep(svcs,iAddr,iObj,1);
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_createRep(): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  C_Services_delete(svcs);
  
  return(0);
}


