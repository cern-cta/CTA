/******************************************************************************
 *                      deleteSvcClass.c
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
 * @(#)$RCSfile $ $Revision: 1.4 $ $Release$ $Date: 2005/08/11 14:30:51 $ $Author: itglp $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: deleteSvcClass.c,v $ $Revision: 1.4 $ $Release$ $Date: 2005/08/11 14:30:51 $ Olof Barring";
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
#include <u64subr.h>
#include <Cns_api.h>

int help_flag =0;
int cns_flag = 0;

enum FileClassAttributes {
  Name = 1,
};

static struct Coptions longopts[] = {
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {"Name",REQUIRED_ARGUMENT,0,Name},
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
  int ch, rc;
  char *cmd, *name = NULL;
  struct C_BaseAddress_t *baseAddr = NULL;
  struct C_IAddress_t *iAddr;
  struct C_IObject_t *iObj = NULL;
  struct Cstager_IFSSvc_t *fsSvc = NULL;
  struct C_Services_t *svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_SvcClass_t *svcClassOld = NULL;
  
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
    default:
      break;
    }
  }
  if ( help_flag != 0 || name == NULL ) {
    if ( name == NULL ) fprintf(stderr,"SvcClass 'name' is required\n");
    usage(cmd);
    return(0);
  }

  rc = Cstager_IFSSvc_selectSvcClass(fsSvc,&svcClassOld,name);
  if ( (rc == -1) || (svcClassOld == NULL) ) {
    if ( rc == -1 ) {
      fprintf(stderr,"Cstager_IFSSvc_selectSvcClass(%s): %s, %s\n",
              name,sstrerror(serrno),
              Cstager_IFSSvc_errorMsg(fsSvc));
      return(1);
    }
    fprintf(stderr,
            "SvcClass %s does not exists\n",name);
    return(1);
  }

  fprintf(stdout,"Deleting SvcClass: %s\n",name);
  Cstager_SvcClass_print(svcClassOld);

  rc = C_BaseAddress_create(&baseAddr);
  if ( rc == -1 ) return(-1);

  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_SvcClass_getIObject(svcClassOld);
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
  
  rc = C_Services_deleteRep(svcs,iAddr,iObj,1);
  if ( rc == -1 ) {
    fprintf(stderr,"C_Services_deleteRep(): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1);
  }
  C_Services_delete(svcs);
  
  return(0);
}



